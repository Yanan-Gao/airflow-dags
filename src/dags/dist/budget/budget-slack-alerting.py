import io
from datetime import datetime, timedelta
import logging
from typing import List, Dict
import boto3

from airflow.operators.python import PythonOperator

from dags.datperf.datasets import campaignthrottlemetric_dataset, adgroupthrottlemetric_dataset
from dags.dist.budget.alerting.budget_delivery_alerts import BUDGET_DELIVERY_ALERTS
from dags.dist.budget.alerting.budget_virtual_campaign_alerts import BUDGET_VIRTUAL_CAMPAIGN_ALERTS
from dags.dist.budget.alerting.budget_r_value_control_alerts import BUDGET_R_VALUE_CONTROL_ALERTS
from dags.dist.budget.alerting.budget_cpm_proximity_alerts import CPM_PROXIMITY_ALERTS
from dags.dist.budget.alerting.budget_slack_alert_processor import SlackAlertProcessor, \
    BaseAlert
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.el_dorado.v2.base import TtdDag
from ttd.ec2.emr_instance_types.general_purpose.m7a import M7a
from ttd.ec2.emr_instance_types.memory_optimized.r7a import R7a
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import dist
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

logger = logging.getLogger(__name__)


class BudgetAlertingDag:

    def __init__(self, *alerts_lists: List[BaseAlert]):
        self.env = TtdEnvFactory.get_from_system()
        self.version = '1'
        self.bucket = 'thetradedesk-mlplatform-us-east-1'
        self.dag_name = "budget-slack-alerting"

        self.dag = self._create_dag()
        self.cluster = self._create_cluster()
        self.final_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=self.dag.airflow_dag))
        self.alerts: Dict[str, BaseAlert] = {}

        for alerts in alerts_lists:
            for alert in alerts:
                if alert.name in self.alerts:
                    raise ValueError(f'Alert with the name "{alert.name}" already exists.')
                self.alerts[alert.name] = alert

        self.run_date = '{{ ds_nodash }}'

        self.s3_prefix = self.get_s3_prefix(self.run_date)

    def get_s3_prefix(self, run_date: str) -> str:
        return f"model_monitor/mission_control/env={self.env.dataset_write_env}/metric=slack-alerting/v={self.version}/frequency=daily/date={run_date}"

    def _create_dag(self) -> TtdDag:
        budget_slack_alerting_dag = TtdDag(
            dag_id=self.dag_name,
            start_date=datetime(2025, 1, 8),
            schedule_interval='0 11 * * *',
            dag_tsg='https://thetradedesk.atlassian.net/l/cp/aK79cXPo',
            retries=3,
            max_active_runs=10,
            retry_delay=timedelta(minutes=5),
            slack_channel="#taskforce-budget-metrics-alarms",
            slack_alert_only_for_prod=True,
            tags=["DIST"],
        )
        return budget_slack_alerting_dag

    def _create_cluster(self) -> EmrClusterTask:
        master_fleet = EmrFleetInstanceTypes(
            instance_types=[M7a.m7a_2xlarge().with_fleet_weighted_capacity(1)],
            on_demand_weighted_capacity=1,
        )

        core_fleet = EmrFleetInstanceTypes(
            instance_types=[R7a.r7a_4xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
        )

        return EmrClusterTask(
            name=self.dag_name,
            master_fleet_instance_type_configs=master_fleet,
            cluster_tags={"Team": dist.jira_team},
            core_fleet_instance_type_configs=core_fleet,
            emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5_2,
        )

    def get_spark_options(self) -> List[tuple[str, str]]:
        return [
            ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
            ("conf", "spark.kryoserializer.buffer.max=1g"),
            ("conf", "spark.sql.adaptive.enabled=true"),
            ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
            (
                "conf",
                "spark.jars=/usr/share/aws/delta/lib/delta-spark.jar,/usr/share/aws/delta/lib/delta-storage.jar,/usr/share/aws/delta/lib/delta-storage-s3-dynamodb.jar"
            ),
            ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
            ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
            ("conf", "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"),
            ("conf", "spark.databricks.delta.autoCompact.enabled=true"),
            ("conf", "spark.databricks.delta.optimizeWrite.enabled=true"),
        ]

    def process_alert_results(self, alert_name: str, sql_rendered: str, **context):

        try:
            import pandas as pd

            if alert_name not in self.alerts:
                raise ValueError(f"Budget alert not found: {alert_name}")

            alert = self.alerts[alert_name]
            run_date = context['data_interval_start']
            prefix = self.get_s3_prefix(run_date.strftime("%Y%m%d"))
            s3_prefix = alert.get_output_prefix(prefix)
            logger.info(f"Processing budget alert: {alert_name} for {run_date} at {s3_prefix}")

            # Initialize S3 client
            s3_client = boto3.client('s3')

            # List all matching Parquet files
            response = s3_client.list_objects_v2(Bucket=self.bucket, Prefix=s3_prefix)

            # Read and concatenate all Parquet files
            dfs = []
            for obj in response.get('Contents', []):
                if obj['Key'].endswith('.parquet'):
                    logger.debug(f"Reading Parquet file: {obj['Key']}")

                    # Get the Parquet file from S3
                    response = s3_client.get_object(Bucket=self.bucket, Key=obj['Key'])

                    # Read Parquet file into pandas DataFrame
                    parquet_buffer = io.BytesIO(response['Body'].read())
                    df = pd.read_parquet(parquet_buffer)
                    dfs.append(df)

            if not dfs:
                logger.warning(f"No budget data found for: {alert_name}")
                return

            # Combine all DataFrames
            df = pd.concat(dfs, ignore_index=True)

            if df.empty:
                logger.warning(f"No budget data for: {alert_name}")
                return

            # Get EMR task duration from XCom
            dag_run = context['dag_run']

            processor = SlackAlertProcessor(s3_bucket=self.bucket, s3_base_path=prefix, run_date=run_date)
            processor.process_alert_output(df=df, alert=alert, sql_rendered=sql_rendered, run_date=run_date)

        except Exception as e:
            logger.error(f"Budget alert processing failed: {alert_name}", extra={"alert": alert_name, "error": str(e)}, exc_info=True)
            raise


# Create the DAG instance
budget_alerting = BudgetAlertingDag(
    BUDGET_DELIVERY_ALERTS, BUDGET_VIRTUAL_CAMPAIGN_ALERTS, BUDGET_R_VALUE_CONTROL_ALERTS, CPM_PROXIMITY_ALERTS
)
dag = budget_alerting.dag.airflow_dag

data_sources = {
    "throttle_metric_campaign_daily":
    'parquet.`s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/env=prod/aggregate-pacing-statistics/v=2/metric=throttle_metric_campaign_parquet/date={{ ds_nodash }}/*.parquet`',
    "throttle_metric_adgroup_daily":
    'parquet.`s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/env=prod/aggregate-pacing-statistics/v=2/metric=throttle_metric_parquet/date={{ ds_nodash }}/*.parquet`',
    "virtual_campaign_calculation_results":
    'delta.`s3://ttd-budget-calculation-lake/env=prod/CalculationResultTables/budgetvirtualcampaigncalculationresult/`',
    "virtual_ad_group_calculation_results":
    'delta.`s3://ttd-budget-calculation-lake/env=prod/CalculationResultTables/budgetvirtualadgroupcalculationresult/`',
    "campaign_calculation_results":
    'delta.`s3://ttd-budget-calculation-lake/env=prod/CalculationResultTables/budgetcampaigncalculationresult/`',
    "ad_group_calculation_results":
    'delta.`s3://ttd-budget-calculation-lake/env=prod/CalculationResultTables/budgetadgroupcalculationresult/`',
    "throttle_metric_virtual_campaign_daily":
    'delta.`s3://ttd-budget-calculation-lake/env=prod/virtual-aggregate-pacing-statistics/v=2/metric=throttle_metric_campaign_delta/`',
    "throttle_metric_virtual_adroup_hardmin_daily":
    'delta.`s3://ttd-budget-calculation-lake/env=prod/virtual-aggregate-pacing-statistics/v=2/metric=hard_min_throttle_delta/`',
    "cpm_metrics":
    'delta.`s3://ttd-budget-calculation-lake/env=prod/budget-cpm-metrics/v=1/metric=cpm_metrics_delta/`',
    "budgetsplitexperimentinfo":
    'delta.`s3://ttd-budget-calculation-lake/env=prod/experiments/budgetsplitexperimentinfo/`',
    "volume_control_calculation_results":
    'delta.`s3://ttd-budget-calculation-lake/env=prod/CalculationResultTables/budgetvolumecontrolcalculationresult/`',
    "r_value_calculation_results":
    'delta.`s3://ttd-budget-calculation-lake/env=prod/CalculationResultTables/budgetrvaluecalculationresult/`',
    "ad_group_cpm_proximity":
    'delta.`s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/env=prod/aggregate-pacing-statistics/v=2/ad_group_cpm_proximity_metrics_delta/`',
}
data_sources_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='alert_data_available',
        datasets=[
            campaignthrottlemetric_dataset,
            adgroupthrottlemetric_dataset,
        ],
        ds_date="{{data_interval_start.to_datetime_string() }}",
        poke_interval=60 * 10,
        # wait up to 6 hours
        timeout=60 * 60 * 6
    )
)
# Create all EMR tasks and add them to the cluster
spark_options = budget_alerting.get_spark_options()

template_args = data_sources | {'run_time': '{{ ds }}', 'run_date': '{{ ds }}', 'data_interval_start': '{{ data_interval_start }}'}

# Create all tasks and set dependencies at DAG definition time
alert_tasks = []

# First create all EMR tasks
for alert in budget_alerting.alerts.values():
    (emr_task, sql) = alert.create_emr_task(
        s3_bucket=budget_alerting.bucket,
        s3_base_path=budget_alerting.s3_prefix,
        queries_template_arg=template_args,
        spark_options_list=spark_options
    )

    # Create corresponding alert processing task
    alert_task = OpTask(
        op=PythonOperator(
            task_id=f'process_alert_{alert.name}',
            python_callable=budget_alerting.process_alert_results,
            op_kwargs={
                'alert_name': alert.name,
                'sql_rendered': sql
            },
            provide_context=True,
            retries=1,
            dag=dag
        )
    )
    budget_alerting.cluster.add_parallel_body_task(emr_task)
    emr_task >> alert_task

    alert_tasks.append(alert_task)

# Set DAG to cluster dependency
budget_alerting.dag >> data_sources_sensor >> budget_alerting.cluster

# Set individual dependencies for each alert task
for alert_task in alert_tasks:
    alert_task >> budget_alerting.final_status_step
