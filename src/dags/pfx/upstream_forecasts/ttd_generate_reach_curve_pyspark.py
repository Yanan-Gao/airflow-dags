import logging
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Optional

from airflow.models import DagRun, BaseOperator
from airflow.models.dag import ScheduleInterval, DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dags.pfx.constants import PFX_PYSPARK_JOBS_PATH, PFX_PYSPARK_JOBS_WHL_NAME
from dags.pfx.upstream_forecasts.ctv_cpm_adjustments import xcom_template_to_get_value
from dags.pfx.upstream_forecasts.ttd_reach_curve_validation_og import validation_dag_id as og_validation_dag_id
from dags.pfx.upstream_forecasts.ttd_reach_curve_validation_v2 import validation_dag_id as v2_validation_dag_id
from dags.pfx.upstream_forecasts.ttd_reach_curve_validation_v3 import validation_dag_id as v3_validation_dag_id
from dags.pfx.upstream_forecasts.upstream_utils import get_most_recent_successful_dag_run
from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_pyspark import S3PysparkEmrTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import PFX
from ttd.ttdenv import TtdEnvFactory

job_start_date = datetime(2024, 10, 20)
base_job_name = 'ttd-upstream-reach-curve-pyspark'
input_date = '{{ dag_run.conf.get("input_date", data_interval_end.strftime("%Y-%m-%d")) }}'
# By default, output_date is set to input_date
output_date = '{{ dag_run.conf.get("output_date", dag_run.conf.get("input_date", data_interval_end.strftime("%Y-%m-%d"))) }}'
ttd_env = TtdEnvFactory.get_from_system()

version = "latest"
whl_path = f"{PFX_PYSPARK_JOBS_PATH}/{version}/{PFX_PYSPARK_JOBS_WHL_NAME}"
entry_file_path = f"{PFX_PYSPARK_JOBS_PATH}/{version}/reach_curve_generation_job.py"

emr_version = AwsEmrVersions.AWS_EMR_SPARK_3_5
python_version = "3.10.14"

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M7g.m7g_4xlarge().with_fleet_weighted_capacity(1),
        M7g.m7g_8xlarge().with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=1
)
core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M7g.m7g_2xlarge().with_fleet_weighted_capacity(2),
        M7g.m7g_4xlarge().with_fleet_weighted_capacity(4),
        M7g.m7g_8xlarge().with_fleet_weighted_capacity(8),
    ],
    on_demand_weighted_capacity=40
)


# DAG Creation
def create_reach_curve_pyspark_dag(config_section: str, schedule: Optional["ScheduleInterval"] = None) -> tuple[DAG, BaseOperator]:
    job_name = f"{base_job_name}-{config_section}"

    ttd_dag = TtdDag(
        dag_id=job_name,
        start_date=job_start_date,
        schedule_interval=schedule,
        slack_channel=PFX.team.alarm_channel,
        tags=["pfx", "reach curve"],
        max_active_runs=3,
    )

    cluster_task = EmrClusterTask(
        name=job_name,
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        cluster_tags={"Team": PFX.team.jira_team},
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        cluster_auto_terminates=True,
        emr_release_label=emr_version,
        whls_to_install=[whl_path],
        python_version=python_version,
    )

    arguments = [
        f"--ttd_env={ttd_env.execution_env}", f"--config_section={config_section}", f"--date={input_date}", f"--output_date={output_date}"
    ]

    job_task = S3PysparkEmrTask(
        name=f"{job_name}-emr",
        entry_point_path=entry_file_path,
        additional_args_option_pairs_list=[("conf", "spark.sql.execution.arrow.pyspark.enabled=true"), ("conf", "spark.driver.memory=16G"),
                                           ("conf", "spark.driver.cores=8")],
        cluster_specs=cluster_task.cluster_specs,
        command_line_arguments=arguments,
    )

    cluster_task.add_parallel_body_task(job_task)

    ttd_dag >> cluster_task
    return ttd_dag.airflow_dag, cluster_task.last_airflow_op()


# Push-based scheduling system
class ReachCurveScheduling:
    CONFIG_TRIGGER_DOWNSTREAM_DAG = "trigger-downstream-dag"
    XCOM_REACH_CURVE_DATE_KEY = "reach-curve-target-date"
    XCOM_DEMO_DATE_KEY = "demo-target-date"

    @classmethod
    def get_output_date_from_dag_run(cls, dag_run: DagRun) -> datetime:
        conf: dict = dag_run.conf
        if "output_date" in conf:
            return datetime.strptime(conf["output_date"], "%Y-%m-%d")
        return cls.get_input_date_from_dag_run(dag_run)

    @classmethod
    def get_input_date_from_dag_run(cls, dag_run: DagRun) -> datetime:
        conf: dict = dag_run.conf
        if "input_date" in conf:
            return datetime.strptime(conf["input_date"], "%Y-%m-%d")
        return dag_run.data_interval_end

    @classmethod
    def should_trigger_reach_curve_gen(cls, reach_curve_dag_id: str, trigger_task_id: str, nothing_task_id: str, **context):
        most_recent_run = get_most_recent_successful_dag_run(reach_curve_dag_id, key=cls.get_output_date_from_dag_run)
        most_recent_run_str = "None" if most_recent_run is None else most_recent_run.strftime("%Y-%m-%d")
        logging.info(f'trigger-reach-curve-gen: most_recent_run={most_recent_run_str}')
        if most_recent_run is None:
            return trigger_task_id
        now = datetime.now(None if most_recent_run.tzinfo is None else timezone.utc)
        if now - timedelta(days=28) > most_recent_run:
            return trigger_task_id
        return nothing_task_id

    @classmethod
    def create_trigger_reach_curve_gen_dag_task(cls, parent_dag: DAG, trigger_dag_id: str, trigger_input_date: str) -> BaseOperator:
        trigger_rc_gen_operator = TriggerDagRunOperator(
            task_id=f"trigger_{trigger_dag_id}", trigger_dag_id=trigger_dag_id, conf={
                "input_date": trigger_input_date,
            }
        )

        do_nothing_task = EmptyOperator(task_id='do_nothing_task', dag=parent_dag)

        branch_op = BranchPythonOperator(
            task_id='branch_task',
            provide_context=True,
            python_callable=partial(
                cls.should_trigger_reach_curve_gen,
                reach_curve_dag_id=trigger_dag_id,
                trigger_task_id=trigger_rc_gen_operator.task_id,
                nothing_task_id=do_nothing_task.task_id
            ),
            dag=parent_dag
        )

        branch_op >> [trigger_rc_gen_operator, do_nothing_task]
        return branch_op

    @classmethod
    def calculate_validation_dates(cls, demo_weights: DateGeneratedDataset, **context):
        task_instance = context['task_instance']
        dag_run = context['dag_run']

        input_date_value = cls.get_input_date_from_dag_run(dag_run)
        logging.info(f'Using input date to calculate demo target date {input_date_value.strftime("%Y-%m-%d")}')
        cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()
        demo_date = demo_weights.check_recent_data_exist(
            cloud_storage=cloud_storage, ds_date=input_date_value, max_lookback=15
        ).get().strftime("%Y-%m-%d")
        logging.info(f'Using demo date {demo_date}')
        task_instance.xcom_push(key=cls.XCOM_DEMO_DATE_KEY, value=demo_date)

        output_date_dashes = cls.get_output_date_from_dag_run(dag_run).strftime("%Y-%m-%d")
        task_instance.xcom_push(key=cls.XCOM_REACH_CURVE_DATE_KEY, value=output_date_dashes)
        logging.info(f'Using output date {demo_date}')

    @classmethod
    def create_trigger_validation_dag_task(cls, parent_dag: DAG, trigger_dag_id: str, demo_weights: DateGeneratedDataset):
        calculate_demo_date_step = PythonOperator(
            task_id="calculate_demo_date",
            python_callable=partial(cls.calculate_validation_dates, demo_weights=demo_weights),
            dag=parent_dag,
            provide_context=True
        )
        trigger_rc_gen_operator = TriggerDagRunOperator(
            task_id=f"trigger_{trigger_dag_id}",
            trigger_dag_id=trigger_dag_id,
            conf={
                "reach-curve-target-date":
                xcom_template_to_get_value(parent_dag.dag_id, calculate_demo_date_step.task_id, cls.XCOM_REACH_CURVE_DATE_KEY),
                "demo-target-date":
                xcom_template_to_get_value(parent_dag.dag_id, calculate_demo_date_step.task_id, cls.XCOM_DEMO_DATE_KEY)
            }
        )
        calculate_demo_date_step >> trigger_rc_gen_operator

        return trigger_rc_gen_operator


# Initialise and chain DAGs
v2_dag, v2_last_op = create_reach_curve_pyspark_dag(config_section="v2")
v2_trigger_validation_dag = ReachCurveScheduling.create_trigger_validation_dag_task(
    v2_dag, v2_validation_dag_id, Datasources.ctv.upstream_forecast_demo_weights_v2
)
v2_last_op >> v2_trigger_validation_dag

v3_dag, _ = create_reach_curve_pyspark_dag(config_section="v3")

v3_extrapolated_dag, v3_extrapolated_last_op = create_reach_curve_pyspark_dag(config_section="v3-extrapolated")
v3_trigger_validation_dag = ReachCurveScheduling.create_trigger_validation_dag_task(
    v3_extrapolated_dag, v3_validation_dag_id, Datasources.ctv.upstream_forecast_demo_weights_v3
)
v3_extrapolated_last_op >> v3_trigger_validation_dag

og_dag, og_last_op = create_reach_curve_pyspark_dag(config_section="og")

og_trigger_validation_dag = ReachCurveScheduling.create_trigger_validation_dag_task(
    og_dag, og_validation_dag_id, Datasources.ctv.upstream_forecast_demo_weights_og
)
og_last_op >> og_trigger_validation_dag
