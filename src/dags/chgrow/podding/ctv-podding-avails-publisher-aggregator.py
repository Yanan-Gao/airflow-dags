# from airflow import DAG
from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from datasources.sources.avails_datasources import AvailsDatasources
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.ec2.emr_instance_types.memory_optimized.r6g import R6g
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import CHGROW
from ttd.interop.logworkflow_callables import ExternalGateOpen
from airflow.operators.python_operator import PythonOperator
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

dag_id = "ctv-inventory-podding-avails-hourly-aggregator"
job_start_date = datetime(2024, 8, 6, 14, 00)
job_schedule_interval = timedelta(hours=1)

podding_avails_dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    retries=5,
    slack_channel=CHGROW.channels_growth().alarm_channel,
    slack_tags=CHGROW.channels_growth().sub_team,
    retry_delay=timedelta(minutes=120),
    tags=[CHGROW.team.jira_team],
    default_args={'owner': CHGROW.team.jira_team},
    max_active_runs=3
)

cluster = EmrClusterTask(
    name=dag_id + '-cluster',
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M6g.m6g_xlarge().with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=
    EmrFleetInstanceTypes(instance_types=[R6g.r6g_4xlarge().with_fleet_weighted_capacity(32)], on_demand_weighted_capacity=128),
    cluster_tags={"Team": CHGROW.team.jira_team},
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=False,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

job_step = EmrJobTask(
    name=dag_id + "-step",
    class_name="jobs.ctv.podding.PublisherAvailsAgg",
    executable_path="s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/el-dorado-assembly.jar",
    additional_args_option_pairs_list=[('conf', 'spark.executor.extraJavaOptions=-server -XX:+UseParallelGC'),
                                       ('executor-memory', '11043m'), ('executor-cores', '3'), ('conf', 'spark.executor.memory=11043m'),
                                       ('conf', 'spark.driver.maxResultSize=6G'), ('conf', 'spark.sql.shuffle.partitions=128'),
                                       ('conf', 'yarn.nodemanager.resource.memory-mb=52000')],
    eldorado_config_option_pairs_list=[('processingTime', '{{ (logical_date).strftime(\"%Y-%m-%dT%H:00:00\") }}')],
    cluster_specs=cluster.cluster_specs
)

cluster.add_parallel_body_task(job_step)

wait_complete = DatasetCheckSensor(
    dag=podding_avails_dag.airflow_dag,
    ds_date="{{ (logical_date).to_datetime_string() }}",
    poke_interval=60 * 30,  # poke every 10 minutes - more friendly to the scheduler
    datasets=[AvailsDatasources.publisher_agg_hourly_dataset.with_check_type("hour").with_region("us-east-1")],
    timeout=60 * 60 * 48
)

ctv_publisher_import_open_gate = PythonOperator(
    python_callable=ExternalGateOpen,
    provide_context=True,
    op_kwargs={
        'mssql_conn_id': 'lwdb' if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else 'sandbox-lwdb',
        'sproc_arguments': {
            'gatingType': 2000213,  # dbo.fn_enum_GatingType_ImportCTVPublisherHourly()
            'grain': 100001,  # dbo.fn_Enum_TaskBatchGrain_Hourly()
            'dateTimeToOpen': '{{ (logical_date).strftime(\"%Y-%m-%dT%H:00:00\") }}'
        }
    },
    task_id="ctv_publisher_import_open_gate",
    retries=3,
    retry_delay=timedelta(minutes=60),
    retry_exponential_backoff=True
)
adag = podding_avails_dag.airflow_dag
podding_avails_dag >> OpTask(op=wait_complete) >> cluster >> OpTask(op=ctv_publisher_import_open_gate)
