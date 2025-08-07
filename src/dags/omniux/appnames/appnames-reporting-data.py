from datetime import datetime

from airflow.operators.python import PythonOperator

from dags.omniux.utils import get_jar_file_path
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.datasets.date_external_dataset import DateExternalDataset
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.slack.slack_groups import OMNIUX
from ttd.ttdenv import TtdEnvFactory
from ttd.datasets.dataset import (default_date_part_format)

# Dag Configs
job_name = "appnames-reporting-data"
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_5

job_schedule_interval = "0 2 * * *"  # Same as ParquetSync Daily
job_start_date = datetime(2024, 6, 20)

env = TtdEnvFactory.get_from_system()
logworkflow_connection_open_gate = "lwdb" if env == TtdEnvFactory.prod else "sandbox-lwdb"

dag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    tags=[OMNIUX.team.name, "appnames"],
    slack_channel=OMNIUX.omniux().alarm_channel,
    slack_tags=OMNIUX.omniux().sub_team,
    enable_slack_alert=True,
    run_only_latest=True
)

adag = dag.airflow_dag

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(32)],
    on_demand_weighted_capacity=1,
    spot_weighted_capacity=0
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5d.r5d_2xlarge().with_fleet_weighted_capacity(1),
                    R5d.r5d_4xlarge().with_fleet_weighted_capacity(1)],
    spot_weighted_capacity=1
)

cluster_task = EmrClusterTask(
    name=f"{job_name}-cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={"Team": OMNIUX.omniux().jira_team},
    emr_release_label=emr_release_label,
    cluster_auto_terminates=True,
)

job_task = EmrJobTask(
    name=f"{job_name}-task",
    class_name="com.thetradedesk.ctv.upstreaminsights.pipelines.appnames.GenerateAppNamesReportingDataJob",
    executable_path=get_jar_file_path(),
    configure_cluster_automatically=True,
    eldorado_config_option_pairs_list=[("datetime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}")],
    additional_args_option_pairs_list=[
        ("conf", "spark.sql.shuffle.partitions=100"),
        ("conf", "spark.default.parallelism=100"),
    ]
)

cluster_task.add_parallel_body_task(job_task)

poke_interval = 60 * 10  # poke_interval is in seconds - poke every 10 minutes
timeout = 60 * 60 * 12  # timeout is in seconds - wait 12 hours

appnames_export_data = DateExternalDataset(
    bucket="ttd-ctv",
    path_prefix="prod/appnames",
    data_name="ExportSiteAppNamesAssociationToS3/Default",
    date_format=default_date_part_format
)

mobileapplicationidnamemap_parquet_export_data = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="mobileapplicationidnamemap",
    success_file=None,
    env_aware=False,
)

data_sensor = DatasetCheckSensor(
    dag=adag,
    datasets=[appnames_export_data, mobileapplicationidnamemap_parquet_export_data],
    ds_date='{{data_interval_start.to_datetime_string()}}',
    poke_interval=60 * 10,
    timeout=60 * 60 * 12
)

logworkflow_open_vertica_import_gate = PythonOperator(
    dag=adag,
    python_callable=ExternalGateOpen,
    provide_context=True,
    op_kwargs={
        'mssql_conn_id': logworkflow_connection_open_gate,
        'sproc_arguments': {
            'gatingType': 2000255,  # dbo.fn_Enum_GatingType_ImportMobileApplicationIdNameMapWithAppNames()
            'grain': 100002,  # dbo.fn_Enum_TaskBatchGrain_Daily()
            'dateTimeToOpen':
            '{{ data_interval_start.strftime(\"%Y-%m-%dT00:00:00\") }}'  # Ignore hour component as schedule is offset from start of day
        }
    },
    task_id="logworkflow_open_appnames_vertica_import_gate"
)

dag >> cluster_task
data_sensor >> cluster_task.first_airflow_op()
cluster_task.last_airflow_op() >> logworkflow_open_vertica_import_gate
