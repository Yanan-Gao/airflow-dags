from datetime import datetime, timedelta

from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from airflow.operators.python import PythonOperator

from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProviders
from ttd.operators.dataset_recency_operator import DatasetRecencyOperator
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5a import R5a
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.tasks.op import OpTask

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
import copy
# Prod variables
from ttd.ttdenv import TtdEnvFactory

job_start_date = datetime(2024, 8, 6, 12, 0, 0)  # change the start time to noon because counts-hmh produces the data set around 6 am
dag_name = "adpb-sibv3-datasources-for-lal"  # IMPORTANT: add "dev-" prefix when testing
job_environment = TtdEnvFactory.get_from_system()

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"
job_schedule_interval = timedelta(days=1)
logworkflow_connection = 'lwdb'
logworkflow_sandbox_connection = 'sandbox-lwdb'
sibv3_bucket = 'ttd-identity'

device_path = f"datapipeline/{job_environment.dataset_write_env}/seeninbiddingdevicedatauniques/v=3/date="
devices_sib_datehour = "{{ task_instance.xcom_pull(dag_id='" + dag_name + "', task_ids='get_sib_datasets_datehour_step', key='" + Datasources.sib.devices_active_counts_data_collection.data_name + "').strftime(\"%Y-%m-%dT%H:00:00\") }}"
devices_sib_date = "{{ task_instance.xcom_pull(dag_id='" + dag_name + "', task_ids='get_sib_datasets_datehour_step', key='" + Datasources.sib.devices_active_counts_data_collection.data_name + "').strftime(\"%Y-%m-%d\") }}"

person_path = f"datapipeline/{job_environment.dataset_write_env}/seeninbiddingpersongroupdatauniques/v=3/date="
persons_sib_datehour = "{{ task_instance.xcom_pull(dag_id='" + dag_name + "', task_ids='get_sib_datasets_datehour_step', key='" + Datasources.sib.persons_households_data_export.data_name + "').strftime(\"%Y-%m-%dT%H:00:00\") }}"
persons_sib_date = "{{ task_instance.xcom_pull(dag_id='" + dag_name + "', task_ids='get_sib_datasets_datehour_step', key='" + Datasources.sib.persons_households_data_export.data_name + "').strftime(\"%Y-%m-%d\") }}"

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "150",
        "fs.s3.sleepTimeSeconds": "10"
    }
}, {
    "Classification": "core-site",
    "Properties": {
        "fs.s3a.connection.maximum": "1000",
        "fs.s3a.threads.max": "50"
    }
}, {
    "Classification": "capacity-scheduler",
    "Properties": {
        "yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
    }
}, {
    "Classification": "yarn-site",
    "Properties": {
        "yarn.nodemanager.pmem-check-enabled": "false",
        "yarn.nodemanager.vmem-check-enabled": "false"
    }
}]

# DAG
sibv3_datasources_for_lal_dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel="#scrum-adpb-alerts",
    dag_tsg="https://atlassian.thetradedesk.com/confluence/x/qdkMCQ",
    slack_tags=ADPB.team.sub_team,
)

dag = sibv3_datasources_for_lal_dag.airflow_dag

# Pipeline dependencies
# Get the most recent sib datasets
get_sib_datasets_datehour_step = OpTask(
    op=DatasetRecencyOperator(
        dag=dag,
        recency_start_date=datetime.today(),
        cloud_provider=CloudProviders.aws,
        datasets_input=[Datasources.sib.devices_active_counts_data_collection, Datasources.sib.persons_households_data_export],
        lookback_days=2,
        xcom_push=True,
        task_id='get_sib_datasets_datehour_step'
    )
)


def add_success_file(**kwargs):
    date_partition = kwargs['runDate'].replace("-", "")
    aws_storage = AwsCloudStorage(conn_id='aws_default')
    key = f"{kwargs['prefix']}{date_partition}/_SUCCESS"
    aws_storage.load_string(string_data='', key=key, bucket_name=sibv3_bucket, replace=True)

    return f'Written key {key}'


####################################################################################################################
# SeenInBidding_V3_Device
####################################################################################################################

sibv3_device_cluster_name = 'SeenInBidding_V3_Device_Cluster'

# Fleet Cluster
device_cluster = EmrClusterTask(
    name=sibv3_device_cluster_name,
    cluster_tags={
        "Team": ADPB.team.jira_team,
    },
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5a.r5a_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(2)
        ],
        on_demand_weighted_capacity=48
    ),
    use_on_demand_on_timeout=True,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

spark_options_list = [("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

# Step device agg
device_agg_options = [
    ("sibDateHourToProcess", devices_sib_datehour),
]

device_agg_step = EmrJobTask(
    name="SeenInBiddingV3_Device_Agg_Step",
    class_name="jobs.agg_etl.SeenInBiddingV3DeviceAgg",
    eldorado_config_option_pairs_list=device_agg_options,
    additional_args_option_pairs_list=spark_options_list,
    executable_path=jar_path,
    cluster_specs=device_cluster.cluster_specs,
)
device_cluster.add_sequential_body_task(device_agg_step)

# Step device uniques and overlap
device_uniques_and_overlap_options = [
    ("runDate", "{{ ds }}"),
    ("sibDateToProcess", devices_sib_date),
    ("ttd.ds.SeenInBiddingV3DeviceDataSet.isInChain", "true"),
]

device_uniques_and_overlap_step = EmrJobTask(
    name="SeenInBiddingV3_Device_Uniques_And_Overlap",
    class_name="jobs.agg_etl.SeenInBiddingV3DeviceUniquesAndOverlapsJob",
    timeout_timedelta=timedelta(hours=2),
    eldorado_config_option_pairs_list=device_uniques_and_overlap_options,
    additional_args_option_pairs_list=spark_options_list,
    executable_path=jar_path,
    cluster_specs=device_cluster.cluster_specs,
)
device_cluster.add_sequential_body_task(device_uniques_and_overlap_step)

# Step device add success file
device_success_file = OpTask(
    op=PythonOperator(
        task_id='device_success_file',
        provide_context=True,
        python_callable=add_success_file,
        op_kwargs={
            'prefix': device_path,
            'runDate': "{{ ds }}"
        },
        dag=dag
    )
)

# Step device vertica import
device_vertica_import = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=ExternalGateOpen,
        provide_context=True,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
            'sproc_arguments': {
                'gatingType': 10085,  # dbo.fn_Enum_GatingType_ImportSeenInBiddingV3DeviceUniques()
                'grain': 100002,  # dbo.fn_Enum_TaskBatchGrain_Daily()
                'dateTimeToOpen': '{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}'
            }
        },
        task_id="device_vertica_import",
    )
)

####################################################################################################################
# SeenInBidding_V3_Person
####################################################################################################################
sibv3_person_cluster_name = 'SeenInBidding_V3_Person_Cluster'

# Fleet Cluster
person_cluster = EmrClusterTask(
    name=sibv3_person_cluster_name,
    cluster_tags={
        "Team": ADPB.team.jira_team,
    },
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5a.r5a_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(2)
        ],
        on_demand_weighted_capacity=32
    ),
    use_on_demand_on_timeout=True,
    additional_application_configurations=copy.deepcopy(application_configuration),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
)

# Step person agg
person_agg_options = [
    ("sibDateHourToProcess", persons_sib_datehour),
]

person_agg_step = EmrJobTask(
    name="SeenInBiddingV3_Person_Agg_Step",
    class_name="jobs.agg_etl.SeenInBiddingV3PersonAgg",
    eldorado_config_option_pairs_list=person_agg_options,
    additional_args_option_pairs_list=spark_options_list,
    executable_path=jar_path,
    cluster_specs=person_cluster.cluster_specs,
)
person_cluster.add_sequential_body_task(person_agg_step)

# Step person uniques and overlap
person_uniques_and_overlap_options = [
    ("runDate", "{{ ds }}"),
    ("sibDateToProcess", persons_sib_date),
    ("ttd.ds.SeenInBiddingV3PersonGroupDataSet.isInChain", "true"),
]

person_uniques_and_overlap_step = EmrJobTask(
    name="SeenInBiddingV3_Person_Uniques_And_Overlap",
    class_name="jobs.agg_etl.SeenInBiddingV3PersonUniquesAndOverlapsJob",
    timeout_timedelta=timedelta(hours=2),
    eldorado_config_option_pairs_list=person_uniques_and_overlap_options,
    additional_args_option_pairs_list=spark_options_list,
    executable_path=jar_path,
    cluster_specs=person_cluster.cluster_specs,
)
person_cluster.add_sequential_body_task(person_uniques_and_overlap_step)

# Step person add success file
person_success_file = OpTask(
    op=PythonOperator(
        task_id='person_success_file',
        provide_context=True,
        python_callable=add_success_file,
        op_kwargs={
            'prefix': person_path,
            'runDate': "{{ ds }}"
        },
        dag=dag
    )
)

# Step person vertica import
person_vertica_import = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=ExternalGateOpen,
        provide_context=True,
        op_kwargs={
            'mssql_conn_id': logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
            'sproc_arguments': {
                'gatingType': 10086,  # dbo.fn_Enum_GatingType_ImportSeenInBiddingV3PersonGroupUniques()
                'grain': 100002,  # dbo.fn_Enum_TaskBatchGrain_Daily()
                'dateTimeToOpen': '{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}'
            }
        },
        task_id="person_vertica_import",
    )
)

check = OpTask(op=FinalDagStatusCheckOperator(dag=sibv3_datasources_for_lal_dag.airflow_dag))

sibv3_datasources_for_lal_dag >> get_sib_datasets_datehour_step
get_sib_datasets_datehour_step >> device_cluster
get_sib_datasets_datehour_step >> person_cluster

device_cluster >> device_success_file >> device_vertica_import >> check
person_cluster >> person_success_file >> person_vertica_import >> check
