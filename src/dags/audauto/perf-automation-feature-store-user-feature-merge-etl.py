from datetime import datetime, timedelta

from airflow.sensors.external_task import ExternalTaskSensor

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.operators.write_date_to_s3_file_operator import WriteDateToS3FileOperator
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "100",
        "fs.s3.sleepTimeSeconds": "15",
        "mapreduce.input.fileinputformat.list-status.num-threads": "32"
    }
}]

run_date = "{{ data_interval_start.to_date_string() }}"
FS_JAR = "s3://thetradedesk-mlplatform-us-east-1/libs/feature_store/jars/prod/feature_store.jar"

fs_user_feature_merge_etl_dag = TtdDag(
    dag_id="perf-automation-feature-store-user-feature-merge-etl",
    start_date=datetime(2024, 7, 16, 2, 0),
    schedule_interval=timedelta(hours=24),
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    retry_delay=timedelta(hours=1),
    slack_channel="#scrum-perf-auto-audience-alerts",
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=True,
    tags=["AUDAUTO", "FEATURE_STORE"]
)
adag = fs_user_feature_merge_etl_dag.airflow_dag

fs_user_bidimpression_feature_etl_dag_sensor = OpTask(
    op=ExternalTaskSensor(
        task_id='fs_user_bidimpression_feature_etl_dag_sensor',
        external_dag_id="perf-automation-feature-store-user-bidimpression-feature-etl",
        external_task_id=None,  # wait for the entire DAG to complete
        allowed_states=["success"],
        check_existence=False,
        poke_interval=5 * 60,  # poke_interval is in seconds - poke every 5 minutes
        timeout=36 * 60 * 60,  # timeout in seconds - wait 36 hours
        mode='reschedule',  # release the worker slot between pokes
        dag=adag
    )
)

fs_user_daily_density_feature_etl_dag_sensor = OpTask(
    op=ExternalTaskSensor(
        task_id='fs_user_daily_density_feature_etl_dag_sensor',
        external_dag_id="perf-automation-feature-store-daily-tdid-density-feature-parameterized",
        external_task_id=None,  # wait for the entire DAG to complete
        allowed_states=["success"],
        check_existence=False,
        execution_delta=timedelta(hours=2),
        poke_interval=5 * 60,  # poke_interval is in seconds - poke every 5 minutes
        timeout=36 * 60 * 60,  # timeout in seconds - wait 36 hours
        mode='reschedule',  # release the worker slot between pokes
        dag=adag
    )
)

###############################################################################
# clusters
###############################################################################
master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1),
    ],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(32),
    ],
    on_demand_weighted_capacity=4096,
)

fs_user_feature_merge_cluster_task = EmrClusterTask(
    name="FS_User_Feature_Merge_ETL_Cluster",
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={'Team': AUDAUTO.team.jira_team},
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    cluster_auto_termination_idle_timeout_seconds=60
)

fs_user_feature_merge_generation_step = EmrJobTask(
    name="UserFeatureMergeJob",
    class_name="com.thetradedesk.featurestore.jobs.UserFeatureMergeJob",
    eldorado_config_option_pairs_list=[('date', run_date), ('userFeatureMergeDefinitionPath', '/userFeatureMergeDefinition.Prod.json')],
    executable_path=FS_JAR,
    timeout_timedelta=timedelta(hours=8)
)
fs_user_feature_merge_cluster_task.add_parallel_body_task(fs_user_feature_merge_generation_step)

###############################################################################
# Modify S3 file operators
###############################################################################
update_merged_feature_current_file_task = OpTask(
    op=WriteDateToS3FileOperator(
        task_id="update_policy_table_current_file_task",
        s3_bucket="thetradedesk-mlplatform-us-east-1",
        s3_key=f"features/feature_store/{TtdEnvFactory.get_from_system().execution_env}/user_features_merged_meta/v=1/_CURRENT",
        date="{{ data_interval_start.strftime(\"%Y%m%d00\") }}",
        append_file=True,
        dag=adag,
    )
)

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=adag))

# Flow
fs_user_feature_merge_etl_dag >> fs_user_bidimpression_feature_etl_dag_sensor >> fs_user_daily_density_feature_etl_dag_sensor >> fs_user_feature_merge_cluster_task >> update_merged_feature_current_file_task >> final_dag_status_step
