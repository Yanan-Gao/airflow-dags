from typing import List, Tuple
from airflow.utils.state import State

from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.tasks.op import OpTask
from datetime import datetime, timedelta

from ttd.slack.slack_groups import ADPB
import copy

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

cluster_name = "AudiencePredictor"

job_step_retries: int = 2
job_step_retry_delay: timedelta = timedelta(minutes=30)

date_macro = "{{ data_interval_end.strftime(\"%Y-%m-%dT%H:00:00\") }}"

conversionLookbackDays = "14"
bidFeedbackLookbackDays = "19"
bidFeedbackTdidLookbackDays = "5"

java_settings_list: List[Tuple[str, str]] = []

spark_options_list = [("executor-memory", "92G"), ("executor-cores", "16"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                      ("conf", "dfs.client.use.datanode.hostname=true"), ("conf", "spark.driver.memory=40G"),
                      ("conf", "spark.sql.shuffle.partitions=3000"), ("conf", "spark.driver.maxResultSize=15G"),
                      ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000",
        "fs.s3.maxRetries": "30",
        "fs.s3.sleepTimeSeconds": "15"
    }
}]

cluster_tags = {
    'Team': ADPB.team.jira_team,
}

audience_predictor = TtdDag(
    dag_id="adpb-audience-predictor",
    start_date=datetime(2024, 8, 22, 2),
    schedule_interval=timedelta(hours=24),
    retries=job_step_retries,
    retry_delay=job_step_retry_delay,
    slack_tags=ADPB.team.sub_team,
    enable_slack_alert=True,
)

cluster = EmrClusterTask(
    name="AudiencePredictor",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_4xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7g.r7g_8xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7g.r7g_12xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7g.r7g_16xlarge().with_ebs_size_gb(4096).with_max_ondemand_price().with_fleet_weighted_capacity(4),
            R7gd.r7gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7gd.r7gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7gd.r7gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(4),
        ],
        on_demand_weighted_capacity=40
    ),
    enable_prometheus_monitoring=True,
    additional_application_configurations=copy.deepcopy(application_configuration),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

export_cluster = EmrClusterTask(
    name="AudiencePredictorExport",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    cluster_tags=cluster_tags,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_4xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7g.r7g_8xlarge().with_ebs_size_gb(2048).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7g.r7g_12xlarge().with_ebs_size_gb(3072).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7g.r7g_16xlarge().with_ebs_size_gb(4096).with_max_ondemand_price().with_fleet_weighted_capacity(4),
            R7gd.r7gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7gd.r7gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7gd.r7gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(4),
        ],
        on_demand_weighted_capacity=18
    ),
    enable_prometheus_monitoring=True,
    additional_application_configurations=copy.deepcopy(application_configuration),
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

pixel_tdid_step = EmrJobTask(
    name="PixelTDID",
    class_name="model.AudiencePredictor.PixelTDID",
    timeout_timedelta=timedelta(minutes=90),
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_settings_list + [("conversionLookbackDays", conversionLookbackDays), ("runTime", date_macro)],
    executable_path=jar_path,
    cluster_specs=cluster.cluster_specs,
)

site_advertiser_score_step = EmrJobTask(
    name="SiteAdvertiserScore",
    class_name="model.AudiencePredictor.SiteAdvertiserScore",
    timeout_timedelta=timedelta(minutes=90),
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_settings_list + [("bidFeedbackLookbackDays", bidFeedbackLookbackDays), ("runTime", date_macro)],
    executable_path=jar_path,
    cluster_specs=cluster.cluster_specs,
)

tdid_sites_and_hits_step = EmrJobTask(
    name="TDIDSitesAndHits",
    class_name="model.AudiencePredictor.TDIDSitesAndHits",
    timeout_timedelta=timedelta(minutes=45),
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_settings_list + [("bidFeedbackTdidLookbackDays", bidFeedbackTdidLookbackDays),
                                                            ("runTime", date_macro)],
    executable_path=jar_path,
    cluster_specs=cluster.cluster_specs,
)

tdid_advertiser_data_elements_step = EmrJobTask(
    name="TDIDAdvertiserDataElements",
    class_name="model.AudiencePredictor.TDIDAdvertiserDataElements",
    timeout_timedelta=timedelta(hours=8),
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_settings_list + [("runTime", date_macro)],
    executable_path=jar_path,
    cluster_specs=cluster.cluster_specs,
)

export_results_data_import = EmrJobTask(
    name="ExportResultToDataImport",
    class_name="model.AudiencePredictor.ExportResultToDataImport",
    timeout_timedelta=timedelta(hours=3),
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_settings_list + [("runTime", date_macro),
                                                            ("combinedDataImportBucket", "thetradedesk-useast-data-import")],
    executable_path=jar_path,
    cluster_specs=export_cluster.cluster_specs,
)

normalize_overlapping_user_data_step = EmrJobTask(
    name="NormalizeOverlappingUserRatio",
    class_name="model.AudiencePredictor.OverLappingUsersCountNormalizer",
    timeout_timedelta=timedelta(hours=1),
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=java_settings_list + [("runTime", date_macro)],
    executable_path=jar_path,
    cluster_specs=cluster.cluster_specs,
)


# Final status check to ensure that all tasks have completed successfully
# Iterates through all dag tasks from the current dag run and fails the dag if any steps were unsuccessful
def check_final_status(**kwargs):
    for task_instance in kwargs['dag_run'].get_task_instances():
        if task_instance.current_state() == State.FAILED and task_instance.task_id != kwargs['task_instance'].task_id:
            raise Exception("Task {task_id} has failed. Entire DAG run will be reported as a failure".format(task_id=task_instance.task_id))


# Python operator that calls our check_final_status() function after all other user_lal tasks have completed
final_dag_status_step = OpTask(
    op=PythonOperator(
        task_id="final_dag_status",
        provide_context=True,
        python_callable=check_final_status,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=audience_predictor
        .airflow_dag  # Need to call get_airflow_dag() as the actual airflow dag object is defined within the ElDoradoDag wrapper
    )
)

cluster.add_sequential_body_task(pixel_tdid_step)
cluster.add_sequential_body_task(site_advertiser_score_step)
cluster.add_sequential_body_task(tdid_sites_and_hits_step)
cluster.add_sequential_body_task(tdid_advertiser_data_elements_step)

export_cluster.add_sequential_body_task(export_results_data_import)

cluster.add_sequential_body_task(normalize_overlapping_user_data_step)

audience_predictor >> cluster >> final_dag_status_step
tdid_advertiser_data_elements_step >> export_cluster >> final_dag_status_step

dag = audience_predictor.airflow_dag
