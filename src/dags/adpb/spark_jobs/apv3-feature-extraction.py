from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask

from ttd.slack.slack_groups import ADPB

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

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
}, {
    "Classification": "spark-defaults",
    "Properties": {
        "spark.driver.maxResultSize": "40G",
        "spark.network.timeout": "360",
    }
}]

apv3_feature_extraction = TtdDag(
    dag_id="adpb-apv3-feature-extraction",
    start_date=datetime(2024, 7, 25, 13, 0, 0),
    schedule_interval=timedelta(days=1),
    max_active_runs=2,
    slack_channel="#scrum-adpb-alerts",
    slack_tags=ADPB.team.sub_team,
    enable_slack_alert=True
)
dag = apv3_feature_extraction.airflow_dag

cluster = EmrClusterTask(
    name="Apv3_Feature_Extraction",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_4xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": ADPB.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_4xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7g.r7g_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7g.r7g_12xlarge().with_ebs_size_gb(768).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7g.r7g_16xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(4),
            R7gd.r7gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7gd.r7gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R7gd.r7gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(4)
        ],
        on_demand_weighted_capacity=150
    ),
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

pfs_step = EmrJobTask(
    cluster_specs=cluster.cluster_specs,
    name="Apv3_Feature_Extraction",
    class_name="jobs.userfeatureextraction.DailyUserFeatureExtraction",
    eldorado_config_option_pairs_list=[("date", "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}")],
    additional_args_option_pairs_list=[("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED"), ("conf", "spark.driver.memory=4G")],
    executable_path=jar_path
)

mapping_step = EmrJobTask(
    cluster_specs=cluster.cluster_specs,
    name="Apv3_UID2_Mapping",
    class_name="jobs.userfeatureextraction.DailyMappingTdidUid2Extraction",
    eldorado_config_option_pairs_list=[("date", "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}")],
    additional_args_option_pairs_list=[("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED"), ("conf", "spark.driver.memory=4G")],
    executable_path=jar_path
)

check = OpTask(op=FinalDagStatusCheckOperator(dag=apv3_feature_extraction.airflow_dag))

cluster.add_sequential_body_task(pfs_step)
cluster.add_sequential_body_task(mapping_step)

apv3_feature_extraction >> cluster >> check
