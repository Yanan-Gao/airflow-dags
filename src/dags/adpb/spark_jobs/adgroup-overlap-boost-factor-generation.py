from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask

# Parameters
dag_name = "adpb-adgroup-overlap-boost-factor-generation"
job_start_date = datetime(2024, 10, 16, 0, 0, 0)
job_schedule_interval = timedelta(hours=24)
owner = ADPB.team

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "1000"
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

spark_options_list = [("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

eldorado_options_list = [("generationDate", "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}")]

# DAG definition
adgroup_overlap_boost_factor_generation = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=owner.alarm_channel,
    slack_tags=owner.sub_team,
    tags=[owner.jira_team],
    enable_slack_alert=True
)

cluster = EmrClusterTask(
    name="AdGroupOverlapBoostFactorGeneration_Cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_4xlarge().with_ebs_size_gb(64).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": owner.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_8xlarge().with_ebs_size_gb(64).with_max_ondemand_price().with_fleet_weighted_capacity(2)],
        on_demand_weighted_capacity=2
    ),
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

pfs_step = EmrJobTask(
    cluster_specs=cluster.cluster_specs,
    name="AdGroupOverlapBoostFactorGeneratorStep",
    class_name="jobs.adgroupaudiencemetadata.AdGroupOverlapBoostFactorGeneratorJob",
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=eldorado_options_list,
    executable_path=jar_path
)
cluster.add_sequential_body_task(pfs_step)

check = OpTask(op=FinalDagStatusCheckOperator(dag=adgroup_overlap_boost_factor_generation.airflow_dag))

adgroup_overlap_boost_factor_generation >> cluster >> check

dag = adgroup_overlap_boost_factor_generation.airflow_dag
