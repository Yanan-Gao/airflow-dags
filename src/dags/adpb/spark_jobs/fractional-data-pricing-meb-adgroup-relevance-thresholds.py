from datetime import datetime, timedelta

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.ec2.emr_instance_types.memory_optimized.r7gd import R7gd
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask

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

fdp_meb_adgroup_relevance_thresholds_generation = TtdDag(
    dag_id="adpb-fdp-meb-adgroup-relevance-thresholds",
    start_date=datetime(2024, 8, 9, 2, 0, 0),
    schedule_interval=timedelta(hours=24),
    slack_channel="#scrum-adpb-alerts",
    slack_tags=ADPB.team.sub_team,
    enable_slack_alert=True
)

cluster = EmrClusterTask(
    name="FdpMEBAdGroupRelevanceThresholdsGeneration_Cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": ADPB.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R7g.r7g_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7g.r7g_16xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R7gd.r7gd_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R7gd.r7gd_16xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(2),
        ],
        on_demand_weighted_capacity=32
    ),
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

spark_options_list = [("executor-memory", "188G"), ("executor-cores", "30"), ("num-executors", "32"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"), ("conf", "spark.driver.memory=50G"),
                      ("conf", "spark.driver.cores=10"), ("conf", "spark.sql.shuffle.partitions=5000"),
                      ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

pfs_step = EmrJobTask(
    cluster_specs=cluster.cluster_specs,
    name="FdpAdGroupRelevanceThresholdCalculatorStep",
    class_name="jobs.fractionaldatapricing.AdGroupRelevanceThresholdCalculatorJob",
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=[("calculationDate", "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}")],
    executable_path=jar_path
)
cluster.add_sequential_body_task(pfs_step)
check = OpTask(op=FinalDagStatusCheckOperator(dag=fdp_meb_adgroup_relevance_thresholds_generation.airflow_dag))

fdp_meb_adgroup_relevance_thresholds_generation >> cluster >> check

dag = fdp_meb_adgroup_relevance_thresholds_generation.airflow_dag
