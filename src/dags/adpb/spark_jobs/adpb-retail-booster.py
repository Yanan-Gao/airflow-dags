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

retailbrandmap = ["placed-3pd-att:eltororetail", "iriwgreens:walgpeoplecloud", "amc_offline:lr2022amc", "dgoffline:lr2022dg"]

retail_booster_dag = TtdDag(
    dag_id="adpb-retail-booster",
    start_date=datetime(2024, 8, 2, 0, 0, 0),
    schedule_interval=timedelta(hours=24),
    slack_channel="#scrum-adpb-alerts",
    slack_tags=ADPB.team.sub_team,
    enable_slack_alert=True
)
dag = retail_booster_dag.airflow_dag

cluster = EmrClusterTask(
    name="RetailBoosterCalculation",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_4xlarge().with_ebs_size_gb(1024).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": ADPB.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_4xlarge().with_ebs_size_gb(256).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R5.r5_8xlarge().with_ebs_size_gb(512).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5.r5_12xlarge().with_ebs_size_gb(768).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R5.r5_16xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(4)
        ],
        on_demand_weighted_capacity=12
    ),
    additional_application_configurations=application_configuration,
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

retail_booster_adgroup_mapping_options = [
    ("date", "{{ ds }}"),
    ("useConsolidatedAccessManagement", "true"),
    ("retailMeasurementAndTargetBrandMap", ",".join(retailbrandmap)),  # deprecated
    ("useSystemRetailBrandSettings", "true"),  # deprecated
    ("lookAlikeModelLookBackDays", 14),
    ("relevanceScoreThreshold", 1.25),
    ("numberSelectedSegments", 10),
    ("ignoreAudienceBoosterChecked", "true")
]
retail_booster_mapping_step = EmrJobTask(
    cluster_specs=cluster.cluster_specs,
    name="RetailBoosterMappingCalculation",
    class_name="jobs.retailbooster.RetailBoosterMappingCalculation",
    eldorado_config_option_pairs_list=retail_booster_adgroup_mapping_options,
    additional_args_option_pairs_list=[("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")],
    executable_path=jar_path
)

check = OpTask(op=FinalDagStatusCheckOperator(dag=retail_booster_dag.airflow_dag))

cluster.add_parallel_body_task(retail_booster_mapping_step)

retail_booster_dag >> cluster >> check
