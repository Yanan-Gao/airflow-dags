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
dag_name = "adpb-customer-insights-dimension-category-index-refresh"
job_start_date = datetime(2025, 7, 13, 12, 0, 0)
job_schedule_interval = timedelta(hours=24)
owner = ADPB.team

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

eldorado_options_list = [("generationDate", "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}")]

# DAG definition
dimension_category_index_refresh = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=owner.alarm_channel,
    slack_tags=owner.sub_team,
    tags=[owner.jira_team],
    enable_slack_alert=True
)

generation_cluster = EmrClusterTask(
    name="DimensionCategoryIndexGenerationCluster",
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
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3
)

generation_step = EmrJobTask(
    cluster_specs=generation_cluster.cluster_specs,
    name="DimensionCategoryIndexGenerationStep",
    class_name="jobs.customerinsights.DimensionCategoryIndexGenerationJob",
    eldorado_config_option_pairs_list=eldorado_options_list,
    executable_path=jar_path
)
generation_cluster.add_sequential_body_task(generation_step)

check = OpTask(op=FinalDagStatusCheckOperator(dag=dimension_category_index_refresh.airflow_dag))

dimension_category_index_refresh >> generation_cluster >> check

dag = dimension_category_index_refresh.airflow_dag
