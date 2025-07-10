from datetime import timedelta
from typing import Tuple, List, Any, Optional, Sequence

from dags.tv.constants import FORECAST_JAR_PATH
from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.slack.slack_groups import PFX

# Allows easy testing that the DAG is correct (clusters are spun up with a single
# machine and the Spark jobs exit immediately when the -DdryRun parameter is
# passed to them.
is_dry_run = False

prometheus_monitoring = True
class_namespace = "com.thetradedesk.etlforecastjobs.upstreamforecasting.datapreparation"


def create_cpm_cluster(
    emr_version: str,
    name: str,
    num_xls: int,
    master_config: List[EmrInstanceType],
    instance_config: List[EmrInstanceType],
):
    global is_dry_run
    if is_dry_run:
        num_xls = 1
    return EmrClusterTask(
        emr_release_label=emr_version,
        name=name,
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=master_config,
            on_demand_weighted_capacity=1,
        ),
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(instance_types=instance_config, on_demand_weighted_capacity=num_xls),
        enable_prometheus_monitoring=prometheus_monitoring,
        cluster_auto_terminates=True,
        cluster_tags={"Team": PFX.team.jira_team},
        additional_application_configurations=[{
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            },
        }]
    )


def create_cpm_step(
    cluster: EmrClusterTask,
    class_name: str,
    timeout_timedelta: timedelta,
    job_params: List[Tuple[str, Any]],
    additional_args_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
    emr_step_name: str = None,
):
    global is_dry_run
    emr_step_name = f"{emr_step_name}" if emr_step_name else class_name
    if is_dry_run:
        job_params.append(("dryRun", "true"))

    return EmrJobTask(
        name=emr_step_name,
        cluster_specs=cluster.cluster_specs,
        configure_cluster_automatically=False,
        class_name=f"{class_namespace}.{class_name}",
        executable_path=FORECAST_JAR_PATH,
        timeout_timedelta=timeout_timedelta,
        eldorado_config_option_pairs_list=job_params,
        additional_args_option_pairs_list=additional_args_option_pairs_list,
    )


def xcom_template_to_get_value(dag_id: str, gen_dates_op_id: str, key: str) -> str:
    return (f"{{{{ "
            f'task_instance.xcom_pull(dag_id="{dag_id}", '
            f'task_ids="{gen_dates_op_id}", '
            f'key="{key}") '
            f"}}}}")
