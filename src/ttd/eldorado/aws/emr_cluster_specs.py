import logging

from typing import Optional, List, Tuple, Dict, Any

from airflow.models import Variable

from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.eldorado.emr_cluster_scaling_properties import EmrClusterScalingProperties

from ttd.eldorado.base import (
    ClusterSpecs,
)
from ttd.emr_version import EmrVersion
from ttd.ttdenv import TtdEnv, TtdEnvFactory

EMR_MIN_AUTO_TERMINATION_TIME: int = 60
EMR_MAX_AUTO_TERMINATION_TIME: int = 604800
EMR_MIN_AUTO_TERMINATION_SUPPORTED_VERSION = "emr-5.30.0"


class EmrClusterSpecs(ClusterSpecs):

    def __init__(
        self,
        cluster_name: str,
        environment: TtdEnv,
        core_fleet_instance_type_configs: EmrFleetInstanceTypes,
        task_fleet_instance_type_configs: Optional[EmrFleetInstanceTypes] = None,
    ):
        super(EmrClusterSpecs, self).__init__(cluster_name, environment)

        self.core_fleet_instance_type_configs = core_fleet_instance_type_configs
        self.task_fleet_instance_type_configs = task_fleet_instance_type_configs


def get_emr_cluster_roles(region: str, dag_id: str, team: str, environment: TtdEnv) -> Tuple[str, str]:

    def match_environment() -> str:
        match environment:
            case TtdEnvFactory.prod:
                return "prod"
            case TtdEnvFactory.dev:
                return "dev"
            case _:
                return "prodtest"

    emr_compute_role_template = "medivac-compute-{team}-{env}-useast"
    emr_service_role_template = "medivac-emr-service-{env}"

    # TODO remove when avails DAGs are stable in production
    allowed_list = ["avails-pipeline-aggregate-delta-table-cleanup", "avails-pipeline-hourly", "contributing-deal-avails-agg-hourly"]
    allowed_regions = ["us-east-1"]
    is_avails_dag = any(dag_id.startswith(prefix) for prefix in allowed_list)
    avails_medivac_off = Variable.get(
        "medivac_for_avails_is_off",
        deserialize_json=True,
        default_var=True,
    )
    # TODO remove when all existing DAGs are stable in production
    global_role_on = is_avails_dag and (avails_medivac_off or region not in allowed_regions)
    emr_compute_role = "DataPipelineDefaultResourceRole" if global_role_on else (
        emr_compute_role_template.format(env=match_environment(), team=team.lower())
    )
    emr_service_role = "DataPipelineDefaultRole" if global_role_on else (emr_service_role_template.format(env=match_environment()))
    return emr_compute_role, emr_service_role


def get_emr_fleet_spec(
    emr_release_label: str,
    name: str,
    log_uri: str,
    core_fleet_config: EmrFleetInstanceTypes,
    master_fleet_config: EmrFleetInstanceTypes,
    cluster_tags,
    ec2_subnet_ids,
    ec2_key_name: str,
    emr_managed_master_security_group: str,
    emr_managed_slave_security_group: str,
    service_access_security_group: Optional[str] = None,
    instance_configuration_spark_log4j=None,
    additional_application_configurations=None,
    additional_application: Optional[List[str]] = None,
    enable_debugging: bool = False,
    enable_s3_dist_cp: bool = False,
    use_on_demand_on_timeout: bool = False,
    timeout_in_minutes: int = 60,
    bootstrap_script_actions: Optional[List[ScriptBootstrapAction]] = None,
    cluster_auto_terminates: bool = False,
    cluster_auto_termination_idle_timeout_seconds: int = 20 * 60,
    task_fleet_config: EmrFleetInstanceTypes = None,
    managed_cluster_scaling_config: Optional[EmrClusterScalingProperties] = None,
    ebs_root_volume_size: Optional[int] = None,
) -> Dict[str, Any]:
    if bootstrap_script_actions is None:
        bootstrap_script_actions = []
    # turn the tags dictionary into the keyval format we need
    if cluster_tags is None:
        cluster_tags = {
            "Environment": TtdEnvFactory.get_from_system().execution_env,
            "process": "unknown",
            "Resource": "EMR",
            "Source": "Airflow",
        }
    cluster_tags_map = [{"Key": i, "Value": j} for (i, j) in cluster_tags.items()]
    """
    Get EMR workflow specification

    :param ec2_subnet_id: The subnet in a VPC. We recommend using these in vpc-4431e821 ("USEAST_PRIVATE"):
         subnet-09a5010d (us-east-1a); subnet-0bb63319 (us-east-1b);
         subnet-0e82171b (us-east-1d); subnet-0151af90 (us-east-1e).
    :type ec2_subnet_id: str
    """
    # instance configuration. For more info see https://docs.aws.amazon.com/emr/latest/APIReference/API_InstanceGroup.html

    fleet_core_type = {
        "InstanceFleetType": "CORE",
        "Name": "Core - 2",
        "TargetOnDemandCapacity": core_fleet_config.on_demand_weighted_capacity,
        "TargetSpotCapacity": core_fleet_config.spot_weighted_capacity,
        "InstanceTypeConfigs": core_fleet_config.get_fleet_instance_type_configs(EmrVersion(emr_release_label)),
        "LaunchSpecifications": core_fleet_config.get_launch_spec(use_on_demand_on_timeout, timeout_in_minutes)
    }

    # configure the fleet
    emr_fleet_spec: Dict[str, Any] = {
        "Name": name,
        "LogUri": log_uri,
        "ReleaseLabel": emr_release_label,
        "Applications": _get_applications(emr_release_label),
        "Instances": {
            "Ec2SubnetIds":
            ec2_subnet_ids,
            # The identifier of the Amazon EC2 security group for the master node.
            "EmrManagedMasterSecurityGroup":
            emr_managed_master_security_group,
            # The identifier of the Amazon EC2 security group for the core and task nodes.
            "EmrManagedSlaveSecurityGroup":
            emr_managed_slave_security_group,
            "InstanceFleets": [
                fleet_core_type,
                {
                    "InstanceFleetType": "MASTER",
                    "Name": "Master - 1",
                    "TargetOnDemandCapacity": master_fleet_config.on_demand_weighted_capacity,
                    "TargetSpotCapacity": master_fleet_config.spot_weighted_capacity,
                    "InstanceTypeConfigs": master_fleet_config.get_fleet_instance_type_configs(EmrVersion(emr_release_label)),
                },
            ],
            "TerminationProtected":
            False,
            "KeepJobFlowAliveWhenNoSteps":
            not cluster_auto_terminates,
        },
        "VisibleToAllUsers": True,
        # Tags are propagated to EC2 instances
        "Tags": cluster_tags_map,
        "Steps": [],
        "BootstrapActions": list(map(vars, bootstrap_script_actions)),
    }

    if additional_application is not None:
        for app_name in additional_application:
            app_enabled = any(application["Name"] == app_name for application in emr_fleet_spec["Applications"])
            if not app_enabled:
                emr_fleet_spec["Applications"].append({"Name": app_name})

    if ec2_key_name is not None:
        emr_fleet_spec["Instances"]["Ec2KeyName"] = ec2_key_name  # SSH key

    if service_access_security_group is not None:
        emr_fleet_spec["Instances"]["ServiceAccessSecurityGroup"] = service_access_security_group

    if task_fleet_config is not None:
        fleet_task_type = {
            "InstanceFleetType": "TASK",
            "Name": "Task - 3",
            "TargetOnDemandCapacity": task_fleet_config.on_demand_weighted_capacity,
            "TargetSpotCapacity": task_fleet_config.spot_weighted_capacity,
            "InstanceTypeConfigs": task_fleet_config.get_fleet_instance_type_configs(EmrVersion(emr_release_label)),
            "LaunchSpecifications": core_fleet_config.get_launch_spec(use_on_demand_on_timeout, timeout_in_minutes)
        }

        emr_fleet_spec["Instances"]["InstanceFleets"].append(fleet_task_type)

    if instance_configuration_spark_log4j and additional_application_configurations:
        log4j_configs = None
        for spark_configs_index, entry in enumerate(additional_application_configurations[:]):
            if entry['Classification'] == 'spark-log4j':
                log4j_configs = entry["Properties"]
                additional_application_configurations.pop(spark_configs_index)
                break
        if log4j_configs is not None:
            log4j_configs.update(instance_configuration_spark_log4j)
            instance_configuration_spark_log4j = log4j_configs

    if instance_configuration_spark_log4j:
        emr_fleet_spec["Configurations"] = [{
            "Classification": "spark-log4j",
            # Optional nested configurations
            # 'Configurations': {
            # },
            # A set of properties specified within a configuration classification.
            "Properties": instance_configuration_spark_log4j,
        }]

    if additional_application_configurations:
        if "Configurations" in emr_fleet_spec and emr_fleet_spec["Configurations"]:
            emr_fleet_spec["Configurations"].extend(additional_application_configurations)
        else:
            emr_fleet_spec["Configurations"] = additional_application_configurations

    if enable_debugging:
        # https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-debugging.html#emr-plan-debugging-logs-archive-debug
        emr_fleet_spec["Steps"].append({
            "Name": "Enable Debugging",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["state-pusher-script"],
            },
        })

    if enable_s3_dist_cp:
        # Requires the Hadoop application to run "s3-dist-cp"
        hadoop_enabled = any(application["Name"] == "Hadoop" for application in emr_fleet_spec["Applications"])
        if not hadoop_enabled:
            emr_fleet_spec["Applications"].append({"Name": "Hadoop"})

    if managed_cluster_scaling_config is not None:
        emr_fleet_spec["ManagedScalingPolicy"] = managed_cluster_scaling_config.get_config()

    if cluster_auto_termination_idle_timeout_seconds > 0:
        if emr_release_label < EMR_MIN_AUTO_TERMINATION_SUPPORTED_VERSION:
            logging.warning(
                f"Current EMR version is set to {emr_release_label} but " +
                "AutoTerminationPolicy is not supported by EMR version lower than " + f"{EMR_MIN_AUTO_TERMINATION_SUPPORTED_VERSION}." +
                "Auto Termination was not enabled."
            )
        else:
            if (cluster_auto_termination_idle_timeout_seconds < EMR_MIN_AUTO_TERMINATION_TIME
                    or cluster_auto_termination_idle_timeout_seconds > EMR_MAX_AUTO_TERMINATION_TIME):
                raise Exception("EMR Cluster Auto Termination Idle Timeout must be between 60 and 604800")

            emr_fleet_spec["AutoTerminationPolicy"] = {"IdleTimeout": cluster_auto_termination_idle_timeout_seconds}

    if ebs_root_volume_size is not None:
        emr_fleet_spec["EbsRootVolumeSize"] = ebs_root_volume_size

    # TODO: emulate Amazon Data Pipeline's useOnDemandOnLastAttempt?
    # https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-emrcluster.html

    # TODO: emulate Amazon Data Pipeline's terminateAfter? i.e. how many hours to wait after cluster is complete
    # or simply use existing KeepJobFlowAliveWhenNoSteps?
    # https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-emrcluster.html

    return emr_fleet_spec


def _get_version(version_str: str) -> tuple[int, ...]:
    return tuple(int(sub_version) for sub_version in version_str.split("-")[-1].split("."))


def _get_applications(emr_release_label: str) -> list[dict[str, str]]:
    # https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-ganglia.html
    # The last release of Amazon EMR to include Ganglia was Amazon EMR 6.15.0.
    # To monitor your cluster, releases higher than 6.15.0 include the Amazon CloudWatch agent.
    emr_version = _get_version(emr_release_label)
    if (4, 2, 0) <= emr_version <= (6, 15, 0):
        return [{"Name": "Spark"}, {"Name": "Ganglia"}]
    if emr_version >= (7, 0, 0):
        return [{"Name": "Spark"}, {"Name": "AmazonCloudWatchAgent"}, {"Name": "Hive"}]
    return [{"Name": "Spark"}]
