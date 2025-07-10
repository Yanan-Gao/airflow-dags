import logging
import os
import re
from dataclasses import dataclass
from typing import List, Any, Dict, Set
from urllib.parse import urlparse

from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.pydantic import BaseModel
from cachetools import cached

from ttd.ec2.ec2_subnet import DevComputeAccount
from ttd.monads.maybe import Just
from ttd.semver import SemverVersion

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def convert_ebs_config(x: List[dict], ebs_optimized: bool):
    return {"EbsBlockDeviceConfigs": x, "EbsOptimized": ebs_optimized}


def convert_instance_type_spec(x: dict):
    z = {
        "InstanceType": x["InstanceType"],
        "WeightedCapacity": x["WeightedCapacity"],
        "BidPriceAsPercentageOfOnDemandPrice": x["BidPriceAsPercentageOfOnDemandPrice"],
    }

    if "Priority" in x:
        z.update({"Priority": x["Priority"]})

    if "EbsBlockDevices" in x:
        ebs_config = convert_ebs_config(x["EbsBlockDevices"], x["EbsOptimized"])
        z.update([("EbsConfiguration", ebs_config)])

    return z


def convert_instance_type_specs(x: list[Any]):
    z = [convert_instance_type_spec(i) for i in x]
    return z


def convert_instance_fleet(x):
    z = {
        "Name": x["Name"],
        "InstanceFleetType": x["InstanceFleetType"],
        "TargetOnDemandCapacity": x["TargetOnDemandCapacity"],
        "TargetSpotCapacity": x["TargetSpotCapacity"],
        "InstanceTypeConfigs": convert_instance_type_specs(x["InstanceTypeSpecifications"]),
    }

    if "LaunchSpecifications" in x:
        z.update([("LaunchSpecifications", x["LaunchSpecifications"])])
    if "ResizeSpecifications" in x:
        z.update([("ResizeSpecifications", x["ResizeSpecifications"])])

    return z


def convert_instance_fleets(x):
    z = [convert_instance_fleet(i) for i in x["InstanceFleets"]]
    return z


def convert_bootstrap_action(x):
    z = {
        "Name": x["Name"],
        "ScriptBootstrapAction": {
            "Path": x["ScriptPath"],
            "Args": x["Args"],
        },
    }
    return z


def convert_bootstrap_actions(x):
    z = [convert_bootstrap_action(i) for i in x]
    return z


def map_service_role_to_dev(role: str) -> str:
    return "medivac-emr-service-dev"


def map_instance_profile_to_dev(instance_profile: str) -> str:
    instance_profile_regex = re.compile(r"^medivac-compute-(?P<team_name>[a-z0-9]+)-(?P<comp_env>[a-zA-Z]+)-useast$")
    m = instance_profile_regex.match(instance_profile)
    return f"medivac-compute-{m.group('team_name')}-dev-useast"  # type: ignore


def convert_applications(apps: List[dict]) -> List[dict[str, Any]]:
    z = [{"Name": i["Name"]} for i in apps]
    return z


def get_dev_subnets():
    return DevComputeAccount.default_vpc.all()


def convert_tags(tags: List[Dict[str, str]], username: str) -> List[dict]:
    return ([item for item in tags if item["Key"] not in {"Creator", "Environment"}] + [{
        "Key": "Creator",
        "Value": username
    }, {
        "Key": "Environment",
        "Value": "dev"
    }])


def map_ttdenv_in_args(args: List[str]):
    return [(re.sub(r"-Dttd\.env=([a-zA-Z]+)", "-Dttd.env=prodTest", arg) if arg.find("-Dttd.env=") != -1 else arg) for arg in args]


def covert_step(step: dict[str, Any]) -> dict[str, Any]:
    z = {
        "Name": step["Name"],
        "ActionOnFailure": step["ActionOnFailure"],
        "HadoopJarStep": {
            "Properties": [],
            "Jar": step["Config"]["Jar"],
            # 'MainClass': 'string',
            "Args": map_ttdenv_in_args(step["Config"]["Args"]),
        },
    }
    return z


def convert_steps(steps: List[dict[str, Any]]) -> List[dict[str, Any]]:
    if steps is None:
        return []
    return [covert_step(i) for i in steps]


def create_job_flow_config_from_cluster_specs(
    cluster_desc, termination_policy, bootstrap_actions, instance_fleets, steps, username
) -> dict:
    job_flow_config = {
        "Name": cluster_desc["Name"] + " :: Test",
        "ReleaseLabel": cluster_desc["ReleaseLabel"],
        # "Steps": convert_steps(steps["Steps"]) if steps is not None else [],  # type: ignore
        "Steps": steps,
        "VisibleToAllUsers": True,
        "Applications": convert_applications(cluster_desc["Applications"]),
        "Tags": convert_tags(cluster_desc["Tags"], username),
        "ServiceRole": map_service_role_to_dev(cluster_desc["ServiceRole"]),
        "Configurations": cluster_desc["Configurations"],
        "JobFlowRole": map_instance_profile_to_dev(cluster_desc["Ec2InstanceAttributes"]["IamInstanceProfile"]),
        "SecurityConfiguration": "",
        # 'AutoScalingRole': '',
        "EbsRootVolumeSize": 15,
        "StepConcurrencyLevel": 1,
        "AutoTerminationPolicy": termination_policy["AutoTerminationPolicy"],
        "BootstrapActions": convert_bootstrap_actions(bootstrap_actions["BootstrapActions"]),
        "Instances": {
            "InstanceFleets":
            convert_instance_fleets(instance_fleets),
            "Ec2KeyName":
            "ttd.developer.dev.use1",
            "Placement": {
                # 'AvailabilityZone': cluster_desc['Ec2InstanceAttributes']['Ec2AvailabilityZone'],
                # 'AvailabilityZones': cluster_desc['Ec2InstanceAttributes']['RequestedEc2AvailabilityZones']
            },
            # 'KeepJobFlowAliveWhenNoSteps': True | False,
            "TerminationProtected":
            cluster_desc["TerminationProtected"],
            "UnhealthyNodeReplacement":
            cluster_desc["UnhealthyNodeReplacement"],
            # 'HadoopVersion': 'string',
            # 'Ec2SubnetId': cluster_desc['Ec2InstanceAttributes']['Ec2SubnetId'],
            "Ec2SubnetIds":
            get_dev_subnets(),  # cluster_desc['Ec2InstanceAttributes']['RequestedEc2SubnetIds'],
            "EmrManagedMasterSecurityGroup":
            DevComputeAccount.default_vpc.security_groups.emr_managed_master_sg.id,
            # cluster_desc['Ec2InstanceAttributes']['EmrManagedMasterSecurityGroup'],
            "EmrManagedSlaveSecurityGroup":
            DevComputeAccount.default_vpc.security_groups.emr_managed_slave_sg.id,
            # cluster_desc['Ec2InstanceAttributes']['EmrManagedSlaveSecurityGroup'],
            "ServiceAccessSecurityGroup":
            DevComputeAccount.default_vpc.security_groups.service_access_sg.map(lambda sg: sg.id).or_else(lambda: Just(None)).get(),
            # 'AdditionalMasterSecurityGroups': [
            #     'string',
            # ],
            # 'AdditionalSlaveSecurityGroups': [
            #     'string',
            # ]
        },
    }

    return job_flow_config


def get_cluster_desc(cluster_id: str) -> Dict[str, Any]:
    from ttd.secrets import _WEB_CONTEXT

    # import boto3
    _WEB_CONTEXT.append("cluster_cloning")

    client_prod = EmrHook(aws_conn_id="aws_default").conn

    cluster_desc = client_prod.describe_cluster(ClusterId=cluster_id)["Cluster"]

    return cluster_desc


@dataclass
class EmrVersion:
    emr_release_label: str
    spark_version: SemverVersion


aws_emr_versions_cache: Any = {}


# @lru_cache(None, True)
@cached(aws_emr_versions_cache)
def get_aws_emr_versions() -> List[EmrVersion]:
    from ttd.secrets import _WEB_CONTEXT

    _WEB_CONTEXT.append("cluster_cloning")

    client_prod = EmrHook(aws_conn_id="aws_default").conn
    release_labels = client_prod.list_release_labels(MaxResults=1000)

    emr_versions = []
    release_label: str
    for release_label in release_labels["ReleaseLabels"]:
        release_label_desc = client_prod.describe_release_label(ReleaseLabel=release_label)
        if release_label_desc["ReleaseLabel"].startswith("emr-5.") or release_label_desc["ReleaseLabel"].startswith("emr-4.") or \
                release_label_desc["ReleaseLabel"].startswith("emr-6.5.") or release_label_desc["ReleaseLabel"].startswith("emr-6.4.") or \
                release_label_desc["ReleaseLabel"].startswith("emr-6.3.") or release_label_desc["ReleaseLabel"].startswith("emr-6.2.") or \
                release_label_desc["ReleaseLabel"].startswith("emr-6.1.") or release_label_desc["ReleaseLabel"].startswith("emr-6.0."):
            continue
        apps: List[Dict[str, str]] = release_label_desc["Applications"]
        spark_version = next((x["Version"] for x in apps if x["Name"] == "Spark"), None)
        emr_versions.append(EmrVersion(release_label_desc["ReleaseLabel"], SemverVersion.from_string(spark_version)))
    return emr_versions


def get_converted_steps(cluster_id: str) -> list[str]:
    from ttd.secrets import _WEB_CONTEXT
    import json

    # import boto3
    _WEB_CONTEXT.append("cluster_cloning")

    client_prod = EmrHook(aws_conn_id="aws_default").conn
    source_steps = client_prod.list_steps(ClusterId=cluster_id)["Steps"]

    steps = [json.dumps(step, default=str, indent=2) for step in convert_steps(source_steps)]

    return steps


def copy_cluster(cluster_id: str, steps: list[str], username: str) -> str:
    from ttd.secrets import _WEB_CONTEXT
    import json

    # import boto3
    _WEB_CONTEXT.append("cluster_cloning")
    result = ""
    try:
        # session_prod = boto3.session.Session(profile_name='003576902480:scrum-dataproc-prod-admin')
        # client_prod = session_prod.client(service_name='emr', region_name="us-east-1")
        client_prod = EmrHook(aws_conn_id="aws_default").conn

        cluster_desc = client_prod.describe_cluster(ClusterId=cluster_id)["Cluster"]
        # steps = client_prod.list_steps(ClusterId=cluster_id) if include_steps else None
        instance_fleets = client_prod.list_instance_fleets(ClusterId=cluster_id)
        bootstrap_actions = client_prod.list_bootstrap_actions(ClusterId=cluster_id)
        termination_policy = client_prod.get_auto_termination_policy(ClusterId=cluster_id)

        steps_obj = [json.loads(step) for step in steps]

        #
        # import json
        # print(json.dumps(cluster_desc, default=str, indent=2), file=sys.stdout)
        # print(json.dumps(bootstrap_actions, default=str, indent=2), file=sys.stdout)
        # print(json.dumps(instance_fleets, default=str, indent=2), file=sys.stdout)
        # print(json.dumps(termination_policy, default=str, indent=2), file=sys.stdout)
        # print(json.dumps(steps, default=str, indent=2), file=sys.stdout)

        job_flow_config = create_job_flow_config_from_cluster_specs(
            cluster_desc,
            termination_policy,
            bootstrap_actions,
            instance_fleets,
            steps_obj,
            username,
        )

        import json

        print(json.dumps(job_flow_config, default=str, indent=2))

        # session_dev = boto3.session.Session(profile_name='503911722519:scrum-dataproc-default')
        # client_dev = session_dev.client(service_name='emr', region_name="us-east-1")
        client_dev = EmrHook(aws_conn_id="aws_dev").conn
        result = client_dev.run_job_flow(**job_flow_config)
    finally:
        _WEB_CONTEXT.remove("cluster_cloning")

    return result


class LogFile(BaseModel):
    file_name: str
    file_path: str  # unique to each log file

    def __eq__(self, other):
        return self.file_path == other.file_path

    def __hash__(self):
        return hash(self.file_path)


class Container(BaseModel):
    name: str

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)


class Application(BaseModel):
    name: str

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)


class ClusterLogMetadata(BaseModel):
    bucket: str
    app_container_logs: Dict[Application, Dict[Container, Set[LogFile]]] = dict()

    def __eq__(self, other):
        return self.bucket == other.bucket and self.app_container_logs == other.app_container_logs


def get_root_directory():
    """
    Returns the root directory of the filesystem.
    """
    current_dir = os.getcwd()
    while True:
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            return current_dir
        current_dir = parent_dir


def extract_cluster_log_metadata(bucket: str, file_key_dicts: List[Dict[str, str]]) -> ClusterLogMetadata:
    cluster_log_metadata = ClusterLogMetadata(bucket=bucket)
    files_of_interest = ["stderr.gz", "stdout.gz"]
    for file_key_dict in file_key_dicts:
        # file keys are in this format: emr-logs/j-1NAOJBVBZSV3B/containers/application_1745297292664_0001/container_1745297292664_0001_01_000001/directory.info.gz
        file_key = file_key_dict["Key"]
        broken_key = file_key.split("/")
        try:
            application = broken_key[-3]
            container = broken_key[-2]
            file_name = broken_key[-1]
            # only collecting driver logs right now -- this may change in the future
            if container.endswith("000001") and file_name in files_of_interest:
                add_log_into_cluster_log_metadata(
                    cluster_log_metadata=cluster_log_metadata,
                    application_name=application,
                    container_name=container,
                    file_name=file_name,
                    file_path=file_key,
                )
        except Exception as e:
            logger.info(f"Exception {e} occurred while attempting to extract driver log paths.")
            raise e
    return cluster_log_metadata


def add_log_into_cluster_log_metadata(
    cluster_log_metadata: ClusterLogMetadata,
    application_name: str,
    container_name: str,
    file_name: str,
    file_path: str,
):
    new_log = LogFile(file_name=file_name, file_path=file_path)
    app = Application(name=application_name)
    if app in cluster_log_metadata.app_container_logs.keys():
        containers: Dict[Container, Set[LogFile]] = cluster_log_metadata.app_container_logs[app]
    else:
        containers = dict()
    container = Container(name=container_name)
    if container in containers.keys():
        containers[container].add(new_log)
    else:
        containers[container] = {new_log}

    cluster_log_metadata.app_container_logs[app] = containers


def fetch_cluster_logs(cluster_id: str, region_name: str) -> ClusterLogMetadata:
    try:
        emr_client = EmrHook(aws_conn_id="aws_default", region_name=region_name).conn
        cluster_desc = emr_client.describe_cluster(ClusterId=cluster_id)["Cluster"]
        parsed = urlparse(cluster_desc["LogUri"])

        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        key += cluster_id

        s3_hook = S3Hook(aws_conn_id="aws_default").conn
        dir_keys = s3_hook.list_objects_v2(Bucket=bucket, Prefix=key)["Contents"]
        return extract_cluster_log_metadata(bucket=bucket, file_key_dicts=dir_keys)

    except Exception as e:
        logger.info(f"Exception {e} occurred while attempting to fetch cluster logs.")
        raise e
