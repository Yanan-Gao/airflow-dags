import re
from typing import List, Dict, Optional

from ttd.cloud_provider import CloudProvider, CloudProviders

from ttd.ttdenv import TtdEnvFactory, ProdTestEnv

from os import environ


class ClusterTagError(Exception):
    pass


TAG_INVALID_CHARS_REGEX = r"[^\w\s_.:/=+\-@]"


def get_raw_cluster_tags(cluster_tags: List[Dict[str, str]], cloud_provider: CloudProvider = CloudProviders.aws) -> Dict[str, str]:
    return {cluster_tag["TagKey"]: cluster_tag["TagValue"] for cluster_tag in cluster_tags} if cloud_provider == CloudProviders.ali \
        else {cluster_tag["Key"]: cluster_tag["Value"] for cluster_tag in cluster_tags}


def replace_tag_invalid_chars(tag: str) -> str:
    return re.sub(TAG_INVALID_CHARS_REGEX, "_", tag)


def get_creator(creator_env_var_nm: str = "AIRFLOW_VAR_USER_LOGIN") -> Optional[str]:
    env = TtdEnvFactory.get_from_system()
    if isinstance(env, ProdTestEnv):
        if creator_env_var_nm not in environ:
            raise ClusterTagError(f"Creator not specified through env variable {creator_env_var_nm} in prodtest environment")
        return environ[creator_env_var_nm]
    return None


def add_creator_tag(tags: Optional[Dict[str, str]],
                    creator_env_var_nm: str = "AIRFLOW_VAR_USER_LOGIN",
                    tag_name: str = "Creator") -> Optional[Dict[str, str]]:
    creator = get_creator(creator_env_var_nm)
    if creator is not None:
        return {**(tags or {}), tag_name: environ[creator_env_var_nm]}
    return tags


def add_task_tag(cluster_tags: List[Dict[str, str]], task_name: str) -> None:
    tasks_count = 0
    tasks_tag = next((tag for tag in cluster_tags if tag["Key"] == "Tasks"), None)
    if tasks_tag is not None:
        tasks_count = int(tasks_tag["Value"]) + 1
        tasks_tag["Value"] = f"{tasks_count}"
    else:
        tasks_count += 1
        cluster_tags.append({"Key": "Tasks", "Value": f"{tasks_count}"})

    cluster_tags.append({"Key": f"Task{tasks_count}", "Value": task_name})
