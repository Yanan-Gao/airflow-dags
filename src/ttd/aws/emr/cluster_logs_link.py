from airflow.models import BaseOperatorLink, BaseOperator, XCom
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.amazon.aws.links.base_aws import BaseAwsLink, BASE_AWS_CONSOLE_LINK


class EmrLogsLink(BaseAwsLink):
    """Helper class for constructing Amazon EMR Logs Link."""

    name = "S3 EMR Cluster Logs"
    key = "emr_logs"
    format_str = BASE_AWS_CONSOLE_LINK + "/s3/buckets/{log_uri}{job_flow_id}/?region={region_name}"

    def format_link(self, **kwargs) -> str:
        if not kwargs.get("log_uri"):
            return ""
        return super().format_link(**kwargs)


class ClusterLogsLink(BaseOperatorLink):
    name = "Airflow EMR Cluster Logs"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        job_flow_id = XCom.get_value(key="emr_cluster", ti_key=ti_key)["job_flow_id"]
        region = XCom.get_value(key="emr_cluster", ti_key=ti_key)["region_name"]
        return f"/awsemrclusterlogs/log_search?cluster_id={job_flow_id}&chosen_region={region}"
