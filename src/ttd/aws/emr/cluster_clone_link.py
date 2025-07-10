from airflow.models import BaseOperatorLink, BaseOperator, XCom
from airflow.models.taskinstancekey import TaskInstanceKey


class ClusterCloneLink(BaseOperatorLink):
    name = "Airflow Cluster Clone"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        job_flow_id = XCom.get_value(key="emr_cluster", ti_key=ti_key)["job_flow_id"]
        return f"/awsemrclusterclone/cluster?cluster_id={job_flow_id}"
