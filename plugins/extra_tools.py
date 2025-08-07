from airflow.plugins_manager import AirflowPlugin
from ttd.aws.emr.cluster_clone_link import ClusterCloneLink
from plugins.aws_cluster_cloning import cluster_clone_bp, AwsEmrClusterClone
from plugins.aws_cluster_logs import AwsEmrClusterLogs, cluster_logs_bp
from ttd.aws.emr.cluster_logs_link import ClusterLogsLink, EmrLogsLink


class ExtraTools(AirflowPlugin):
    """Defining the plugin class"""

    name = "Extra Tools"
    flask_blueprints = [cluster_clone_bp, cluster_logs_bp]
    appbuilder_views = [{
        "name": "AWS EMR Cluster Cloning",
        "category": "Extra Tools",
        "view": AwsEmrClusterClone()
    }, {
        "name": "AWS EMR Cluster Logs",
        "category": "Extra Tools",
        "view": AwsEmrClusterLogs()
    }]
    operator_extra_links = [ClusterCloneLink(), ClusterLogsLink(), EmrLogsLink()]
