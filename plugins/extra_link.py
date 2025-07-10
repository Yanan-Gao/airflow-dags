from airflow.plugins_manager import AirflowPlugin

from ttd.hdinsight.hdi_create_cluster_operator import HDILink, HDILogContainer


class AirflowExtraLinkPlugin(AirflowPlugin):
    name = "extra_link_plugin"
    operator_extra_links = [
        HDILink(),
        HDILogContainer(),
    ]
