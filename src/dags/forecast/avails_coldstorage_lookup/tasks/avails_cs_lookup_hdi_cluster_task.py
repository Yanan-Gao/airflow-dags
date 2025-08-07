from dags.forecast.avails_coldstorage_lookup.constants import SHORT_DAG_NAME
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.el_dorado.v2.hdi import HDIClusterTask
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.hdinsight.script_action_spec import HdiScriptActionSpec
from ttd.slack.slack_groups import FORECAST

_VM_CONFIG = HDIVMConfig(
    headnode_type=HDIInstanceTypes.Standard_E8_v3(),
    workernode_type=HDIInstanceTypes.Standard_D32A_v4(),
    num_workernode=10,
    disks_per_node=0,
)
_CLUSTER_TAGS = {
    'Team': FORECAST.team.jira_team,
}


class AvailsCsIdsLookupHdiClusterTask(HDIClusterTask):
    """
    Class to encapsulate the HDIClusterTask creation for Avails ColdStorage Sync.

    More info: https://atlassian.thetradedesk.com/confluence/display/EN/Universal+Forecasting+Walmart+Data+Sovereignty
    """

    def __init__(self):
        super().__init__(
            name=SHORT_DAG_NAME + '-azure-cluster',
            vm_config=_VM_CONFIG,
            cluster_tags=_CLUSTER_TAGS,
            cluster_version=HDIClusterVersions.AzureHdiSpark33,
            extra_script_actions=self._build_extra_script_actions(),  # type: ignore
            enable_openlineage=False
        )

    @staticmethod
    def _build_extra_script_actions():
        # Hacky way to get the password
        # See this https://gitlab.adsrvr.org/thetradedesk/teams/dataproc/airflow-dags/-/blame/main-airflow-2/src/ttd/hdinsight/script_action_spec.py#L42
        sas = "{{conn.azure_ttd_build_artefacts_sas.get_password()}}"
        url = 'https://ttdartefacts.blob.core.windows.net/ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/azure-scripts/ttd-internal-root-ca-truststore.jks'
        full_url = f"'{url}?{sas}'"
        script_uri = 'https://ttdartefacts.blob.core.windows.net/ttd-build-artefacts/eldorado-core/release/v1-spark-3.2.1/latest/azure-scripts/download_from_url.sh'
        action_name = 'download_public_certs'
        destination_path = '/tmp/ttd-internal-root-ca-truststore.jks'

        return [HdiScriptActionSpec(action_name=action_name, script_uri=script_uri, parameters=[full_url, destination_path])]
