import functools
import json
from abc import abstractmethod
from typing import List, Optional

from airflow.hooks.base import BaseHook
from azure.mgmt.hdinsight.models import ScriptAction

from ttd.eldorado.hdiversion import HDIVersion


class HdiScriptActionSpecBase:

    @abstractmethod
    def to_azure_script_action(self) -> ScriptAction:
        pass


class HdiScriptActionSpec(HdiScriptActionSpecBase):

    def __init__(
        self,
        action_name: str,
        script_uri: str,
        parameters: List[str],
        sas_params_string: Optional[str] = None,
    ):
        self.sas_params_string = sas_params_string
        self.parameters = parameters
        self.script_uri = script_uri
        self.action_name = action_name

    @functools.cache
    def to_azure_script_action(self) -> ScriptAction:
        from typing import cast

        sas_password = BaseHook.get_connection("azure_ttd_build_artefacts_sas").get_password()
        default_sas_params = cast(str, sas_password)

        parameters = " ".join(self.parameters)

        # hardcoded templating as Jinja won't work just like that without extra arguments and stuff,
        # however it is subject for refactoring
        parameters = parameters.replace("{{conn.azure_ttd_build_artefacts_sas.get_password()}}", default_sas_params)

        _script_action = ScriptAction(
            name=self.action_name,
            uri=f"{self.script_uri}?{self.sas_params_string or default_sas_params}",
            parameters=parameters,
        )
        return _script_action


ScriptActionSpec = HdiScriptActionSpec


class HdiVaultScriptActionSpec(HdiScriptActionSpecBase):

    def __init__(self, cluster_version: HDIVersion, artefacts_storage_account: str):
        self.artefacts_storage_account = artefacts_storage_account
        self.cluster_version = cluster_version

    def to_azure_script_action(self) -> ScriptAction:
        core_version = "v1-spark-3.2.1"
        script_uri = \
            (f'https://{self.artefacts_storage_account}/ttd-build-artefacts/eldorado-core/release/{core_version}'
             f'/latest/azure-scripts/send_azure_vault_certificate.sh')

        azure_vault_conn = BaseHook.get_connection('azure_vault_sp')

        client_id = azure_vault_conn.login
        cert_encoded = azure_vault_conn.get_password()
        extra = json.loads(azure_vault_conn.get_extra())
        sp_name = extra['SP_NAME']
        tenant_id = extra['TENANT_ID']

        action_name = 'script-eldorado-v2-api'
        parameters = [sp_name, client_id, tenant_id, cert_encoded]

        default_sas_params = BaseHook.get_connection("azure_ttd_build_artefacts_sas").get_password()

        script_action = ScriptAction(
            name=action_name,
            uri=f"{script_uri}?{default_sas_params}",
            parameters=" ".join(parameters),
        )
        return script_action
