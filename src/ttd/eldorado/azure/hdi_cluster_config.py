from dataclasses import dataclass


@dataclass
class HdiClusterConfig:
    resource_group: str
    virtual_network_profile_id: str
    virtual_network_profile_subnet: str
    logs_storage_account: str
    logs_storage_account_resource_id: str
    artefacts_storage_account: str
    msi_resource_id: str
    rest_conn_id: str
    ssh_conn_id: str
