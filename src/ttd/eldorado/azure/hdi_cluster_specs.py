from ttd.azure_vm.hdi_instance_types import HDIInstanceType
from ttd.eldorado.base import ClusterSpecs
from ttd.ttdenv import TtdEnv


class HdiClusterSpecs(ClusterSpecs):

    def __init__(
        self, cluster_task_id: str, cluster_name: str, rest_conn_id: str, worker_instance_type: HDIInstanceType, worker_instance_num: int,
        storage_account: str, environment: TtdEnv
    ):
        super(HdiClusterSpecs, self).__init__(cluster_name, environment)

        self.full_cluster_name = "{{ task_instance.xcom_pull(task_ids='" + cluster_task_id + "', key='return_value') }}"
        self.rest_conn_id = rest_conn_id
        self.worker_instance_type = worker_instance_type
        self.worker_instance_num = worker_instance_num
        self.artefacts_storage_account = storage_account
        self.environment = environment
