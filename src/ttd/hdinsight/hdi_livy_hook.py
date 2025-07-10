from typing import Optional, Any

from requests import Session

from ttd.livy_base_hook import LivyBaseHook


class HDILivyHook(LivyBaseHook):

    def __init__(self, cluster_name: str, livy_conn_id: Optional[str] = None):
        self.cluster_name = cluster_name
        super().__init__(livy_conn_id=livy_conn_id)

    def get_endpoint(self) -> str:
        endpoint = ("https://" + self.cluster_name + "-int.azurehdinsight.net/livy/batches")
        self.log.info(f"Livy endpoint for the current cluster: {endpoint}")
        return endpoint

    def get_conn(self, headers: Optional[dict[Any, Any]] = None) -> Session:
        login = self.get_connection(self.livy_conn_id).login
        _headers = {"X-Requested-By": login}
        if headers is not None:
            _headers.update(headers)
        session = super().get_conn(headers=_headers)
        return session
