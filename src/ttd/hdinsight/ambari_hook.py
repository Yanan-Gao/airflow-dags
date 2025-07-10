import json
from typing import Optional, List, Dict, Tuple

import requests
from airflow.hooks.base import BaseHook
from requests import Session


class AmbariHook(BaseHook):

    def __init__(self, cluster_name: str, auth_info: Optional[Tuple[str, str]] = None, rest_conn_id: Optional[str] = None) -> None:
        if auth_info is None and rest_conn_id is None:
            raise ValueError("Either auth_info or rest_conn_id should be specified for AmbariHook")
        self.rest_conn_id = rest_conn_id
        self.auth_info = auth_info
        self.cluster_name = cluster_name
        self.base_uri = (f"https://{cluster_name}.azurehdinsight.net/api/v1/clusters/{cluster_name}")

        self.session = self.get_conn()
        super().__init__()

    def get_conn(self) -> Session:
        if self.auth_info is not None:
            auth_info = self.auth_info
        else:
            conn = self.get_connection(self.rest_conn_id)  # type: ignore
            auth_info = (conn.login, conn.password)

        session = requests.Session()
        session.auth = auth_info
        session.headers = {"X-Requested-By": auth_info[0]}
        return session

    def is_ready(self) -> bool:
        try:
            self.session.get(url=self.base_uri)
            return True
        except IOError as e:
            self.log.info("Ambari is not ready to accept requests, cannot connect")

        return False

    def get_headnodes(self) -> List[str]:
        hosts_endpoint = self.base_uri + "/hosts"
        hosts_info = self.session.get(url=hosts_endpoint)
        headnodes = [x["Hosts"]["host_name"] for x in hosts_info.json()["items"] if x["Hosts"]["host_name"].startswith("hn")]

        return headnodes

    def get_component_states(self, component_name: str) -> Dict[str, Dict[str, str]]:
        headnodes = self.get_headnodes()
        component_states = {}
        for headnode in headnodes:
            endpoint = (f"{self.base_uri}/hosts/{headnode}/host_components/{component_name}")
            component_meta = self.session.get(url=endpoint).json()["HostRoles"]
            component_states[headnode] = {
                "desired_state": component_meta["desired_state"],
                "state": component_meta["state"],
            }

        return component_states

    def start_host_component(self, hostname: str, component_name: str):
        self.log.info(f"Requesting to start {component_name} on {hostname}")

        endpoint = f"{self.base_uri}/hosts/{hostname}/host_components/{component_name}"
        state_data = {"HostRoles": {"state": "STARTED"}}
        response = self.session.put(url=endpoint, data=json.dumps(state_data))

        if response.status_code not in (200, 202):
            self.log.warning(
                f"Start request for {component_name} on {hostname} was not accepted",
                response.reason,
            )
        else:
            self.log.info(f"{component_name} start request was accepted")

    def check_and_start_component(self, component_name: str):
        component_states = self.get_component_states(component_name=component_name)
        for host, state_info in component_states.items():
            if state_info["desired_state"] == "STARTED" and state_info["state"] in (
                    "INIT",
                    "INSTALLED",
                    "INSTALL_FAILED",
            ):
                self.log.warning(f'{component_name} component is not in "STARTED" status')
                self.start_host_component(hostname=host, component_name=component_name)


class ClusterComponents:
    HDFS_CLIENT = "HDFS_CLIENT"
    LIVY2_SERVER = "LIVY2_SERVER"
    SPARK2_CLIENT = "SPARK2_CLIENT"
    SPARK2_JOBHISTORYSERVER = "SPARK2_JOBHISTORYSERVER"
    SPARK2_THRIFTSERVER = "SPARK2_THRIFTSERVER"
    YARN_CLIENT = "YARN_CLIENT"
