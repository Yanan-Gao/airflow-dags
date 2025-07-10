from airflow.plugins_manager import AirflowPlugin

import listeners.dag_healthiness_monitoring as dag_healthiness_monitoring


class ListenerPlugin(AirflowPlugin):
    name = "listener_plugin"
    listeners = [dag_healthiness_monitoring]
