import json
import inspect
import logging
from ttd.ttdenv import TtdEnvFactory
from airflow.models import Variable

is_prodtest = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest else False
config_variable_name = 'data-governance-dsr-delete-configuration'
parquet_delete_operations_key = 'parquet_delete_operations'
dag_delete_operations_key = 'dag_operations'


def _get_dag_config():
    try:
        return json.loads(Variable.get(config_variable_name, default_var='{}'))
    except json.decoder.JSONDecodeError:
        raise Exception(f"Unable to decode variable '{config_variable_name}'")


def get_parquet_operations():
    dag_config = _get_dag_config()
    return dag_config.get(parquet_delete_operations_key, [])


def get_configured_dag_tasks():
    dag_config = _get_dag_config()
    return dag_config.get(dag_delete_operations_key, [])


def get_prodtest_uiids():
    dag_config = _get_dag_config()
    uid2s = dag_config.get('uid2s', [])
    euids = dag_config.get('euids', [])
    tdids = dag_config.get('tdids', [])

    ids = euids + uid2s + tdids
    return ','.join(ids)


# Use inspect to get the name of the function that called this one. Then, look in the configured DAGs
# to see if the function name is listed there. That will determine whether we allow the task to run.
def is_dag_operation_enabled() -> bool:
    frame = inspect.currentframe()
    if not frame or not frame.f_back:
        return False

    if frame.f_back.f_code.co_name in get_configured_dag_tasks():
        return True
    logging.info(f'{frame.f_back.f_code.co_name} not found in configured tasks')
    return False
