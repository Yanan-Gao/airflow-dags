from ttd.ttdenv import TtdEnvFactory


def xcom_pull_str(dag_id: str, task_id: str, key: str):
    return f"{{{{task_instance.xcom_pull(dag_id='{dag_id}', task_ids='{task_id}', key='{key}')}}}}"


def choose(prod, test):
    return prod if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else test
