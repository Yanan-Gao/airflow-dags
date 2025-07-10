from airflow.utils.xcom import XCOM_RETURN_KEY


def get_push_xcom_task_id(name: str) -> str:
    """
    Get the task id for the push xcom task
    """
    return f"{name}_push_xcom"


def get_xcom_pull_jinja_string(task_ids: str, key: str = XCOM_RETURN_KEY) -> str:
    """
    Gets a jinja string for an xcom pull so we have less of these ugly strings!
    """
    # In order for jinja templating to work with f-string,
    # we need to use {{ to escape, so 4 {{{{ results in 2 {{
    return f"{{{{ task_instance.xcom_pull(task_ids='{task_ids}', key='{key}' )}}}}"
