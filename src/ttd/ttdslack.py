from typing import Optional, List
import urllib.parse

from airflow.hooks.base import BaseHook
from airflow.models import TaskInstance, DagRun
from airflow.operators.subdag import SubDagOperator
from pendulum import datetime

from slack import WebClient
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import TaskInstanceState
from airflow.operators.python import PythonOperator

from ttd.ttdenv import TtdEnvFactory

SLACK_CLIENT: Optional[WebClient] = None


def get_slack_client():
    global SLACK_CLIENT
    if not SLACK_CLIENT:
        api_token = BaseHook.get_connection("slack-emrbot").password
        SLACK_CLIENT = WebClient(token=api_token)
    return SLACK_CLIENT


def find_failed_tis(top_level_dag: DAG, top_level_dag_run: DagRun, execution_date: datetime) -> List[TaskInstance]:  # type: ignore

    def find_failed_tis_in_dag(dag: DAG, dag_run: DagRun, failed_tis: List[TaskInstance]) -> List[TaskInstance]:
        for ti in dag_run.get_task_instances(state=TaskInstanceState.FAILED):  # type: ignore
            task = dag.get_task(ti.task_id)
            if isinstance(task, SubDagOperator):
                subdag = task.subdag
                subdag_run = subdag.get_dagrun(execution_date)
                find_failed_tis_in_dag(subdag, subdag_run, failed_tis)  # type: ignore
            else:
                failed_tis.append(ti)
        return failed_tis

    failed_tis: List[TaskInstance] = []
    return find_failed_tis_in_dag(top_level_dag, top_level_dag_run, failed_tis)


def dag_post_to_slack_callback(
    dag_name: str,
    step_name: str,
    slack_channel: Optional[str],
    message: Optional[str] = None,
    tsg: Optional[str] = None,
    slack_tags: Optional[str] = None,
    enable_slack_alert: bool = True,
    only_for_prod: bool = True,
):
    """
    Returns callback method to post message to Slack upon failure of DAG
    :param dag_name:
    :param step_name:
    :param slack_channel:
    :param message:
    :param tsg:
    :param slack_tags:
    :param enable_slack_alert:
    :param only_for_prod:
    :return:
    """

    def post_callback(context):
        if slack_channel is None:
            return
        env = TtdEnvFactory.get_from_system()
        if not enable_slack_alert or only_for_prod and env != TtdEnvFactory.prod:
            return

        base_url = context["var"]["value"].get("BASE_URL")
        dag_id = context["dag"].dag_id
        data_interval_start = context["data_interval_start"]
        formatted_dag_run_id = urllib.parse.quote_plus(context["run_id"])

        failed_tis = find_failed_tis(context["dag"], context["dag_run"], data_interval_start)

        init_text = f""":airflow-tire-fire: Airflow DAG `{dag_id}` has *failed* in `{env.execution_env}`! \n\nDetails in :thread:"""

        init_message = get_slack_client().chat_postMessage(channel=slack_channel, text=init_text)
        thread_id = init_message["message"]["ts"]

        thread_text = f"""*Execution date:* {data_interval_start.to_datetime_string()}\n\n*Environment:* `{env.execution_env}`\n\n\n"""

        if len(failed_tis) == 0:
            thread_text += "*No failed tasks found!* :cry:\n"
        else:
            thread_text += "*Failed tasks:*\n"
        for ti in failed_tis:
            thread_text += f"• `{ti.task_id}`: <{base_url}/dags/{ti.dag_id}/grid?tab=logs&dag_run_id={formatted_dag_run_id}&task_id={ti.task_id}|Logs>\n\n"

        thread_text += f"\n<{base_url}/dags/{dag_id}/grid?dag_run_id={formatted_dag_run_id}&tab=graph|*Graph view*>\n\n"
        thread_text += f"<{base_url}/dags/{dag_id}/grid?dag_run_id={formatted_dag_run_id}|*Grid view*>\n\n"
        if tsg is not None:
            thread_text += f"<{tsg}|*TSG*>\n"
        if slack_tags is not None:
            thread_text += f"*Tags:* {slack_tags}\n"
        if message is not None:
            thread_text += f"*Message:* {message}\n"

        thread_text += f"*Step:* `{step_name}`\n"

        get_slack_client().chat_postMessage(channel=slack_channel, text=thread_text, thread_ts=thread_id)

    return post_callback


def post_to_slack_callback(slack_channel: str, message: str):

    def post_callback(context):
        get_slack_client().chat_postMessage(channel=slack_channel, text=message)

    return post_callback


def post_to_slack_operator(
    dag: DAG,
    slack_channel: str,
    message: str,
    trigger_rule: TriggerRule.ALL_SUCCESS,  # type: ignore
    task_id: str = None,
):
    if not task_id:
        task_id = f"Post To {slack_channel}"
    return PythonOperator(
        task_id=task_id,
        provide_context=True,
        python_callable=post_to_slack_callback(slack_channel, message),
        dag=dag,
        trigger_rule=trigger_rule,
    )
