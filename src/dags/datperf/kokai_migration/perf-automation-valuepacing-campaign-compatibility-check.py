"""
Runs sprocs in provisioning to disable VP on any campaigns that become ineligible

Job Details:
    - Runs every 15 minutes
    - Can retry once after 5 minutes
    - Terminate in 1 hour if not finished
    - Expected to not take much time to run
    - Can only run one job at a time
"""
import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mssql_hook import MsSqlHook
from ttd.ttdenv import TtdEnvFactory
from ttd.ttdslack import dag_post_to_slack_callback, get_slack_client

dag_name = 'perf-automation-valuepacing-campaign-compatibility-check'
job_slack_channel = '#dev-distributed-algos-rollout-alarms'

job_schedule_interval = timedelta(minutes=15)
job_start_date = datetime(2024, 11, 6, 0, 0)

valuepacing_campaigns_compatibilty_check_dag = DAG(
    dag_name,
    schedule_interval=job_schedule_interval,
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": None,
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": job_start_date,
    },
    max_active_runs=1,  # we only want 1 run at a time. Sproc shouldn't run multiple times concurrently
    catchup=False,  # Just execute the latest run. It's fine to skip old runs
    dagrun_timeout=timedelta(hours=1),
    on_failure_callback=
    dag_post_to_slack_callback(dag_name=dag_name, step_name="DAG", slack_channel=job_slack_channel, message="DAG has failed")
)

max_campaigns_to_report = 4000


def update_valuepacing_campaigns_sproc(**kwargs):
    logger = logging.getLogger(__name__)
    sql_hook = MsSqlHook(mssql_conn_id='ttd_perfauto_provdb', schema='Provisioning')
    conn = sql_hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor()
    sql = "exec dbo.prc_UpdateIncompatibleValuePacingCampaigns @debug = 1"
    logger.info("Executing `%s`.", sql)
    cursor.execute(sql)
    logger.info("Fetching results.")

    # Skip to the last result set
    while cursor.nextset():
        pass

    changed_campaigns = cursor.fetchmany(max_campaigns_to_report)
    logger.info("Fetched %i rows.", len(changed_campaigns))
    if len(changed_campaigns) > 0:
        post_result_to_slack(changed_campaigns)


campaigns_per_message = 400


def post_message(text, thread_id=None):
    if thread_id is not None:
        return get_slack_client().chat_postMessage(channel=job_slack_channel, text=text, thread_ts=thread_id)
    else:
        return get_slack_client().chat_postMessage(channel=job_slack_channel, text=text)


def post_result_to_slack(changed_campaigns, enable_slack_alert=True, only_for_prod=True):
    logger = logging.getLogger(__name__)
    env = TtdEnvFactory.get_from_system()
    logger.info("Environment: %s.", env)
    if not enable_slack_alert or only_for_prod and env != TtdEnvFactory.prod:
        logger.info("Skipping slack alert.")
        return

    init_text = f'''
    `update-valuepacing-campaigns` task has disabled value pacing for `{len(changed_campaigns)}`
    campaigns because they were detected to be incompatible with value pacing.
    See a full list in thread.\n
    *Environment:* `{env.execution_env}`
    '''

    init_message = post_message(init_text)
    thread_id = init_message['message']['ts']

    if len(changed_campaigns) == 0:
        logger.warn("No changed campaigns found. Exiting.")
        thread_text = '*No changed campaigns found!* :cry:\n'
        post_message(thread_text, thread_id=thread_id)
        return

    thread_text_intro = ''
    if len(changed_campaigns) >= max_campaigns_to_report:
        thread_text_intro = '*Changed campaigns:* (possibly truncated to {max_campaigns_to_report})\n'
    else:
        thread_text_intro = '*Changed campaigns:*\n'

    if len(changed_campaigns) < campaigns_per_message:
        thread_text = thread_text_intro
        for campaign in changed_campaigns:
            thread_text += f'`{campaign[0]}` {campaign[1]}, Reasons: {campaign[2]}\n'
        post_message(thread_text, thread_id=thread_id)
        return

    post_message(thread_text_intro, thread_id=thread_id)
    for i in range(0, len(changed_campaigns), campaigns_per_message):
        thread_text = ''
        for campaign in changed_campaigns[i:i + campaigns_per_message]:
            thread_text += f'`{campaign[0]}` {campaign[1]}, Reasons: {campaign[2]}\n'
        post_message(thread_text, thread_id=thread_id)


update_valuepacing_campaigns_sproc_task = PythonOperator(
    task_id='update_valuepacing_campaigns_sproc',
    python_callable=update_valuepacing_campaigns_sproc,
    dag=valuepacing_campaigns_compatibilty_check_dag,
    provide_context=True
)
