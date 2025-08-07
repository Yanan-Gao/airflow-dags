from datetime import datetime, timedelta

from airflow.sensors.external_task_sensor import ExternalTaskSensor

import dags.audauto.perf_analyzer as perf_analyzer
from dags.audauto.utils import utils
from dags.audauto.utils.benchmark_jobs import read_mlflow_tag_task, create_kubernetes_benchmark_task
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import AUDAUTO, DATPERF

EXTERNAL_DAG_ID = "perf-automation-kongming-training"
INPUT_JSON_URI = "s3://thetradedesk-mlplatform-us-east-1/users/dennis.tran/input.json"  # For kongming for now, temporary
LOCATION_TAG_NAME = "BIDREQUEST_ONNX_MODEL_S3_LOCATION"

# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
kongming_training_dag = TtdDag(
    dag_id="perf-automation-kongming-benchmark",
    start_date=datetime(2025, 5, 1),
    schedule_interval=utils.schedule_interval,
    retries=2,
    max_active_runs=1,
    retry_delay=timedelta(minutes=10),
    slack_channel="#dev-perf-auto-alerts-model-serving-infra",
    slack_tags=None,
    enable_slack_alert=True,
    teams_allowed_to_access=[AUDAUTO.team.jira_team, DATPERF.team.jira_team]
)
dag = kongming_training_dag.airflow_dag

# need training to complete
kongming_training_dag_sensor = ExternalTaskSensor(
    task_id='kongming_training_sensor',
    external_dag_id=EXTERNAL_DAG_ID,
    external_task_id=None,  # wait for the entire DAG to complete
    allowed_states=["success"],
    check_existence=False,
    poke_interval=5 * 60,  # poke_interval is in seconds - poke every 5 minutes
    timeout=18 * 60 * 60,  # timeout in seconds - wait 18 hours
    mode='reschedule',  # release the worker slot between pokes
    dag=dag
)

get_model_path_task = read_mlflow_tag_task(
    task_id='get_model_path',
    model_name='kongming',
    job_cluster_id=f"{{{{ task_instance.xcom_pull(dag_id='{EXTERNAL_DAG_ID}', " +
    "task_ids='KongmingLanternModelStack_add_task_PyTorchModelStacking', key='job_flow_id' ) }}",
    tag_name=LOCATION_TAG_NAME
)

benchmark_task = create_kubernetes_benchmark_task(
    name='run_benchmark',
    task_id='run_benchmark',
    model_s3_path=f"{{{{ task_instance.xcom_pull(task_ids='{get_model_path_task.task_id}') }}}}",
    input_s3_path=INPUT_JSON_URI,
    embedding_length=96
)

perf_analyzer_cluster = perf_analyzer.create_perf_analyzer_cluster(
    model_path=f"{{{{ task_instance.xcom_pull(task_ids='{get_model_path_task.task_id}') }}}}"
)

kongming_training_dag >> perf_analyzer_cluster
kongming_training_dag_sensor >> get_model_path_task.first_airflow_op() >> benchmark_task.first_airflow_op()
get_model_path_task.last_airflow_op() >> perf_analyzer_cluster.first_airflow_op()
