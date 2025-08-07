from datetime import datetime, timedelta
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction

from ttd.eldorado.databricks.tasks.python_wheel_databricks_task import PythonWheelDatabricksTask
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from dags.datperf.datasets import plutus_etl_ch_v3_dataset

from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.slack.slack_groups import DATPERF, AUDAUTO
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

DATE = '{{ data_interval_end.strftime("%Y%m%d") }}'

WHEEL_PATH = "s3://thetradedesk-mlplatform-us-east-1/libs/plutus/wheels/prod/plutus-1.0.2-py3-none-any.whl"
INIT_SCRIPT = "s3://thetradedesk-mlplatform-us-east-1/libs/plutus/scripts/prod/db_init_script_new.sh"

# Testing config
TEST_BRANCH = "abm-DATPERF-6742-register-model-in-mlflow"
if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest and TEST_BRANCH is not None:
    WHEEL_PATH = f"s3://thetradedesk-mlplatform-us-east-1/libs/plutus/wheels/mergerequests/{TEST_BRANCH}/plutus-1.0.2-py3-none-any.whl"
    INIT_SCRIPT = f"s3://thetradedesk-mlplatform-us-east-1/libs/plutus/scripts/mergerequests/{TEST_BRANCH}/db_init_script_new.sh"

ENV = TtdEnvFactory.get_from_system().dataset_write_env
SLACK_ALERT = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False
# might make this configurable, for now get it from taskValues in DataBricks
JOB_ARTIFACT_LOC = "{{ dag_run.conf.get('job_artifact_loc', None) }}"
LAYERS = "{{ dag_run.conf.get('layers', None) }}"

plutus_train: TtdDag = TtdDag(
    dag_id="perf-automation-plutus-ray-training",
    start_date=datetime(2025, 4, 3),
    schedule_interval="0 3 * * *",  # every day at 3 am
    max_active_runs=1,
    run_only_latest=True,
    retry_delay=timedelta(minutes=20),
    slack_tags="DATPERF",
    tags=["DATPERF", "Plutus"],
    enable_slack_alert=SLACK_ALERT,
    teams_allowed_to_access=[DATPERF.team.jira_team, AUDAUTO.team.jira_team]
)

dag = plutus_train.airflow_dag

etl_v3_dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        dag=dag,
        task_id='plutus_etl_available',
        poke_interval=60 * 30,
        timeout=60 * 60 * 20,  # wait up to 20 hours
        ds_date="{{ data_interval_start.to_datetime_string() }}",
        datasets=[plutus_etl_ch_v3_dataset]
    )
)

training_task = PythonWheelDatabricksTask(
    package_name="plutus",
    # referencing the entry point defined in setup.py in plutus job run() should be in this location
    entry_point="plutus_train",
    # we need the wheel location in code to set up the run time env for ray
    parameters=[f"--env={ENV}", f"--date={DATE}", f"--wheel_path={WHEEL_PATH}", f"--job_artifact_loc={JOB_ARTIFACT_LOC}"],
    job_name="model_train",
    # and the wheel here is what actually gets run by the workflow
    whl_paths=[WHEEL_PATH],
)

eval_task = PythonWheelDatabricksTask(
    package_name="plutus",
    # referencing the entry point defined in setup.py in plutus job run() should be in this location
    entry_point="plutus_eval",
    # we need the wheel location in code to set up the run time env for ray
    parameters=
    [f"--env={ENV}", f"--date={DATE}", f"--wheel_path={WHEEL_PATH}", f"--job_artifact_loc={JOB_ARTIFACT_LOC}", f"--layers={LAYERS}"],
    # and the wheel here is what actually gets run by the workflow
    job_name="model_eval",
    whl_paths=[WHEEL_PATH],
    depends_on=[training_task]
)

db_workflow = DatabricksWorkflow(
    job_name="perf-automation-plutus-ray-training",
    cluster_name="perf-automation-plutus-ray-training-cluster",
    cluster_tags={"Team": "DATPERF"},
    worker_node_type="g5.12xlarge",
    worker_node_count=12,
    retries=0,
    databricks_spark_version="16.3.x-gpu-ml-scala2.12",
    # so this here piece means that even though we see 1 step in the Airflow UI it runs both DB tasks
    tasks=[training_task, eval_task],
    bootstrap_script_actions=[(ScriptBootstrapAction(INIT_SCRIPT))]
)

final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

plutus_train >> etl_v3_dataset_sensor >> db_workflow >> final_dag_status_step
