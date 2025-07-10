import os

from datetime import datetime, timedelta

from dags.datperf.datasets import plutus_etl_dataset
from ttd.eldorado.base import TtdDag

from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.tasks.op import OpTask

# Job start is midnight, therefore execution_date will be previous day (execution_date == 2021-06-02 will happen at some
# time just after 2021-06-03 00:00)
YEAR = '{{ (data_interval_start).strftime("%Y") }}'
MONTH = '{{ (data_interval_start).strftime("%m") }}'
DAY = '{{ (data_interval_start).strftime("%d") }}'

MODELVERSION = '{{ (data_interval_start).strftime("%Y%m%d%H%M") }}'
LOOKBACK = 3

# If changing the start date/interval please see this: https://gtoonstra.github.io/etl-with-airflow/gotchas.html
plutus_training_dag = TtdDag(
    dag_id="perf-automation-plutus-training-eks",
    start_date=datetime(2023, 8, 25, 12, 0),
    schedule_interval=timedelta(hours=24),
    retries=0,
    retry_delay=timedelta(hours=1),
    dag_tsg="https://atlassian.thetradedesk.com/confluence/x/yrMMCQ",
    slack_channel="#scrum-perf-automation-alerts-testing",
    default_args={"owner": "DATPERF"},
    slack_alert_only_for_prod=True,
    tags=["DATPERF", "Plutus"],
)
dag = plutus_training_dag.airflow_dag

plutus_etl_dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        dag=dag,
        task_id='plutus_etl_available',
        poke_interval=60 * 30,
        timeout=60 * 60 * 24,  # wait up to 24 hours
        ds_date="{{ data_interval_start.to_datetime_string() }}",
        datasets=[plutus_etl_dataset]
    )
)

yamlpath = os.path.join(os.path.dirname(__file__), "perf-automation-plutus-training-eks.yaml")

# This step trains a model and writes to test
plutus_train_step = OpTask(
    op=TtdKubernetesPodOperator(
        service_account_name="perfauto-plutus-training",
        namespace="perf-automation-plutus-training-test",
        image="internal.docker.adsrvr.org/plutus-training:release",
        image_pull_policy="Always",
        name="perf-automation-plutus-train-model",
        task_id="plutus_model_training",
        dnspolicy="ClusterFirst",
        get_logs=True,
        is_delete_operator_pod=True,
        dag=dag,
        startup_timeout_seconds=500,
        execution_timeout=timedelta(hours=12),
        log_events_on_failure=True,
        pod_template_file=yamlpath,
        arguments=[
            "--nodummy",
            "--s3_output_path=s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/test/",
            "--s3_input_csv_path=s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/prod/singledayprocessed",
            "--batch_size=262144",
            "--eval_batch_size=262144",
            f"--model_creation_date={MODELVERSION}",
            "--num_epochs=10",
            "--exclude_features=AliasedSupplyPublisherId,AspSvpId",
            "--training_verbosity=2",
            "--model_arch=dlrm",
            "--early_stopping_patience=3",
            "--push_training_logs=true",
            "--push_training_metrics=true",
            "--learning_rate=0.000001",
            f"--lookback={LOOKBACK}",
            "--zero_non_trained_sv=false",
            "--serve_excluded_features=false",
            "--steps_per_epoch=600",
        ],
    )
)

plutus_training_dag >> plutus_etl_dataset_sensor >> plutus_train_step
