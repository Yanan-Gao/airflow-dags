from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from datasources.sources.avails_datasources import AvailsDatasources
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.tasks.op import OpTask

dag = TtdDag(
    dag_id="avails_pipeline_hourly_success_checker",
    schedule_interval=timedelta(hours=1),
    run_only_latest=False,
    max_active_runs=1,
    start_date=datetime(2024, 8, 27, 0, 0),
    enable_slack_alert=True,
    slack_channel="#scrum-data-products-alarms",
    slack_alert_only_for_prod=True
)

wait_for_avails_data = OpTask(
    op=DatasetCheckSensor(
        dag=dag.airflow_dag,
        task_id="avails_pipeline_hourly_success_checker",
        ds_date="{{ data_interval_start.strftime('%Y-%m-%d %H:%M:%S')}}",
        poke_interval=60 * 10,
        datasets=[
            AvailsDatasources.identity_agg_hourly_dataset.with_check_type("hour").with_region("us-east-1"),
        ],
        timeout=60 * 60 * 24
    )
)


def put_success_file(**kwargs):
    s3_hook = CloudStorageBuilder(CloudProviders.aws).set_conn_id('aws_default').build()
    s3_hook.put_object(
        bucket_name=AvailsDatasources.identity_agg_hourly_dataset.bucket,
        key=f"{AvailsDatasources.identity_agg_hourly_dataset._get_full_key(ds_date=kwargs['logical_date'])}/_SUCCESS",
        body="",
        replace=True
    )


add_success_file = OpTask(op=PythonOperator(task_id='add_success_file', python_callable=put_success_file, provide_context=True))

dag >> wait_for_avails_data >> add_success_file
adag = dag.airflow_dag
