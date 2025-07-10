from datetime import datetime, timedelta

from dags.datperf.datasets import geosegment_dataset, metro_dataset, city_dataset, zip_dataset, \
    flattenedstandardlocation_dataset, country_dataset, region_dataset
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.python_wheel_databricks_task import PythonWheelDatabricksTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.eldorado.base import TtdDag

from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

DATE = '{{ data_interval_end.strftime("%Y%m%d") }}'
ENV = TtdEnvFactory.get_from_system().dataset_write_env
OUTPUT_PATH = f"s3://thetradedesk-mlplatform-us-east-1/env={ENV}/value-pacing-aion/data/"
WHEEL_PATH = f"s3://thetradedesk-mlplatform-us-east-1/libs/aion/{ENV}/latest/value_pacing_aion-0.0.0-py3-none-any.whl"
SLACK_ALERT = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False

geosegment_etl_dag: TtdDag = TtdDag(
    dag_id="perf-automation-aion-geosegment-etl",
    start_date=datetime(2025, 4, 3),
    schedule_interval="0 3 * * *",  # every day at 3 am
    max_active_runs=1,
    retry_delay=timedelta(minutes=20),
    slack_tags="DATPERF",
    slack_channel="#scrum-perf-automation-alerts-testing" if ENV == 'test' else '#pod-aion-alerts',
)

dag = geosegment_etl_dag.airflow_dag

daily_dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        dag=dag,
        task_id='daily_dataset_sensor',
        poke_interval=60 * 30,
        timeout=60 * 60 * 12,
        ds_date="{{ data_interval_end.to_datetime_string() }}",
        datasets=[
            geosegment_dataset, flattenedstandardlocation_dataset, country_dataset, region_dataset, metro_dataset, city_dataset, zip_dataset
        ]
    )
)

export_geosegment_task = PythonWheelDatabricksTask(
    package_name="value_pacing_aion",
    entry_point="geo_segment_map_etl",
    parameters=[f"--env={ENV}", f"--date={DATE}", f"--output_path={OUTPUT_PATH}"],
    job_name="export_geosegment_task",
    whl_paths=[WHEEL_PATH]
)

db_workflow = DatabricksWorkflow(
    job_name="perf-automation-aion-geosegment-etl",
    cluster_name="perf-automation-aion-geosegment-etl-cluster",
    cluster_tags={"Team": "DATPERF"},
    worker_node_type="i3.2xlarge",
    worker_node_count=4,
    databricks_spark_version="15.4.x-scala2.12",
    tasks=[export_geosegment_task],
    ebs_config=DatabricksEbsConfiguration(ebs_volume_count=1, ebs_volume_size_gb=64),
)

###############################################################################
# Dependencies
###############################################################################

# Final status check to ensure that all tasks have completed successfully
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

geosegment_etl_dag >> daily_dataset_sensor >> db_workflow >> final_dag_status_step
