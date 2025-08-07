from datetime import datetime

from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.eldorado.databricks.tasks.spark_databricks_task import SparkDatabricksTask
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.eldorado.base import TtdDag
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import MEASUREMENT
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

# S3 path: s3://thetradedesk-useast-vertica-parquet-export/ExportAttributedEvent/VerticaAws/date=20250623/hour=00/_SUCCESS
attributedevent_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-useast-vertica-parquet-export",
    path_prefix="ExportAttributedEvent",
    env_aware=False,
    data_name="VerticaAws",
    version=None,
    success_file="_SUCCESS"
)

# S3 path: s3://thetradedesk-useast-vertica-parquet-export/ExportAttributedEventResult/VerticaAws/date=20250623/hour=00/_SUCCESS
attributedeventresult_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-useast-vertica-parquet-export",
    path_prefix="ExportAttributedEventResult",
    env_aware=False,
    data_name="VerticaAws",
    version=None,
    success_file="_SUCCESS"
)

job_name = "combined-attributedevent-delta-load"
ttd_dag: TtdDag = TtdDag(
    dag_id=f"{job_name}",
    start_date=datetime(2025, 7, 16, 0, 0),
    schedule_interval="0 3,7,11,15,19,23 * * *",
    slack_tags=MEASUREMENT.team.jira_team,
    slack_channel="#taskforce-cat-alarms",
    max_active_runs=1,
    run_only_latest=False,
    depends_on_past=False,
)

partition_end_time = '{{ data_interval_start.strftime("%Y-%m-%d %H:%M:%S") }}'

attributedevent_dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='attributedevent_dataset_sensor',
        datasets=[attributedevent_dataset.with_check_type("hour"),
                  attributedeventresult_dataset.with_check_type("hour")],
        ds_date=partition_end_time,
        poke_interval=60 * 20,
        timeout=60 * 60 * 12
    )
)

run_task = DatabricksWorkflow(
    job_name=job_name,
    cluster_name=f"{job_name}-cluster",
    cluster_tags={"Team": MEASUREMENT.team.jira_team},
    databricks_spark_version="16.4.x-scala2.12",
    driver_node_type="r6gd.2xlarge",
    worker_node_type="r6gd.2xlarge",
    worker_node_count=8,
    spark_configs={
        "fs.s3a.acl.default": "BucketOwnerFullControl",
        "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.io.cache.enabled": "false",
    },
    tasks=[
        SparkDatabricksTask(
            class_name="com.thetradedesk.jobs.combinedattributedevent.CombinedAttributedEventTableWriter",
            executable_path="s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-measure-assembly.jar",
            job_name="delta-load",
            additional_command_line_parameters=[partition_end_time, str(TtdEnvFactory.get_from_system()).lower()],
        )
    ]
)

ttd_dag >> attributedevent_dataset_sensor >> run_task
DAG = ttd_dag.airflow_dag
