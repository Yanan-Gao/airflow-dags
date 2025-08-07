from datetime import datetime

from ttd.eldorado.databricks.tasks.spark_databricks_task import SparkDatabricksTask
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import MEASUREMENT
from ttd.ttdenv import TtdEnvFactory

job_name = "combined-attributedevent-data-purge"
ttd_dag: TtdDag = TtdDag(
    dag_id=f"{job_name}-daily",
    start_date=datetime(2025, 7, 7, 0, 0),
    schedule_interval="0 0 * * *",
    slack_tags=MEASUREMENT.team.jira_team,
    slack_channel="#taskforce-cat-alarms",
    max_active_runs=1,
    run_only_latest=True,
)

run_task = DatabricksWorkflow(
    job_name=job_name,
    cluster_name=f"{job_name}-cluster",
    cluster_tags={"Team": MEASUREMENT.team.jira_team},
    databricks_spark_version="16.4.x-scala2.12",
    driver_node_type="r6gd.2xlarge",
    worker_node_type="r6gd.2xlarge",
    worker_node_count=1,
    spark_configs={
        "fs.s3a.acl.default": "BucketOwnerFullControl",
        "spark.databricks.delta.vacuum.parallelDelete.enabled": "true",
        "spark.databricks.io.cache.enabled": "false",
    },
    tasks=[
        SparkDatabricksTask(
            class_name="com.thetradedesk.jobs.combinedattributedevent.CombinedAttributedEventTablePurger",
            executable_path="s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-measure-assembly.jar",
            job_name="data-purge",
            additional_command_line_parameters=
            ['{{ data_interval_start.strftime("%Y-%m-%dT%H:%M:%S") }}',
             str(TtdEnvFactory.get_from_system()).lower()],
        )
    ]
)

ttd_dag >> run_task
DAG = ttd_dag.airflow_dag
