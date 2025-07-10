from datetime import datetime, timedelta

from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.python_wheel_databricks_task import PythonWheelDatabricksTask
from ttd.python_versions import PythonVersions
from ttd.spark import AllBackends, EbsConfiguration, SparkVersionSpec, SparkWorkflow
from ttd.eldorado.base import TtdDag

from airflow import DAG
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.spark_workflow.tasks.pyspark_task import PySparkTask

databricks_spark_version = "13.3.x-scala2.12"

avails_pipeline_dag: TtdDag = TtdDag(
    dag_id="databricks-pyspark-test",
    start_date=datetime(2024, 10, 2, 0, 0),
    schedule_interval=timedelta(hours=1),
    max_active_runs=1,
    retry_delay=timedelta(minutes=20),
    slack_tags="AIFUN",
    enable_slack_alert=False,
)

task_start = PySparkTask(
    task_name="test-running-on-both-1",
    python_entrypoint_location=
    "s3://ttd-build-artefacts/eldorado-core/mergerequests/mwp-AIFUN-1734-whl-install-bootstrap/2313628-1c3d3b8b/bootstrap/hello-world.py",
)

task_end = PySparkTask(
    task_name="test-running-on-both-2",
    python_entrypoint_location=
    "s3://ttd-build-artefacts/eldorado-core/mergerequests/mwp-AIFUN-000-test-python-script/2246287-cee1be38/bootstrap/hello-world.py",
)

task_start >> task_end

spark_test = SparkWorkflow(
    job_name="test-running-on-both",
    instance_type=R5.r5_xlarge(),
    instance_count=2,
    spark_version=SparkVersionSpec.SPARK_3_5_0.value,
    python_version=PythonVersions.PYTHON_3_10.value,
    whls_to_install=[
        "s3://thetradedesk-mlplatform-us-east-1/libs/koa-three-dot-five/model-pyspark/dist/mergerequests/jyw-DATPERF-5328-clean-up-blocked-dim/latest/koa_job_pyspark-0.0.0-py3-none-any.whl"
    ],
    ebs_configuration=EbsConfiguration(ebs_volume_count=1, ebs_volume_size_gb=64),
    tasks=[task_start],
    retries=0,
    backend_chooser=AllBackends(parallel=True),
    tags={
        "Process": "databricks-pyspark-test",
        "Team": "AIFUN"
    }
)

avails_pipeline_dag >> spark_test

python_wheel_test = PythonWheelDatabricksTask(
    package_name="koa_job_pyspark",
    entry_point="hello_world",
    parameters=[""],
    job_name="test-python-wheel",
    whl_paths=[
        "s3://thetradedesk-mlplatform-us-east-1/libs/koa-three-dot-five/model-pyspark/dist/mergerequests/nrs-AIFUN-000-send-wheel-to-s3/2583910-3a1704cf/koa_job_pyspark-0.7.16-py3-none-any.whl"
    ]
)

databricks_python_wheel_dag: TtdDag = TtdDag(
    dag_id="databricks-python-wheel-support-test",
    start_date=datetime(2025, 1, 22, 0, 0),
    max_active_runs=1,
    retry_delay=timedelta(minutes=20),
    slack_tags="AIFUN",
    enable_slack_alert=False,
)

task = DatabricksWorkflow(
    job_name="python-wheel-test-job",
    cluster_name="python-wheel-test-cluster",
    cluster_tags={"Team": "AIFUN"},
    worker_node_type="m5.large",
    worker_node_count=1,
    databricks_spark_version="12.2.x-scala2.12",
    tasks=[python_wheel_test],
    ebs_config=DatabricksEbsConfiguration(ebs_volume_count=1, ebs_volume_size_gb=64),
)

databricks_python_wheel_dag >> task

adag: DAG = avails_pipeline_dag.airflow_dag
pywheeldag: DAG = databricks_python_wheel_dag.airflow_dag
