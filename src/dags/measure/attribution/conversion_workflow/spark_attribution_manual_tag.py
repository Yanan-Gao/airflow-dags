from datetime import datetime

from airflow import DAG
from airflow.models import Param

from dags.measure.attribution.conversion_workflow.spark_attribution_job_dag import create_attribution_tasks
from dags.measure.attribution.databricks_config import DatabricksConfig, create_attribution_dag
from ttd.cloud_provider import CloudProviders
from ttd.eldorado.databricks.region import DatabricksRegion
from ttd.eldorado.databricks.workflow import DatabricksWorkflow


def create_dag(databricks_region: DatabricksRegion) -> DAG:
    ttd_dag = create_attribution_dag(
        dag_name="workflow_steps",
        cloud_provider=CloudProviders.aws,
        start_date=datetime(2025, 7, 1, 0, 0),
        schedule_interval=None,
        params={
            "JobIntervalStart":
            Param(type="string", default="2025-07-01T00:00:00", description="UTC time rounded to hour like 2025-07-08T05:00:00"),
            "JobIntervalEnd":
            Param(type="string", default="2025-07-01T01:00:00", description="UTC time rounded to hour like 2025-07-08T06:00:00"),
            "SessionId":
            Param(type="string", default="ProdTest-Default", description="Unique job run identifier"),
            "Bucketed_DailyIndex_LastEntryDate":
            Param(
                type="string",
                default="2025-07-01T00:00:00",
                description="latest partition key of prod.attribution.bidfeedback_bucketed_identity_weekly"
            ),
            "Bucketed_WeeklyIndex_LastEntryDate":
            Param(
                type="string",
                default="2025-07-01T00:00:00",
                description="latest partition key of prod.attribution.bidfeedback_bucketed_identity_v2"
            ),
            "task_assembly_location":
            Param(
                type="string",
                default=DatabricksConfig.ATTRIBUTION_DAG_SPARK_EXECUTABLE_PATH,
                description="The JAR assembly cloud storage path used in Databricks task"
            ),
            "DriverNodeType":
            Param(type="string", default="r6gd.8xlarge", description="Databricks Job Compute driver node type"),
            "WorkerNodeType":
            Param(type="string", default="r6gd.8xlarge", description="Databricks Job Compute worker node type"),
            "NodeCount":
            Param(type="integer", default=32, description="Number of worker nodes", minimum=1),
            "EmptyMode":
            Param(type="boolean", default=False, description="When activated a zero input SQL dry run is executed"),
        }
    )
    task_params = [
        "{{ params.JobIntervalStart }}",
        "{{ params.JobIntervalEnd }}",
        "{{ params.SessionId }}",
        "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:%M:%S\") }}",
        "{{ params.Bucketed_DailyIndex_LastEntryDate }}",
        "{{ params.Bucketed_WeeklyIndex_LastEntryDate }}",
        # Always use test environment to ensure test generated data are written to test unity schema, not prod
        "test",
        "{{ params.EmptyMode }}",
    ]
    attribution_tasks = create_attribution_tasks(executable_path="{{ params.task_assembly_location }}", task_params=task_params)
    attribution_workflow = DatabricksWorkflow(
        job_name="attribution_workflow_test",
        cluster_name="measure_spark_attribution_scheduler_cluster",
        cluster_tags={
            **DatabricksConfig.BASE_TAG,
            **DatabricksConfig.ATTRIBUTION_JOB_TAG
        },
        databricks_spark_version=DatabricksConfig.DATABRICKS_VERSION,
        use_photon=True,
        driver_node_type="{{ params.DriverNodeType }}",
        worker_node_type="{{ params.WorkerNodeType }}",
        worker_node_count="{{ params.NodeCount | int }}",
        spark_configs=DatabricksConfig.SPARK_CONF_ATTRIBUTION_PHOTON,
        region=databricks_region,
        retries=0,
        tasks=[*attribution_tasks.values()]
    )
    ttd_dag >> attribution_workflow
    return ttd_dag.airflow_dag


attribution_manual_dag = create_dag(DatabricksRegion.use())
