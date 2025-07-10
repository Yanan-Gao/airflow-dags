"""
PII Cleanse for Identity Datasets

This is now dataset maintenance job instead of PII cleanse only. Including
the deletion of rows subjects to PII deletion requests as well expiring
old delta table partitions that are not subject to S3 expiry policies, since
those deletions would break the delta logs.
"""

from airflow import DAG
from datetime import datetime
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType

dag = DagHelpers.identity_dag(dag_id="cleanse-pii", schedule_interval="5 0 1 * *", start_date=datetime(2024, 9, 1))

cluster = IdentityClusters.get_cluster("cleanse-pii-cluster", dag, 2500, ComputeType.STORAGE)

job_classes = (
    "jobs.cleansepii.SxdScoreCleansePii",
    "jobs.cleansepii.SxdDeterministicCleansePii",
    "jobs.cleansepii.InsightDailyAggCleansePii",
    "jobs.cleansepii.GraphCleansePii",
    "jobs.cleansepii.FirstPartyDataCleansePii",
    "jobs.cleansepii.ConversionLiftCleansePii",
    "jobs.cleansepii.BidFeedbackCleansePii",
)

for job_class in job_classes:
    cluster.add_parallel_body_task(IdentityClusters.task(job_class, runDate_arg="date"))

# We need a much larger cluster for Identity avails
large_cluster = IdentityClusters.get_cluster("cleanse-pii-cluster-large", dag, 96 * 192, ComputeType.STORAGE)

# Historically this has been known to run for > 20 hours. Let's just set this timeout on the higher end.
large_cluster.add_parallel_body_task(
    IdentityClusters.task("jobs.cleansepii.IdentityAvailsCleansePii", runDate_arg="date", timeout_hours=15)
)

delta_vacuum_cluster = IdentityClusters.get_cluster("identity-delta-vacuum", dag, 3000, ComputeType.ARM_GENERAL, ComputeType.ARM_GENERAL)
delta_vacuum_cluster.add_sequential_body_task(
    IdentityClusters.task(
        "com.thetradedesk.idnt.identity.pipelines.DeltaVacuum",
        timeout_hours=8,
        extra_args=[
            # these are necessary for efficient vacuuming
            ("conf", "spark.databricks.delta.optimize.maxThreads=5000"),
            ("conf", "spark.databricks.delta.optimize.repartition.enabled=true"),
            ("conf", "spark.databricks.delta.vacuum.parallelDelete.enabled=true"),
            ("conf", "spark.databricks.delta.retentionDurationCheck.enabled=false"),
            ("conf", "spark.databricks.delta.merge.repartitionBeforeWrite.enabled=false"),
            ("conf", "spark.databricks.delta.autoCompact.enabled=true"),
            ("conf", "spark.databricks.delta.optimizeWrite.enabled=true"),
        ]
    )
)

dag >> cluster
dag >> large_cluster
dag >> delta_vacuum_cluster

final_dag: DAG = dag.airflow_dag
