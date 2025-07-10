"""
This DAG runs the monthly UID2 vendor financial report job on the 1st of the month at 1 PM,
covering data for the entire previous calendar month. It aggregates vendor-specific delivery counts
used by the finance team to calculate payouts for our vendor partners. The output includes per-country
and per-delivery metrics for UID2 data contributions.
"""
from dags.idnt.identity_helpers import DagHelpers
from datetime import datetime, timedelta
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.statics import RunTimes, Directories

test_folder = Directories.test_dir_no_prod()

uid2_vendor_financial_report_dag = DagHelpers.identity_dag(
    dag_id="uid2-vendor-financial-report",
    schedule_interval="0 13 1 * *",
    start_date=datetime(2025, 4, 1),
    run_only_latest=True,
    retries=1,
    retry_delay=timedelta(minutes=30),
)

cluster = IdentityClusters.get_cluster(
    'uid2_vendor_financial_report_cluster', uid2_vendor_financial_report_dag, 96 * 30, ComputeType.STORAGE
)
cluster.add_parallel_body_task(
    IdentityClusters.task(
        class_name='jobs.identity.uid2.vendors.Uid2VendorFinancialReport',
        timeout_hours=6,
        eldorado_configs=[(
            "jobs.identity.uid2.vendors.Uid2VendorFinancialReport.outPath",
            f's3://thetradedesk-useast-data-import/sxd-etl/uid2/uid2-financial-report{test_folder}'
        )],
        runDate=RunTimes.current_full_day  # Setting the runDate to the interval end
    )
)

uid2_vendor_financial_report_dag >> cluster
dag = uid2_vendor_financial_report_dag.airflow_dag
