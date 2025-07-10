"""
Handles all event stream data processing and the corresponding monitoring:
  * Avails
  * Bidfeedback
  * Bidrequest

Set to run at the cadence necessary for the hourly avails v2/v3 processing. Details for
that are in the avails doc.

data interval end is the time of processing. It is also used for the daily cluster.
Daily tasks are run under the daily_branch at 06:00 (data interval end).
"""
from airflow import DAG
from datetime import datetime
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.event_stream_data.avails import AvailsV2, AvailsV3
from dags.idnt.event_stream_data.avails_monitoring import AvailsMonitoring
from dags.idnt.event_stream_data.bidfeedback import Bidfeedback, BidfeedbackEu
from dags.idnt.event_stream_data.bidstream_daily_agg import BidStreamDailyAgg

# if you change this, make sure to change the schedule interval too!
hour_group_size = 6

dag = DagHelpers.identity_dag(
    dag_id="bidstream-data",
    schedule_interval="3 0,6,12,18 * * *",
    start_date=datetime(2025, 3, 26),
    # we can catchup a full day in one go
    max_active_runs=24 // hour_group_size,
    # max_active_tasks need to increase to allow parallel runs, with all the emr overheads setting this to 3x the num hours
    max_active_tasks=72,
    run_only_latest=True,
    doc_md=__doc__
)

# Avails processing -- hourly and daily
AvailsV2(dag, hour_group_size).get_hourly_and_daily_aggs()

avails_v3_daily_cluster = AvailsV3(dag, hour_group_size).get_hourly_and_daily_aggs()

daily_processing_branch = DagHelpers.skip_unless_hour(["12"])

# Avails monitoring -- daily
AvailsMonitoring(dag).build_pipeline(daily_processing_branch)

# Bidfeedback processing -- daily
Bidfeedback(dag).build_pipeline(daily_processing_branch)

BidfeedbackEu(dag).build_pipeline(daily_processing_branch)

dag >> daily_processing_branch

# BidRequest,feedback and conversion processing for ttdgraph_v2 and iav2
avails_v3_daily_cluster >> BidStreamDailyAgg(dag).build()

final_dag: DAG = dag.airflow_dag
