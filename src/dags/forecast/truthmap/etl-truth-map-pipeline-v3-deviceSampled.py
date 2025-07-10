# from airflow import DAG
from airflow.utils.dates import days_ago

from dags.forecast.truthmap.etl_truth_map_pipeline_v3_base import TruthMapDag

device_sampled = TruthMapDag(
    job_start_date=days_ago(2),
    job_schedule_interval="0 12 * * *",
    avail_sampling_rate=30,
    avail_stream="deviceSampled",
    combination_list_names=["availSourcedRandomIntersect", "availSourcedRandomUnion"],
    combination_list_types=["intersect", "union"],
    id_types=["tdid"],
    time_ranges=(1, 3, 5, 7, 14, 30),
    version_number="1"
)

dag = device_sampled.dag
