# from airflow import DAG
from airflow.utils.dates import days_ago
from dags.forecast.frequencymap.etl_frequency_map_forecasting_base import FrequencyMapDag, IdTypeToSet

device_sampled = FrequencyMapDag(
    job_start_date=days_ago(1),
    job_schedule_interval="0 8 * * *",
    avail_stream="deviceSampled",
    id_types_with_production_aerospike_set_roots=[IdTypeToSet("tdid", "fmapv2")],
    time_ranges=[1, 3, 5, 7, 14, 30],
    include_people_household_curve=True,
    version="v2",
    export_enabled=True,
)

dag = device_sampled.dag
