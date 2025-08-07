# from airflow import DAG
from airflow.utils.dates import days_ago
from dags.forecast.frequencymap.etl_frequency_map_forecasting_base import FrequencyMapDag, IdTypeToSet

household_sampled = FrequencyMapDag(
    job_start_date=days_ago(1),
    job_schedule_interval="0 20 * * *",
    avail_stream="householdSampled",
    id_types_with_production_aerospike_set_roots=[IdTypeToSet("personId", "fmapv3p"),
                                                  IdTypeToSet("householdId", "fmapv3h")],
    time_ranges=[1, 3, 5, 7, 14, 30],
    include_people_household_curve=False,
    version="v1",
    export_enabled=True,
    graph_name="openGraphIav2"
)

dag = household_sampled.dag
