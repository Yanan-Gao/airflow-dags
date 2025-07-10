# from airflow import DAG
from airflow.utils.dates import days_ago

from dags.forecast.truthmap.etl_truth_map_pipeline_v3_base_audience_prep import AudienceEnhancedTruthMapDag

device_sampled_audience_enriched = AudienceEnhancedTruthMapDag(
    job_start_date=days_ago(2),
    job_schedule_interval="0 12 * * *",
    frequency_map_dag_version=2,
    avail_sampling_rate=30,
    avail_stream="deviceSampled",
    # This execution delta matches the datetime difference between this DAG and etl-frequency-map-forecasting-v2-deviceSampled
    id_to_targeting_data_mapping_execution_delta=4,
    id_types=["tdid"],
    time_ranges=(1, 3, 5, 7, 14, 30),
    version_number="1"
)

dag = device_sampled_audience_enriched.dag
