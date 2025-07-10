# from airflow import DAG
from dags.forecast.audience_insights.city_insights_base import CityInsightsDag

city_dag_1p = CityInsightsDag(use_first_party=True, total_uniques_count=200_000_000_000)

dag = city_dag_1p.dag
