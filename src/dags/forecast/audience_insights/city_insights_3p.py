# from airflow import DAG
from dags.forecast.audience_insights.city_insights_base import CityInsightsDag

city_dag_3p = CityInsightsDag(use_first_party=False, total_uniques_count=20_000_000_000)

dag = city_dag_3p.dag
