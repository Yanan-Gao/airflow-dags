# from airflow import DAG
from dags.forecast.audience_insights.site_insights_base import SiteInsightsDag

site_dag_1p = SiteInsightsDag(use_first_party=True, total_pixel_count=40000, total_uniques_count=200_000_000_000)

dag = site_dag_1p.dag
