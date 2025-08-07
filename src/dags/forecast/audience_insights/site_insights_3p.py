# from airflow import DAG
from dags.forecast.audience_insights.site_insights_base import SiteInsightsDag

site_dag_3p = SiteInsightsDag(use_first_party=False, total_pixel_count=10000, total_uniques_count=20_000_000_000)

dag = site_dag_3p.dag
