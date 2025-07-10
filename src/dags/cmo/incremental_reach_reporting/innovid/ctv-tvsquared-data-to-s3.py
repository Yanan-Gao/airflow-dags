"""
Daily DAG to copy TvSquared campaign details into S3.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from dags.cmo.incremental_reach_reporting.irr_provider_data_upload import IrrProviderDataUpload
from ttd.el_dorado.v2.base import TtdDag
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask

pipeline_name = "ctv-tvsquared-data-to-s3"
job_start_date = datetime(2024, 7, 16, 0, 0)
job_schedule_interval = timedelta(days=1)

provider_upload = IrrProviderDataUpload(
    s3_bucket="tvsquared-receivedata-ttd-produsa",
    s3_base_path="ttd-tvsquared-campaigns/",
    provisioning_query="exec acr.prc_GetIrrCampaigns @providerId = 2"
)

# DAG
dag_pipeline: TtdDag = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    retries=1,
    retry_delay=timedelta(hours=1),
    depends_on_past=False,
    run_only_latest=False,
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    tags=["irr"]
)
dag: DAG = dag_pipeline.airflow_dag

####################################################################################################################
# Steps
####################################################################################################################

start_task = OpTask(op=EmptyOperator(task_id='Start', dag=dag))

write_tvsquared_data_to_s3 = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=provider_upload.load_and_write_provider_data_to_s3,
        provide_context=True,
        task_id="load_and_write_tvsquared_data_to_s3",
    )
)

end_task = OpTask(op=EmptyOperator(task_id='End', dag=dag))

start_task >> write_tvsquared_data_to_s3 >> end_task
