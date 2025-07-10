from datetime import datetime, timedelta
from ttd.el_dorado.v2.base import TtdDag
from ttd.ttdenv import TtdEnvFactory
from ttd.tasks.op import OpTask
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.decorators import apply_defaults
from ttd.slack.slack_groups import fineng
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ttdslack import dag_post_to_slack_callback, get_slack_client
import logging
from dags.fineng.edw.utils.get_credentials import credentials
import json
import requests
import io

job_start_date = datetime(2025, 4, 15, 8)
aws_conn_id = 'aws_default'

dag = TtdDag(
    dag_id="MAIN-EDW-Youku-To-S3",
    start_date=job_start_date,
    schedule_interval="0 6 * * *",
    dagrun_timeout=timedelta(minutes=15),
    tags=["fineng", "edw", "youku"],
    run_only_latest=True,
    max_active_runs=1,
    retry_delay=timedelta(minutes=5),
    slack_alert_only_for_prod=True,
    slack_channel=fineng.alarm_channel,
)

adag = dag.airflow_dag


def youku_api():
    import pandas as pd

    url = 'http://adx-open-service.youku.com/dsp/api/data/query'
    pageno = 1
    current_date = datetime.now()
    yesterday = current_date - timedelta(days=1)
    yesterday_date_str = yesterday.strftime("%Y%m%d")
    thirtyone_prior_date = current_date - timedelta(days=31)
    thirtyone_prior_date_str = thirtyone_prior_date.strftime("%Y%m%d")
    logging.info(f"Lower Date - {thirtyone_prior_date_str}")
    logging.info(f"Upper Date - {yesterday_date_str}")
    headers = {"Content-Type": "application/json"}
    results = []
    while True:
        payload = json.dumps({
            "dspid": "31",
            "token": credentials('youku_token'),
            "pageNo": pageno,
            "pageSize": 100,
            "dsLower": thirtyone_prior_date_str,
            "dsUpper": yesterday_date_str
        })
        response = requests.post(url, data=payload, headers=headers)
        data = response.json()
        if (response.status_code != 200) or data['result'] == 1:
            raise AirflowException(f"Not able to fetch data")
        else:
            if len(data['data']['result']) == 0:
                break
            else:
                logging.info(f"Page number - {pageno}")
                results.extend(data["data"]["result"])
                pageno = pageno + 1
    df = pd.DataFrame(results)
    df_selected = df[["ds", "expCnt", "expAmount", "adExchangeType"]].copy()
    df_selected["expCnt"] = df_selected["expCnt"].astype(int)
    df_selected["expAmount"] = df_selected["expAmount"].astype(float)
    grouped_df = df_selected.groupby(["ds", "adExchangeType"], as_index=False).sum()
    market_map = {"RTB": "Non-FixedPrice", "PD": "FixedPrice"}
    grouped_df["Market Type"] = grouped_df["adExchangeType"].map(market_map)
    grouped_df.drop(columns=["adExchangeType"], inplace=True)
    grouped_df["Time Zone"] = "Asia/Shanghai"
    grouped_df["Currency Type"] = "CNY"
    grouped_df['ds'] = pd.to_datetime(grouped_df['ds'].astype(str), format='%Y%m%d').dt.strftime('%m/%d/%Y')
    grouped_df.rename(columns={"ds": "Date", "expCnt": "Impressions", "expAmount": "Media Cost"}, inplace=True)

    csv_buffer = io.StringIO()
    grouped_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    s3_hook = AwsCloudStorage(conn_id=aws_conn_id)
    bucket = 'thetradedesk-supply-vendor-reporting'
    file_name = f"{datetime.now().strftime('%Y-%m-%d')}_daily-report_youku_{datetime.now().strftime('%H-%M-%S')}.csv"
    s3_object_path = f"youku/{file_name}"
    s3_hook.load_string(csv_buffer.getvalue(), s3_object_path, bucket)


youku = OpTask(op=PythonOperator(
    task_id="api_extract",
    python_callable=youku_api,
    dag=adag,
))

dag >> youku
