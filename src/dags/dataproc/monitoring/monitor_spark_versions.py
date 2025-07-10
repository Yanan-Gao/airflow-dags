import time
from datetime import timedelta, datetime
from typing import Dict, Any

import boto3
from airflow.hooks.base import BaseHook
from alibabacloud_emr20160408.models import DescribeClusterV2ResponseBody
from botocore.client import BaseClient

from airflow.operators.python import PythonOperator

from ttd.metrics.opentelemetry.ttdopentelemetry import get_or_register_gauge, push_all, TtdGauge
from ttd.eldorado import tag_utils
from ttd.eldorado.base import TtdDag
from ttd.hdinsight.hdi_hook import HDIHook
from ttd.alicloud.ali_hook import AliEMRHook
from ttd.tasks.op import OpTask

dag_id = 'monitor-spark-version-migration'

dag = TtdDag(
    dag_id=dag_id,
    schedule_interval=timedelta(minutes=10),
    tags=['Monitoring'],
    max_active_runs=3,
    start_date=datetime(2024, 6, 24, 16),
)


def get_emr_client(region_name: str = 'us-east-1') -> BaseClient:
    return boto3.client('emr', region_name=region_name)


def get_active_emr_clusters(emr_client: BaseClient) -> Dict[str, Dict[str, Any]]:
    active_clusters = {}
    paginator = emr_client.get_paginator('list_clusters')
    pages = paginator.paginate(ClusterStates=[
        'STARTING',
        'WAITING',
        'BOOTSTRAPPING',
        'RUNNING',
    ])
    for cluster in list(cluster for page in pages for cluster in page['Clusters']):
        details = emr_client.describe_cluster(ClusterId=cluster['Id'])
        active_clusters[cluster['Id']] = details
        time.sleep(0.5)

    return active_clusters


def push_metric(
    tags: Dict[str, str],
    cloud_provider: str,
    spark_version: str,
    jar_name: str,
    class_name: str,
):
    if tags.get('Environment') == 'prod':

        version_gauge: TtdGauge = get_or_register_gauge(
            job=dag_id,
            name='spark_cluster_version_3',
            description="Version and details about running Spark jobs",
        )

        version_gauge.labels({
            "version": spark_version,
            "source": tags.get('Source', 'UnspecifiedSource'),
            "owner": tags.get('Team', 'UNKNOWN'),
            "jar_name": jar_name,
            "spark_job": tags.get('Job', 'UnknownJob'),
            "cloud_provider": cloud_provider,
            "class_name": class_name,
        }).set(1)

        push_all(job=dag_id)

        print(
            f"METRIC PUSHED: Spark: {spark_version} - Source: {tags.get('Source')} - Team: {tags.get('Team')} - "
            f"Job: {tags.get('Job')} - Class: {class_name} - Jar: {jar_name}"
        )
    else:
        pass


def parse_emr_spark_version(cluster_properties: Dict) -> str:
    for app in cluster_properties.get('Applications', []):
        if app.get('Name').upper() == 'SPARK':
            return app.get('Version')
    print(cluster_properties.get('Applications', []))
    print(f"Couldn't find a Spark version: {cluster_properties.get('Applications', [])}")
    return 'unknown'


def monitor_emr_clusters():
    client = get_emr_client()
    clusters = get_active_emr_clusters(client)

    for cluster_id, cluster_details in clusters.items():

        tags = tag_utils.get_raw_cluster_tags(cluster_details['Cluster'].get('Tags', []))

        if tags.get('Environment') == 'prod':
            steps = client.list_steps(ClusterId=cluster_id)

            for step in steps['Steps']:
                if step['Name'] == 'Enable Debugging':
                    continue

                step_args = step['Config']['Args']

                jar_name = next((s for s in step_args if s.startswith('s3') and s.endswith('.jar')), None)

                if jar_name is None:
                    print(f"Couldn't find jar in: {step_args}!")
                    jar_name = 'UnknownJar'

                try:
                    flag_index = step_args.index('--class')
                    class_name = step_args[flag_index + 1]
                except (ValueError, IndexError):
                    print(f"Couldn't find class in: {step_args}!")
                    class_name = 'UnknownClass'

                spark_version = parse_emr_spark_version(cluster_details['Cluster'])

                push_metric(tags, 'AWS', spark_version, jar_name, class_name)

            time.sleep(0.5)


def monitor_hdi_clusters():
    import requests
    from requests.auth import HTTPBasicAuth

    hdi_hook = HDIHook()
    clusters = hdi_hook.get_clusters()

    for cluster in clusters:
        spark_version = cluster.properties.cluster_definition.component_version['Spark']
        tags = cluster.tags

        if tags.get("Environment") == "prod":
            url = f'https://{cluster.name}-int.azurehdinsight.net/sparkhistory/api/v1/applications/'
            print(url)

            azure_rest_conn = BaseHook.get_connection('azure_hdi_rest_creds')

            try:
                response = requests.get(url, auth=HTTPBasicAuth(azure_rest_conn.login, azure_rest_conn.password))
                response.raise_for_status()
                data = response.json()

                class_name = 'UnknownClass'
                for app in data:
                    name = app.get('name')
                    if name != 'Thrift JDBC/ODBC Server' and not name.startswith('SparkSQL::'):
                        class_name = app.get('name')
                        break

                push_metric(tags, 'Azure', spark_version, 'UnknownJar', class_name)

            except requests.exceptions.ConnectionError as e:
                print(f"Connection error: {e}")
                continue
            except requests.exceptions.HTTPError as e:
                print(f"HTTP error: {e}")
                continue
            except requests.exceptions.RequestException as e:
                print(f"Request exception: {e}")
                continue
        else:
            pass


def parse_ali_spark_version(description: DescribeClusterV2ResponseBody) -> str:
    softwares = description.cluster_info.software_info.softwares.software
    for software in softwares:
        if software.name == 'SPARK':
            return software.version
    print(f"Couldn't find a Spark version: {softwares}")
    return 'unknown'


def monitor_ali_clusters():
    ali_hook = AliEMRHook()
    clusters = ali_hook.get_clusters()

    for cluster in clusters:
        tags = {tag.tag_key: tag.tag_value for tag in cluster.tags.tag}

        cluster_description = ali_hook.describe_cluster(cluster_id=cluster.id)
        spark_version = parse_ali_spark_version(cluster_description)

        push_metric(tags, 'AliCloud', spark_version, 'UnknownJar', 'UnknownClass')


emr_monitoring = OpTask(
    op=PythonOperator(
        task_id='emr_monitoring',
        python_callable=monitor_emr_clusters,
        retries=3,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
        retry_exponential_backoff=True,
    )
)

hdi_monitoring = OpTask(
    op=PythonOperator(
        task_id='hdi_monitoring',
        python_callable=monitor_hdi_clusters,
        retries=3,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
        retry_exponential_backoff=True,
    )
)

ali_monitoring = OpTask(
    op=PythonOperator(
        task_id='ali_monitoring',
        python_callable=monitor_ali_clusters,
        retries=3,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
        retry_exponential_backoff=True,
    )
)

dag >> emr_monitoring
dag >> hdi_monitoring
dag >> ali_monitoring

adag = dag.airflow_dag
