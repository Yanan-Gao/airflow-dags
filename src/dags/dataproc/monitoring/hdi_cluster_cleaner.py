import logging
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from azure.core.exceptions import AzureError

from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.hdinsight.hdi_hook import HDIHook
from ttd.slack.slack_groups import dataproc
from ttd.metrics.opentelemetry.ttdopentelemetry import get_or_register_counter, push_all, TtdCounter

job_schedule_interval = "0 * * * *"
job_start_date = datetime(2023, 3, 3)
dag_name = "hdi-clusters-cleaner"
slack_channel = dataproc.alarm_channel
slack_tags = dataproc.sub_team

default_args = {
    "owner": dataproc.jira_team,
    "depends_on_past": False,
}

dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    tags=["DATAPROC", "Maintenance"],
    max_active_runs=1,
    slack_channel=slack_channel,
    slack_tags=slack_tags,
    default_args=default_args,
    enable_slack_alert=False,
)
adag = dag.airflow_dag


def delete_abandoned_clusters():
    hdi_hook = HDIHook()
    resource_group = "eldorado-rg"
    old_clusters = hdi_hook.get_old_clusters(resource_group=resource_group, filter_tags={"ClusterLifecycle": "OnDemand"})

    deleted_clusters: TtdCounter = get_or_register_counter(
        job=dag_name,
        name="hdinsight_abandoned_clusters_deleted",
        description="The number of abandoned HDInsight clusters deleted during execution of hdi-cluster-cleaner DAG.",
    )

    for cluster in old_clusters:
        logging.info(f"Deleting cluster: {cluster.name}")

        try:
            hdi_hook.delete_cluster2(resource_group=resource_group, cluster_name=cluster.name)
        except AzureError as e:
            logging.warning(f"Cluster {cluster.name} cannot be deleted", exc_info=e)

        tags = cluster.tags
        process_name = (tags.get("Job") if tags.get("Job") is not None else tags.get("Process"))

        deleted_clusters.labels({
            "env": tags.get("Environment"),
            "process": process_name,
            "source": tags.get("Source"),
            "team": tags.get("Team"),
        }).inc()

    push_all(job=dag_name)


delete_abandoned_clusters = OpTask(
    op=PythonOperator(
        task_id="delete-abandoned-clusters",
        python_callable=delete_abandoned_clusters,
        dag=adag,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
)

dag >> delete_abandoned_clusters
