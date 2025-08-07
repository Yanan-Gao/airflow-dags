import logging
from collections import defaultdict
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.eldorado.base import TtdDag
from ttd.metrics.metric_pusher import MetricPusher
from ttd.metrics.opentelemetry.ttdopentelemetry import get_or_register_gauge, push_all, TtdGauge
from ttd.tasks.op import OpTask

default_args = {
    "owner": "dataproc",
    "start_date": datetime(2023, 5, 11),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

dag_name = "monitor-hdi-resources"


def monitor_hdi_quota_usage() -> None:
    from ttd.hdinsight.hdi_hook import HDIHook

    hdi_hook = HDIHook()
    client = hdi_hook.client
    usages = client.locations.list_usages(hdi_hook.region)
    cores_usages = [usage for usage in usages.value if usage.name.value == "cores" or usage.current_value != 0]

    logging.info("Pushing HDInsight to Prometheus cores usage info")

    cores_usage_current_gauge: TtdGauge = get_or_register_gauge(
        job=dag_name,
        name="hdinsight_cores_usage_current",
        description="Total Regional vCPUs usage",
    )
    cores_usage_limit_gauge = get_or_register_gauge(
        job=dag_name,
        name="hdinsight_cores_usage_limit",
        description="Total Regional vCPUs limit",
    )

    for usage in cores_usages:
        cores_usage_current_gauge.labels({"name": usage.name.value, "region": hdi_hook.region}).set(usage.current_value)
        cores_usage_limit_gauge.labels({"name": usage.name.value, "region": hdi_hook.region}).set(usage.limit)

    push_all(job=dag_name)


def monitor_hdi_resource_usage() -> None:
    from ttd.hdinsight.hdi_hook import HDIHook

    hdi_hook = HDIHook()
    clusters = hdi_hook.get_clusters()
    total_memory = 0
    total_vcpus = 0

    for cluster in clusters:
        logging.info(cluster.name)
        instance_types = defaultdict(int)
        memory = 0

        for role in cluster.properties.compute_profile.roles:
            instance_types[role.hardware_profile.vm_size] = role.target_instance_count

        for instance_type_name, count in instance_types.items():
            instance_type = HDIInstanceTypes.get_from_name(instance_type_name)

            if instance_type is None:
                logging.warning(f"Cluster {cluster.name} has instancetype {instance_type_name} which isn't defined in code")
            else:
                memory += count * instance_type.memory

        total_memory += memory
        total_vcpus += cluster.properties.quota_info.cores_used

    labels = {
        "Memory": total_memory,
        "Cores": total_vcpus,
    }

    metric_pusher = MetricPusher()
    metric_pusher.push('hdi_resource_usage', total_memory, labels)


quota_usage = OpTask(op=PythonOperator(
    task_id="monitor_hdi_quota_usage",
    python_callable=monitor_hdi_quota_usage,
))

resource_usage = OpTask(op=PythonOperator(
    task_id="monitor_hdi_resource_usage",
    python_callable=monitor_hdi_resource_usage,
))

dag = TtdDag(
    dag_id=dag_name,
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    tags=["Monitoring", "Operations", "DATAPROC"],
    run_only_latest=True,
)

adag = dag.airflow_dag

dag >> [quota_usage, resource_usage]
