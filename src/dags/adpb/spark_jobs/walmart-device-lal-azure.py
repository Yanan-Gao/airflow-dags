from datetime import datetime

from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.hdi import HDIClusterTask, HDIJobTask
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask

# Prod variables

job_start_date = datetime(2024, 8, 11, 18, 0, 0)
dag_name = "adpb-walmart-device-lal-azure"
job_schedule_interval = "0 18 * * *"
job_slack_channel = "#scrum-adpb-alerts"
aerospike_hosts = '{{macros.ttd_extras.resolve_consul_url("ttd-lal.aerospike.service.va9.consul", port=None, limit=1)}}'
jar_path = "abfs://ttd-build-artefacts@ttdartefacts.dfs.core.windows.net/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=job_slack_channel,
    run_only_latest=True
)

airflow_dag = dag.airflow_dag

sibv3_device_cluster_name = 'sibv3-walmart-device-lal'

cluster_tags = {
    "Purpose": "LAL",
    "Team": "ADPB",
}

run_date = '{{ data_interval_end.strftime("%Y-%m-%d") }}'
run_time = '{{ data_interval_end.strftime("%Y-%m-%dT%H:%M:%S") }}'
aerospike_truststore_password = "{{ conn.aerospike_truststore_password.get_password() }}"

# LAL cluster

lal_cluster = HDIClusterTask(
    name=sibv3_device_cluster_name,
    vm_config=HDIVMConfig(
        headnode_type=HDIInstanceTypes.Standard_D32A_v4(),
        workernode_type=HDIInstanceTypes.Standard_D32A_v4(),
        num_workernode=3,
        disks_per_node=1
    ),
    cluster_tags=cluster_tags,
    cluster_version=HDIClusterVersions.AzureHdiSpark33,
    enable_openlineage=False
)

lal_options = [
    ("sibDateHourToProcess", run_time),
    ("sibDateToProcess", run_date),
    ("runDate", run_date),
    ("date", run_date),
    ("availCoresPerNode", "15"),
    ("numNodes", "2"),
    ('innerParallelism', '16'),
    ('partitionMultiplier', '4'),
    ("isRestrictedDataPipeline", "true"),
    ("ttd.defaultcloudprovider", "azure"),
    ("ttd.ds.default.cloudprovider", "azure"),
    ('generatePrometheusMetrics', 'true'),
    ("azure.key", "eastusttdlogs,ttdexportdata"),
    ("ttd.ds.SelectedPixelsDataSet.isInChain", "true"),
    ("ttd.ds.TargetingDataPermissionsDataSet.isInChain", "true"),
    ("ttd.ds.CountsDataSet.isInChain", "true"),
    ("ttd.ds.AllModelResultsDataSet.isInChain", "true"),
    ('javax.net.ssl.trustStore', '/tmp/ttd-internal-root-ca-truststore.jks'),
    ('aerospikeSet', 'lal'),
    ('aerospikeNamespace', 'ttd-lal'),
    ('aerospikeHosts', aerospike_hosts),
    ('aerospike-port', 3000),
    ('tls-name', ''),
    ('openlineage.enable', 'false'),
]

lal_hdi_step_spark_options_list = [
    ("spark.yarn.maxAppAttempts", 1), ("spark.sql.files.ignoreCorruptFiles", "true"), ("spark.executor.cores", 15),
    ("spark.executor.memory", "45G"), ("spark.driver.memory", "45G"), ("spark.driver.cores", 15),
    ("conf", "spark.dynamicAllocation.enabled=false"), ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED"),
    (
        'spark.executor.extraJavaOptions',
        f'-Djavax.net.ssl.trustStorePassword={aerospike_truststore_password} -Djavax.net.ssl.trustStore=/tmp/ttd-internal-root-ca-truststore.jks'
    )
]

uniques_and_overlaps_task = HDIJobTask(
    name="Walmart_Uniques_And_Overlaps",
    class_name="jobs.agg_etl.WalmartDeviceUniquesAndOverlapsJob",
    cluster_specs=lal_cluster.cluster_specs,
    eldorado_config_option_pairs_list=lal_options,
    additional_args_option_pairs_list=lal_hdi_step_spark_options_list,
    jar_path=jar_path,
)

pixel_selection_and_permission_step = HDIJobTask(
    name="LAL_Pixel_Selection",
    class_name="jobs.lal.device.WalmartDevicePixelSelectionAndPermissionComputer",
    cluster_specs=lal_cluster.cluster_specs,
    eldorado_config_option_pairs_list=lal_options,
    additional_args_option_pairs_list=lal_hdi_step_spark_options_list,
    configure_cluster_automatically=True,
    jar_path=jar_path,
)

counts_step = HDIJobTask(
    name="LAL_Counts",
    class_name="jobs.lal.device.CountsServiceBasedWalmartDeviceTargetingDataCountJob",
    cluster_specs=lal_cluster.cluster_specs,
    eldorado_config_option_pairs_list=lal_options,
    additional_args_option_pairs_list=lal_hdi_step_spark_options_list,
    jar_path=jar_path,
)

ratio_compute_step = HDIJobTask(
    name="LAL_Ratio_Computer",
    class_name="jobs.lal.device.WalmartDeviceRelevanceRatioComputer",
    cluster_specs=lal_cluster.cluster_specs,
    eldorado_config_option_pairs_list=lal_options,
    additional_args_option_pairs_list=lal_hdi_step_spark_options_list,
    configure_cluster_automatically=True,
    jar_path=jar_path,
)

lal_cluster.add_sequential_body_task(uniques_and_overlaps_task)
lal_cluster.add_sequential_body_task(pixel_selection_and_permission_step)
lal_cluster.add_sequential_body_task(counts_step)
lal_cluster.add_sequential_body_task(ratio_compute_step)

# Write cluster

write_cluster = HDIClusterTask(
    name="walmart-lal-write",
    vm_config=HDIVMConfig(
        headnode_type=HDIInstanceTypes.Standard_D32A_v4(),
        workernode_type=HDIInstanceTypes.Standard_D96A_v4(),
        num_workernode=2,
        disks_per_node=1
    ),
    cluster_tags=cluster_tags,
    cluster_version=HDIClusterVersions.AzureHdiSpark33,
    enable_openlineage=False
)

write_step = HDIJobTask(
    name="LAL_WriteToAerospike",
    class_name="jobs.lal.PublishToAzureAerospikeLal",
    cluster_specs=write_cluster.cluster_specs,
    eldorado_config_option_pairs_list=lal_options,
    additional_args_option_pairs_list=lal_hdi_step_spark_options_list,
    configure_cluster_automatically=True,
    jar_path=jar_path,
)

write_cluster.add_sequential_body_task(write_step)

final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=airflow_dag))

dag >> lal_cluster >> write_cluster >> final_dag_check
