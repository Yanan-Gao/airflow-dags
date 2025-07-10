from datetime import datetime

from ttd.eldorado.base import TtdDag
from ttd.eldorado.hdi import HDIClusterTask, HDIJobTask
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]
additional_args = [("spark.driver.maxResultSize", "2g")]

job_schedule_interval = "0 0 1 1 *"
job_start_date = datetime(2022, 3, 8, 17, 00)

name = "demo-eldorado-v2-azure"

dag = TtdDag(
    dag_id=name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    tags=["DATAPROC", "Demo"],
)

adag = dag.airflow_dag

cluster_task = HDIClusterTask(
    name="demo-eldorado-v2-azure-cluster",
    vm_config=HDIVMConfig(
        headnode_type=HDIInstanceTypes.Standard_D12_v2(),
        workernode_type=HDIInstanceTypes.Standard_A8_v2(),
        num_workernode=2,
        disks_per_node=1,
    ),
    cluster_version=HDIClusterVersions.AzureHdiSpark33,
)

job_task = HDIJobTask(
    name="dummy-job",
    class_name="jobs.dataproc.datalake.DummySparkJob",
    cluster_specs=cluster_task.cluster_specs,
    eldorado_config_option_pairs_list=java_settings_list + [
        ("aggType", "hour"),
        ("logLevel", "DEBUG"),
        ("runtime", '{{ execution_date.strftime("%Y-%m-%dT%H:00:00") }}'),
        ("ttd.defaultcloudprovider", "azure"),
    ],
    additional_args_option_pairs_list=additional_args,
    configure_cluster_automatically=True,
    command_line_arguments=["--version"],
)

cluster_task.add_parallel_body_task(job_task)

dag >> cluster_task
