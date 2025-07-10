from datetime import datetime

from ttd.alicloud.emr.alicloud_emr_versions import AliCloudEmrVersions
from ttd.eldorado.base import TtdDag
from ttd.eldorado.alicloud import (
    AliCloudClusterTask,
    AliCloudJobTask,
)
from ttd.alicloud.alicloud_instance_types import AliCloudInstanceTypes
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes

additional_args = [("spark.driver.maxResultSize", "2g")]

job_schedule_interval = "0 12 * * *"
job_start_date = datetime(2022, 12, 6)

name = "demo-eldorado-v2-ali"

dag = TtdDag(
    dag_id=name,
    start_date=job_start_date,
    schedule_interval=None,
    tags=["DATAPROC", "Demo"],
)

adag = dag.airflow_dag
cluster_task = AliCloudClusterTask(
    name="demo-eldorado-v2-ali-cluster",
    master_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X(
    )).with_node_count(1).with_data_disk_count(1).with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
    core_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X()).with_node_count(2).with_data_disk_count(4)
    .with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
    emr_version=AliCloudEmrVersions.ALICLOUD_EMR_SPARK_3_2,
)

job_task = AliCloudJobTask(
    name="dummy-job",
    jar_path="oss://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-dataproc-assembly.jar",
    class_name="com.thetradedesk.spark.jobs.AliSparkTest",
    cluster_spec=cluster_task.cluster_specs,
    additional_args_option_pairs_list=additional_args,
    configure_cluster_automatically=True,
    command_line_arguments=["--version"],
)

cluster_task.add_parallel_body_task(job_task)

dag >> cluster_task
