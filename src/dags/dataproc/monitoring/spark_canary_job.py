from __future__ import annotations

from datetime import datetime, timedelta
from typing import Union, List, Tuple, Any, Dict

from ttd.alicloud.alicloud_instance_types import AliCloudInstanceTypes
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.alicloud.emr.alicloud_emr_versions import AliCloudEmrVersions
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.cloud_provider import CloudProviders, CloudProvider, CloudProviderMapper
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.el_dorado.v2.alicloud import AliCloudClusterTask, AliCloudJobTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.spark_databricks_task import SparkDatabricksTask
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.hdi import HDIClusterTask, HDIJobTask
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.openlineage import OpenlineageConfig
from ttd.slack.slack_groups import AIFUN
from ttd.spark import SparkWorkflow
from ttd.ttdenv import TtdEnvFactory

job_start_date = datetime(2024, 10, 1)

cloud_providers = [
    CloudProviders.aws,
    CloudProviders.azure,
    CloudProviders.ali,
    CloudProviders.databricks,
]
schedule_intervals: Dict[CloudProvider, str] = {
    CloudProviders.aws: "13,43 * * * *",
    CloudProviders.azure: "14 * * * *",
    CloudProviders.ali: "16,46 * * * *",
    CloudProviders.databricks: "17,47 * * * *",
}
schedule_interval_non_prod = None
max_active_runs = 1 if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else 5

aws_jar: str = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-dataproc-assembly.jar"
azure_jar: str = "abfs://ttd-build-artefacts@ttdartefacts.dfs.core.windows.net/eldorado/release-spark-3/main-spark-3/latest/eldorado-dataproc-assembly.jar"
ali_jar: str = "oss://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-dataproc-assembly.jar"
class_name: str = "jobs.dataproc.canary.CanaryJob"


def create_dag(cloud_provider: CloudProvider) -> TtdDag:
    name = f"spark-canary-job-{cloud_provider}"

    canary_dag = TtdDag(
        dag_id=name,
        start_date=job_start_date,
        schedule_interval=schedule_intervals[cloud_provider]
        if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else schedule_interval_non_prod,
        max_active_runs=max_active_runs,
        tags=["Canary", "Regression", "Monitoring", "Operations"],
        enable_slack_alert=False,
        run_only_latest=True,
        retry_delay=timedelta(minutes=5),
        teams_allowed_to_access=[AIFUN.team.jira_team],
    )

    cluster = create_cluster(name, cloud_provider)
    canary_dag >> cluster

    return canary_dag


def create_cluster(name: str, cloud_provider: CloudProvider) -> Union[EmrClusterTask, HDIClusterTask, AliCloudClusterTask, SparkWorkflow]:
    cluster = None
    common_eldorado_config = get_common_eldorado_config(cloud_provider)

    if cloud_provider == CloudProviders.databricks:
        cluster_tags = {"Cloud": cloud_provider.__str__()}

        cluster = DatabricksWorkflow(
            job_name=name,
            cluster_name=name,
            worker_node_type=M6g.m6g_xlarge().instance_name,
            worker_node_count=1,
            driver_node_type=M6g.m6g_xlarge().instance_name,
            databricks_spark_version=DatabricksWorkflow.DATABRICKS_SPARK_VERSION,
            ebs_config=DatabricksEbsConfiguration(ebs_volume_count=1, ebs_volume_size_gb=32),
            tasks=[
                SparkDatabricksTask(
                    job_name=name,
                    executable_path=aws_jar,
                    class_name=class_name,
                    eldorado_run_options_list=get_run_eldorado_config(),
                    openlineage_config=OpenlineageConfig(enabled=False),
                )
            ],
            eldorado_cluster_options_list=common_eldorado_config,
            cluster_tags=cluster_tags,
            use_unity_catalog=True,
        )
    else:
        cluster = get_cluster_task(name, cloud_provider)
        pipeline_step = get_job_task(cloud_provider)
        cluster.add_parallel_body_task(pipeline_step)

    return cluster


def get_common_eldorado_config(cloud_provider: CloudProvider) -> List[Tuple[str, Any]]:
    return [
        ("ttd.clusterService", CloudProviderMapper.get_cluster_service(cloud_provider).__str__()),
        ("ttd.ds.default.storageProvider", cloud_provider.__str__()),
        ("logLevel", "DEBUG"),
        ("ttd.ds.CanaryDataset.isInChain", "true"),
        ("azure.key", "eastusttdlogs,ttdairflowserviceuseast"),
        ("ttd.azure.enable", "true" if cloud_provider == CloudProviders.azure else "false"),
    ]


def get_run_eldorado_config() -> List[Tuple[str, Any]]:
    return [
        ("origin", "{{ dag_run.conf['origin']|default('scheduled') }}"),
        ("datetime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
        ("exec_id", "{{ dag_run.conf['exec_id']|default(macros.datetime.now().timestamp() | int) }}"),
    ]


def get_cluster_task(name: str, cloud_provider: CloudProvider) -> EmrClusterTask | HDIClusterTask | AliCloudClusterTask:
    etl_cluster = None

    cluster_tags = {"Cloud": cloud_provider.__str__()}

    if cloud_provider == CloudProviders.aws:
        etl_cluster = EmrClusterTask(
            name=name,
            master_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[M6g.m6g_xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
            ),
            core_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[
                    (M6g.m6g_xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)),
                ],
                on_demand_weighted_capacity=2,
            ),
            emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
            enable_prometheus_monitoring=True,
            cluster_auto_terminates=True,
            cluster_tags=cluster_tags,
        )

    if cloud_provider == CloudProviders.azure:
        etl_cluster = HDIClusterTask(
            name=name,
            vm_config=HDIVMConfig(
                headnode_type=HDIInstanceTypes.Standard_D8ads_v5(),
                workernode_type=HDIInstanceTypes.Standard_D8ads_v5(),
                num_workernode=1,
            ),
            cluster_tags=cluster_tags,
            permanent_cluster=False,
            cluster_version=HDIClusterVersions.AzureHdiSpark33,
            enable_openlineage=False
        )

    if cloud_provider == CloudProviders.ali:
        etl_cluster = AliCloudClusterTask(
            name=name,
            master_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X()
                                                               ).with_sys_disk_size_gb(60).with_data_disk_count(1).with_node_count(1),
            core_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X()
                                                             ).with_sys_disk_size_gb(60).with_data_disk_count(1).with_node_count(2),
            cluster_tags=cluster_tags,
            emr_version=AliCloudEmrVersions.ALICLOUD_EMR_SPARK_3_2
        )

    return etl_cluster


def get_job_task(cloud_provider: CloudProvider) -> EmrJobTask | HDIJobTask | AliCloudJobTask:
    pipeline_step = None
    task_name = "CanaryPipeline"
    eldorado_config = get_common_eldorado_config(cloud_provider) + get_run_eldorado_config()

    if cloud_provider == CloudProviders.aws:
        pipeline_step = EmrJobTask(
            name=task_name,
            executable_path=aws_jar,
            class_name=class_name,
            configure_cluster_automatically=True,
            eldorado_config_option_pairs_list=eldorado_config,
            cluster_calc_defaults=ClusterCalcDefaults(memory_tolerance=0.9970),
        )

    if cloud_provider == CloudProviders.azure:
        pipeline_step = HDIJobTask(
            name=task_name,
            jar_path=azure_jar,
            class_name=class_name,
            configure_cluster_automatically=True,
            eldorado_config_option_pairs_list=eldorado_config + [('openlineage.enable', 'false')],
        )

    if cloud_provider == CloudProviders.ali:
        pipeline_step = AliCloudJobTask(
            name=task_name,
            jar_path=ali_jar,
            class_name=class_name,
            configure_cluster_automatically=True,
            eldorado_config_option_pairs_list=eldorado_config,
            cluster_calc_defaults=ClusterCalcDefaults(memory_tolerance=0.9970),
        )
    return pipeline_step


for provider in cloud_providers:
    dag = create_dag(provider)
    globals()[dag.airflow_dag.dag_id] = dag.airflow_dag
