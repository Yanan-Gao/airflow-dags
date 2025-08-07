from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import List

from dags.dataproc.datalake.datalake_logs_gate_sensor import DatalakeLogsGateSensor
from dags.dataproc.datalake.datalake_parquet_utils import LogNames, CloudJobConfig
from datasources.datasources import Datasources
from ttd.alicloud.alicloud_instance_types import AliCloudInstanceTypes
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.alicloud.emr.alicloud_emr_versions import AliCloudEmrVersions
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.cloud_provider import CloudProviders
from ttd.cluster_service import ClusterServices, ClusterService
from ttd.ec2.emr_instance_class import EmrInstanceClass
from ttd.ec2.cluster_params import ClusterCalcDefaults, Defaults
from ttd.ec2.emr_instance_types.compute_optimized.graviton import ComputeOptimisedGraviton
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.graviton import MemoryOptimisedGraviton
from ttd.ec2.emr_instance_types.memory_optimized.graviton_disk import MemoryOptimisedGravitonDisk
from ttd.el_dorado.v2.alicloud import AliCloudClusterTask, AliCloudJobTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.aws.allocation_strategies import OnDemandStrategy, AllocationStrategyConfiguration
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.hdi import HDIClusterTask, HDIJobTask
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack import slack_groups
from ttd.slack.slack_groups import AIFUN
from ttd.tasks.op import OpTask

job_schedule_interval = "0 * * * *"
job_start_date = datetime(2025, 5, 8, 00, 00)

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]
additional_args = [("conf", "spark.driver.maxResultSize=2g")]

aws_jar: str = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-dataproc-assembly.jar"
azure_jar: str = "abfs://ttd-build-artefacts@ttdartefacts.dfs.core.windows.net/eldorado/release-spark-3/main-spark-3/latest/eldorado-dataproc-assembly.jar"
ali_jar: str = "oss://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-dataproc-assembly.jar"


def preemptively_create_cluster(cluster_service: "ClusterService") -> bool:
    return cluster_service == ClusterServices.HDInsight


def create_dag(logname: str, job_config: CloudJobConfig):
    name = f"datalake3-{logname.lower()}-{job_config.provider.__str__()}"

    logging.info(f'job_config.provider: ${job_config.provider}, clusterService: ${job_config.cluster_service}')

    datalake_dag = TtdDag(
        dag_id=name,
        start_date=job_start_date,
        schedule_interval=job_schedule_interval,
        tags=["Datalake", "DATAPROC"],
        max_active_runs=48,
        max_active_tasks=48 * 2,
        dagrun_timeout=timedelta(hours=48),
        default_args={"owner": "DATAPROC"},
        enable_slack_alert=False,
        teams_allowed_to_access=[AIFUN.team.jira_team],
    )

    logs_gate_sensor = OpTask(
        op=DatalakeLogsGateSensor(
            logname=logname,
            cloud_provider=job_config.provider,
            task_id="logs_gate_sensor",
            poke_interval=60,  # in seconds
            timeout=60 * 60 * 24,  # in seconds
            mode="reschedule",
        ),
    )

    etl_cluster = get_cluster_task(name, job_config, logs_gate_sensor)
    pipeline_step = get_job_task(logname, job_config)
    etl_cluster.add_parallel_body_task(pipeline_step)

    if preemptively_create_cluster(job_config.cluster_service):
        datalake_dag >> [etl_cluster, logs_gate_sensor]
        logs_gate_sensor >> etl_cluster.first_body_tasks
    else:
        datalake_dag >> logs_gate_sensor >> etl_cluster

    check_hour_completed = OpTask(
        op=DatasetCheckSensor(
            datasets=[Datasources.rtb_datalake.get_by_logname(logname).as_write()],
            ds_date='{{execution_date.to_datetime_string()}}',
            poke_interval=60 * 7,  # in seconds
            timeout=60 * 60 * 24,  # in seconds
            cloud_provider=job_config.provider,
        ),
    )

    metrics_cluster = get_metrics_cluster_task(name, job_config)
    metrics_step = get_metrics_job_task(logname, job_config)
    metrics_cluster.add_parallel_body_task(metrics_step)

    etl_cluster >> check_hour_completed >> metrics_cluster

    return datalake_dag


def get_cluster_task(
    name: str, job_config: CloudJobConfig, logs_gate_sensor: OpTask
) -> EmrClusterTask | HDIClusterTask | AliCloudClusterTask:
    etl_cluster = None
    cluster_tags = {"Cloud": job_config.provider.__str__(), "Team": slack_groups.dataproc.jira_team}

    if job_config.cluster_service == ClusterServices.AwsEmr:
        etl_cluster = EmrClusterTask(
            name=name,
            master_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_classes=[MemoryOptimisedGraviton.g_2xlarge().with_ebs_size_gb(20).with_weighted_capacity(1)],
                on_demand_weighted_capacity=1,
            ),
            core_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_classes=emr_fleet_instance_classes(job_config.capacity, job_config.base_disk_space),
                on_demand_weighted_capacity=job_config.capacity,
                allocation_strategy=AllocationStrategyConfiguration(on_demand=OnDemandStrategy.Prioritized),
            ),
            emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4,
            enable_prometheus_monitoring=True,
            cluster_auto_terminates=True,
            cluster_tags=cluster_tags,
            custom_java_version=17,
        )  # yapf: disable

    if job_config.cluster_service == ClusterServices.HDInsight:
        etl_cluster = HDIClusterTask(
            name=name,
            vm_config=HDIVMConfig(
                headnode_type=HDIInstanceTypes.Standard_E8_v3(),
                workernode_type=HDIInstanceTypes.Standard_D32ads_v5(),
                num_workernode=job_config.capacity,
                disks_per_node=job_config.disks_per_hdi_vm,
            ),
            cluster_tags=cluster_tags,
            permanent_cluster=job_config.hdi_permanent_cluster,
            cluster_version=HDIClusterVersions.AzureHdiSpark33,
            enable_openlineage=False,
            retries=6,
            retry_exponential_backoff=True,
        )

    if job_config.cluster_service == ClusterServices.AliCloudEmr:
        etl_cluster = AliCloudClusterTask(
            name=name,
            master_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X(
            )).with_node_count(1).with_data_disk_count(1).with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
            core_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X()).with_node_count(job_config.capacity)
            .with_data_disk_count(2).with_data_disk_size_gb(job_config.base_disk_space).with_sys_disk_size_gb(60),
            cluster_tags=cluster_tags,
            emr_version=AliCloudEmrVersions.ALICLOUD_EMR_SPARK_3_2,
        )

    return etl_cluster


def get_job_task(logname: str, job_config: CloudJobConfig) -> EmrJobTask | HDIJobTask | AliCloudJobTask:
    pipeline_step = None
    task_name = "EtlPipeline"
    class_name = f"jobs.dataproc.datalake.{logname}DatalakeEtlPipeline"
    common_eldorado_config = java_settings_list + [("runtime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
                                                   ("ttd.defaultcloudprovider", job_config.provider.__str__()),
                                                   ("coalesceSize", job_config.output_files)]

    if job_config.cluster_service == ClusterServices.AwsEmr:
        pipeline_step = EmrJobTask(
            name=task_name,
            executable_path=aws_jar,
            class_name=class_name,
            configure_cluster_automatically=True,
            eldorado_config_option_pairs_list=common_eldorado_config +
            [("ttd.azure.enable", "true" if job_config.provider == CloudProviders.azure else "false"), ("azure.key", "azure-account-key")],
            additional_args_option_pairs_list=additional_args,
            cluster_calc_defaults=ClusterCalcDefaults(memory_tolerance=0.9970),
        )

    if job_config.cluster_service == ClusterServices.HDInsight:
        pipeline_step = HDIJobTask(
            name=task_name,
            jar_path=azure_jar,
            class_name=class_name,
            configure_cluster_automatically=True,
            eldorado_config_option_pairs_list=common_eldorado_config + [("ttd.azure.enable", "true"), ('openlineage.enable', 'false')],
            additional_args_option_pairs_list=additional_args,
            command_line_arguments=['--version'],
            watch_step_timeout=timedelta(hours=3),
        )

    if job_config.cluster_service == ClusterServices.AliCloudEmr:
        pipeline_step = AliCloudJobTask(
            name=task_name,
            jar_path=ali_jar,
            class_name=class_name,
            configure_cluster_automatically=True,
            eldorado_config_option_pairs_list=common_eldorado_config + [("ttd.azure.enable", "false"), ("openlineage.enable", "false")],
            additional_args_option_pairs_list=additional_args + [("spark.hadoop.fs.oss.https.enable", "true")],
            cluster_calc_defaults=ClusterCalcDefaults(memory_tolerance=0.9970),
        )
    return pipeline_step


def get_metrics_cluster_task(name: str, job_config: CloudJobConfig) -> EmrClusterTask | HDIClusterTask | AliCloudClusterTask:
    metrics_cluster = None
    task_name = f"{name}-metrics"
    cluster_tags = {"Cloud": job_config.provider.__str__(), "Team": slack_groups.dataproc.jira_team}

    if job_config.cluster_service == ClusterServices.AwsEmr:
        metrics_cluster = EmrClusterTask(
            name=task_name,
            master_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[M7g.m7g_xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)],
                on_demand_weighted_capacity=1,
            ),
            core_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_classes=[
                    (
                        ComputeOptimisedGraviton.c_2xlarge().with_ebs_size_gb(job_config.metrics_base_disk_space * 2).with_ebs_iops(10000)
                        .with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC).with_weighted_capacity(2)
                    ),
                    (
                        ComputeOptimisedGraviton.c_4xlarge().with_ebs_size_gb(job_config.metrics_base_disk_space * 4).with_ebs_iops(10000)
                        .with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC).with_weighted_capacity(4)
                    ),
                    (
                        ComputeOptimisedGraviton.c_8xlarge().with_ebs_size_gb(job_config.metrics_base_disk_space * 8).with_ebs_iops(10000)
                        .with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC).with_weighted_capacity(8)
                    ),
                    (
                        ComputeOptimisedGraviton.c_12xlarge().with_ebs_size_gb(job_config.metrics_base_disk_space * 12)
                        .with_ebs_iops(10000, ).with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC).with_weighted_capacity(12)
                    ),
                ],
                on_demand_weighted_capacity=job_config.metrics_capacity,
            ),
            emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_4,
            enable_prometheus_monitoring=True,
            cluster_auto_terminates=True,
            cluster_tags=cluster_tags,
            custom_java_version=17,
        )

    if job_config.cluster_service == ClusterServices.HDInsight:
        metrics_cluster = HDIClusterTask(
            name=task_name,
            vm_config=HDIVMConfig(
                headnode_type=HDIInstanceTypes.Standard_E8_v3(),
                workernode_type=HDIInstanceTypes.Standard_F8(),
                num_workernode=job_config.metrics_capacity,
                disks_per_node=job_config.metrics_disks_per_hdi_vm,
            ),
            cluster_tags=cluster_tags,
            permanent_cluster=job_config.hdi_permanent_cluster,
            cluster_version=HDIClusterVersions.AzureHdiSpark33,
            enable_openlineage=False,
        )

    if job_config.cluster_service == ClusterServices.AliCloudEmr:
        metrics_cluster = AliCloudClusterTask(
            name=task_name,
            master_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X(
            ), ).with_node_count(1).with_data_disk_count(1).with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
            core_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X()).with_node_count(2).with_data_disk_count(2)
            .with_data_disk_size_gb(job_config.base_disk_space).with_sys_disk_size_gb(60),
            cluster_tags=cluster_tags,
            emr_version=AliCloudEmrVersions.ALICLOUD_EMR_SPARK_3_2,
        )
    return metrics_cluster


def get_metrics_job_task(logname: str, job_config: CloudJobConfig) -> EmrJobTask | HDIJobTask | AliCloudJobTask:
    metrics_step = None
    task_name = "Metrics"
    class_name = f"jobs.dataproc.datalake.{logname}DatalakeEtlPipelineMetrics"
    common_eldorado_config = java_settings_list + [
        ("dataTime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
        ("ttd.defaultcloudprovider", job_config.provider.__str__()),
        ("aggType", "hour"),
        # ("logLevel", "DEBUG"),
        Datasources.rtb_datalake.get_by_logname(logname).in_chain_config
    ]

    if job_config.cluster_service == ClusterServices.AwsEmr:
        metrics_step = EmrJobTask(
            name=task_name,
            executable_path=aws_jar,
            class_name=class_name,
            configure_cluster_automatically=True,
            eldorado_config_option_pairs_list=common_eldorado_config +
            [("ttd.azure.enable", "true" if job_config.provider == CloudProviders.azure else "false"), ("azure.key", "azure-account-key")],
            additional_args_option_pairs_list=additional_args,
            cluster_calc_defaults=ClusterCalcDefaults(memory_tolerance=0.9970),
        )

    if job_config.cluster_service == ClusterServices.HDInsight:
        metrics_step = HDIJobTask(
            name=task_name,
            jar_path=azure_jar,
            class_name=class_name,
            configure_cluster_automatically=True,
            eldorado_config_option_pairs_list=common_eldorado_config + [("ttd.azure.enable", "true"), ('openlineage.enable', 'false')],
            additional_args_option_pairs_list=additional_args,
            command_line_arguments=['--version'],
        )

    if job_config.cluster_service == ClusterServices.AliCloudEmr:
        metrics_step = AliCloudJobTask(
            name=task_name,
            jar_path=ali_jar,
            class_name=class_name,
            configure_cluster_automatically=True,
            eldorado_config_option_pairs_list=common_eldorado_config,
            additional_args_option_pairs_list=additional_args,
            cluster_calc_defaults=ClusterCalcDefaults(memory_tolerance=0.9970),
        )

    return metrics_step


def emr_fleet_instance_classes(capacity: int, base_disk_space: int) -> List[EmrInstanceClass]:
    # yapf: disable
    _4xlarge = [
        MemoryOptimisedGravitonDisk.gd_4xlarge().with_weighted_capacity(1),
        (MemoryOptimisedGraviton.g_4xlarge()
         .with_ebs_size_gb(base_disk_space * 1)
         .with_ebs_iops(Defaults.MAX_GP3_IOPS)
         .with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC)
         .with_weighted_capacity(1)
         ),
    ]
    _8xlarge = [
        MemoryOptimisedGravitonDisk.gd_8xlarge().with_weighted_capacity(2),
        (MemoryOptimisedGraviton.g_8xlarge()
         .with_ebs_size_gb(base_disk_space * 2)
         .with_ebs_iops(Defaults.MAX_GP3_IOPS)
         .with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC)
         .with_weighted_capacity(2)
         ),

    ]
    _12xlarge = [
        MemoryOptimisedGravitonDisk.gd_12xlarge().with_weighted_capacity(3),
        (MemoryOptimisedGraviton.g_12xlarge()
         .with_ebs_size_gb(base_disk_space * 3)
         .with_ebs_iops(Defaults.MAX_GP3_IOPS)
         .with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC)
         .with_weighted_capacity(3)
         ),
    ]
    _16xlarge = [
        MemoryOptimisedGravitonDisk.gd_16xlarge().with_weighted_capacity(4),
        (MemoryOptimisedGraviton.g_16xlarge()
         .with_ebs_size_gb(base_disk_space * 4)
         .with_ebs_iops(Defaults.MAX_GP3_IOPS)
         .with_ebs_throughput(Defaults.MAX_GP3_THROUGHPUT_MIB_PER_SEC)
         .with_weighted_capacity(4)
         ),
    ]
    # yapf: enable

    if capacity <= 10:
        return _4xlarge + _8xlarge
    else:
        return _8xlarge + _12xlarge + _16xlarge


# Some source logs don't exist in Azure Blob yet
dag_info = [
    (
        LogNames.bidfeedback,
        [
            CloudJobConfig(CloudProviders.aws, capacity=150, metrics_capacity=2, base_disk_space=180, output_files=3000),
            CloudJobConfig(CloudProviders.azure, capacity=8, metrics_capacity=2, output_files=300),
            CloudJobConfig(CloudProviders.ali, capacity=5, metrics_capacity=2, base_disk_space=40, output_files=100),
        ],
    ),
    (
        LogNames.bidfeedbackverticaload,
        [
            CloudJobConfig(CloudProviders.aws, capacity=24, metrics_capacity=2, base_disk_space=180, output_files=1000),
        ],
    ),
    (
        LogNames.bidrequest,
        [
            CloudJobConfig(CloudProviders.aws, capacity=600, metrics_capacity=5, base_disk_space=300, output_files=7000),
            CloudJobConfig(
                CloudProviders.azure,
                capacity=30,
                metrics_capacity=2,
                disks_per_hdi_vm=1,
                output_files=600,
                hdi_permanent_cluster=True,
            ),
            CloudJobConfig(CloudProviders.ali, capacity=8, metrics_capacity=2, base_disk_space=40, output_files=100),
        ],
    ),
    (
        LogNames.clicktracker,
        [
            CloudJobConfig(CloudProviders.aws, capacity=1, metrics_capacity=2, output_files=200),
            CloudJobConfig(CloudProviders.azure, capacity=1, metrics_capacity=2, output_files=100),
            CloudJobConfig(CloudProviders.ali, capacity=2, metrics_capacity=2, base_disk_space=40, output_files=100),
        ],
    ),
    (
        LogNames.clicktrackerverticaload,
        [
            CloudJobConfig(CloudProviders.aws, capacity=1, metrics_capacity=2, output_files=200),
        ],
    ),
    (
        LogNames.controlbidrequest,
        [CloudJobConfig(CloudProviders.aws, capacity=6, metrics_capacity=2, base_disk_space=150, output_files=300)],
    ),
    (
        LogNames.conversiontracker,
        [CloudJobConfig(CloudProviders.aws, capacity=15, metrics_capacity=2, base_disk_space=150, output_files=350)],
    ),
    (
        LogNames.attributedeventverticaload,
        [CloudJobConfig(CloudProviders.aws, capacity=16, metrics_capacity=2, output_files=250)],
    ),
    (
        LogNames.attributedeventresultverticaload,
        [CloudJobConfig(CloudProviders.aws, capacity=16, metrics_capacity=2, output_files=250)],
    ),
    (
        LogNames.attributedeventdataelementverticaload,
        [CloudJobConfig(CloudProviders.aws, capacity=16, metrics_capacity=2, output_files=250)],
    ),
    (
        LogNames.conversiontrackerverticaload,
        [CloudJobConfig(CloudProviders.aws, capacity=16, metrics_capacity=2, output_files=250)],
    ),
    (
        LogNames.eventtrackerverticaload,
        [CloudJobConfig(CloudProviders.aws, capacity=4, metrics_capacity=2, output_files=300)],
    ),
    (
        LogNames.videoevent, [
            CloudJobConfig(CloudProviders.aws, capacity=170, metrics_capacity=2, base_disk_space=230, output_files=7000),
            CloudJobConfig(CloudProviders.azure, capacity=27, metrics_capacity=2, disks_per_hdi_vm=1, output_files=500),
            CloudJobConfig(CloudProviders.ali, capacity=2, metrics_capacity=2, base_disk_space=40, output_files=100),
        ]
    ),
]

for (logname, cloud_configs) in dag_info:
    config: CloudJobConfig
    for config in cloud_configs:
        dag = create_dag(logname, config)
        globals()[dag.airflow_dag.dag_id] = dag.airflow_dag
