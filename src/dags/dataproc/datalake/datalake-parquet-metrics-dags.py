from typing import Union

from datetime import datetime, timedelta

import ttd.slack.slack_groups
from dags.dataproc.datalake.datalake_parquet_utils import MetricCloudJobConfig, LogNames
from datasources.datasources import Datasources
from ttd.alicloud.alicloud_instance_types import AliCloudInstanceTypes
from ttd.alicloud.eldorado_alicloud_instance_types import ElDoradoAliCloudInstanceTypes
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.cloud_provider import CloudProviders
from ttd.cluster_service import ClusterServices
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.compute_optimized.c6g import C6g
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.el_dorado.v2.alicloud import AliCloudClusterTask, AliCloudJobTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.el_dorado.v2.hdi import HDIClusterTask, HDIJobTask
from ttd.alicloud.emr.alicloud_emr_versions import AliCloudEmrVersions
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask
from ttd.slack.slack_groups import AIFUN

job_schedule_interval = "0 5 * * *"
job_start_date = datetime(2025, 3, 4, 00, 00)

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]
additional_args = [("conf", "spark.driver.maxResultSize=2g")]
days_to_aggregate = 4

aws_jar: str = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-dataproc-assembly.jar"
azure_jar: str = "abfs://ttd-build-artefacts@ttdartefacts.dfs.core.windows.net/eldorado/release-spark-3/main-spark-3/latest/eldorado-dataproc-assembly.jar"
ali_jar: str = "oss://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-dataproc-assembly.jar"


def create_dag(logname: str, job_config: MetricCloudJobConfig):
    name = f"datalake3-{logname.lower()}-metrics-{job_config.provider}"

    datalake_dag = TtdDag(
        dag_id=name,
        start_date=job_start_date,
        schedule_interval=job_schedule_interval,
        tags=["Datalake", "DATAPROC"],
        max_active_runs=3,
        default_args={"owner": "DATAPROC"},
        enable_slack_alert=False,
        teams_allowed_to_access=[AIFUN.team.jira_team]
    )

    check_day_completed = OpTask(
        op=DatasetCheckSensor(
            task_id=f"dataset_check-{days_to_aggregate}-days",
            datasets=[Datasources.rtb_datalake.get_by_logname(logname).with_check_type('day').as_write()],
            lookback=days_to_aggregate - 1,
            ds_date="{{ data_interval_start.strftime(\"%Y-%m-%d %H:00:00\") }}",
            poke_interval=60 * 5,  # in seconds
            timeout=60 * 60 * 6,  # in seconds
            cloud_provider=job_config.provider
        )
    )

    cluster = get_cluster_task(name, job_config)

    for lookback in range(days_to_aggregate):
        day_metrics_step = get_job_task(logname, lookback, job_config)
        cluster.add_sequential_body_task(day_metrics_step) if job_config.provider == CloudProviders.azure \
            else cluster.add_parallel_body_task(day_metrics_step)

    datalake_dag >> check_day_completed >> cluster
    final_dag_check = OpTask(op=FinalDagStatusCheckOperator(dag=datalake_dag.airflow_dag))
    cluster >> final_dag_check

    return datalake_dag


def get_cluster_task(name: str, job_config: MetricCloudJobConfig) -> Union[EmrClusterTask, HDIClusterTask, AliCloudClusterTask]:
    cluster = None
    cluster_tags = {
        "Cloud": job_config.provider.__str__(),
        "Team": ttd.slack.slack_groups.dataproc.jira_team,
    }

    if job_config.cluster_service == ClusterServices.AwsEmr:
        cluster = EmrClusterTask(
            name=name,
            master_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[M6g.m6g_xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
            ),
            core_fleet_instance_type_configs=EmrFleetInstanceTypes(
                instance_types=[
                    (C6g.c6g_2xlarge().with_ebs_size_gb(job_config.base_disk_space * 2).with_fleet_weighted_capacity(2)),
                    (C6g.c6g_4xlarge().with_ebs_size_gb(job_config.base_disk_space * 4).with_fleet_weighted_capacity(4)),
                    (C6g.c6g_8xlarge().with_ebs_size_gb(job_config.base_disk_space * 8).with_fleet_weighted_capacity(8)),
                ],
                on_demand_weighted_capacity=job_config.capacity
            ),
            emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
            enable_prometheus_monitoring=True,
            cluster_auto_terminates=False,
            cluster_tags=cluster_tags
        )

    if job_config.cluster_service == ClusterServices.HDInsight:
        cluster = HDIClusterTask(
            name=name,
            vm_config=HDIVMConfig(
                headnode_type=HDIInstanceTypes.Standard_E8_v3(),
                workernode_type=HDIInstanceTypes.Standard_F8(),
                num_workernode=job_config.capacity,
                disks_per_node=job_config.disks_per_hdi_vm
            ),
            cluster_tags=cluster_tags,
            permanent_cluster=job_config.hdi_permanent_cluster,
            cluster_version=HDIClusterVersions.AzureHdiSpark33,
            enable_openlineage=False
        )

    if job_config.cluster_service == ClusterServices.AliCloudEmr:
        cluster = AliCloudClusterTask(
            name=name,
            master_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_X(
            )).with_node_count(1).with_data_disk_count(1).with_data_disk_size_gb(40).with_sys_disk_size_gb(60),
            core_instance_type=ElDoradoAliCloudInstanceTypes(AliCloudInstanceTypes.ECS_G6_2X()).with_node_count(job_config.capacity)
            .with_data_disk_count(1).with_data_disk_size_gb(job_config.base_disk_space).with_sys_disk_size_gb(60),
            cluster_tags=cluster_tags,
            emr_version=AliCloudEmrVersions.ALICLOUD_EMR_SPARK_3_2
        )

    return cluster


def get_job_task(logname: str, lookback: int, job_config: MetricCloudJobConfig) -> Union[EmrJobTask, HDIJobTask, AliCloudJobTask]:
    aggregation_step = None
    task_name = f"MetricsAggregation-{lookback}d"
    class_name = f"jobs.dataproc.datalake.{logname}DatalakeEtlPipelineMetrics"
    common_eldorado_config = java_settings_list + [
        ("dataTime", f"{{{{ data_interval_start.subtract(days={lookback}).strftime(\"%Y-%m-%dT%H:00:00\") }}}}"),
        ("ttd.defaultcloudprovider", job_config.provider.__str__()), ("aggType", "day")
    ]

    if job_config.cluster_service == ClusterServices.AwsEmr:
        aggregation_step = EmrJobTask(
            name=task_name,
            executable_path=aws_jar,
            class_name=class_name,
            configure_cluster_automatically=True,
            eldorado_config_option_pairs_list=common_eldorado_config + [("azure.key", "azure-account-key")],
            additional_args_option_pairs_list=additional_args,
            cluster_calc_defaults=ClusterCalcDefaults(parallelism_factor=3, memory_tolerance=0.9970),
            timeout_timedelta=timedelta(hours=5),
        )

    if job_config.cluster_service == ClusterServices.HDInsight:
        aggregation_step = HDIJobTask(
            name=task_name,
            jar_path=azure_jar,
            class_name=class_name,
            configure_cluster_automatically=True,
            eldorado_config_option_pairs_list=common_eldorado_config + [('openlineage.enable', 'false')],
            additional_args_option_pairs_list=additional_args,
            command_line_arguments=['--version']
        )

    if job_config.cluster_service == ClusterServices.AliCloudEmr:
        aggregation_step = AliCloudJobTask(
            name=task_name,
            jar_path=ali_jar,
            class_name=class_name,
            configure_cluster_automatically=True,
            eldorado_config_option_pairs_list=common_eldorado_config,
            additional_args_option_pairs_list=additional_args,
            cluster_calc_defaults=ClusterCalcDefaults(memory_tolerance=0.9970),
        )

    return aggregation_step


# Some source logs don't exist in Azure Blob yet
dag_info = [
    (
        LogNames.bidfeedback, [
            MetricCloudJobConfig(CloudProviders.aws, capacity=14),
            MetricCloudJobConfig(CloudProviders.azure, capacity=2),
            MetricCloudJobConfig(CloudProviders.ali, capacity=2),
        ]
    ),
    (LogNames.bidfeedbackverticaload, [MetricCloudJobConfig(CloudProviders.aws, capacity=6)]),
    (
        LogNames.bidrequest, [
            MetricCloudJobConfig(CloudProviders.aws, capacity=60),
            MetricCloudJobConfig(CloudProviders.azure, capacity=2),
            MetricCloudJobConfig(CloudProviders.ali, capacity=2),
        ]
    ),
    (
        LogNames.clicktracker, [
            MetricCloudJobConfig(CloudProviders.aws, capacity=2),
            MetricCloudJobConfig(CloudProviders.azure, capacity=2),
            MetricCloudJobConfig(CloudProviders.ali, capacity=2),
        ]
    ),
    (LogNames.clicktrackerverticaload, [MetricCloudJobConfig(CloudProviders.aws, capacity=2)]),
    (LogNames.conversiontracker, [MetricCloudJobConfig(CloudProviders.aws, capacity=6)]),
    (LogNames.conversiontrackerverticaload, [MetricCloudJobConfig(CloudProviders.aws, capacity=6)]),
    (LogNames.attributedeventverticaload, [MetricCloudJobConfig(CloudProviders.aws, capacity=2)]),
    (LogNames.attributedeventresultverticaload, [MetricCloudJobConfig(CloudProviders.aws, capacity=2)]),
    (LogNames.attributedeventdataelementverticaload, [MetricCloudJobConfig(CloudProviders.aws, capacity=2)]),
    (LogNames.eventtrackerverticaload, [MetricCloudJobConfig(CloudProviders.aws, capacity=4)]),
    (
        LogNames.videoevent, [
            MetricCloudJobConfig(CloudProviders.aws, capacity=22),
            MetricCloudJobConfig(CloudProviders.azure, capacity=2),
            MetricCloudJobConfig(CloudProviders.ali, capacity=2),
        ]
    ),
]

for (logname, cloud_configs) in dag_info:
    config: MetricCloudJobConfig
    for config in cloud_configs:
        dag = create_dag(logname, config)
        globals()[dag.airflow_dag.dag_id] = dag.airflow_dag  # record the dag in a place where airflow can see it
