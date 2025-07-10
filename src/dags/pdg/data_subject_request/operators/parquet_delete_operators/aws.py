from datetime import timedelta

from dags.pdg.data_subject_request.operators.parquet_delete_operators import shared
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.tasks.base import BaseTask
from ttd.openlineage import IgnoreOwnershipType, OpenlineageConfig


class AWSParquetDeleteOperation(shared.IParquetDeleteOperation):
    JAR_PATH = "s3://ttd-build-artefacts/data-subject-requests/prod/jars/latest/data-subject-request-processing.jar"
    EMR_RELEASE_LABEL = AwsEmrVersions.AWS_EMR_SPARK_3_2
    job_type = "UserDsr"

    def _create_delete_job_task(self, dataset_name: str) -> BaseTask:
        """
        Create delete job task for a dataset. The job will be one downstream job of upstream_task.

        @param dataset_name Dataset name
        @return delete task
        """

        cluster_task = EmrClusterTask(
            name=f"{self.cluster_name}-{dataset_name}",
            emr_release_label=AWSParquetDeleteOperation.EMR_RELEASE_LABEL,
            master_fleet_instance_type_configs=self.dataset_configs[dataset_name]["master_fleet_instance_type_configs"],
            cluster_tags={
                "Team": "PDG",
                "Job": AWSParquetDeleteOperation.job_type,
                "very_long_running": "na"
            },
            core_fleet_instance_type_configs=self.dataset_configs[dataset_name]["core_fleet_instance_type_configs"],
            managed_cluster_scaling_config=self.dataset_configs.get(dataset_name, {}).get("managed_cluster_scaling_config", None),
            additional_application_configurations=[
                {
                    "Classification": "spark",
                    "Properties": {
                        "maximizeResourceAllocation": "true"
                    }
                },
                {
                    "Classification": "spark-defaults",
                    "Properties": {
                        "spark.dynamicAllocation.enabled": "true",
                        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                        "spark.yarn.heterogeneousExecutors.enabled": "false"  # should be true if we use different instance types
                    }
                },
            ],
            enable_prometheus_monitoring=True,
            retries=1,
        )

        timeout = timedelta(hours=36)

        job_task = EmrJobTask(
            name=dataset_name,
            class_name=self.job_class_name,
            executable_path=AWSParquetDeleteOperation.JAR_PATH,
            additional_args_option_pairs_list=[
                ("conf", "spark.driver.maxResultSize=40g"),
                ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseG1GC"),
                ("conf", "spark.sql.files.ignoreCorruptFiles=true"),
                ("conf", "spark.kryoserializer.buffer.max=128m"),
                ("conf", "spark.sql.legacy.parquet.int96RebaseModeInWrite=LEGACY"),
                ("conf", "spark.sql.parquet.int96RebaseModeInWrite=LEGACY"),
            ],
            eldorado_config_option_pairs_list=self
            ._get_arguments_dsr_processing_job(dataset_name, log_output_path="s3://ttd-data-subject-requests/delete"),
            timeout_timedelta=timeout,
            retries=0,
            openlineage_config=OpenlineageConfig(ignore_ownership=IgnoreOwnershipType.PDG_SCRUBBING)
        )

        cluster_task.add_parallel_body_task(job_task)

        return cluster_task
