"""
This module creates the MicroTargeting Airflow tasks for AWS most of which are run on Spark clusters.
These are sub-DAGs which require cluster management and scaling.
"""
import logging
from datetime import timedelta, datetime
from typing import List, Tuple

from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator

from dags.pdg.microtargeting_tasks.shared import MicrotargetingTasksShared
from dags.pdg.spark_utils import spark_fully_qualified_name, scale_emr_cluster_up_to, \
    create_emr_cluster_task
from dags.pdg.task_utils import xcom_pull_str, choose
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.ec2.emr_instance_types.general_purpose.m7a import M7a
from ttd.el_dorado.v2.emr import EmrClusterTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.tasks.op import OpTask


class MicrotargetingTasksAws(MicrotargetingTasksShared):
    MICROTARGETING_JAR_S3 = "s3://ttd-build-artefacts/microtargeting-pipeline/prod/jars/2.1/latest/microtargeting-pipeline-job.jar"
    MICROTARGETING_JAR_PACKAGE = "com.thetradedesk.microtargeting.pipeline.spark"

    CONTEXT_TASK_ID = "set-context-aws"
    AWS_EXPORT_AUDIENCES_SERVICE_TASK_ID = "export-active-audiences-aws"
    CHECK_AWS_EXPORT_AUDIENCES_STATUS_TASK_ID = "get-export-active-audiences-status-aws"
    AWS_EXPORT_AUDIENCES_GROUP_TASK_ID = "export-audience-aws"

    KEY_S3_BUCKET_NAME = "s3_bucket_name"
    KEY_PREFIX = "prefix_key"
    KEY_AWS_EXPORT_AUDIENCES_REQUEST_ID = "aws_export_audiences_request_id"
    KEY_AWS_EXPORT_AUDIENCES_PREFIX = "aws_export_audiences_prefix"
    KEY_AWS_EXPORT_AUDIENCES_BASE_URI = "aws_export_audiences_base_uri"
    KEY_AWS_EXPORT_AUDIENCES_PARQUET = "aws_export_audiences_parquet"

    # These values are based on the current spark arguments. If these are updated, the spark args will also need to be updated
    PROCESS_AUDIENCE_NUM_INSTANCES = 500  # Based upon Segment Service team's example settings for ExportAudiences Notebook (2024/05/01)
    GEO_RAIL_PIPELINE_NUM_INSTANCES = 400  # TODO(PDG-1309): Review, currently based on our full run (without excluding LargeUID2Audience flagged Audiences)

    PROV_PROD_AND_TEST_SECRETS = MicrotargetingTasksShared.PROV_PROD_AND_TEST_SECRETS(
        prod="prod/ttd_microtargeting/Provisioning", test="intsb/ttd_microtargeting/Provisioning"
    )

    def _bucket_params(self) -> List[Tuple[str, str]]:
        return [("baseUri", f"s3://{self._get_context_value(MicrotargetingTasksAws.KEY_S3_BUCKET_NAME)}"),
                ("prefixKey", self._get_context_value(MicrotargetingTasksAws.KEY_PREFIX))]

    def _geo_bucket_params(self) -> List[Tuple[str, str]]:
        return [("geoBaseUri", f"s3://{self.get_shared_context_value(MicrotargetingTasksShared.KEY_S3_GEO_BUCKET_NAME)}"),
                ("geoPrefixKey", self.get_shared_context_value(MicrotargetingTasksShared.KEY_S3_GEO_PREFIX))]

    def _geo_store_data_params(self) -> List[Tuple[str, str]]:
        return [(
            "GeoStoreLatestGeneratedDataS3PrefixFilePath",
            f"s3a://{self.get_shared_context_value(MicrotargetingTasksShared.KEY_S3_GEO_BUCKET_NAME)}/{self.get_shared_context_value(MicrotargetingTasksShared.KEY_S3_GEO_PREFIX)}/GeoStoreNg/GeoStoreGeneratedData/LatestPrefix.txt"
        ),
                (
                    "GeoStoreLatestGeneratedDataS3RootPath",
                    f"s3a://{self.get_shared_context_value(MicrotargetingTasksShared.KEY_S3_GEO_BUCKET_NAME)}/{self.get_shared_context_value(MicrotargetingTasksShared.KEY_S3_GEO_PREFIX)}/GeoStoreNg/GeoStoreGeneratedData"
                )]

    def _exported_audience_params(self):
        params = []
        keys = [(MicrotargetingTasksAws.KEY_AWS_EXPORT_AUDIENCES_PREFIX, "exportedAudiencePrefixKey"),
                (MicrotargetingTasksAws.KEY_AWS_EXPORT_AUDIENCES_BASE_URI, "exportedAudienceBaseUri"),
                (MicrotargetingTasksAws.KEY_AWS_EXPORT_AUDIENCES_PARQUET, "exportedAudienceParquetKey")]

        for key, param_name in keys:
            value = xcom_pull_str(dag_id=self.dag_id, task_id=MicrotargetingTasksAws.CHECK_AWS_EXPORT_AUDIENCES_STATUS_TASK_ID, key=key)
            params.append((param_name, value))

        return params

    CLOUD_PROVIDER_PARAMS: List[Tuple[str, str]] = [("cloudProvider", "aws")]

    def _get_context_value(self, key: str):
        return xcom_pull_str(dag_id=self.dag_id, task_id=MicrotargetingTasksAws.CONTEXT_TASK_ID, key=key)

    def _set_aws_context(self, task_instance: TaskInstance, **kwargs):
        now = datetime.now()
        s3_bucket_name = "ttd-microtargeting-use"
        prefix_key = f"env={choose('prod', 'test')}/{self.dag_id}/{now.strftime('%Y-%m-%d/%H-%M')}"
        task_instance.xcom_push(key=MicrotargetingTasksAws.KEY_S3_BUCKET_NAME, value=s3_bucket_name)
        task_instance.xcom_push(key=MicrotargetingTasksAws.KEY_PREFIX, value=prefix_key)
        logging.info(f"Microtargeting pipeline s3 jar={MicrotargetingTasksAws.MICROTARGETING_JAR_S3}")

    def create_aws_set_context_task(self, airflow_dag):
        return OpTask(
            op=PythonOperator(
                task_id=MicrotargetingTasksAws.CONTEXT_TASK_ID,
                dag=airflow_dag,
                provide_context=True,
                python_callable=self._set_aws_context,
                retry_exponential_backoff=True
            )
        )

    def create_aws_retrieve_adgroup_data_task(self, main_class: str):
        """
        Returns a function which produces a task that retrieves adgroup data from Provisioning.
        """
        return create_emr_cluster_task(
            task_name="retrieve-ad-group-data-aws",
            job_name=self.job_name,
            jar=MicrotargetingTasksAws.MICROTARGETING_JAR_S3,
            main_class=spark_fully_qualified_name(MicrotargetingTasksAws.MICROTARGETING_JAR_PACKAGE, main_class),
            jvm_args=(
                self._bucket_params() +
                self.prov_params(access_mode="read-only", prod_and_test_secrets=MicrotargetingTasksAws.PROV_PROD_AND_TEST_SECRETS) +
                MicrotargetingTasksShared.POLICY_PARAMS + MicrotargetingTasksAws.CLOUD_PROVIDER_PARAMS
            ),
            timeout=timedelta(minutes=20),
            master_fleet=EmrFleetInstanceTypes(
                instance_types=[
                    M7a.m7a_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
                ],
                on_demand_weighted_capacity=1
            ),
            core_fleet=EmrFleetInstanceTypes(
                instance_types=[
                    M7a.m7a_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
                ],
                on_demand_weighted_capacity=1,
            )
        )

    def create_aws_rails_application_task(self) -> EmrClusterTask:
        """
        Creates EMR task suitable to run any of the rail appliers.
        """
        return create_emr_cluster_task(
            task_name="apply-rails-v2-aws",
            job_name=self.job_name,
            jar=MicrotargetingTasksAws.MICROTARGETING_JAR_S3,
            main_class=spark_fully_qualified_name(MicrotargetingTasksAws.MICROTARGETING_JAR_PACKAGE, "ApplyRailsV2"),
            jvm_args=(
                self._bucket_params() + MicrotargetingTasksShared.POLICY_PARAMS + self._geo_store_data_params() +
                self._exported_audience_params()
            ),
            retries=2,
            spark_args=[
                # Based on current cluster setup: 400 r5d-2xLarge instances.
                ("conf", "spark.submit.deployMode=cluster"),
                ("conf", "spark.driver.memory=42G"),
                ("conf", "spark.driver.cores=6"),
                ("conf", "spark.driver.maxResultSize=32G"),
                ("conf", "spark.executor.memory=42G"),
                ("conf", "spark.executor.cores=8"),
                ("conf", f"spark.executor.instances={MicrotargetingTasksAws.GEO_RAIL_PIPELINE_NUM_INSTANCES}"),
                ("conf", "spark.dynamicAllocation.enabled=false"),
                ("conf", "spark.default.parallelism=3200"),
                ("conf", "spark.sql.shuffle.partitions=3200"),
                ("conf", "spark.sql.adaptive.enabled=false"),
            ],
            configure_cluster_automatically=False,  # We set our own configuration, see `conf` params for this job.
            # Base on how many AdGroups are processed / current baseline (2024/08/24)
            timeout=timedelta(hours=5),
            # TODO(PDG-1309): Review cluster configuration
            master_fleet=EmrFleetInstanceTypes(
                instance_types=[
                    R5d.r5d_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
                ],
                on_demand_weighted_capacity=1
            ),
            core_fleet=EmrFleetInstanceTypes(
                instance_types=[
                    R5d.r5d_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
                ],
                on_demand_weighted_capacity=1,  # rely on scaling policy
            ),
            scaling_policy=scale_emr_cluster_up_to(MicrotargetingTasksAws.GEO_RAIL_PIPELINE_NUM_INSTANCES)
        )

    def create_aws_write_output_task(self) -> EmrClusterTask:
        """
        Returns a function which produces a task that writes output data to Provisioning.
        """
        return create_emr_cluster_task(
            task_name="write-output-aws",
            job_name=self.job_name,
            jar=MicrotargetingTasksAws.MICROTARGETING_JAR_S3,
            main_class=spark_fully_qualified_name(MicrotargetingTasksAws.MICROTARGETING_JAR_PACKAGE, "WriteOutput"),
            jvm_args=(
                self._bucket_params() +
                self.prov_params(access_mode="read-write", prod_and_test_secrets=MicrotargetingTasksAws.PROV_PROD_AND_TEST_SECRETS) +
                MicrotargetingTasksShared.PROV_MODE_PARAMS + MicrotargetingTasksShared.POLICY_PARAMS +
                MicrotargetingTasksAws.CLOUD_PROVIDER_PARAMS + self._exported_audience_params()
            ),
            timeout=timedelta(minutes=15),
            # Base on performance for  US Political AdGroups / current baseline (2024/08/22)
            master_fleet=EmrFleetInstanceTypes(
                instance_types=[
                    M7a.m7a_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
                ],
                on_demand_weighted_capacity=1
            ),
            core_fleet=EmrFleetInstanceTypes(
                instance_types=[
                    M7a.m7a_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
                ],
                on_demand_weighted_capacity=1,  # For database interactions and size of this data (2024/08/22)
            ),
            retries=0,  # No automatic retries to prevent writing duplicate data to the database.
        )

    def create_aws_check_adjacent_zip_codes_task(self) -> EmrClusterTask:
        """
        Returns a function which produces a task that performs the zip code adjacency check
        """
        return create_emr_cluster_task(
            task_name="check-adjacent-zip-codes-aws",
            job_name=self.job_name,
            jar=MicrotargetingTasksAws.MICROTARGETING_JAR_S3,
            main_class=spark_fully_qualified_name(MicrotargetingTasksAws.MICROTARGETING_JAR_PACKAGE, "CheckAdjacentZipCodes"),
            jvm_args=(self._bucket_params() + self._geo_bucket_params() + MicrotargetingTasksShared.POLICY_PARAMS),
            timeout=timedelta(hours=2),
            master_fleet=EmrFleetInstanceTypes(
                instance_types=[
                    R5d.r5d_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
                ],
                on_demand_weighted_capacity=1
            ),
            core_fleet=EmrFleetInstanceTypes(
                instance_types=[
                    R5d.r5d_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
                ],
                on_demand_weighted_capacity=1,
            ),
            retries=2,
        )

    def create_aws_write_zip_code_check_history_task(self) -> EmrClusterTask:
        """
        Returns a function which produces a task that writes zip code check history data to Provisioning.
        """
        return create_emr_cluster_task(
            task_name="write-zip-code-check-history-aws",
            job_name=self.job_name,
            jar=MicrotargetingTasksAws.MICROTARGETING_JAR_S3,
            main_class=spark_fully_qualified_name(MicrotargetingTasksAws.MICROTARGETING_JAR_PACKAGE, "WriteZipCodeCheckHistory"),
            jvm_args=(
                self._bucket_params() +
                self.prov_params(access_mode="read-write", prod_and_test_secrets=MicrotargetingTasksAws.PROV_PROD_AND_TEST_SECRETS) +
                MicrotargetingTasksShared.PROV_MODE_PARAMS + MicrotargetingTasksShared.POLICY_PARAMS +
                MicrotargetingTasksAws.CLOUD_PROVIDER_PARAMS
            ),
            timeout=timedelta(minutes=15),
            master_fleet=EmrFleetInstanceTypes(
                instance_types=[
                    M7a.m7a_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
                ],
                on_demand_weighted_capacity=1
            ),
            core_fleet=EmrFleetInstanceTypes(
                instance_types=[
                    M7a.m7a_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
                ],
                on_demand_weighted_capacity=1,
            ),
            retries=0,  # No automatic retries to prevent writing duplicate data to the database.
        )
