"""
This module creates the MicroTargeting Airflow tasks for Azure most of which are run on Spark clusters.
These are sub-DAGs which require cluster management and scaling.
"""
import logging
from datetime import timedelta, datetime
from typing import List, Tuple

import requests
from airflow.models import TaskInstance, Variable
from airflow.operators.python_operator import PythonOperator

from dags.pdg.microtargeting_tasks.shared import MicrotargetingTasksShared
from dags.pdg.spark_utils import spark_fully_qualified_name, create_hdi_cluster_task
from dags.pdg.task_utils import xcom_pull_str, choose
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from ttd.eldorado.hdi import HDIClusterTask, HDIJobTask
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.slack.slack_groups import pdg
from ttd.tasks.op import OpTask


class MicrotargetingTasksAzure(MicrotargetingTasksShared):
    MICROTARGETING_JAR_AZURE = "abfs://ttd-build-artefacts@ttdartefacts.dfs.core.windows.net/microtargeting-pipeline/prod/jars/2.1/latest/microtargeting-pipeline-job.jar"
    MICROTARGETING_JAR_PACKAGE = "com.thetradedesk.microtargeting.pipeline.spark"

    CONTEXT_TASK_ID = "set-context-azure"
    AZURE_EXPORT_AUDIENCES_SERVICE_TASK_ID = "export-active-audiences-azure"
    CHECK_AZURE_EXPORT_AUDIENCES_STATUS_TASK_ID = "get-export-active-audiences-status-azure"
    AZURE_EXPORT_AUDIENCES_GROUP_TASK_ID = "export-audience-azure"

    KEY_AZURE_BUCKET = "azure_bucket"
    KEY_ENV = "env_key"
    KEY_PREFIX = "prefix_key"
    KEY_STORAGE_ACCOUNT = "storage_account"

    KEY_AZURE_EXPORT_AUDIENCES_REQUEST_ID = "azure_export_audiences_request_id"
    KEY_AZURE_EXPORT_AUDIENCES_PREFIX = "azure_export_audiences_prefix"
    KEY_AZURE_EXPORT_AUDIENCES_BASE_URI = "azure_export_audiences_base_uri"
    KEY_AZURE_EXPORT_AUDIENCES_PARQUET = "azure_export_audiences_parquet"

    CLOUD_PROVIDER_PARAMS: List[Tuple[str, str]] = [("cloudProvider", "azure")]

    PROV_PROD_AND_TEST_SECRETS = MicrotargetingTasksShared.PROV_PROD_AND_TEST_SECRETS(
        prod="prod-ttd-microtargeting-provisioning", test="intsb-ttd-microtargeting-provisioning"
    )

    def _bucket_params(self) -> List[Tuple[str, str]]:
        return [("baseUri", f"wasbs://{self._get_context_value(MicrotargetingTasksAzure.KEY_AZURE_BUCKET)}.blob.core.windows.net"),
                ("prefixKey", self._get_context_value(MicrotargetingTasksAzure.KEY_PREFIX))]

    def _geo_bucket_params(self) -> List[Tuple[str, str]]:
        return [("geoBaseUri", f"wasbs://{self._get_context_value(MicrotargetingTasksAzure.KEY_AZURE_BUCKET)}.blob.core.windows.net"),
                ("geoPrefixKey", self._get_context_value(MicrotargetingTasksAzure.KEY_ENV))]

    def _geo_store_data_params(self) -> List[Tuple[str, str]]:
        return [(
            "GeoStoreLatestGeneratedDataS3PrefixFilePath",
            f"wasbs://{self._get_context_value(MicrotargetingTasksAzure.KEY_AZURE_BUCKET)}.blob.core.windows.net/{self._get_context_value(MicrotargetingTasksAzure.KEY_ENV)}/GeoStoreGeneratedData/LatestPrefix.txt"
        ),
                (
                    "GeoStoreLatestGeneratedDataS3RootPath",
                    f"wasbs://{self._get_context_value(MicrotargetingTasksAzure.KEY_AZURE_BUCKET)}.blob.core.windows.net/{self._get_context_value(MicrotargetingTasksAzure.KEY_ENV)}/GeoStoreGeneratedData"
                )]

    def _exported_audience_params(self):
        params = []
        keys = [(MicrotargetingTasksAzure.KEY_AZURE_EXPORT_AUDIENCES_PREFIX, "exportedAudiencePrefixKey"),
                (MicrotargetingTasksAzure.KEY_AZURE_EXPORT_AUDIENCES_BASE_URI, "exportedAudienceBaseUri"),
                (MicrotargetingTasksAzure.KEY_AZURE_EXPORT_AUDIENCES_PARQUET, "exportedAudienceParquetKey")]

        for key, param_name in keys:
            value = xcom_pull_str(dag_id=self.dag_id, task_id=MicrotargetingTasksAzure.CHECK_AZURE_EXPORT_AUDIENCES_STATUS_TASK_ID, key=key)
            params.append((param_name, value))

        return params

    def _get_service_account_access_token(self):
        request_data = {
            "client_id": "ttd-infra-service-client",
            "username": Variable.get("microtargeting-service-account-user"),
            "password": Variable.get("microtargeting-service-account-password"),
            "grant_type": "password",
            "scope": "openid"
        }
        r = requests.post('https://ops-sso.adsrvr.org/auth/realms/master/protocol/openid-connect/token', data=request_data)
        r.raise_for_status()
        access_token = r.json().get('access_token', None)
        if not access_token:
            raise ValueError("unable to obtain bearer token")
        return access_token

    def _get_context_value(self, key: str):
        return xcom_pull_str(dag_id=self.dag_id, task_id=MicrotargetingTasksAzure.CONTEXT_TASK_ID, key=key)

    def _set_azure_context(self, task_instance: TaskInstance, **kwargs):
        now = datetime.now()
        azure_bucket_name = "ttd-microtargeting-va9"
        env_key = f"env={choose('prod', 'test')}"
        prefix_key = f"{env_key}/{self.dag_id}/{now.strftime('%Y-%m-%d/%H-%M')}"
        storage_account = "ttdmicrotargeting"
        task_instance.xcom_push(key=MicrotargetingTasksAzure.KEY_AZURE_BUCKET, value=f"{azure_bucket_name}@{storage_account}")
        task_instance.xcom_push(key=MicrotargetingTasksAzure.KEY_ENV, value=env_key)
        task_instance.xcom_push(key=MicrotargetingTasksAzure.KEY_PREFIX, value=prefix_key)
        task_instance.xcom_push(key=MicrotargetingTasksAzure.KEY_STORAGE_ACCOUNT, value=storage_account)
        logging.info(f"Microtargeting pipeline azure jar={MicrotargetingTasksAzure.MICROTARGETING_JAR_AZURE}")

    def create_azure_set_context_task(self, airflow_dag):
        return OpTask(
            op=PythonOperator(
                task_id=MicrotargetingTasksAzure.CONTEXT_TASK_ID,
                dag=airflow_dag,
                provide_context=True,
                python_callable=self._set_azure_context,
                retry_exponential_backoff=True
            )
        )

    def create_azure_retrieve_adgroup_data_task(self, main_class: str):
        """
        Returns a function which produces a task that retrieves adgroup data from Provisioning.
        """
        azure_key_param = [("azure.key", self._get_context_value(MicrotargetingTasksAzure.KEY_STORAGE_ACCOUNT))]

        return create_hdi_cluster_task(
            task_name="retrieve-ad-group-data-azure",
            job_name=self.job_name,
            jar=MicrotargetingTasksAzure.MICROTARGETING_JAR_AZURE,
            main_class=spark_fully_qualified_name(MicrotargetingTasksAzure.MICROTARGETING_JAR_PACKAGE, main_class),
            jvm_args=(
                self._bucket_params() +
                self.prov_params(access_mode="read-only", prod_and_test_secrets=MicrotargetingTasksAzure.PROV_PROD_AND_TEST_SECRETS) +
                MicrotargetingTasksShared.POLICY_PARAMS + MicrotargetingTasksAzure.CLOUD_PROVIDER_PARAMS + azure_key_param
            ),
            timeout=timedelta(minutes=20),
            vm_config=HDIVMConfig(
                headnode_type=HDIInstanceTypes.Standard_E8_v3(),
                workernode_type=HDIInstanceTypes.Standard_D16ads_v5(),
                num_workernode=1,
                disks_per_node=1
            )
        )

    def create_azure_rails_and_write_output_application_task(self):
        """
        Creates HDI task suitable to run any of the rail appliers and writes results to Provisioning.
        """
        azure_key_param = [("azure.key", f"{self._get_context_value(MicrotargetingTasksAzure.KEY_STORAGE_ACCOUNT)},ttdsegmentservice")]

        task_name = "apply-rails-v2-and-write-output-azure"
        jar = MicrotargetingTasksAzure.MICROTARGETING_JAR_AZURE

        cluster_task = HDIClusterTask(
            name=task_name,
            cluster_tags={
                "Team": pdg.jira_team,
                "Job": self.job_name,
                "Task": task_name,
                "Version": jar.split("/")[-3],
            },
            cluster_version=HDIClusterVersions.HDInsight51,
            enable_openlineage=False,  # Not supported for Spark 3 on HDI
            vm_config=HDIVMConfig(
                headnode_type=HDIInstanceTypes.Standard_E8_v3(),
                workernode_type=HDIInstanceTypes.Standard_E8_v3(),
                num_workernode=50,
                disks_per_node=1
            ),
            retries=0  # No automatic retries to prevent writing duplicate data to the database.
        )

        rails_job_task = HDIJobTask(
            name="rails-job",
            class_name=spark_fully_qualified_name(MicrotargetingTasksAzure.MICROTARGETING_JAR_PACKAGE, "ApplyRailsV2"),
            jar_path=jar,
            eldorado_config_option_pairs_list=(
                self._bucket_params() + MicrotargetingTasksShared.POLICY_PARAMS + self._exported_audience_params() + azure_key_param +
                self._geo_store_data_params()
            ),
            configure_cluster_automatically=True,
            watch_step_timeout=timedelta(hours=5),
            cluster_specs=cluster_task.cluster_specs
        )

        write_output_job_task = HDIJobTask(
            name="write-output-job",
            class_name=spark_fully_qualified_name(MicrotargetingTasksAzure.MICROTARGETING_JAR_PACKAGE, "WriteOutput"),
            jar_path=jar,
            eldorado_config_option_pairs_list=(
                self._bucket_params() +
                self.prov_params(access_mode="read-write", prod_and_test_secrets=MicrotargetingTasksAzure.PROV_PROD_AND_TEST_SECRETS) +
                MicrotargetingTasksShared.PROV_MODE_PARAMS + MicrotargetingTasksShared.POLICY_PARAMS +
                MicrotargetingTasksAzure.CLOUD_PROVIDER_PARAMS + self._exported_audience_params() + azure_key_param
            ),
            configure_cluster_automatically=True,
            watch_step_timeout=timedelta(minutes=15),
            cluster_specs=cluster_task.cluster_specs
        )

        cluster_task.add_sequential_body_task(rails_job_task)
        cluster_task.add_sequential_body_task(write_output_job_task)

        return cluster_task

    def create_azure_check_adjacent_zip_codes_task(self):
        """
        Returns a function which produces a task that performs the zip code adjacency check
        """
        task_name = "check-adjacent-zip-codes-azure"
        jar = MicrotargetingTasksAzure.MICROTARGETING_JAR_AZURE

        azure_key_param = [("azure.key", self._get_context_value(MicrotargetingTasksAzure.KEY_STORAGE_ACCOUNT))]

        cluster_task = HDIClusterTask(
            name=task_name,
            cluster_tags={
                "Team": pdg.jira_team,
                "Job": self.job_name,
                "Task": task_name,
                "Version": jar.split("/")[-3],
            },
            cluster_version=HDIClusterVersions.HDInsight51,
            enable_openlineage=False,  # Not supported for Spark 3 on HDI
            vm_config=HDIVMConfig(
                headnode_type=HDIInstanceTypes.Standard_E8_v3(),
                workernode_type=HDIInstanceTypes.Standard_E8_v3(),
                num_workernode=1,
                disks_per_node=1
            ),
            retries=0  # No automatic retries to prevent writing duplicate data to the database.
        )

        check_adjacent_zip_codes_task = HDIJobTask(
            name="zip-code-check-job",
            class_name=spark_fully_qualified_name(MicrotargetingTasksAzure.MICROTARGETING_JAR_PACKAGE, "CheckAdjacentZipCodes"),
            jar_path=jar,
            eldorado_config_option_pairs_list=(
                self._bucket_params() + self._geo_bucket_params() + MicrotargetingTasksShared.POLICY_PARAMS + azure_key_param
            ),
            configure_cluster_automatically=True,
            watch_step_timeout=timedelta(hours=2),
            cluster_specs=cluster_task.cluster_specs
        )

        write_adjacent_zip_code_results_task = HDIJobTask(
            name="write-zip-code-check-results-job",
            class_name=spark_fully_qualified_name(MicrotargetingTasksAzure.MICROTARGETING_JAR_PACKAGE, "WriteZipCodeCheckHistory"),
            jar_path=jar,
            eldorado_config_option_pairs_list=(
                self._bucket_params() +
                self.prov_params(access_mode="read-write", prod_and_test_secrets=MicrotargetingTasksAzure.PROV_PROD_AND_TEST_SECRETS) +
                MicrotargetingTasksShared.PROV_MODE_PARAMS + MicrotargetingTasksShared.POLICY_PARAMS +
                MicrotargetingTasksAzure.CLOUD_PROVIDER_PARAMS + azure_key_param
            ),
            configure_cluster_automatically=True,
            watch_step_timeout=timedelta(minutes=15),
            cluster_specs=cluster_task.cluster_specs
        )

        cluster_task.add_sequential_body_task(check_adjacent_zip_codes_task)
        cluster_task.add_sequential_body_task(write_adjacent_zip_code_results_task)

        return cluster_task
