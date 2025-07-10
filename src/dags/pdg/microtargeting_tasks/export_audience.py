"""
This module creates the export audience Airflow tasks for Microtargeting pipelines using the GRPC service.
"""
import logging
import os
import tempfile
import time
from datetime import timedelta
from urllib.parse import urlparse

import grpc
import requests
from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance, Variable
from airflow.operators.python_operator import PythonOperator

from dags.datprd.segment_service import SegmentServiceApi_pb2, SegmentServiceApi_pb2_grpc
from dags.pdg.microtargeting_tasks.aws import MicrotargetingTasksAws
from dags.pdg.microtargeting_tasks.azure import MicrotargetingTasksAzure
from dags.pdg.microtargeting_tasks.shared import MicrotargetingTasksShared
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask


class ExportAudienceTasks:
    ROOT_CERTIFICATE = """
-----BEGIN CERTIFICATE-----
MIIDxzCCAq+gAwIBAgIUDoF84uUPc77argmuKVzzv3LpNFEwDQYJKoZIhvcNAQEL
BQAwLjEsMCoGA1UEAxMjVGhlIFRyYWRlIERlc2sgLSBJbnRlcm5hbCAtIFJvb3Qg
Q0EwHhcNMjEwMzMwMDM0MDMzWhcNMjYwMzI5MDM0MTAzWjA2MTQwMgYDVQQDEytU
aGUgVHJhZGUgRGVzayAtIEludGVybmFsIC0gSW50ZXJtZWRpYXRlIENBMIIBIjAN
BgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtEx2WyJ1xZWNMkIOZixcv6keSup9
MmZ4xOMCj2m1xUuoa31vw9x2mC7p7m3v3xDNBNm/iKXMrFClwH5gnpH5tjCU6cAR
M3q+BvHGMfYQRxpKYsndkLpVlapgWwFNDH5C8DhoKHxYwTeb3n7CCZhhb0JA94Yd
+Yi0QJjemUbbXIqfi4FRx0w1ZbJAkxWw+mLaD/RzVtJlBJH6ock3OlJTcIgIjQaw
9icdAH8zcwwRp3WV3D35AzwpLbDO0F4kF0lAYPK11N+tcAMDLZHUAFII1wxuKX05
dfk+7f6eD2CyzHc9B6SxTf3oUSsIlUGQ6o/ptJVbjh1BpqSL8/s8K4BtWwIDAQAB
o4HUMIHRMA4GA1UdDwEB/wQEAwIBBjAPBgNVHRMBAf8EBTADAQH/MB0GA1UdDgQW
BBQjbaEG67lePpVozHJ8FWboqu3VUjAfBgNVHSMEGDAWgBQ6c6DS599p/M5KZiCo
BP/TRcEa0zA7BggrBgEFBQcBAQQvMC0wKwYIKwYBBQUHMAKGH2h0dHA6Ly8xMjcu
MC4wLjE6ODIwMC92MS9wa2kvY2EwMQYDVR0fBCowKDAmoCSgIoYgaHR0cDovLzEy
Ny4wLjAuMTo4MjAwL3YxL3BraS9jcmwwDQYJKoZIhvcNAQELBQADggEBAAzhy8rK
E3S7gKYSgQin4sdAU7OZeT7m94vFOkX9+aYcCtDWrfpFc/hAeC20Fmfw5jLu9rwd
DFTVEIKJslP0AENTAL7U1DTiYLgZ/+Pcm9J0KhB6c1+V/QoTYcTLztie332M/zoF
RSEEVgkG6RBQaWsN2ucU0QfVdBNb9+BJMa7DYv/+dtwHRcl9725Zh5IuYKiIRxO7
b2VHARBqd/E4iS1+AjMnwXPR3JwJpPKA1RZlhYjSYUfpAA/9cilsYs1u/fgzWNG3
+267VKPf3i6eYABMVb5pQUgUWgOsNahDnsKQvYdudqCBzALnoNVdWMRXtQuHYgHQ
TOoK0iSyZrUrhTc=
-----END CERTIFICATE-----
"""

    SEGMENT_SERVICE_AZURE_GRPC_URL = "segment-prod-azure.gen.adsrvr.org"
    SEGMENT_SERVICE_AWS_GRPC_URL = "segment-prod.gen.adsrvr.org"

    def __init__(self, dag_id: str):
        self.dag_id = dag_id

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

    def _export_audiences_service(self, prefix, bucket, cloud_provider, connection_id, grpc_url):
        import pandas as pd

        audience_id_file = "AudienceId.csv"

        with tempfile.TemporaryDirectory() as temp_folder:
            cloud_storage = CloudStorageBuilder(cloud_provider).set_conn_id(connection_id).build()
            local_file = os.path.join(temp_folder, audience_id_file)

            cloud_storage.download_file(
                file_path=local_file,
                key=f"{prefix}/{audience_id_file}",
                bucket_name=bucket,
            )

            if os.path.getsize(local_file) == 0:
                logging.info("Skipping because there are no audiences to export")
                return None

            df = pd.read_csv(local_file, header=None)
            audience_ids = df[0].astype(str).tolist()
            logging.info(f"Audience ids count: {len(audience_ids)}")

            access_token = self._get_service_account_access_token()
            credentials = grpc.composite_channel_credentials(
                grpc.ssl_channel_credentials(root_certificates=bytes(self.ROOT_CERTIFICATE, 'utf-8')),
                grpc.access_token_call_credentials(access_token)
            )
            channel = grpc.secure_channel(grpc_url, credentials)
            stub = SegmentServiceApi_pb2_grpc.SegmentServiceStub(channel)

            response = stub.ExportActiveAudiences(
                SegmentServiceApi_pb2.ExportActiveAudiencesRequest(
                    audience_ids=audience_ids,
                    columns=[
                        "COLUMN_USER_ID", "COLUMN_COUNTRY_LOCATION_ID", "COLUMN_REGION_LOCATION_IDS", "COLUMN_CITY_LOCATION_ID",
                        "COLUMN_METRO_ID", "COLUMN_ZIP", "COLUMN_LATITUDE", "COLUMN_LONGITUDE", "COLUMN_LAT_LON_SOURCE_ID"
                    ]
                )
            )
            logging.info("Export audiences service called")
            request_id = response.id
            logging.info(f"Export audiences request id = {request_id}")
            return request_id

    def _check_export_audiences_status(self, grpc_url, export_audiences_request_id):
        access_token = self._get_service_account_access_token()
        credentials = grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(root_certificates=bytes(self.ROOT_CERTIFICATE, 'utf-8')),
            grpc.access_token_call_credentials(access_token)
        )
        channel = grpc.secure_channel(grpc_url, credentials)
        stub = SegmentServiceApi_pb2_grpc.SegmentServiceStub(channel)

        while True:
            response = stub.GetExportActiveAudiencesStatus(
                SegmentServiceApi_pb2.GetExportActiveAudiencesStatusRequest(id=export_audiences_request_id)
            )
            response_state = response.state
            response_state_name = SegmentServiceApi_pb2.ActiveAudienceExportState.Name(response_state)
            logging.info(f"Export audiences state = {response_state_name}")
            if response_state == SegmentServiceApi_pb2.ActiveAudienceExportState.ACTIVE_AUDIENCE_EXPORT_STATE_COMPLETED:
                audience_url = response.url
                logging.info(f"Audience URL = {audience_url}")

                parsed_url = urlparse(audience_url)
                return parsed_url
            elif response_state == SegmentServiceApi_pb2.ActiveAudienceExportState.ACTIVE_AUDIENCE_EXPORT_STATE_FAILED:
                raise AirflowFailException("Export audiences request failed")
            time.sleep(60)

    def _azure_export_audiences_service(self, task_instance: TaskInstance, **kwargs):
        prefix = task_instance.xcom_pull(
            dag_id=self.dag_id, task_ids=MicrotargetingTasksAzure.CONTEXT_TASK_ID, key=MicrotargetingTasksAzure.KEY_PREFIX
        )
        bucket = task_instance.xcom_pull(
            dag_id=self.dag_id, task_ids=MicrotargetingTasksAzure.CONTEXT_TASK_ID, key=MicrotargetingTasksAzure.KEY_AZURE_BUCKET
        )

        request_id = self._export_audiences_service(
            prefix=prefix,
            bucket=bucket,
            cloud_provider=CloudProviders.azure,
            connection_id=MicrotargetingTasksShared.AZURE_CONNECTION_ID,
            grpc_url=self.SEGMENT_SERVICE_AZURE_GRPC_URL,
        )

        if request_id is None:
            return
        else:
            task_instance.xcom_push(key=MicrotargetingTasksAzure.KEY_AZURE_EXPORT_AUDIENCES_REQUEST_ID, value=request_id)

    def _check_azure_export_audiences_status(self, task_instance: TaskInstance, **kwargs):
        export_audiences_request_id = task_instance.xcom_pull(
            dag_id=self.dag_id,
            task_ids=MicrotargetingTasksAzure.AZURE_EXPORT_AUDIENCES_SERVICE_TASK_ID,
            key=MicrotargetingTasksAzure.KEY_AZURE_EXPORT_AUDIENCES_REQUEST_ID
        )

        if export_audiences_request_id is None:
            logging.info("Skipping because export audiences was not called")
            return

        parsed_url = self._check_export_audiences_status(
            grpc_url=self.SEGMENT_SERVICE_AZURE_GRPC_URL,
            export_audiences_request_id=export_audiences_request_id,
        )

        if parsed_url is None:
            logging.info("Export audiences service was not called")
            return

        storage_account = parsed_url.netloc.split('.')[0]
        path_parts = parsed_url.path.split('/')
        cleaned_parts = [item for item in path_parts if item]  # Remove empty strings
        if len(cleaned_parts) > 2:
            prefix_key = '/'.join(cleaned_parts[1:-1])
            task_instance.xcom_push(key=MicrotargetingTasksAzure.KEY_AZURE_EXPORT_AUDIENCES_PREFIX, value=prefix_key)
        if len(cleaned_parts) > 1:
            bucket_name = cleaned_parts[0]
            parquet_key = cleaned_parts[-1]
            task_instance.xcom_push(
                key=MicrotargetingTasksAzure.KEY_AZURE_EXPORT_AUDIENCES_BASE_URI,
                value=f"wasbs://{bucket_name}@{storage_account}.blob.core.windows.net"
            )
            task_instance.xcom_push(key=MicrotargetingTasksAzure.KEY_AZURE_EXPORT_AUDIENCES_PARQUET, value=parquet_key)

    def _aws_export_audiences_service(self, task_instance: TaskInstance, **kwargs):
        prefix = task_instance.xcom_pull(
            dag_id=self.dag_id, task_ids=MicrotargetingTasksAws.CONTEXT_TASK_ID, key=MicrotargetingTasksAws.KEY_PREFIX
        )
        bucket = task_instance.xcom_pull(
            dag_id=self.dag_id, task_ids=MicrotargetingTasksAws.CONTEXT_TASK_ID, key=MicrotargetingTasksAws.KEY_S3_BUCKET_NAME
        )

        request_id = self._export_audiences_service(
            prefix=prefix,
            bucket=bucket,
            cloud_provider=CloudProviders.aws,
            connection_id=MicrotargetingTasksShared.AWS_CONNECTION_ID,
            grpc_url=self.SEGMENT_SERVICE_AWS_GRPC_URL,
        )

        if request_id is None:
            return
        else:
            task_instance.xcom_push(key=MicrotargetingTasksAws.KEY_AWS_EXPORT_AUDIENCES_REQUEST_ID, value=request_id)

    def _check_aws_export_audiences_status(self, task_instance: TaskInstance, **kwargs):
        export_audiences_request_id = task_instance.xcom_pull(
            dag_id=self.dag_id,
            task_ids=MicrotargetingTasksAws.AWS_EXPORT_AUDIENCES_SERVICE_TASK_ID,
            key=MicrotargetingTasksAws.KEY_AWS_EXPORT_AUDIENCES_REQUEST_ID
        )

        if export_audiences_request_id is None:
            logging.info("Skipping because export audiences was not called")
            return

        parsed_url = self._check_export_audiences_status(
            grpc_url=self.SEGMENT_SERVICE_AWS_GRPC_URL, export_audiences_request_id=export_audiences_request_id
        )
        bucket = parsed_url.netloc
        path_parts = parsed_url.path.split("/")
        cleaned_parts = [item for item in path_parts if item]  # Remove empty strings
        prefix_key = '/'.join(cleaned_parts[:-1])
        parquet_key = path_parts[-1]

        task_instance.xcom_push(key=MicrotargetingTasksAws.KEY_AWS_EXPORT_AUDIENCES_PREFIX, value=prefix_key)
        task_instance.xcom_push(key=MicrotargetingTasksAws.KEY_AWS_EXPORT_AUDIENCES_BASE_URI, value=f"s3://{bucket}")
        task_instance.xcom_push(key=MicrotargetingTasksAws.KEY_AWS_EXPORT_AUDIENCES_PARQUET, value=parquet_key)

    def create_azure_export_audiences_service_task(self):
        export_active_audiences_task = OpTask(
            op=PythonOperator(
                task_id=MicrotargetingTasksAzure.AZURE_EXPORT_AUDIENCES_SERVICE_TASK_ID,
                provide_context=True,
                python_callable=self._azure_export_audiences_service,
                retries=2,
                retry_exponential_backoff=True
            )
        )

        get_export_active_audiences_status_task = OpTask(
            op=PythonOperator(
                task_id=MicrotargetingTasksAzure.CHECK_AZURE_EXPORT_AUDIENCES_STATUS_TASK_ID,
                provide_context=True,
                python_callable=self._check_azure_export_audiences_status,
                retries=2,
                retry_exponential_backoff=True
            )
        )

        return ChainOfTasks(
            task_id="export_audiences_and_check_status", tasks=[export_active_audiences_task, get_export_active_audiences_status_task]
        ).as_taskgroup(MicrotargetingTasksAzure.AZURE_EXPORT_AUDIENCES_GROUP_TASK_ID).with_retry_op(
            retries=2, retry_delay=timedelta(minutes=1)
        )

    def create_aws_export_audiences_service_task(self):
        export_active_audiences_task = OpTask(
            op=PythonOperator(
                task_id=MicrotargetingTasksAws.AWS_EXPORT_AUDIENCES_SERVICE_TASK_ID,
                provide_context=True,
                python_callable=self._aws_export_audiences_service,
                retries=2,
                retry_exponential_backoff=True
            )
        )

        get_export_active_audiences_status_task = OpTask(
            op=PythonOperator(
                task_id=MicrotargetingTasksAws.CHECK_AWS_EXPORT_AUDIENCES_STATUS_TASK_ID,
                provide_context=True,
                python_callable=self._check_aws_export_audiences_status,
                retries=2,
                retry_exponential_backoff=True
            )
        )

        return ChainOfTasks(
            task_id="export_audiences_and_check_status", tasks=[export_active_audiences_task, get_export_active_audiences_status_task]
        ).as_taskgroup(MicrotargetingTasksAws.AWS_EXPORT_AUDIENCES_GROUP_TASK_ID).with_retry_op(
            retries=2, retry_delay=timedelta(minutes=1)
        )
