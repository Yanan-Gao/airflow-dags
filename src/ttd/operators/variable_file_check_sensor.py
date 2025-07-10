import logging
from typing import List

from airflow.sensors.base import BaseSensorOperator

from ttd.cloud_provider import CloudProvider, CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder


class VariableFileCheckSensor(BaseSensorOperator):
    template_fields = ["path"]

    def __init__(
        self,
        bucket: str,
        path: str,
        data_task_ids: List[str],
        data_file_key: str = "files",
        task_id: str = "s3_file_check",
        poke_interval: int = 60,
        timeout: int = 60 * 60 * 2,
        mode: str = "reschedule",
        cloud_provider: CloudProvider = CloudProviders.aws,
        *args,
        **kwargs,
    ):
        super().__init__(
            task_id=task_id,
            poke_interval=poke_interval,
            timeout=timeout,
            mode=mode,
            *args,
            **kwargs,
        )
        self.bucket = bucket
        self.path = path
        self.data_task_ids = data_task_ids
        self.data_file_key = data_file_key
        self.cloud_provider = cloud_provider

    def poke(self, context):
        ti = context["ti"]
        cloud_storage = CloudStorageBuilder(self.cloud_provider).build()

        xcom_data = ti.xcom_pull(key=self.data_file_key, task_ids=self.data_task_ids)
        files = [file for files in xcom_data for file in files]
        if len(files) == 0:
            logging.error("No files found. Check xcom output from {0}".format(self.data_task_ids))
            return False

        files_found = True
        for file in files:
            key = f'{self.path}/{file}'
            logging.info(f'Checking {self.bucket}/{key}')
            if not cloud_storage.check_for_key(key=key, bucket_name=self.bucket):
                logging.info(f'{self.bucket}/{key} file not found')
                files_found = False
                break

        return files_found
