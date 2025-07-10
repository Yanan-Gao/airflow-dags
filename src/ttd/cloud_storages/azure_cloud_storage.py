from typing import List, Optional

from ttd.azure_blob_hook import AzureBlobHook
from ttd.cloud_storages.cloud_storage import CloudStorage
from ttd.monads.trye import Try


class AzureCloudStorage(CloudStorage):

    def __init__(self, conn_id: Optional[str] = None):
        super().__init__(conn_id=conn_id)
        self._hook = AzureBlobHook(conn_id=conn_id) if conn_id else AzureBlobHook()

    @staticmethod
    def _get_azure_storage_account(bucket_name: str) -> str:
        return bucket_name.split("@")[1]

    @staticmethod
    def _get_azure_container(bucket_name: str) -> str:
        return bucket_name.split("@")[0]

    def check_for_key(self, key: str, bucket_name: str) -> bool:
        return self._hook.check_for_key(
            storage_account=self._get_azure_storage_account(bucket_name=bucket_name),
            container_name=self._get_azure_container(bucket_name=bucket_name),
            blob_name=key,
        )

    def check_for_prefix(self, prefix: str, bucket_name: str, delimiter: str = "/") -> bool:
        return self._hook.check_for_prefix(
            storage_account=self._get_azure_storage_account(bucket_name=bucket_name),
            container_name=self._get_azure_container(bucket_name=bucket_name),
            prefix=prefix,
        )

    def get_file_size(self, key: str, bucket_name: str) -> int:
        return self._hook.get_blob_properties(
            storage_account=self._get_azure_storage_account(bucket_name=bucket_name),
            container_name=self._get_azure_container(bucket_name=bucket_name),
            blob_name=key,
        ).size

    def list_keys(self, prefix: str, bucket_name: str) -> List[str]:
        return self._hook.list_keys(
            storage_account=self._get_azure_storage_account(bucket_name=bucket_name),
            container_name=self._get_azure_container(bucket_name=bucket_name),
            prefix=prefix,
        )

    def download_file(self, file_path: str, key: str, bucket_name: str) -> Try[str]:
        return self._hook.download_file(
            storage_account=self._get_azure_storage_account(bucket_name=bucket_name),
            container_name=self._get_azure_container(bucket_name=bucket_name),
            blob_name=key,
            download_file_path=file_path,
        )

    def put_object(self, bucket_name: str, key: str, body: str, replace: bool = False) -> None:
        self._hook.upload_blob(
            storage_account=self._get_azure_storage_account(bucket_name=bucket_name),
            container_name=self._get_azure_container(bucket_name=bucket_name),
            blob_name=key,
            data=body,
            replace=replace,
        )

    def upload_file(self, key: str, file_path: str, bucket_name: str, replace: bool = False) -> None:
        self._hook.upload_file(
            storage_account=self._get_azure_storage_account(bucket_name=bucket_name),
            container_name=self._get_azure_container(bucket_name=bucket_name),
            blob_name=key,
            upload_file_path=file_path,
            replace=replace,
        )

    def remove_objects(self, bucket_name: str, prefix: str) -> None:
        objects_to_remove = self.list_keys(prefix=prefix, bucket_name=bucket_name)
        for blob in sorted(objects_to_remove, key=len, reverse=True):
            self._hook.delete_blob(
                storage_account=self._get_azure_storage_account(bucket_name=bucket_name),
                container_name=self._get_azure_container(bucket_name=bucket_name),
                blob_name=blob,
            )

    def remove_objects_by_keys(self, bucket_name: str, keys: list) -> None:
        raise NotImplementedError

    def copy_file(self, src_key: str, src_bucket_name: str, dst_key: str, dst_bucket_name: str) -> Try[str]:
        return self._hook.copy_file(
            src_storage_account=self._get_azure_storage_account(bucket_name=src_bucket_name),
            src_container_name=self._get_azure_container(bucket_name=src_bucket_name),
            dst_storage_account=self._get_azure_storage_account(bucket_name=dst_bucket_name),
            dst_container_name=self._get_azure_container(bucket_name=dst_bucket_name),
            src_blob_name=src_key,
            dst_blob_name=dst_key,
        )
