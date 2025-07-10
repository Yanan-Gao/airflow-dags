import json
import logging
import os

from airflow.hooks.base import BaseHook
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobClient, ContainerClient, BlobProperties, BlobBlock
from typing import List, Optional, Union, Iterable, AnyStr, IO

from ttd.monads.trye import Try, Failure, Success


class AzureBlobHook(BaseHook):
    """
    Custom hook for Azure Blob Storage which uses the service account for Airflow for authentication.
    An equivalent of the methods available for S3Hook had to be included (check_for_key, check_for_prefix)
    @param conn_id: The connection id used for authentication, by default is set to the service account for Airflow
    """

    def __init__(self, conn_id="azure_service_account"):
        self.conn_id = conn_id
        self.client_credentials = self.get_conn()

    BYTES_IN_1_MIB = 1_048_576
    MAX_BLOCK_SIZE = 100 * BYTES_IN_1_MIB
    MAX_BLOCKS_NUM = 50000
    MIN_BLOCK_ID_DIGITS = len(str(MAX_BLOCKS_NUM))

    def get_conn(self) -> ClientSecretCredential:
        connection = self.get_connection(self.conn_id)
        extra = json.loads(connection.get_extra())
        return ClientSecretCredential(
            client_id=extra["CLIENT_ID"],
            client_secret=connection.get_password(),  # type: ignore
            tenant_id=extra["TENANT_ID"],
        )

    def check_for_key(self, storage_account: str, container_name: str, blob_name: str) -> bool:
        blob_client = self._get_blob_client(
            account_url=self._get_account_url(storage_account=storage_account),
            container_name=container_name,
            blob_name=blob_name,
        )
        return blob_client.exists()

    def check_for_prefix(self, storage_account: str, container_name: str, prefix: str) -> bool:
        container_client = self._get_container_client(
            account_url=self._get_account_url(storage_account=storage_account),
            container_name=container_name,
        )

        blobs = container_client.list_blobs(name_starts_with=prefix)

        return any(blobs)

    def get_blob_properties(self, storage_account: str, container_name: str, blob_name: str) -> BlobProperties:
        blob_client = self._get_blob_client(
            account_url=self._get_account_url(storage_account=storage_account),
            container_name=container_name,
            blob_name=blob_name,
        )
        return blob_client.get_blob_properties()

    def list_keys(self, storage_account: str, container_name: str, prefix: Optional[str] = None) -> List[str]:
        container_client = self._get_container_client(
            account_url=self._get_account_url(storage_account=storage_account),
            container_name=container_name,
        )

        blobs = container_client.list_blobs(name_starts_with=prefix)
        blob_names = [blob.name for blob in blobs]

        return blob_names

    def download_file(
        self,
        storage_account: str,
        container_name: str,
        blob_name: str,
        download_file_path: str,
    ) -> Try[str]:
        try:
            blob_client = self._get_blob_client(
                account_url=self._get_account_url(storage_account=storage_account),
                container_name=container_name,
                blob_name=blob_name,
            )

            os.makedirs(os.path.dirname(download_file_path), exist_ok=True)

            with open(download_file_path, "wb") as download_file:
                chunks = blob_client.download_blob(read_timeout=60).chunks()
                for chunk in chunks:
                    download_file.write(chunk)
        except Exception as e:
            logging.error(e)
            return Failure(AzureBlobHookException(str(e)))
        return Success(download_file_path)

    def upload_file(
        self,
        storage_account: str,
        container_name: str,
        blob_name: str,
        upload_file_path: str,
        replace: bool = False,
    ) -> None:
        with open(upload_file_path, "rb") as upload_file:
            self.upload_blob(storage_account, container_name, blob_name, upload_file, replace)

    def upload_blob(
        self,
        storage_account: str,
        container_name: str,
        blob_name: str,
        data: Union[Iterable[AnyStr], IO[AnyStr]],
        replace: bool = False,
    ) -> None:
        blob_client = self._get_blob_client(
            account_url=self._get_account_url(storage_account=storage_account),
            container_name=container_name,
            blob_name=blob_name,
        )

        blob_client.upload_blob(data=data, overwrite=replace)

    def delete_blob(self, storage_account: str, container_name: str, blob_name: str) -> None:
        blob_client = self._get_blob_client(
            account_url=self._get_account_url(storage_account=storage_account),
            container_name=container_name,
            blob_name=blob_name,
        )

        if blob_client.exists():
            blob_client.delete_blob()

    def copy_file(
        self,
        src_storage_account: str,
        src_container_name: str,
        dst_storage_account: str,
        dst_container_name: str,
        src_blob_name: str,
        dst_blob_name: str,
    ) -> Try[str]:
        dst_blob_client = self._get_blob_client(
            account_url=self._get_account_url(storage_account=dst_storage_account),
            container_name=dst_container_name,
            blob_name=dst_blob_name,
        )
        src_blob_client = self._get_blob_client(
            account_url=self._get_account_url(storage_account=src_storage_account),
            container_name=src_container_name,
            blob_name=src_blob_name,
        )
        access_token = self.client_credentials.get_token("https://storage.azure.com/.default")
        auth = f"Bearer {access_token.token}"
        source_properties = src_blob_client.get_blob_properties()
        blob_length_bytes = source_properties.size
        block_count = 0
        block_list = []
        for step in range(0, blob_length_bytes, self.MAX_BLOCK_SIZE):
            content_size = self.MAX_BLOCK_SIZE
            if (blob_length_bytes - step) < self.MAX_BLOCK_SIZE:
                content_size = blob_length_bytes - step
            block_count += 1
            block_id = str(block_count).zfill(self.MIN_BLOCK_ID_DIGITS)
            dst_blob_client.stage_block_from_url(
                block_id=block_id,
                source_url=src_blob_client.url,
                source_offset=step,
                source_length=content_size,
                source_authorization=auth,
            )
            block_list.append(BlobBlock(block_id=block_id))
        dst_blob_client.commit_block_list(
            block_list=block_list,
            content_settings=source_properties.content_settings,
        )
        committed_blocks, uncommitted_blocks = dst_blob_client.get_block_list()
        if len(uncommitted_blocks) > 0:
            return Failure(AzureBlobHookException(f"{len(uncommitted_blocks)} blocks are still left to commit"))
        return Success(dst_blob_name)

    def _get_blob_client(self, account_url: str, container_name: str, blob_name: str) -> BlobClient:
        return BlobClient(
            account_url=account_url,
            container_name=container_name,
            blob_name=blob_name,
            credential=self.client_credentials,
        )

    def _get_container_client(self, account_url: str, container_name: str) -> ContainerClient:
        return ContainerClient(
            account_url=account_url,
            container_name=container_name,
            credential=self.client_credentials,
        )

    @staticmethod
    def _get_account_url(storage_account: str) -> str:
        return storage_account + ".blob.core.windows.net"


class AzureBlobHookException(Exception):
    pass
