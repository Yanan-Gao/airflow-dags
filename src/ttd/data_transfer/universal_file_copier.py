import logging
import os
import pathlib

from ttd.cloud_storages.cloud_storage import CloudStorage
from ttd.data_transfer.file_copier import FileCopier


class UniversalFileCopier(FileCopier):

    def __init__(
        self,
        tmp_dir_path: str,
        src_bucket: str,
        dst_bucket: str,
        src_prefix: str,
        dst_prefix: str,
        src_key: str,
        src_cloud_storage: CloudStorage,
        dst_cloud_storage: CloudStorage,
        max_try_count: int,
        check_existence: bool = True,
    ):
        super().__init__(
            src_bucket,
            dst_bucket,
            src_prefix,
            dst_prefix,
            src_key,
            max_try_count,
            check_existence,
        )
        self.tmp_dir_path = tmp_dir_path
        self.tmp_file_path = f"{self.tmp_dir_path}/{self.src_key}"
        self.src_cloud_storage = src_cloud_storage
        self.dst_cloud_storage = dst_cloud_storage

    def _copy_data(self) -> None:
        os.makedirs(os.path.dirname(self.tmp_file_path), exist_ok=True)
        logging.info(f"Downloading file: {self.src_key}")
        self.src_cloud_storage.download_file(bucket_name=self.src_bucket, key=self.src_key, file_path=self.tmp_file_path).get()

        logging.info(f"Uploading file: {self.dst_key}")
        self.dst_cloud_storage.upload_file(
            file_path=self.tmp_file_path,
            bucket_name=self.dst_bucket,
            key=self.dst_key,
            replace=True,
        )

    def _verify_copy(self) -> bool:
        return self.dst_cloud_storage.check_for_key(key=self.dst_key, bucket_name=self.dst_bucket)

    def _cleanup_copy(self) -> None:
        if pathlib.Path(self.tmp_file_path).exists():
            os.remove(self.tmp_file_path)

    def _file_already_exists(self, file_path: str) -> bool:
        return self._check_file_exists(file_path, self.src_cloud_storage, self.dst_cloud_storage)
