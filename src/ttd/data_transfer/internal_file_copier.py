import logging

from ttd.cloud_storages.cloud_storage import CloudStorage
from ttd.data_transfer.file_copier import FileCopier


class InternalFileCopier(FileCopier):

    def __init__(
        self,
        src_bucket: str,
        dst_bucket: str,
        src_prefix: str,
        dst_prefix: str,
        src_key: str,
        cloud_storage: CloudStorage,
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
        self.cloud_storage = cloud_storage

    def _copy_data(self) -> None:
        logging.info(f"Copying file from {self.src_key} in bucket {self.src_bucket} to {self.dst_key} in bucket {self.dst_bucket}")
        self.cloud_storage.copy_file(
            src_key=self.src_key,
            src_bucket_name=self.src_bucket,
            dst_key=self.dst_key,
            dst_bucket_name=self.dst_bucket,
        ).get()

    def _verify_copy(self) -> bool:
        return self.cloud_storage.check_for_key(key=self.dst_key, bucket_name=self.dst_bucket)

    def _cleanup_copy(self) -> None:
        pass

    def _file_already_exists(self, file_path: str) -> bool:
        return self._check_file_exists(file_path, self.cloud_storage, self.cloud_storage)
