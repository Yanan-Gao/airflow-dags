import uuid

from ttd.cloud_storages.cloud_storage import CloudStorage
from ttd.data_transfer.internal_file_copier import InternalFileCopier
from ttd.data_transfer.universal_file_copier import UniversalFileCopier
from ttd.data_transfer.file_copier import FileCopier


class FileCopierFactory:

    def __init__(
        self,
        src_bucket: str,
        dst_bucket: str,
        src_prefix: str,
        dst_prefix: str,
        src_cloud_storage: CloudStorage,
        dst_cloud_storage: CloudStorage,
        max_try_count: int,
        transfer_directory: str,
        try_internal_transfer: bool = True,
        check_existence: bool = True,
    ):
        self.src_bucket = src_bucket
        self.dst_bucket = dst_bucket
        self.src_prefix = src_prefix
        self.dst_prefix = dst_prefix
        self.src_cloud_storage = src_cloud_storage
        self.dst_cloud_storage = dst_cloud_storage
        self.max_try_count = max_try_count
        self.internal_transfer = (try_internal_transfer and FileCopierFactory.is_internal(src_cloud_storage, dst_cloud_storage))
        self.check_existence = check_existence
        self.tmp_dir_path = f"{transfer_directory}/{uuid.uuid4().hex}" if not self.internal_transfer else None

    @staticmethod
    def is_internal(src_cloud_storage: CloudStorage, dst_cloud_storage: CloudStorage) -> bool:
        return isinstance(src_cloud_storage, type(dst_cloud_storage))

    def get_copier(self, src_key: str) -> FileCopier:
        if self.internal_transfer:
            return InternalFileCopier(
                src_bucket=self.src_bucket,
                dst_bucket=self.dst_bucket,
                src_prefix=self.src_prefix,
                dst_prefix=self.dst_prefix,
                src_key=src_key,
                cloud_storage=self.src_cloud_storage,
                max_try_count=self.max_try_count,
                check_existence=self.check_existence,
            )
        else:
            return UniversalFileCopier(
                tmp_dir_path=self.tmp_dir_path,
                src_bucket=self.src_bucket,
                dst_bucket=self.dst_bucket,
                src_prefix=self.src_prefix,
                dst_prefix=self.dst_prefix,
                src_key=src_key,
                src_cloud_storage=self.src_cloud_storage,
                dst_cloud_storage=self.dst_cloud_storage,
                max_try_count=self.max_try_count,
                check_existence=self.check_existence,
            )
