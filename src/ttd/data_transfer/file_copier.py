from abc import ABC, abstractmethod
import os
import logging

from ttd.cloud_storages.cloud_storage import CloudStorage


class FileCopier(ABC):

    def __init__(
        self,
        src_bucket: str,
        dst_bucket: str,
        src_prefix: str,
        dst_prefix: str,
        src_key: str,
        max_try_count: int,
        check_existence: bool,
    ):
        self.src_bucket = src_bucket
        self.dst_bucket = dst_bucket
        self.src_prefix = src_prefix
        self.dst_prefix = dst_prefix
        self.src_key = src_key
        self.max_try_count = max_try_count
        src_file_rel_to_prefix_path = os.path.relpath(self.src_key, self.src_prefix)
        self.dst_key = os.path.normpath(os.path.join(self.dst_prefix, src_file_rel_to_prefix_path))
        self.check_existence = check_existence

    @abstractmethod
    def _copy_data(self) -> None:
        pass

    @abstractmethod
    def _verify_copy(self) -> bool:
        pass

    @abstractmethod
    def _cleanup_copy(self) -> None:
        pass

    @abstractmethod
    def _file_already_exists(self, file_path: str) -> bool:
        pass

    def copy_file(self) -> str:
        try_count = 0

        if self.check_existence and self._file_already_exists(self.dst_key):
            logging.info(f"File {self.dst_key} has already been transferred, skipping.")
            return self.dst_key

        while try_count <= self.max_try_count:
            try:
                try_count += 1
                self._copy_data()
                logging.info(f"Verifying file presence {self.dst_key} in destination bucket {self.dst_bucket}")
                if not self._verify_copy():
                    raise FileCopierError(f"File {self.dst_key} is missing in destination bucket {self.dst_bucket}")
                logging.info(f"File {self.dst_key} is present in destination bucket {self.dst_bucket}")
                return self.dst_key
            except Exception as e:
                logging.error(
                    f"Failed to copy file: {self.src_key}, number of tries: {try_count}",
                    exc_info=e,
                )
                if try_count >= self.max_try_count:
                    logging.error(
                        f"Number of retry attempts ({try_count} tries) exceeded for file: {self.src_key}",
                        exc_info=e,
                    )
                    raise e

            finally:
                self._cleanup_copy()
        return ""

    def _check_file_exists(
        self,
        file_path: str,
        src_cloud_storage: CloudStorage,
        dst_cloud_storage: CloudStorage,
    ) -> bool:
        file_in_dst = dst_cloud_storage.check_for_key(key=file_path, bucket_name=self.dst_bucket)

        if file_in_dst:
            src_file_size = src_cloud_storage.get_file_size(key=self.src_key, bucket_name=self.src_bucket)
            dst_file_size = dst_cloud_storage.get_file_size(key=file_path, bucket_name=self.dst_bucket)

            return src_file_size == dst_file_size

        return file_in_dst


class FileCopierError(Exception):
    pass
