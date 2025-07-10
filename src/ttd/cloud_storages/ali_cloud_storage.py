from typing import Optional, List

from ttd.ali_oss_hook import AliOSSHook
from ttd.cloud_storages.cloud_storage import CloudStorage
from ttd.monads.trye import Try

SHANGHAI_OSS_ENDPOINT = "https://oss-cn-shanghai.aliyuncs.com"


class AliCloudStorage(CloudStorage):

    def __init__(self, conn_id: Optional[str] = None, endpoint: Optional[str] = None):
        super().__init__(conn_id=conn_id)
        self._endpoint = endpoint if endpoint else SHANGHAI_OSS_ENDPOINT
        self._hook = AliOSSHook(conn_id=conn_id) if conn_id else AliOSSHook()

    def create_folder(self, bucket_name: str, path: str, delimiter: str = "/"):
        return self._hook.create_folder(
            bucket_name=bucket_name,
            endpoint=self._endpoint,
            path=path,
            delimiter=delimiter,
        )

    def check_for_key(self, key: str, bucket_name: str) -> bool:
        return self._hook.check_for_key(key=key, bucket_name=bucket_name, endpoint=self._endpoint)

    def check_for_prefix(self, prefix: str, bucket_name: str, delimiter: str = "/") -> bool:
        return self._hook.check_for_prefix(
            bucket_name=bucket_name,
            endpoint=self._endpoint,
            prefix=prefix,
            delimiter=delimiter,
        )

    def get_file_size(self, key: str, bucket_name: str) -> int:
        return self._hook.get_object_meta(key=key, endpoint=self._endpoint, bucket_name=bucket_name).content_length

    def list_keys(self, prefix: str, bucket_name: str) -> List[str]:
        return self._hook.list_keys(bucket_name=bucket_name, endpoint=self._endpoint, prefix=prefix)

    def download_file(self, file_path: str, key: str, bucket_name: str) -> Try[str]:
        return self._hook.download_file(
            bucket_name=bucket_name,
            endpoint=self._endpoint,
            file_path=file_path,
            key=key,
        )

    def upload_file(self, key: str, file_path: str, bucket_name: str, replace: bool = False) -> None:
        self._hook.upload_file(
            bucket_name=bucket_name,
            endpoint=self._endpoint,
            key=key,
            file_path=file_path,
            replace=replace,
        )

    def put_object(self, bucket_name: str, key: str, body: str, replace: bool = False) -> None:
        self._hook.create_object(
            bucket_name=bucket_name,
            endpoint=self._endpoint,
            path=key,
            body=body,
            replace=replace,
        )

    def remove_objects(self, bucket_name: str, prefix: str) -> None:
        for obj_key in self.list_keys(prefix=prefix, bucket_name=bucket_name):
            self._hook.delete_object(bucket_name=bucket_name, key=obj_key, endpoint=self._endpoint)

    def remove_objects_by_keys(self, bucket_name: str, keys: list) -> None:
        raise NotImplementedError

    def copy_file(self, src_key: str, src_bucket_name: str, dst_key: str, dst_bucket_name: str) -> Try[str]:
        return self._hook.copy_file(
            src_bucket_name=src_bucket_name,
            dst_bucket_name=dst_bucket_name,
            src_key=src_key,
            dst_key=dst_key,
            endpoint=self._endpoint,
        )
