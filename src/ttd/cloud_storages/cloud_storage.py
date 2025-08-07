from abc import ABC, abstractmethod
from typing import List, Optional
from io import BytesIO

from ttd.monads.trye import Try


class CloudStorage(ABC):

    def __init__(self, conn_id: Optional[str] = None):
        self._conn_id = conn_id

    @abstractmethod
    def check_for_key(self, key: str, bucket_name: str) -> bool:
        pass

    @abstractmethod
    def check_for_prefix(self, prefix: str, bucket_name: str, delimiter: str = "/") -> bool:
        pass

    @abstractmethod
    def get_file_size(self, key: str, bucket_name: str) -> int:
        pass

    @abstractmethod
    def list_keys(self, prefix: str, bucket_name: str) -> List[str]:
        pass

    @abstractmethod
    def download_file(self, file_path: str, key: str, bucket_name: str) -> Try[str]:
        pass

    @abstractmethod
    def upload_file(self, key: str, file_path: str, bucket_name: str, replace: bool = False) -> None:
        pass

    @abstractmethod
    def put_object(self, bucket_name: str, key: str, body: str, replace: bool = False) -> None:
        pass

    def put_buffer(self, file_obj: BytesIO, bucket_name: str, key: str, replace: bool = False, acl_policy: Optional[str] = None) -> None:
        raise NotImplementedError

    @abstractmethod
    def remove_objects(self, bucket_name: str, prefix: str) -> None:
        pass

    @abstractmethod
    def remove_objects_by_keys(self, bucket_name: str, keys: list) -> None:
        pass

    @abstractmethod
    def copy_file(self, src_key: str, src_bucket_name: str, dst_key: str, dst_bucket_name: str) -> Try[str]:
        pass
