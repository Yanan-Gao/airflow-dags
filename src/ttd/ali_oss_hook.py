import json
import logging
import os
from typing import Optional, Tuple, List, Callable, TypeVar

import oss2
import re
from airflow.hooks.base import BaseHook
from oss2 import determine_part_size, SizedFileAdapter
from oss2.models import PartInfo, GetObjectMetaResult

from ttd.alicloud.ali_hook import Defaults
from urllib.parse import urlparse
from botocore.exceptions import ClientError

from ttd.mixins.retry_mixin import RetryMixin
from ttd.monads.trye import Try, Failure, Success


class AliOSSHook(BaseHook, RetryMixin):
    """
    Custom hook for AliCloud OSS which uses the service account for Airflow for authentication.
    An equivalent of the methods available for S3Hook had to be included (check_for_key, check_for_prefix)
    :param conn_id: The connection id used for authentication, by default is set to the service account for Airflow
    :param max_retries: Max number of times to retry the request in the event of API issues that cause the request to fail. Passed to RetryMixin
    :param retry_interval: Time between retries in seconds. Passed to RetryMixin
    :param exponential_retry: Enable exponential backoff between retries. Passed to RetryMixin
    """

    def __init__(
        self,
        conn_id: Optional[str] = "alicloud_emr_access_key",
        max_retries: int = 5,
        retry_interval: int = 30,
        exponential_retry: bool = True,
    ):
        self.conn_id = conn_id
        self.auth_credentials = self.auth()
        RetryMixin.__init__(self, max_retries=max_retries, retry_interval=retry_interval, exponential_retry=exponential_retry)

    def auth(self) -> oss2.Auth:
        connection = self.get_connection(self.conn_id)  # type: ignore
        extra = json.loads(connection.get_extra())

        return oss2.Auth(extra[Defaults.ACCESS_KEY_ID], extra[Defaults.ACCESS_KEY_SECRET])

    T = TypeVar("T")

    def call_with_retry(self, api_call: Callable[[], T]) -> T:
        return self.with_retry(lambda: api_call(), lambda ex: self.should_retry(ex)).get()

    def should_retry(self, ex: Exception) -> bool:
        import traceback
        traceback.print_exception(type(ex), ex, ex.__traceback__)

        if not isinstance(ex, oss2.exceptions.RequestError):
            self.log.info("Not of type RequestError. Won't retry")
            return False

        inner_ex = ex.exception

        import requests
        if isinstance(inner_ex, requests.exceptions.ConnectTimeout):
            self.log.info("Wrapped exception is a ConnectTimeout. Should retry")
            return True

        self.log.info("Wrapped exception is not a ConnectTimeout. Won't retry")
        return False

    def object_exists(self, bucket: oss2.Bucket, key: str) -> bool:
        return self.call_with_retry(lambda: bucket.object_exists(key))

    def check_for_key(self, key: str, endpoint: str, bucket_name: Optional[str] = None) -> bool:
        if not bucket_name:
            (bucket_name, key) = self.parse_oss_url(key)

        try:
            bucket = oss2.Bucket(auth=self.auth_credentials, endpoint=endpoint, bucket_name=bucket_name)
            return self.object_exists(bucket, key)
        except ClientError as e:
            self.log.info(e.response["Error"]["Message"])
            return False

    def create_folder(self, bucket_name: str, endpoint: str, path: str, delimiter: str = "/") -> None:
        self.create_object(
            bucket_name=bucket_name,
            endpoint=endpoint,
            path=self.format_path(path, delimiter),
            body="",
            delimiter=delimiter,
        )

    def create_object(
        self,
        bucket_name: str,
        endpoint: str,
        path: str,
        body: str,
        delimiter: str = "/",
        replace: bool = False,
    ) -> None:
        bucket = oss2.Bucket(self.auth_credentials, endpoint, bucket_name)

        if not replace and self.object_exists(bucket, path):
            self.log.warning(f"Object {path} already exists in the bucket {bucket_name}")
        else:
            self.call_with_retry(lambda: bucket.put_object(key=path, data=body))

    def check_for_prefix(self, bucket_name: str, endpoint: str, prefix: str, delimiter: str) -> bool:
        """
        Checks that a prefix exists in a bucket
        """
        prefix = self.format_path(prefix, delimiter)
        prefix_split = re.split(r"(\w+[{d}])$".format(d=delimiter), prefix, 1)
        previous_level = prefix_split[0]
        plist = self.list_prefixes(
            bucket_name=bucket_name,
            endpoint=endpoint,
            prefix=previous_level,
            delimiter=delimiter,
        )
        return False if plist is None else prefix in plist

    def get_object_meta(self, key: str, endpoint: str, bucket_name: Optional[str] = None) -> GetObjectMetaResult:
        """
        Get metadata about the object
        """
        if not bucket_name:
            (bucket_name, key) = self.parse_oss_url(key)

        bucket = oss2.Bucket(auth=self.auth_credentials, endpoint=endpoint, bucket_name=bucket_name)
        return self.call_with_retry(lambda: bucket.get_object_meta(key))

    def list_prefixes(
        self,
        bucket_name: str,
        endpoint: str,
        prefix: Optional[str] = "",
        delimiter: Optional[str] = "",
        start_after: Optional[str] = "",
        max_items: Optional[int] = 100,
    ) -> List[str]:
        """
        Lists prefixes in a bucket under prefix
        """
        bucket = oss2.Bucket(self.auth_credentials, endpoint, bucket_name)

        def fetch_prefixes() -> List[str]:
            return [
                obj.key for obj in
                oss2.ObjectIteratorV2(bucket=bucket, prefix=prefix, delimiter=delimiter, start_after=start_after, max_keys=max_items)
            ]

        return self.call_with_retry(fetch_prefixes)

    def list_keys(self, bucket_name: str, endpoint: str, prefix: Optional[str] = "") -> List[str]:
        """
        List keys in a bucket with the specified prefix.
        """
        bucket = oss2.Bucket(self.auth_credentials, endpoint, bucket_name)

        def fetch_keys() -> List[str]:
            return [obj.key for obj in oss2.ObjectIteratorV2(bucket=bucket, prefix=prefix)]

        return self.call_with_retry(fetch_keys)

    def download_file(self, bucket_name: str, endpoint: str, key: str, file_path: str) -> Try[str]:
        """
        Download file from OSS
        """
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            bucket = oss2.Bucket(self.auth_credentials, endpoint, bucket_name)
            self.call_with_retry(lambda: bucket.get_object_to_file(key, file_path))
        except Exception as e:
            logging.error(e)
            return Failure(AliOSSHookException(str(e)))
        return Success(file_path)

    def upload_file(
        self,
        bucket_name: str,
        endpoint: str,
        file_path: str,
        key: str,
        replace: bool = False,
    ) -> None:
        """
        TODO: This method is not tested yet due to OSS access issues

        Upload the specified file to the OSS bucket with the specified key.
        """
        bucket = oss2.Bucket(self.auth_credentials, endpoint, bucket_name)

        if not replace and self.object_exists(bucket, key):
            raise AliOSSHookException(f"Key {key} already exists in bucket {bucket_name}")

        total_size = os.path.getsize(file_path)
        part_size = determine_part_size(total_size, preferred_size=100 * 1024)
        upload_id = bucket.init_multipart_upload(key).upload_id
        parts = []
        with open(file_path, "rb") as fileobj:
            part_number = 1
            offset = 0
            while offset < total_size:
                num_to_upload = min(part_size, total_size - offset)
                result = bucket.upload_part(
                    key,
                    upload_id,
                    part_number,
                    SizedFileAdapter(fileobj, num_to_upload),
                )
                parts.append(PartInfo(part_number, result.etag))
                offset += num_to_upload
                part_number += 1
        bucket.complete_multipart_upload(key, upload_id, parts)

    def copy_file(
        self,
        src_bucket_name: str,
        endpoint: str,
        src_key: str,
        dst_bucket_name: str,
        dst_key: str,
    ) -> Try[str]:
        """
        Copy file from OSS
        """
        try:
            bucket = oss2.Bucket(self.auth_credentials, endpoint, dst_bucket_name)
            self.call_with_retry(lambda: bucket.copy_object(
                source_bucket_name=src_bucket_name,
                source_key=src_key,
                target_key=dst_key,
            ))
        except Exception as e:
            logging.error(e)
            return Failure(AliOSSHookException(str(e)))
        return Success(dst_key)

    def delete_object(self, bucket_name: str, key: str, endpoint: str, delimiter: str = "/") -> None:
        bucket = oss2.Bucket(self.auth_credentials, endpoint, bucket_name)

        if self.object_exists(bucket, key):
            self.call_with_retry(lambda: bucket.delete_object(key=key))

    @staticmethod
    def parse_oss_url(oss_url: str) -> Tuple[str, str]:
        parsed_url = urlparse(oss_url)
        if not parsed_url.netloc:
            raise AliOSSHookException('Please provide a bucket_name instead of "%s"' % oss_url)
        else:
            bucket_name = parsed_url.netloc
            key = parsed_url.path.strip("/")
            return bucket_name, key

    @staticmethod
    def format_path(path: str, delimiter: str) -> str:
        return path + delimiter if path[-1] != delimiter else path


class AliOSSHookException(Exception):
    pass
