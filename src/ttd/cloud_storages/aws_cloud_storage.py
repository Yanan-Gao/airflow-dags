import logging
from datetime import datetime
from typing import Any, List, Optional
from io import BytesIO

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from ttd.cloud_storages.cloud_storage import CloudStorage
from ttd.monads.trye import Failure, Success, Try


class AwsCloudStorage(CloudStorage):

    def __init__(self, conn_id: Optional[str] = None, extra_args: Optional[dict] = None):
        super().__init__(conn_id=conn_id)
        self._hook = S3Hook(aws_conn_id=conn_id, extra_args=extra_args) if conn_id else S3Hook(extra_args=extra_args)

    def put_object(self, bucket_name: str, key: str, body: str, replace: bool = False, acl_policy: Optional[str] = None) -> None:
        self._hook.load_string(string_data=body, key=key, bucket_name=bucket_name, replace=replace, acl_policy=acl_policy)

    def put_buffer(self, file_obj: BytesIO, bucket_name: str, key: str, replace: bool = False, acl_policy: Optional[str] = None) -> None:
        self._hook.load_file_obj(file_obj, key=key, bucket_name=bucket_name, replace=replace, acl_policy=acl_policy)

    def remove_objects(self, bucket_name: str, prefix: str, remove_chunk_size=500) -> None:
        keys_list = self.list_keys(prefix, bucket_name)
        chunk_size = min(len(keys_list), remove_chunk_size)
        keys_chunks = [keys_list[i:i + chunk_size] for i in range(0, len(keys_list), chunk_size)]
        for keys in keys_chunks:
            self._hook.delete_objects(bucket=bucket_name, keys=keys)

    def remove_objects_by_keys(self, bucket_name: str, keys: list) -> None:
        self._hook.delete_objects(bucket=bucket_name, keys=keys)

    def check_for_key(self, key: str, bucket_name: str) -> bool:
        return self._hook.check_for_key(key=key, bucket_name=bucket_name)

    def check_for_wildcard_key(self, wildcard_key: str, bucket_name: str, delimiter: str = "") -> bool:
        return self._hook.check_for_wildcard_key(wildcard_key=wildcard_key, bucket_name=bucket_name, delimiter=delimiter)

    def get_wildcard_key(self, wildcard_key: str, bucket_name: str) -> str | None:
        obj = self._hook.get_wildcard_key(wildcard_key=wildcard_key, bucket_name=bucket_name)
        if obj:
            return obj.key
        return None

    def _parse_bucket_and_key(self, key: str, bucket_name: Optional[str]) -> tuple[str, str]:
        if bucket_name is None:
            # If bucket not provided, assume key is the full path and we should parse to get the bucket
            bucket_name, key = self._hook.parse_s3_url(key)
        return bucket_name, key

    def read_key(self, key: str, bucket_name: Optional[str] = None) -> str:
        bucket_name, key = self._parse_bucket_and_key(key, bucket_name)
        return self._hook.read_key(key=key, bucket_name=bucket_name)

    def get_key_object(self, key: str, bucket_name: Optional[str] = None):
        bucket_name, key = self._parse_bucket_and_key(key, bucket_name)
        return self._hook.get_key(key=key, bucket_name=bucket_name)

    def check_for_prefix(self, prefix: str, bucket_name: str, delimiter: str = "/") -> bool:
        return self._hook.check_for_prefix(bucket_name=bucket_name, prefix=prefix, delimiter=delimiter)

    def get_file_size(self, key: str, bucket_name: str) -> int:
        return self._hook.get_conn().head_object(Bucket=bucket_name, Key=key)["ContentLength"]

    def list_keys(self, prefix: str, bucket_name: str) -> List[str]:
        return self._hook.list_keys(bucket_name=bucket_name, prefix=prefix)

    def list_prefixes(self, bucket_name: str, prefix: str, delimiter: str = "/") -> List[str]:
        return self._hook.list_prefixes(bucket_name=bucket_name, prefix=prefix, delimiter=delimiter)

    def download_file(self, file_path: str, key: str, bucket_name: str) -> Try[str]:
        try:
            self._hook.get_conn().download_file(bucket_name, key, file_path)
        except Exception as e:
            logging.error(e)
            return Failure(AirflowException(str(e)))
        return Success(file_path)

    def upload_file(self, key: str, file_path: str, bucket_name: str, replace: bool = False, acl_policy: Optional[str] = None) -> None:
        self._hook.load_file(filename=file_path, key=key, bucket_name=bucket_name, replace=replace, acl_policy=acl_policy)

    def load_string(self, string_data: str, key: str, bucket_name: str, replace: bool = False, acl_policy: Optional[str] = None) -> None:
        self._hook.load_string(string_data=string_data, key=key, bucket_name=bucket_name, replace=replace, acl_policy=acl_policy)

    def copy_file(self, src_key: str, src_bucket_name: str, dst_key: str, dst_bucket_name: str) -> Try[str]:
        try:
            target_bucket = self._hook.get_bucket(dst_bucket_name)
            target_bucket.copy({"Bucket": src_bucket_name, "Key": src_key}, dst_key)
        except Exception as e:
            logging.error(e)
            return Failure(AirflowException(str(e)))
        return Success(dst_key)

    def list_keys_with_filtered_value(
        self,
        bucket_name: str,
        prefix: str,
        filter_value: str,
        delimiter: str = "",
        page_size: Optional[int] = None,
        max_items: Optional[int] = None,
        start_after_key: str = "",
        from_datetime: Optional[datetime] = None,
        to_datetime: Optional[datetime] = None,
    ) -> dict[str, Any]:

        def extract_key_value_pairs(
            items: list[dict[str, Any]],
            from_datetime: Optional[datetime] = None,
            to_datetime: Optional[datetime] = None,
        ) -> list[tuple[str, Any]]:

            def _has_required_field(item: dict[str, Any]) -> bool:
                return filter_value in item

            def _is_in_period(input_date: datetime) -> bool:
                if from_datetime is not None and input_date < from_datetime:
                    return False

                if to_datetime is not None and input_date > to_datetime:
                    return False
                return True

            return [(item["Key"], item[filter_value]) for item in items
                    if _has_required_field(item) and _is_in_period(item["LastModified"])]

        key_value_pairs = self._hook.list_keys(
            bucket_name=bucket_name,
            prefix=prefix,
            delimiter=delimiter,
            page_size=page_size,
            max_items=max_items,
            start_after_key=start_after_key,
            from_datetime=from_datetime,
            to_datetime=to_datetime,
            object_filter=extract_key_value_pairs
        )

        return dict(key_value_pairs)
