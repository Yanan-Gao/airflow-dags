from typing import Optional


class Datasource:
    """
    Provides base functionality of generating S3 root path for datasources in this domain.
    Usage:
    Use this class as a base class in your datasources class covering domain of similar datasets.
    """

    bucket: Optional[str] = None  # "bucket"
    path_prefix: Optional[str] = None  # "path/prefix"

    @classmethod
    def get_root_path(cls) -> str:
        """
        Get the base location where the data name path exists. Bucket + path prefix.

        :return: S3 Path, Bucket + Prefix
        :rtype: str
        """
        if cls.bucket is None or cls.path_prefix is None:
            raise Exception("Bucket and path_prefix class properties should be defined.")
        return f"s3a://{cls.bucket}/{cls.path_prefix}"
