from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourDataset
from typing import Optional

DEFAULT_BUCKET = "thetradedesk-mlplatform-us-east-1"
DEFAULT_PATH_PREFIX = "mlops/experimentation"
DEFAULT_HOUR_FORMAT = "hour={hour}"
SUCCESS = "_SUCCESS"


class JanusKPIHourDataset(HourDataset):

    def __init__(
        self,
        dataset: str,
        stage: str,
        path_prefix: str = DEFAULT_PATH_PREFIX,
        bucket: str = DEFAULT_BUCKET,
        hour_format: str = DEFAULT_HOUR_FORMAT,
        check_type: str = "hour",
        version: Optional[int] = None,
        success_file: Optional[str] = SUCCESS,
        **kwargs,
    ):
        super().__init__(
            bucket=bucket,
            path_prefix=path_prefix,
            data_name=f"{stage}/{dataset}",
            version=version,
            hour_format=hour_format,
            check_type=check_type,
            success_file=success_file,
            **kwargs
        )


class IntermediateJanusKPIHourDataset(JanusKPIHourDataset):

    def __init__(
        self,
        dataset: str,
        path_prefix: str = DEFAULT_PATH_PREFIX,
        bucket: str = DEFAULT_BUCKET,
        hour_format: str = DEFAULT_HOUR_FORMAT,
        check_type: str = "hour",
        version: Optional[int] = None,
        success_file: Optional[str] = SUCCESS,
        **kwargs,
    ):
        super().__init__(
            dataset=dataset,
            stage="intermediate",
            bucket=bucket,
            path_prefix=path_prefix,
            version=version,
            hour_format=hour_format,
            check_type=check_type,
            success_file=success_file,
            **kwargs,
        )


class JanusKPIDateDataset(DateGeneratedDataset):

    def __init__(
        self,
        dataset: str,
        stage: str,
        path_prefix: str = DEFAULT_PATH_PREFIX,
        bucket: str = DEFAULT_BUCKET,
        version: Optional[int] = None,
        success_file: Optional[str] = SUCCESS,
        **kwargs,
    ):
        super().__init__(
            bucket=bucket, path_prefix=path_prefix, data_name=f"{stage}/{dataset}", version=version, success_file=success_file, **kwargs
        )


class IntermediateJanusKPIDateDataset(JanusKPIDateDataset):

    def __init__(
        self,
        dataset: str,
        path_prefix: str = DEFAULT_PATH_PREFIX,
        bucket: str = DEFAULT_BUCKET,
        version: Optional[int] = None,
        success_file: Optional[str] = SUCCESS,
        **kwargs,
    ):
        super().__init__(
            dataset=dataset,
            stage="intermediate",
            bucket=bucket,
            path_prefix=path_prefix,
            version=version,
            success_file=success_file,
            **kwargs,
        )
