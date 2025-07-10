from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.date_external_dataset import DateExternalDataset


class TivoExternalDatasource:
    bucket = "thetradedesk-useast-data-import"
    path_prefix = "tivo/tv_viewership"

    tivo_raw_adlog: DateExternalDataset = DateExternalDataset(
        bucket=bucket,
        path_prefix=path_prefix,
        data_name="adlog_papaya_1/data/incremental",
        date_format=
        "file_name=%Y_%m_%d",  # the actual file name has a trailing "_{unix timestamp}", so date_format is inaccurate but we won't use it
    )

    tivo_ipaddress: DateExternalDataset = DateExternalDataset(
        bucket=bucket,
        path_prefix=path_prefix,
        data_name="papaya_1/ipaddress",
        date_format=
        "file_name=%Y_%m_%d",  # the actual file name has a trailing "_{unix timestamp}", so date_format is inaccurate but we won't use it
    )

    tivo_experian_luids: DateExternalDataset = DateExternalDataset(
        bucket=bucket,
        path_prefix=path_prefix,
        data_name="adlog_papaya_1/experian/incremental",
        date_format=
        "file_name=%Y_%m_%d",  # the actual file name has a trailing "_{unix timestamp}", so date_format is inaccurate but we won't use it
    )

    tivo_request_to_delete: DateExternalDataset = DateExternalDataset(
        bucket=bucket,
        path_prefix=path_prefix,
        data_name="papaya_1/request_to_delete",
        date_format=
        "file_name=%Y_%m_%d",  # the actual file name has a trailing "_{unix timestamp}", so date_format is inaccurate but we won't use it
        success_file=None,
    )


class TivoGeneratedDatasource:
    bucket = "thetradedesk-useast-data-import"
    path_prefix = "linear/acr"

    tivo_enriched: DateGeneratedDataset = DateGeneratedDataset(bucket=bucket, path_prefix=path_prefix, data_name="tivo-enriched", version=1)

    tivo_enriched_v2: DateGeneratedDataset = DateGeneratedDataset(
        bucket=bucket, path_prefix=path_prefix, data_name="tivo-enriched", version=2
    )

    tivo_complete_enriched: DateGeneratedDataset = DateGeneratedDataset(
        bucket=bucket,
        path_prefix=path_prefix,
        data_name="tivo-complete-enriched",
        version=1,
    )


class TivoDatasources:
    external = TivoExternalDatasource
    generated = TivoGeneratedDatasource
