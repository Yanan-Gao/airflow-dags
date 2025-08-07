from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.date_external_dataset import DateExternalDataset


class FwmExternalDatasource:
    bucket = "thetradedesk-useast-data-import"
    path_prefix = "fwm"

    fwm_raw: DateExternalDataset = DateExternalDataset(
        bucket=bucket,
        path_prefix=path_prefix,
        data_name="",
        date_format="gd=%Y-%m-%d",  # so date_format is inaccurate but we won't use it
    )

    fwm_raw_impression: DateExternalDataset = DateExternalDataset(
        bucket=bucket,
        path_prefix=path_prefix,
        data_name="impression",
        date_format="gd=%Y-%m-%d",
    )

    fwm_demographics: DateExternalDataset = DateExternalDataset(
        bucket=bucket,
        path_prefix=path_prefix,
        data_name="demographics",
        date_format="gd=%Y-%m-%d",
        success_file=None,
    )


class FwmGeneratedDatasource:
    bucket = "thetradedesk-useast-data-import"
    path_prefix = "linear/acr"

    fwm_enriched: DateGeneratedDataset = DateGeneratedDataset(bucket=bucket, path_prefix=path_prefix, data_name="fwm-enriched", version=1)

    fwm_brand_backfill: DateGeneratedDataset = DateGeneratedDataset(
        bucket=bucket, path_prefix=path_prefix, data_name="fwm-brandBackfill", version=1
    )


class FwmDatasources:
    external = FwmExternalDatasource
    generated = FwmGeneratedDatasource
