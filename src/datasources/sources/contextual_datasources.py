from ttd.datasets.hour_dataset import HourGeneratedDataset


class ContextualDatasources:
    bucket = "ttd-contextual"

    contextual_requested_urls: HourGeneratedDataset = HourGeneratedDataset(
        bucket=bucket, path_prefix="prod", data_name="parquet/cxt_requested_urls", version=1, env_aware=False, success_file=None
    )

    cxt_results: HourGeneratedDataset = HourGeneratedDataset(
        bucket=bucket, path_prefix="prod", data_name="parquet/cxt_results", version=2, env_aware=False, success_file=None
    )
