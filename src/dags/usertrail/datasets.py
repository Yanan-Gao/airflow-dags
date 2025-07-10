from ttd.datasets.hour_dataset import HourGeneratedDataset

# A few notes:
# - Url Format is
#   bucket
#   /path_prefix
#   /env=***        | if env_aware=True/default. If false, this is skipped
#   /data_name
#   /version_str    | if version=None, this is skipped
#   /date_str       | Using date_format. Default is `date=yyyymmdd`
#   /hour_str       | Using hour_format. Default is `hour=hh`
#
# - An HourGeneratedDataset is checked for the whole day by the DatasetCheckSensor.
#   If you want it to check only for the specified hour, you need to add
#   `.with_check_type("hour")`

# =======================================
# Parquet DATASETS
# =======================================

# S3 Url: s3://thetradedesk-useast-logs-2/usertrailevent/collected/2025/03/11/13/
user_trail_events_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-useast-logs-2",
    path_prefix="usertrailevent",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None,
)

user_trail_exposures_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-useast-logs-2",
    path_prefix="usertrailexposure",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None,
)
