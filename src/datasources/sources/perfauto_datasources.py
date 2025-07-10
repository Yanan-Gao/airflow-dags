from ttd.datasets.hour_dataset import HourGeneratedDataset
from datasources.datasource import Datasource


class PerformanceAutomationDatasources(Datasource):
    logs_bucket = "thetradedesk-useast-logs-2"
    mlplatform_bucket = "thetradedesk-mlplatform-us-east-1"

    eligible_user_data_raw: HourGeneratedDataset = HourGeneratedDataset(
        bucket=logs_bucket,
        path_prefix="modeleligibleuserdata",
        data_name="collected",
        date_format="%Y/%m/%d",
        hour_format="{hour:0>2d}",
        version=None,
        env_aware=False,
        success_file=None
    )

    eligible_user_data_processed: HourGeneratedDataset = HourGeneratedDataset(
        bucket=mlplatform_bucket,
        path_prefix="prod/features/data",
        data_name="modeleligibleuserdata",
        version=1,
        hour_format="hour={hour}",
        env_aware=False,
        success_file=None
    )

    bid_impression_dataset = HourGeneratedDataset(
        bucket="thetradedesk-mlplatform-us-east-1",
        path_prefix="features/data/koav4/v=1/prod",
        data_name='bidsimpressions',
        version=None,
        env_aware=False,
        date_format='year=%Y/month=%m/day=%d',
        hour_format='hourPart={hour:d}'
    )
