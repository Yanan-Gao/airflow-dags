from typing import Literal
from datasources.datasource import Datasource
from ttd.datasets.avails_hourly_dataset import AvailsHourlyDataset
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.datasets.date_generated_dataset import DateGeneratedDataset


class AvailsDatasources(Datasource):
    bucket = "ttd-identity"
    path_prefix = "datapipeline"

    proto_avails: HourGeneratedDataset = HourGeneratedDataset(
        bucket="thetradedesk-useast-partners-avails",
        path_prefix="tapad",
        data_name="",
        version=None,
        date_format="%Y/%m/%d",
        hour_format="{hour:0>2d}",
        env_aware=False,
        success_file=None,
    )

    ctv_avails: HourGeneratedDataset = HourGeneratedDataset(
        bucket=bucket,
        path_prefix=path_prefix,
        data_name="availsUnsampled/availsType=ctvAvails",
        hour_format="hour={hour}",
        version=1,
        env_aware=True,
        success_file=None,
    )

    # Duplicated dataset from above, for checking if entire day is done.
    ctv_avails_date_generated: DateGeneratedDataset = DateGeneratedDataset(
        bucket=bucket,
        path_prefix=path_prefix,
        data_name="availsUnsampled/availsType=ctvAvails",
        version=1,
        env_aware=True,
    )

    househould_sampled_high_sample_avails: HourGeneratedDataset = HourGeneratedDataset(
        bucket=bucket, path_prefix=path_prefix, data_name="hhAvailsHighSample", hour_format="hour={hour}", version=1, env_aware=True
    )

    household_sampled_high_sample_avails_v2: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-sampled-avails-useast1",
        path_prefix="datasets/withPII",
        data_name="identity-deal-agg-hourly-highSample-iav2-delta",
        version=None,
        date_format="date=%Y-%m-%d",
        env_aware=True
    )

    household_sampled_high_sample_avails_openGraphIav2: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-sampled-avails-useast1",
        path_prefix="datasets/withPII",
        data_name="identity-deal-agg-hourly-highSample-openGraphIav2-delta",
        version=None,
        date_format="date=%Y-%m-%d",
        env_aware=True
    )

    person_sampled_avails_openGraphIav2: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-sampled-avails-useast1",
        path_prefix="datasets/withPII",
        data_name="person-sampled-identity-deal-agg-hourly-openGraphIav2-delta",
        version=None,
        date_format="date=%Y-%m-%d",
        hour_format="hour={hour:0>2d}/sampleRate=30",
        env_aware=True
    )

    def household_sampled_identity_deal_agg_hourly(
        sampleRate: Literal["high", "low"], graph: Literal["adBrain", "iav2", "openGraphIav2", "openGraphAdBrain"]
    ) -> HourGeneratedDataset:
        return HourGeneratedDataset(
            bucket="ttd-sampled-avails-useast1",
            path_prefix="datasets/withPII",
            data_name=f"identity-deal-agg-hourly-{sampleRate}Sample-{graph}-delta",
            version=None,
            date_format="date=%Y-%m-%d",
            env_aware=True
        )

    househould_sampled_low_sample_avails: HourGeneratedDataset = HourGeneratedDataset(
        bucket=bucket, path_prefix=path_prefix, data_name="hhAvailsLowSample", hour_format="hour={hour}", version=1, env_aware=True
    )

    user_sampled_high_sample_avails: HourGeneratedDataset = HourGeneratedDataset(
        bucket=bucket, path_prefix=path_prefix, data_name="avails7day", hour_format="hour={hour}", version=2, env_aware=True
    )

    user_sampled_low_sample_avails: HourGeneratedDataset = HourGeneratedDataset(
        bucket=bucket, path_prefix=path_prefix, data_name="avails30day", hour_format="hour={hour}", version=2, env_aware=True
    )

    user_profiles_avails: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-hadoop",
        path_prefix="unified-data-models",
        data_name="user-profiles/avails",
        version=None,
        date_format="date=%Y-%m-%d",
    )

    # user sampled avails with fast sampling rate 1/30
    avails_7_day: HourGeneratedDataset = HourGeneratedDataset(
        bucket=bucket,
        path_prefix=path_prefix,
        data_name="avails7day",
        hour_format="hour={hour}",
        version=2,
        env_aware=True,
        success_file=None,
    )

    # user sampled avails with slow sampling rate 1/900
    avails_30_day: HourGeneratedDataset = HourGeneratedDataset(
        bucket=bucket, path_prefix=path_prefix, data_name="avails30day", hour_format="hour={hour}", version=2, env_aware=True
    )

    non_pii_dataset_region_dependency_mapping = {
        "us-east-1": [
            "us-east-1",
            "us-west-2",
            "ap-northeast-1",
            "ap-southeast-1",
            "eu-west-1",
            "eu-central-1",
        ],
        "Ali_China_East_2": ["Ali_China_East_2"],
    }

    # Separate mapping for PII data. Currently identical, but when  we add China they will be different. So keeping the
    # two mappings as separate variables for now to make our jobs easier later
    pii_dataset_region_dependency_mapping = {
        "us-east-1": [
            "us-east-1",
            "us-west-2",
            "ap-northeast-1",
            "ap-southeast-1",
            "eu-west-1",
            "eu-central-1",
        ],
        "Ali_China_East_2": ["Ali_China_East_2"],
    }

    raw_avails_region_dependency_mapping = {
        "us-east-1": ["us-east-1"],
        "us-west-2": ["us-west-2"],
        "ap-northeast-1": ["ap-northeast-1"],
        "ap-southeast-1": ["ap-southeast-1"],
        "eu-west-1": ["eu-west-1"],
        "eu-central-1": ["eu-central-1"],
    }

    region_bucket_mapping = {
        "us-east-1": "thetradedesk-useast-avails",
        "us-west-2": "thetradedesk-uswest-2-avails",
        "ap-northeast-1": "thetradedesk-jp1-avails",
        "ap-southeast-1": "thetradedesk-sg2-avails",
        "eu-west-1": "thetradedesk-ie1-avails",
        "eu-central-1": "thetradedesk-de2-avails",
        "Ali_China_East_2": "thetradedesk-cn-avails",
    }

    deal_set_agg_hourly_dataset: AvailsHourlyDataset = AvailsHourlyDataset(
        bucket="thetradedesk-useast-avails",
        oss_bucket="thetradedesk-cn-avails",
        path_prefix="datasets/withoutPII",
        data_name="deal-set-avails-agg-hourly-delta",
        version=None,
        region_dependency_map=non_pii_dataset_region_dependency_mapping,
        buckets_for_other_regions=region_bucket_mapping,
    )

    deal_agg_hourly_dataset: AvailsHourlyDataset = AvailsHourlyDataset(
        bucket="thetradedesk-useast-avails",
        oss_bucket="thetradedesk-cn-avails",
        path_prefix="datasets/withoutPII",
        data_name="deal-avails-agg-hourly-delta",
        version=None,
        region_dependency_map=non_pii_dataset_region_dependency_mapping,
        buckets_for_other_regions=region_bucket_mapping,
    )

    deal_agg_daily_dataset: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-avails",
        oss_bucket="thetradedesk-cn-avails",
        path_prefix="datasets/withoutPII",
        data_name="deal-avails-agg-daily-delta",
        version=None,
        env_aware=True,
        date_format="date=%Y-%m-%d"
    )

    identity_agg_hourly_dataset: AvailsHourlyDataset = AvailsHourlyDataset(
        bucket="thetradedesk-useast-avails",
        oss_bucket="thetradedesk-cn-avails",
        path_prefix="datasets/withPII",
        data_name="identity-avails-agg-hourly-v2-delta",
        version=None,
        region_dependency_map=pii_dataset_region_dependency_mapping,
        buckets_for_other_regions=region_bucket_mapping,
    )

    identity_agg_daily_dataset: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-avails",
        oss_bucket="thetradedesk-cn-avails",
        path_prefix="datasets/withPII",
        data_name="identity-avails-agg-daily-v2-delta",
        version=None,
        env_aware=True,
        date_format="date=%Y-%m-%d"
    )

    publisher_agg_hourly_dataset: AvailsHourlyDataset = AvailsHourlyDataset(
        bucket="thetradedesk-useast-avails",
        oss_bucket="thetradedesk-cn-avails",
        path_prefix="datasets/withoutPII",
        data_name="publisher-agg-hourly-delta",
        version=None,
        region_dependency_map=non_pii_dataset_region_dependency_mapping,
        buckets_for_other_regions=region_bucket_mapping,
    )

    publisher_agg_daily_dataset: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-avails",
        oss_bucket="thetradedesk-cn-avails",
        path_prefix="datasets/withoutPII",
        data_name="publisher-agg-daily-delta",
        version=None,
        env_aware=True,
        date_format="date=%Y-%m-%d"
    )

    identity_and_deal_agg_hourly_dataset: AvailsHourlyDataset = AvailsHourlyDataset(
        bucket="thetradedesk-useast-avails",
        oss_bucket="thetradedesk-cn-avails",
        path_prefix="datasets/withPII",
        data_name="identity-deal-agg-hourly-delta",
        version=None,
        region_dependency_map=pii_dataset_region_dependency_mapping,
        buckets_for_other_regions=region_bucket_mapping,
    )

    clean_room_avails_agg_hourly_dataset: HourGeneratedDataset = HourGeneratedDataset(
        bucket="thetradedesk-useast-avails",
        oss_bucket="thetradedesk-cn-avails",
        path_prefix="datasets/withPII",
        data_name="clean-room-forecasting-avails-agg-hourly-delta",
        version=None,
        buckets_for_other_regions=region_bucket_mapping,
        date_format="date=%Y-%m-%d"
    )

    avails_pipeline_raw_delta_dataset: AvailsHourlyDataset = AvailsHourlyDataset(
        bucket="thetradedesk-useast-avails",
        oss_bucket="thetradedesk-cn-avails",
        path_prefix="datasets/withPII",
        data_name="avails-raw-delta",
        version=None,
        region_dependency_map=raw_avails_region_dependency_mapping,
        buckets_for_other_regions=region_bucket_mapping,
    )

    full_url_agg_hourly_dataset: AvailsHourlyDataset = AvailsHourlyDataset(
        bucket="thetradedesk-useast-avails",
        oss_bucket="thetradedesk-cn-avails",
        path_prefix="datasets/withoutPII",
        data_name="full-url-hourly-dataset-parquet",
        version=None,
        date_format="date=%Y%m%d",
        region_dependency_map=non_pii_dataset_region_dependency_mapping,
        buckets_for_other_regions=region_bucket_mapping,
    )
