from ttd.datasets.avails_hourly_dataset import AvailsHourlyDataset
from ttd.datasets.dataset import (
    default_short_date_part_format,
    default_hour_part_format,
)
from ttd.datasets.env_path_configuration import (
    MigratedDatasetPathConfiguration,
    MigratingDatasetPathConfiguration,
)
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.date_external_dataset import DateExternalDataset
from ttd.datasets.hour_dataset import HourDataset, HourGeneratedDataset
from ttd.datasets.rtb_datalake_dataset import RtbDatalakeDataset


class TestDatasources:
    ######################
    # Date Datasets
    ######################
    success_file: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="testdatasets",
        data_name="successfile",
        version=1,
    )

    migrating_dategenerated_dataset: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="testdatasets",
        data_name="migratingdataset",
        version=1,
        env_path_configuration=MigratingDatasetPathConfiguration(new_bucket="new-ttd-identity", new_path_prefix='newtestdatasets')
    )

    migrated_dategenerated_env_path_dataset: DateGeneratedDataset = (
        DateGeneratedDataset(
            bucket="ttd-identity",
            path_prefix="testdatasets",
            data_name="dataset",
            version=1,
            env_path_configuration=MigratedDatasetPathConfiguration(),
        )
    )

    migrating_dategenerated_env_path_dataset: DateGeneratedDataset = (
        DateGeneratedDataset(
            bucket="ttd-identity",
            path_prefix="testdatasets",
            data_name="dataset",
            version=1,
            buckets_for_other_regions={
                "region1": "ttd-identity-region1",
                "region2": "ttd-identity-region2",
            },
            env_path_configuration=MigratingDatasetPathConfiguration(
                new_bucket="new-ttd-identity",
                new_path_prefix="newtestdatasets",
                new_buckets_for_other_regions={
                    "region1": "new-ttd-identity-region1",
                    "region2": "new-ttd-identity-region2",
                },
            ),
        )
    )

    new_dategenerated_env_path_dataset: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="testdatasets",
        data_name="dataset",
        version=1,
        env_path_configuration=MigratedDatasetPathConfiguration(),
    )

    no_success_file: DateExternalDataset = DateExternalDataset(
        bucket="ttd-identity",
        path_prefix="testdatasets",
        data_name="successless",
        success_file=None,
    )

    no_success_file_no_data_name: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="testdatasets/successless",
        data_name="",
        date_format=default_short_date_part_format,
        success_file=None,
        env_aware=False,
        version=None,
    )

    date_success_file: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="testdatasets",
        data_name="datesuccessfile",
        date_format="%Y-%m-%d",
        version=1,
        success_file="",
    )

    date_success_file_no_version: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="testdatasets",
        data_name="datesuccessfile",
        date_format="%Y-%m-%d",
        success_file="",
    )

    oss_test_file: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-airflow-eldorado",
        oss_bucket="ttd-airflow-eldorado-test",
        path_prefix="dataset",
        data_name="",
        version=1,
    )
    ######################
    # Date Hour Datasets
    ######################

    migrating_hour_dataset: HourDataset = HourDataset(
        bucket="ttd-identity",
        path_prefix="testdatasets",
        data_name="successless",
        date_format=default_short_date_part_format,
        hour_format=default_hour_part_format,
        success_file=None,
        env_aware=False,
        version=None,
        env_path_configuration=MigratingDatasetPathConfiguration(new_bucket="ttd-identity-new", new_path_prefix="new-data"),
    )

    hour_no_success_file: HourDataset = HourDataset(
        bucket="ttd-identity",
        path_prefix="testdatasets",
        data_name="successless",
        date_format=default_short_date_part_format,
        hour_format=default_hour_part_format,
        success_file=None,
        env_aware=False,
        version=None,
    )

    hour_no_success_file_no_data_name: HourDataset = HourDataset(
        bucket="ttd-identity",
        path_prefix="testdatasets/successless",
        data_name="",
        date_format=default_short_date_part_format,
        hour_format=default_hour_part_format,
        success_file=None,
        env_aware=False,
        version=None,
    )

    date_dataset: DateGeneratedDataset = DateGeneratedDataset(
        bucket="not-bucket",
        azure_bucket="not-container@not-sa",
        path_prefix="not/path/prefix",
        data_name="not/data",
        version=1,
    )

    hour_dataset: HourGeneratedDataset = HourGeneratedDataset(
        bucket="not-bucket",
        azure_bucket="not-container@not-sa",
        path_prefix="not/path/prefix",
        data_name="not/data",
        success_file=None,
        env_aware=False,
        version=2,
    )

    hour_dataset_with_success_file: HourGeneratedDataset = HourGeneratedDataset(
        bucket="hour-data-bucket",
        azure_bucket="hour-data-container@hour-data-sa",
        path_prefix="path/prefix",
        data_name="hourly_data",
        env_aware=False,
        version=2,
    )

    rtb_dataset: RtbDatalakeDataset = RtbDatalakeDataset(
        bucket="rtb-bucket",
        path_prefix="path/prefix",
        success_file="_SUCCESS-sx-%Y%m%d-%H",
        data_name="rtb_data",
        env_aware=False,
        version=2,
    )

    avails_hourly_dataset: AvailsHourlyDataset = AvailsHourlyDataset(
        region_dependency_map={"us-east-1": ["us-east-1", "us-west-2", "ap-northeast-1", "ap-southeast-1"]},
        buckets_for_other_regions={"us-east-1": "avails-bucket"},
        env_aware=False,
        bucket="avails-bucket",
        path_prefix="datasets/withoutPII",
        data_name="avails-hourly-data",
        version=None,
    )
