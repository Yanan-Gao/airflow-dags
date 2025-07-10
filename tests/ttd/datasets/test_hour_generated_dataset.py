import datetime
from unittest import TestCase
from unittest.mock import patch

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder


class HourGeneratedDatasetCheckTest(TestCase):

    def test_RtDatalakeDataset_check_paths_of_two_adjacent_hours(self):
        expected_list = [
            ("rtb-bucket", "path/prefix/rtb_data/v=2/_SUCCESS-sx-20200617-11"),
            ("rtb-bucket", "path/prefix/rtb_data/v=2/_SUCCESS-sx-20200617-10"),
        ]

        capture_list = []

        def check_for_key_mock(self, key, bucket_name):
            capture_list.append((bucket_name, key))
            return True

        with patch.object(S3Hook, "check_for_key", check_for_key_mock):
            result = Datasources.test.rtb_dataset.with_check_type(check_type="hour").check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime.datetime(2020, 6, 17, hour=11),
            )

            self.assertTrue(result)
            self.assertListEqual(capture_list, expected_list)

    def test_AvailsHourlyDataset_checks_paths_for_all_dependent_regions_and_hour(self):
        expected_list = [
            (
                "avails-bucket",
                "datasets/withoutPII/avails-hourly-data/date=2020-06-17/hour=11/originatingRegion=US_EAST_1/_SUCCESS",
            ),
            (
                "avails-bucket",
                "datasets/withoutPII/avails-hourly-data/date=2020-06-17/hour=11/originatingRegion=US_WEST_2/_SUCCESS",
            ),
            (
                "avails-bucket",
                "datasets/withoutPII/avails-hourly-data/date=2020-06-17/hour=11/originatingRegion=AP_NORTHEAST_1/_SUCCESS",
            ),
            (
                "avails-bucket",
                "datasets/withoutPII/avails-hourly-data/date=2020-06-17/hour=11/originatingRegion=AP_SOUTHEAST_1/_SUCCESS",
            ),
        ]

        capture_list = []

        def check_for_key_mock(self, key, bucket_name):
            capture_list.append((bucket_name, key))
            return True

        with patch.object(S3Hook, "check_for_key", check_for_key_mock):
            result = (
                Datasources.test.avails_hourly_dataset.with_check_type(check_type="hour").with_region("us-east-1").check_data_exist(
                    cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                    ds_date=datetime.datetime(2020, 6, 17, hour=11),
                )
            )

            self.assertTrue(result)
            self.assertListEqual(capture_list, expected_list)

    def test_HourGeneratedDataset_check_path_list_complete(self):
        expected_list = [(
            "hour-data-bucket",
            "path/prefix/hourly_data/v=2/date=20200617/hour=11/_SUCCESS",
        )]

        capture_list = []

        def check_for_key_mock(self, key, bucket_name):
            capture_list.append((bucket_name, key))
            return True

        with patch.object(S3Hook, "check_for_key", check_for_key_mock):
            result = Datasources.test.hour_dataset_with_success_file.with_check_type(check_type="hour").check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime.datetime(2020, 6, 17, hour=11),
            )

            self.assertTrue(result)
            self.assertListEqual(capture_list, expected_list)
