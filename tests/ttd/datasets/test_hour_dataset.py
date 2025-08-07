import datetime
import logging
import unittest
from unittest import mock

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder


class TestDataset(unittest.TestCase):
    """
    Tests for Dataset custom logic
    """

    logging.root.setLevel(logging.WARNING)

    def test_HourDataset_no_success_file__succeeds_on_existing_data(self):
        expected = [
            ("ttd-identity", "testdatasets/successless/20200616/hour=23"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=22"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=21"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=20"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=19"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=18"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=17"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=16"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=15"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=14"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=13"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=12"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=11"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=10"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=09"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=08"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=07"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=06"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=05"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=04"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=03"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=02"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=01"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=00"),
        ]
        catch = []

        def check(self, bucket_name, prefix, delimiter):
            catch.append((bucket_name, prefix))
            return True

        with mock.patch.object(S3Hook, "check_for_prefix", check):
            result = Datasources.test.hour_no_success_file.with_check_type("day").check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime.datetime(2020, 6, 16),
            )
        self.assertTrue(result)
        self.assertEqual(expected, catch)

    def test_HourDataset_no_success_file__fails_on_missed_data(self):
        expected = [
            ("ttd-identity", "testdatasets/successless/20200618/hour=23"),
            ("ttd-identity", "testdatasets/successless/20200618/hour=22"),
            ("ttd-identity", "testdatasets/successless/20200618/hour=21"),
            ("ttd-identity", "testdatasets/successless/20200618/hour=20"),
            ("ttd-identity", "testdatasets/successless/20200618/hour=19"),
        ]
        catch = []

        def check(self, bucket_name, prefix, delimiter):
            if (bucket_name, prefix) in expected:
                catch.append((bucket_name, prefix))
                return True
            return False

        with mock.patch.object(S3Hook, "check_for_prefix", check):
            result = Datasources.test.hour_no_success_file.with_check_type("day").check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime.datetime(2020, 6, 18),
            )

            self.assertFalse(result)
            self.assertListEqual(expected, catch)

    def test_HourDataset_no_data_name_no_success_file_no_version__succeed_on_existing_data_for_day(self, ):
        expected = [
            ("ttd-identity", "testdatasets/successless/20200616/hour=23"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=22"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=21"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=20"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=19"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=18"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=17"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=16"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=15"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=14"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=13"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=12"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=11"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=10"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=09"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=08"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=07"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=06"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=05"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=04"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=03"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=02"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=01"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=00"),
        ]
        catch = []

        def check(self, bucket_name, prefix, delimiter):
            catch.append((bucket_name, prefix))
            return True

        with mock.patch.object(S3Hook, "check_for_prefix", check):
            result = Datasources.test.hour_no_success_file_no_data_name.with_check_type("day").check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime.datetime(2020, 6, 16),
            )

        self.assertTrue(result)
        self.assertListEqual(catch, expected)

    def test_HourDataset_no_data_name_no_success_file_no_version__fails_on_missing_data_for_day(self, ):

        def check(self, bucket_name, prefix, delimiter):
            return False

        with mock.patch.object(S3Hook, "check_for_prefix", check):
            result = (result) = Datasources.test.hour_no_success_file_no_data_name.check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime.datetime(2020, 6, 17),
            )
        self.assertFalse(result)

    def test_HourDataset_no_success_file__succeed_on_any_existing_hour_of_data(self):

        def check(self, bucket_name, prefix, delimiter):
            if (bucket_name == "ttd-identity" and prefix == "testdatasets/successless/20200618/hour=15"):
                return True
            return False

        with mock.patch.object(S3Hook, "check_for_prefix", check):
            result = Datasources.test.hour_no_success_file.check_any_hour_data_exist(
                CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime.date(2020, 6, 18),
            )

        self.assertTrue(result)

    def test_HourDataset_no_success_file__fails_on_missing_any_hour_of_data(self):

        def check(self, bucket_name, prefix, delimiter):
            if (bucket_name == "ttd-identity" and prefix == "testdatasets/successless/20200616/hour=23"):
                return True
            return False

        with mock.patch.object(S3Hook, "check_for_prefix", check):
            result = Datasources.test.hour_no_success_file.with_check_type("day").check_any_hour_data_exist(
                CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime.date(2020, 6, 17),
            )

        self.assertFalse(result, "dataset check should be failing and returning False")

    def test_migrating_smart_read_HourDataset__succeeds_on_data_existing_in_both_paths(self):
        expected = [
            ("ttd-identity", "testdatasets/successless/20200616/hour=23"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=22"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=21"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=20"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=19"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=18"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=17"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=16"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=15"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=14"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=13"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=12"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=11"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=10"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=09"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=08"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=07"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=06"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=05"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=04"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=03"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=02"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=01"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=00"),
        ]
        catch = []

        def check(self, bucket_name, prefix, delimiter):
            if (bucket_name, prefix) in expected:
                catch.append((bucket_name, prefix))
                return True
            return False

        with mock.patch.object(S3Hook, "check_for_prefix", check):
            result = Datasources.test.migrating_hour_dataset.with_check_type("day").check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime.datetime(2020, 6, 16),
            )
        self.assertEqual(expected, catch)
        self.assertTrue(result)

    def test_migrating_smart_read_HourDataset__succeeds_on_data_existing_in_old_path(self):
        expected = [
            ("ttd-identity", "testdatasets/successless/20200616/hour=23"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=22"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=21"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=20"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=19"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=18"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=17"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=16"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=15"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=14"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=13"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=12"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=11"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=10"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=09"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=08"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=07"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=06"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=05"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=04"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=03"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=02"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=01"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=00"),
        ]
        catch = []

        def check(self, bucket_name, prefix, delimiter):
            if (bucket_name, prefix) in expected:
                catch.append((bucket_name, prefix))
                return True
            return False

        with mock.patch.object(S3Hook, "check_for_prefix", check):
            result = Datasources.test.migrating_hour_dataset.with_check_type("day").check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime.datetime(2020, 6, 16),
            )
        self.assertEqual(expected, catch)
        self.assertTrue(result)

    def test_migrating_smart_read_HourDataset__succeeds_on_data_existing_in_new_path(self):
        expected = [
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=23"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=22"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=21"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=20"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=19"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=18"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=17"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=16"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=15"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=14"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=13"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=12"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=11"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=10"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=09"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=08"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=07"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=06"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=05"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=04"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=03"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=02"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=01"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=00"),
        ]
        catch = []

        def check(self, bucket_name, prefix, delimiter):
            if (bucket_name, prefix) in expected:
                catch.append((bucket_name, prefix))
                return True
            return False

        with mock.patch.object(S3Hook, "check_for_prefix", check):
            result = Datasources.test.migrating_hour_dataset.with_check_type("day").check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime.datetime(2020, 6, 16),
            )
        self.assertEqual(expected, catch)
        self.assertTrue(result)

    def test_migrating_smart_read_HourDataset__fails_on_incomplete_data_existing_in_both_paths(self):
        expected = [
            ("ttd-identity", "testdatasets/successless/20200616/hour=23"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=22"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=21"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=20"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=19"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=18"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=17"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=16"),
            ("ttd-identity", "testdatasets/successless/20200616/hour=15"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=14"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=13"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=12"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=11"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=10"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=09"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=08"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=07"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=06"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=05"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=04"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=03"),
        ]
        catch = []

        def check(self, bucket_name, prefix, delimiter):
            if (bucket_name, prefix) in expected:
                catch.append((bucket_name, prefix))
                return True
            return False

        with mock.patch.object(S3Hook, "check_for_prefix", check):
            result = Datasources.test.migrating_hour_dataset.with_check_type("day").check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime.datetime(2020, 6, 16),
            )
        self.assertEqual(expected, catch)
        self.assertFalse(result)

    def test_migrating_smart_read_HourDataset__fails_on_incomplete_data_existing_in_new_path(self):
        expected = [
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=23"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=22"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=21"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=20"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=19"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=18"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=17"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=16"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=15"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=14"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=13"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=12"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=11"),
            ("ttd-identity-new", "env=test/new-data/successless/20200616/hour=10"),
        ]
        catch = []

        def check(self, bucket_name, prefix, delimiter):
            if (bucket_name, prefix) in expected:
                catch.append((bucket_name, prefix))
                return True
            return False

        with mock.patch.object(S3Hook, "check_for_prefix", check):
            result = Datasources.test.migrating_hour_dataset.with_check_type("day").check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime.datetime(2020, 6, 16),
            )
        self.assertEqual(expected, catch)
        self.assertFalse(result)
