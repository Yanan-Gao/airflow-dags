import logging
import unittest
from datetime import datetime
from typing import List
from unittest import mock
from unittest.mock import patch

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.datasets.ds_side import DsSides


class TestDataset(unittest.TestCase):
    """
    Tests for Dataset custom logic
    """

    logging.root.setLevel(logging.WARNING)

    def test_DateGeneratedDataset__succeeds_on_existing_data(self):
        capture_list = []

        def check_for_key_mock(self, key, bucket_name):
            capture_list.append((bucket_name, key))
            return True

        with mock.patch.object(S3Hook, "check_for_key", check_for_key_mock):
            result = Datasources.test.success_file.check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime(2020, 6, 16),
            )

        self.assertTrue(result)
        self.assertListEqual(
            capture_list,
            [(
                "ttd-identity",
                "testdatasets/test/successfile/v=1/date=20200616/_SUCCESS",
            )],
        )

    def test_migrating_DateGeneratedDataset__succeeds_on_data_existing_on_old_path(self):
        expected_list = [(
            "ttd-identity",
            "testdatasets/test/migratingdataset/v=1/date=20200616/_SUCCESS",
        )]
        capture_list = []

        def check_for_key_mock(self, key, bucket_name):
            if (bucket_name, key) in expected_list:
                capture_list.append((bucket_name, key))
                return True
            return False

        with mock.patch.object(S3Hook, "check_for_key", check_for_key_mock):
            result = Datasources.test.migrating_dategenerated_dataset.check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime(2020, 6, 16),
            )

        self.assertTrue(result)
        self.assertListEqual(
            capture_list,
            expected_list,
        )

    def test_migrating_DateGeneratedDataset__succeeds_on_data_existing_on_new_path(self):
        expected_list = [(
            "new-ttd-identity",
            "env=test/newtestdatasets/migratingdataset/v=1/date=20200616/_SUCCESS",
        )]
        capture_list = []

        def check_for_key_mock(self, key, bucket_name):
            if (bucket_name, key) in expected_list:
                capture_list.append((bucket_name, key))
                return True
            return False

        with mock.patch.object(S3Hook, "check_for_key", check_for_key_mock):
            result = Datasources.test.migrating_dategenerated_dataset.check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime(2020, 6, 16),
            )

        self.assertTrue(result)
        self.assertListEqual(
            capture_list,
            expected_list,
        )

    def test_migrating_non_smart_read_DateGeneratedDataset__does_not_read_from_new_path(self):
        new_path_list = [(
            "ttd-identity-new",
            "env=test/newtestdatasets/migratingdataset/v=1/date=20200616/_SUCCESS",
        )]
        capture_list: List[object] = []

        def check_for_key_mock(self, key, bucket_name):
            if (bucket_name, key) in new_path_list:
                return True

        with mock.patch.object(S3Hook, "check_for_key", check_for_key_mock):
            dataset = Datasources.test.migrating_dategenerated_dataset
            dataset.env_path_configuration.smart_read = False
            result = Datasources.test.migrating_dategenerated_dataset.check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime(2020, 6, 16),
            )

        self.assertFalse(result)
        self.assertListEqual(
            capture_list,
            [],
        )

    def test_DateGeneratedDataset__fails_on_missing_data(self):

        def check_for_key_mock(self, key, bucket_name):
            return False

        with mock.patch.object(S3Hook, "check_for_key", check_for_key_mock):
            result = Datasources.test.success_file.check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime(2020, 6, 17),
            )

        self.assertFalse(result)

    def test_DateExternalDataset_no_success_file__succeeds_on_existing_data(self):
        capture_list = []

        def check(self, bucket_name, prefix, delimiter):
            capture_list.append((bucket_name, prefix))
            return True

        with mock.patch.object(S3Hook, "check_for_prefix", check):
            result = Datasources.test.no_success_file.check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime(2020, 6, 16),
            )

        self.assertTrue(result)
        self.assertListEqual(capture_list, [("ttd-identity", "testdatasets/successless/20200616")])

    def test_DateExternalDataset_no_success_file__fails_on_missing_data(self):

        def check(self, bucket_name, prefix, delimiter):
            return False

        with mock.patch.object(S3Hook, "check_for_prefix", check):
            result = Datasources.test.no_success_file.check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime(2020, 6, 17),
            )
        self.assertFalse(result)

    def test_DateGeneratedDataset_empty_success_file__succeeds_on_existing_data(self):
        capture_list = []

        def check_for_key_mock(self, key, bucket_name):
            capture_list.append((bucket_name, key))
            return True

        with mock.patch.object(S3Hook, "check_for_key", check_for_key_mock):
            result = Datasources.test.date_success_file.check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime(2020, 6, 16),
            )
        self.assertTrue(result)
        self.assertListEqual(
            capture_list,
            [("ttd-identity", "testdatasets/test/datesuccessfile/v=1/2020-06-16")],
        )

    def test_DateGeneratedDataset_empty_success_file__fails_on_missing_data(self):

        def check_for_key_mock(self, key, bucket_name):
            return False

        with mock.patch.object(S3Hook, "check_for_key", check_for_key_mock):
            result = Datasources.test.date_success_file.check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime(2020, 6, 17),
            )
        self.assertFalse(result)

    def test_DateGeneratedDataset_empty_success_file_no_version__succeeds_on_existing_data(self, ):
        capture_list = []

        def check_for_key_mock(self, key, bucket_name):
            capture_list.append((bucket_name, key))
            return True

        with mock.patch.object(S3Hook, "check_for_key", check_for_key_mock):
            result = Datasources.test.date_success_file_no_version.check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime(2020, 6, 16),
            )
        self.assertTrue(result)

    def test_DateGeneratedDataset_no_success_file_no_version__fails_on_missing_data(self, ):

        def check_for_key_mock(self, key, bucket_name):
            return False

        with mock.patch.object(S3Hook, "check_for_key", check_for_key_mock):
            result = Datasources.test.date_success_file_no_version.check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime(2020, 6, 17),
            )
        self.assertFalse(result)

    def test_DateGeneratedDataset_no_success_file_no_data_name__succeeds_on_existing_data(self, ):
        capture_list = []

        def check(self, bucket_name, prefix, delimiter):
            capture_list.append((bucket_name, prefix))
            return True

        with mock.patch.object(S3Hook, "check_for_prefix", check):
            result = Datasources.test.no_success_file_no_data_name.check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime(2020, 6, 16),
            )
        self.assertTrue(result)
        self.assertListEqual(capture_list, [("ttd-identity", "testdatasets/successless/20200616")])

    def test_DateGeneratedDataset_no_success_file_no_data_name__fails_no_missing_data(self, ):

        def check(self, bucket_name, prefix, delimiter):
            return False

        with mock.patch.object(S3Hook, "check_for_prefix", check):
            result = Datasources.test.no_success_file_no_data_name.check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime(2020, 6, 17),
            )
        self.assertFalse(result)

    def test_Dataset_created_on_old_env_path___generates_correct_read_n_write_full_key(self):
        # make sure no env path is added to prefix for regular datasets
        old_date_dataset = Datasources.test.success_file

        read_old_date_dataset = old_date_dataset.with_side(side=DsSides.read)
        write_old_date_dataset = old_date_dataset.with_side(side=DsSides.write)

        date = datetime(2022, 4, 5)
        read_full_key = read_old_date_dataset._get_full_key(ds_date=date)
        write_full_key = write_old_date_dataset._get_full_key(ds_date=date)

        assert (read_old_date_dataset.path_prefix == "testdatasets")
        assert (write_old_date_dataset.path_prefix == "testdatasets")
        assert (read_full_key == "testdatasets/test/successfile/v=1/date=20220405")
        assert (write_full_key == "testdatasets/test/successfile/v=1/date=20220405")

    def test_Dataset_created_on_new_env_path___generates_correct_read_n_write_full_key(self):
        new_env_date_dataset = Datasources.test.new_dategenerated_env_path_dataset

        read_new_env_date_dataset = new_env_date_dataset.with_side(side=DsSides.read)
        write_new_env_date_dataset = new_env_date_dataset.with_side(side=DsSides.write)

        date = datetime(2022, 4, 5)
        read_full_key = read_new_env_date_dataset._get_full_key(ds_date=date)
        write_full_key = write_new_env_date_dataset._get_full_key(ds_date=date)

        assert read_new_env_date_dataset.path_prefix == "env=test/testdatasets"
        assert write_new_env_date_dataset.path_prefix == "env=test/testdatasets"
        assert read_full_key == "env=test/testdatasets/dataset/v=1/date=20220405"
        assert write_full_key == "env=test/testdatasets/dataset/v=1/date=20220405"

    def test_migrating_Dataset___generates_correct_full_key_for_old_pathing(self):
        old_path_date_dataset = (Datasources.test.migrating_dategenerated_env_path_dataset)
        date = datetime(2022, 4, 5)

        full_key = old_path_date_dataset._get_full_key(ds_date=date)

        assert old_path_date_dataset.path_prefix == "testdatasets"
        assert full_key == "testdatasets/test/dataset/v=1/date=20220405"

    def test_migrating_Dataset___generates_correct_full_key_for_new_pathing(self):
        new_path_date_dataset = (Datasources.test.migrating_dategenerated_env_path_dataset.with_new_env_pathing)
        date = datetime(2022, 4, 5)

        full_key = new_path_date_dataset._get_full_key(ds_date=date)

        assert new_path_date_dataset.path_prefix == "env=test/newtestdatasets"
        assert full_key == "env=test/newtestdatasets/dataset/v=1/date=20220405"

    def test_migrating_Dataset___correctly_sets_bucket_to_new_region(self):
        migrating_date_dataset = (Datasources.test.migrating_dategenerated_env_path_dataset)

        new_region_migrating_date_dataset = migrating_date_dataset.with_region(region="region1")

        assert new_region_migrating_date_dataset.bucket == "ttd-identity-region1"
        assert (new_region_migrating_date_dataset.env_path_configuration.new_bucket == "new-ttd-identity-region1")

    def test_static_dataset___generates_correct_full_key_for_new_pathing(self):
        static_dataset = (Datasources.test.static_dataset)

        full_key = static_dataset._get_full_key()

        assert static_dataset.path_prefix == "env=test/testdatasets"
        assert full_key == "env=test/testdatasets/dataset/v=1"

    def test_time_dataset_check_path_list_complete(self):
        expected_list = [("time-bucket", "path/prefix/time_data/v=2/date=20250714/time=111009/postfix/_SUCCESS")]

        capture_list = []

        def check_for_key_mock(self, key, bucket_name):
            capture_list.append((bucket_name, key))
            return True

        with patch.object(S3Hook, "check_for_key", check_for_key_mock):
            result = Datasources.test.time_dataset.check_data_exist(
                cloud_storage=CloudStorageBuilder(CloudProviders.aws).build(),
                ds_date=datetime(2025, 7, 14, hour=11, minute=10, second=9),
            )

            self.assertTrue(result)
            self.assertListEqual(capture_list, expected_list)
