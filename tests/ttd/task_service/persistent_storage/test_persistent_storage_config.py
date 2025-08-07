import random
from unittest import TestCase

from ttd.task_service.persistent_storage.persistent_storage_config import (
    PersistentStorageConfig,
    PersistentStorageType,
    DeletionPeriodValidationError,
)


class TestPersistentStorageConfig(TestCase):

    def test_constructor__accepts_random_inactive_days_within_range_for_one_pod_type(self, ):
        days = random.randint(
            PersistentStorageConfig.MIN_INACTIVE_DAYS,
            PersistentStorageConfig.MAX_INACTIVE_DAYS,
        )
        config = PersistentStorageConfig(
            storage_type=PersistentStorageType.ONE_POD,
            storage_size="100Gi",
            inactive_days_before_expire=days,
        )
        self.assertEqual(config.inactive_days_before_expire, days)

    def test_constructor__raises_error_when_inactive_days_above_range_for_one_pod_temp_type(self, ):
        with self.assertRaises(DeletionPeriodValidationError):
            config = PersistentStorageConfig(
                storage_type=PersistentStorageType.ONE_POD_TEMP,
                storage_size="20Gi",
                inactive_days_before_expire=PersistentStorageConfig.MAX_INACTIVE_DAYS + 1,
            )

    def test_constructor__raises_error_when_inactive_days_below_range_for_many_pods_type(self, ):
        with self.assertRaises(DeletionPeriodValidationError):
            config = PersistentStorageConfig(
                storage_type=PersistentStorageType.MANY_PODS,
                storage_size="20Gi",
                inactive_days_before_expire=PersistentStorageConfig.MIN_INACTIVE_DAYS - 1,
            )
