from enum import Enum
from typing import Optional, Dict


class PersistentStorageType(Enum):
    ONE_POD = "one-pod"
    ONE_POD_TEMP = "one-pod-temp"
    MANY_PODS = "many-pods"


class PersistentStorageConfig:
    """
    Configuration class for Persistent Storage attached to TaskService K8s pods.

    :param storage_type: The type of persistent storage (see PersistentStorageType to check supported types)
    :param storage_size: The size of the storage (e.g. '10Gi')
    :param inactive_days_before_expire: The number of inactive days after which persistent storage is expired
    :param base_name: The base name used for naming the persistent storage K8s resources.
        If not provided, the task name will be used as the default base name
    :param mount_path: The file system path where the storage will be mounted in the container.
        If not provided, the default mount path of the TaskService will be used
    :param annotations: The dictionary of annotations to add to the storage resource
    :param labels: The dictionary of labels to add to the storage resource
    """

    MIN_INACTIVE_DAYS = 1
    MAX_INACTIVE_DAYS = 30

    def __init__(
        self,
        storage_type: PersistentStorageType,
        storage_size: str,
        inactive_days_before_expire: int = MAX_INACTIVE_DAYS,
        base_name: Optional[str] = None,
        mount_path: Optional[str] = None,
        annotations: Optional[Dict[str, str]] = None,
        labels: Optional[Dict[str, str]] = None,
    ):
        if not (self.MIN_INACTIVE_DAYS <= inactive_days_before_expire <= self.MAX_INACTIVE_DAYS):
            raise DeletionPeriodValidationError(
                f"Invalid 'inactive_days_before_expire': {inactive_days_before_expire}. Valid range: {self.MIN_INACTIVE_DAYS}-{self.MAX_INACTIVE_DAYS}"
            )

        self.storage_type = storage_type
        self.storage_size = storage_size
        self.inactive_days_before_expire = inactive_days_before_expire
        self.base_name = base_name
        self.mount_path = mount_path
        self.annotations = annotations or {}
        self.labels = labels or {}


class DeletionPeriodValidationError(ValueError):
    pass
