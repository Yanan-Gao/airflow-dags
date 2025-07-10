import hashlib
import random
import string
from abc import ABC, abstractmethod

from typing import Tuple, List

from kubernetes.client import (
    V1Volume,
    V1VolumeMount,
    V1PersistentVolumeClaimVolumeSource,
)

from ttd.cloud_provider import CloudProvider, CloudProviders
from ttd.task_service.persistent_storage.persistent_storage_config import (
    PersistentStorageConfig,
    PersistentStorageType,
)
from ttd.ttdenv import TtdEnvFactory


class PersistentStorage(ABC):
    K8S_LABEL_ANNOTATION_MAX_LENGTH = 63
    ID_SUFFIX_LENGTH = 6
    PVC_NAME_SEPARATOR = "-"

    def __init__(
        self,
        config: PersistentStorageConfig,
        cloud_provider: CloudProvider = CloudProviders.aws,
    ):
        self.config = config
        default_labels = {
            "base-name": self.config.base_name[:self.K8S_LABEL_ANNOTATION_MAX_LENGTH],
            "type": self.config.storage_type.value,
        }
        self.config.labels.update(default_labels)
        self.cloud_provider = cloud_provider
        self.pvc_name = self._generate_pvc_name_based_on_storage_type()

    def _get_max_base_name_length(self, separator_length: int) -> int:
        return (self.K8S_LABEL_ANNOTATION_MAX_LENGTH - len(self.config.storage_type.value) - self.ID_SUFFIX_LENGTH - separator_length)

    def _get_truncated_base_name(self, components_length: int) -> str:
        separator_length = len(self.PVC_NAME_SEPARATOR) * (components_length - 1)
        max_base_name_length = self._get_max_base_name_length(separator_length)
        return self.config.base_name[:max_base_name_length].rstrip(string.punctuation)

    def _generate_pvc_name_based_on_storage_type(self) -> str:
        pvc_name_placeholders = ["{name}", "{storage_type}", "{id_suffix}"]
        name = self._get_truncated_base_name(len(pvc_name_placeholders))
        storage_type = self.config.storage_type.value
        id_suffix = self._get_id_suffix()

        return (
            f"{self.PVC_NAME_SEPARATOR}".join(placeholder for placeholder in pvc_name_placeholders
                                              ).format(name=name, storage_type=storage_type, id_suffix=id_suffix).lower()
        )

    def _get_id_suffix(self) -> str:
        if self.config.storage_type == PersistentStorageType.ONE_POD_TEMP:
            id_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=self.ID_SUFFIX_LENGTH))
        else:
            enriched_name = (self.config.base_name + TtdEnvFactory.get_from_system().execution_env)
            id_suffix = hashlib.md5(enriched_name.encode()).hexdigest()
            id_suffix = id_suffix[:self.ID_SUFFIX_LENGTH]
        return id_suffix

    def get_volume_configs(self) -> Tuple[V1Volume, V1VolumeMount]:
        volume = V1Volume(
            name=self.pvc_name,
            persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name=self.pvc_name),
        )

        volume_mount = V1VolumeMount(
            name=volume.name,
            mount_path=self.config.mount_path,
            sub_path=None,
            read_only=False,
        )

        return volume, volume_mount

    @abstractmethod
    def get_storage_class_name(self) -> str:
        pass

    @abstractmethod
    def get_access_modes(self) -> List[str]:
        pass


class OnePodPersistentStorage(PersistentStorage):
    STORAGE_CLASS_NAME = "standard-ssd-encrypted"
    ACCESS_MODES = ["ReadWriteOnce"]

    def __init__(
        self,
        config: PersistentStorageConfig,
        cloud_provider: CloudProvider = CloudProviders.aws,
    ):
        config.annotations["inactive-days-before-expire"] = f"{config.inactive_days_before_expire}"
        super().__init__(config=config, cloud_provider=cloud_provider)

    def get_storage_class_name(self) -> str:
        return self.STORAGE_CLASS_NAME

    def get_access_modes(self) -> List[str]:
        return self.ACCESS_MODES


class OnePodTempPersistentStorage(PersistentStorage):
    STORAGE_CLASS_NAME = "standard-ssd-encrypted"
    ACCESS_MODES = ["ReadWriteOnce"]

    def get_storage_class_name(self) -> str:
        return self.STORAGE_CLASS_NAME

    def get_access_modes(self) -> List[str]:
        return self.ACCESS_MODES


class ManyPodsPersistentStorage(PersistentStorage):
    AWS_PROD_STORAGE_CLASS_NAME = "taskservice-prod"
    AWS_TEST_STORAGE_CLASS_NAME = "taskservice-nonprod"
    ACCESS_MODES = ["ReadWriteMany"]

    def __init__(
        self,
        config: PersistentStorageConfig,
        cloud_provider: CloudProvider = CloudProviders.aws,
    ):
        config.annotations["inactive-days-before-expire"] = f"{config.inactive_days_before_expire}"
        super().__init__(config=config, cloud_provider=cloud_provider)

    def get_storage_class_name(self) -> str:
        if self.cloud_provider == CloudProviders.aws:
            if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
                return self.AWS_PROD_STORAGE_CLASS_NAME
            else:
                return self.AWS_TEST_STORAGE_CLASS_NAME
        else:
            raise PersistentStorageNotImplementedError(
                f"{self.__class__.__name__} for cloud provider='{self.cloud_provider}' is not implemented."
            )

    def get_access_modes(self) -> List[str]:
        return self.ACCESS_MODES


class PersistentStorageFactory:

    @staticmethod
    def create_storage(
        config: PersistentStorageConfig,
        cloud_provider: CloudProvider = CloudProviders.aws,
    ) -> PersistentStorage:
        if config.storage_type == PersistentStorageType.ONE_POD:
            return OnePodPersistentStorage(config=config, cloud_provider=cloud_provider)
        elif config.storage_type == PersistentStorageType.ONE_POD_TEMP:
            return OnePodTempPersistentStorage(config=config, cloud_provider=cloud_provider)
        elif config.storage_type == PersistentStorageType.MANY_PODS:
            return ManyPodsPersistentStorage(config=config, cloud_provider=cloud_provider)
        else:
            raise PersistentStorageNotImplementedError(f"Storage type '{config.storage_type}' is not implemented.")


class PersistentStorageNotImplementedError(NotImplementedError):
    pass
