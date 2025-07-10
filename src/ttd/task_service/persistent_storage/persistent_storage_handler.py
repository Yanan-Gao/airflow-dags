import logging
from datetime import datetime
from typing import Optional, Dict, Tuple

from kubernetes.client import (
    CoreV1Api,
    V1PersistentVolumeClaim,
    V1ObjectMeta,
    V1PersistentVolumeClaimSpec,
    V1ResourceRequirements,
)

from ttd.task_service.persistent_storage.persistent_storage import PersistentStorage


class PersistentStorageHandler:
    DEFAULT_NAMESPACE = "task-service"

    def __init__(self, client: CoreV1Api):
        self.client = client

    def get_pvc(self, storage: PersistentStorage) -> Optional[V1PersistentVolumeClaim]:
        logging.info(f"Getting PVC={storage.pvc_name}")
        pvc_list = self.client.list_namespaced_persistent_volume_claim(self.DEFAULT_NAMESPACE)
        for pvc in pvc_list.items:
            if pvc.metadata.name == storage.pvc_name:
                logging.info("Found existing PVC")
                return pvc
        logging.info("No PVC found")
        return None

    def validate_pvc_changes(self, pvc: V1PersistentVolumeClaim, storage: PersistentStorage) -> None:
        self._validate_storage_size(pvc, storage)
        self._validate_labels(pvc, storage)

    def _validate_storage_size(self, pvc: V1PersistentVolumeClaim, storage: PersistentStorage) -> None:
        current_storage_size = pvc.spec.resources.requests["storage"]
        requested_storage_size = storage.config.storage_size
        if not current_storage_size.endswith("Gi") or not requested_storage_size.endswith("Gi"):
            logging.info("Can't compare persistent storage sizes other than in Gi units")
            return

        requested_value = int(requested_storage_size.rstrip("Gi"))
        current_value = int(current_storage_size.rstrip("Gi"))

        if requested_value < current_value:
            raise StorageSizeDecreaseError(
                f"Decreasing storage size from {current_storage_size} to {requested_storage_size} is not allowed"
            )
        elif requested_value > current_value:
            logging.info(f"Request to change storage size from {current_storage_size} to {requested_storage_size}")

    def _validate_labels(self, pvc: V1PersistentVolumeClaim, storage: PersistentStorage) -> None:
        current_labels = pvc.metadata.labels
        requested_labels = storage.config.labels
        label_diff = self._get_label_diff(current_labels, requested_labels)
        if label_diff is not None and len(label_diff) != 0:
            logging.warning(
                f"Labels on existing resources cannot be modified via API calls, proceeding without any changes."
                f"Difference in labels: {label_diff}"
            )

    def _get_label_diff(
        self,
        current_labels: Optional[Dict[str, str]] = None,
        requested_labels: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Tuple[str, str]]:
        current_labels = current_labels or {}
        requested_labels = requested_labels or {}
        return {
            key: (current_labels.get(key), requested_labels.get(key))
            for key in set(current_labels) | set(requested_labels) if current_labels.get(key) != requested_labels.get(key)
        }

    def create_pvc(self, storage: PersistentStorage) -> V1PersistentVolumeClaim:
        logging.info(
            f"Creating PVC with configuration: "
            f"Storage Type: {storage.config.storage_type}, "
            f"Storage Size: {storage.config.storage_size}, "
            f"Inactive Days Before Expire: {storage.config.inactive_days_before_expire}, "
            f"Base Name: {storage.config.base_name}, "
            f"Mount Path: {storage.config.mount_path}, "
            f"Annotations: {storage.config.annotations}, "
            f"Labels: {storage.config.labels}"
        )

        pvc = V1PersistentVolumeClaim(
            metadata=V1ObjectMeta(
                name=storage.pvc_name,
                labels=storage.config.labels,
                annotations=storage.config.annotations,
            ),
            spec=V1PersistentVolumeClaimSpec(
                storage_class_name=storage.get_storage_class_name(),
                access_modes=storage.get_access_modes(),
                resources=V1ResourceRequirements(requests={"storage": storage.config.storage_size}),
            ),
        )
        return self.client.create_namespaced_persistent_volume_claim(namespace=self.DEFAULT_NAMESPACE, body=pvc)

    def update_pvc(self, pvc: V1PersistentVolumeClaim, storage: PersistentStorage) -> V1PersistentVolumeClaim:
        pvc.metadata.annotations = storage.config.annotations
        pvc.spec.resources.requests["storage"] = storage.config.storage_size
        return self.client.patch_namespaced_persistent_volume_claim(name=storage.pvc_name, namespace=self.DEFAULT_NAMESPACE, body=pvc)

    def delete_pvc(self, storage: PersistentStorage) -> None:
        logging.info(f"Deleting PVC={storage.pvc_name}")
        self.client.delete_namespaced_persistent_volume_claim(namespace=self.DEFAULT_NAMESPACE, name=storage.pvc_name)

    def provision_pvc(self, storage: PersistentStorage) -> None:
        logging.info(f"Provisioning PVC={storage.pvc_name}")
        current_time = datetime.utcnow().isoformat()
        storage.config.annotations["last-used-at"] = current_time
        existing_pvc = self.get_pvc(storage)
        if existing_pvc is not None:
            self.validate_pvc_changes(existing_pvc, storage)
            self.update_pvc(existing_pvc, storage)
            logging.info(f"PVC={storage.pvc_name} updated")
        else:
            self.create_pvc(storage)
            logging.info(f"PVC={storage.pvc_name} created")


class StorageSizeDecreaseError(ValueError):
    pass
