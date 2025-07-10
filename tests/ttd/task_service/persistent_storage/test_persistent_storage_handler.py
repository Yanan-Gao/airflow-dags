from typing import List, Dict
from unittest import TestCase, mock
from unittest.mock import MagicMock

import kubernetes
from kubernetes.client import CoreV1Api, V1PersistentVolumeClaim, V1ObjectMeta

from ttd.task_service.persistent_storage.persistent_storage import (
    PersistentStorageFactory,
)
from ttd.task_service.persistent_storage.persistent_storage_config import (
    PersistentStorageConfig,
    PersistentStorageType,
)
from ttd.task_service.persistent_storage.persistent_storage_handler import (
    PersistentStorageHandler,
    StorageSizeDecreaseError,
)
from ttd.ttdenv import TtdEnvFactory


class TestPersistentStorageHandler(TestCase):

    def setUp(self):
        TtdEnvFactory.get_from_system = MagicMock(return_value=TtdEnvFactory.prod)

    def test_get_pvc__find_and_return_pvc_if_it_exists_in_k8s_client_output(self):
        storage = PersistentStorageFactory.create_storage(
            PersistentStorageConfig(
                storage_type=PersistentStorageType.ONE_POD,
                storage_size="100Gi",
                base_name="my-test-name",
            )
        )
        pvc_name = storage.pvc_name
        mock_pvcs = [
            V1PersistentVolumeClaim(metadata=V1ObjectMeta(name="test-pvc-1")),
            V1PersistentVolumeClaim(metadata=V1ObjectMeta(name="test-pvc-2")),
            V1PersistentVolumeClaim(metadata=V1ObjectMeta(name=pvc_name)),
        ]

        def list_pvc(self, namespace, **kwargs):
            return MagicMock(items=mock_pvcs)

        with mock.patch.object(
                kubernetes.client.CoreV1Api,
                "list_namespaced_persistent_volume_claim",
                list_pvc,
        ):
            handler = PersistentStorageHandler(CoreV1Api())
            result = handler.get_pvc(storage)

            self.assertEqual(pvc_name, result.metadata.name)

    def test_get_pvc__returns_none_when_k8s_client_output_empty(self):
        storage = PersistentStorageFactory.create_storage(
            PersistentStorageConfig(
                storage_type=PersistentStorageType.ONE_POD,
                storage_size="100Gi",
                base_name="my-test-name",
            )
        )
        mock_pvcs: List[V1PersistentVolumeClaim] = []

        def list_pvc(self, namespace, **kwargs):
            return MagicMock(items=mock_pvcs)

        with mock.patch.object(
                kubernetes.client.CoreV1Api,
                "list_namespaced_persistent_volume_claim",
                list_pvc,
        ):
            handler = PersistentStorageHandler(CoreV1Api())
            result = handler.get_pvc(storage)

            self.assertIsNone(result)

    def test_get_pvc__returns_none_if_pvc_not_found_in_k8s_client_output(self):
        storage = PersistentStorageFactory.create_storage(
            PersistentStorageConfig(
                storage_type=PersistentStorageType.ONE_POD,
                storage_size="100Gi",
                base_name="my-test-name",
            )
        )
        mock_pvcs = [
            V1PersistentVolumeClaim(metadata=V1ObjectMeta(name="test-pvc-1")),
            V1PersistentVolumeClaim(metadata=V1ObjectMeta(name="test-pvc-2")),
            V1PersistentVolumeClaim(metadata=V1ObjectMeta(name="test-pvc-3")),
        ]

        def list_pvc(self, namespace, **kwargs):
            return MagicMock(items=mock_pvcs)

        with mock.patch.object(
                kubernetes.client.CoreV1Api,
                "list_namespaced_persistent_volume_claim",
                list_pvc,
        ):
            handler = PersistentStorageHandler(CoreV1Api())
            result = handler.get_pvc(storage)

            self.assertIsNone(result)

    def test_get_label_diff__returns_no_diff_when_same_labels(self):
        handler = PersistentStorageHandler(CoreV1Api())
        current_labels = {"label1": "value1", "label2": "value2"}
        requested_labels = {"label1": "value1", "label2": "value2"}
        diff = handler._get_label_diff(current_labels, requested_labels)
        self.assertEqual({}, diff)

    def test_get_label_diff__returns_correct_diff_with_mixed_labels(self):
        handler = PersistentStorageHandler(CoreV1Api())
        current_labels = {"label1": "value1", "label2": "value2"}
        requested_labels = {"label1": "value1", "label3": "value3"}
        diff = handler._get_label_diff(current_labels, requested_labels)
        expected_diff = {"label2": ("value2", None), "label3": (None, "value3")}
        self.assertEqual(expected_diff, diff)

    def test_get_label_diff__returns_correct_diff_when_one_label_empty(self):
        handler = PersistentStorageHandler(CoreV1Api())
        current_labels: Dict[str, str] = {}
        requested_labels = {"label1": "value1"}
        diff = handler._get_label_diff(current_labels, requested_labels)
        expected_diff = {"label1": (None, "value1")}
        self.assertEqual(expected_diff, diff)

    def test_validate_storage_size__logs_can_not_compare_if_requested_size_not_in_Gi_unit(self, ):
        current_size = "10Gi"
        requested_size = "5Mi"
        expected_log = "Can't compare persistent storage sizes other than in Gi units"

        handler = PersistentStorageHandler(CoreV1Api())

        mock_storage = MagicMock()
        mock_storage.config.storage_size = requested_size

        mock_pvc = MagicMock()
        mock_pvc.spec.resources.requests = {"storage": current_size}

        with self.assertLogs(level="INFO") as logs:
            handler._validate_storage_size(mock_pvc, mock_storage)
        self.assertIn(expected_log, logs.output[0])

    def test_validate_storage_size__raise_error_if_requested_size_decreased(self):
        current_size = "10Gi"
        requested_size = "5Gi"

        handler = PersistentStorageHandler(CoreV1Api())

        mock_storage = MagicMock()
        mock_storage.config.storage_size = requested_size

        mock_pvc = MagicMock()
        mock_pvc.spec.resources.requests = {"storage": current_size}

        self.assertRaises(
            StorageSizeDecreaseError,
            handler._validate_storage_size,
            mock_pvc,
            mock_storage,
        )

    def test_validate_storage_size__logs_change_request_if_requested_size_greater(self):
        current_size = "10Gi"
        requested_size = "20Gi"
        expected_log = (f"Request to change storage size from {current_size} to {requested_size}")

        handler = PersistentStorageHandler(CoreV1Api())

        mock_storage = MagicMock()
        mock_storage.config.storage_size = requested_size

        mock_pvc = MagicMock()
        mock_pvc.spec.resources.requests = {"storage": current_size}

        with self.assertLogs(level="INFO") as logs:
            handler._validate_storage_size(mock_pvc, mock_storage)
        self.assertIn(expected_log, logs.output[0])
