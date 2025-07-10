import re
from unittest import TestCase
from unittest.mock import MagicMock

from ttd.task_service.persistent_storage.persistent_storage import (
    PersistentStorage,
    PersistentStorageFactory,
)
from ttd.task_service.persistent_storage.persistent_storage_config import (
    PersistentStorageConfig,
    PersistentStorageType,
)
from ttd.ttdenv import TtdEnvFactory

pvc_name_regex_placeholder = "^(?P<name>[a-z0-9-]+){separator}(?P<storage_type>{storage_type}){separator}[a-z0-9]{{{id_suffix_length}}}$"


class TestPersistentStorage(TestCase):

    def setUp(self):
        TtdEnvFactory.get_from_system = MagicMock(return_value=TtdEnvFactory.prod)

    def test_constructor__pvc_name_matches_regex_and_does_not_truncate_base_name_within_char_limit_for_one_pod_type(self, ):
        base_name_within_limit = "test-persistent-storage"
        one_pod = PersistentStorageType.ONE_POD
        pvc_name_regex = pvc_name_regex_placeholder.format(
            separator=PersistentStorage.PVC_NAME_SEPARATOR,
            storage_type=one_pod.value,
            id_suffix_length=PersistentStorage.ID_SUFFIX_LENGTH,
        )

        storage = PersistentStorageFactory.create_storage(
            PersistentStorageConfig(
                storage_type=one_pod,
                storage_size="100Gi",
                base_name=base_name_within_limit,
            )
        )
        pvc_name = storage.pvc_name
        match = re.match(pvc_name_regex, pvc_name)

        self.assertIsNotNone(match)
        self.assertEqual(base_name_within_limit, match.group("name"))  # type: ignore
        self.assertEqual(one_pod.value, match.group("storage_type"))  # type: ignore

    def test_constructor__pvc_name_matches_regex_and_truncates_long_base_name_for_many_pods_type(self, ):
        long_base_name = "a" * (PersistentStorage.K8S_LABEL_ANNOTATION_MAX_LENGTH + 20)
        many_pods = PersistentStorageType.MANY_PODS
        pvc_name_regex = pvc_name_regex_placeholder.format(
            separator=PersistentStorage.PVC_NAME_SEPARATOR,
            storage_type=many_pods.value,
            id_suffix_length=PersistentStorage.ID_SUFFIX_LENGTH,
        )
        separator_length = 2

        storage = PersistentStorageFactory.create_storage(
            PersistentStorageConfig(
                storage_type=many_pods,
                storage_size="100Gi",
                base_name=long_base_name,
            )
        )
        max_base_name_length = storage._get_max_base_name_length(separator_length)
        expected_name = long_base_name[:max_base_name_length]

        pvc_name = storage.pvc_name
        match = re.match(pvc_name_regex, pvc_name)

        self.assertIsNotNone(match)
        self.assertEqual(expected_name, match.group("name"))  # type: ignore
        self.assertEqual(many_pods.value, match.group("storage_type"))  # type: ignore

    def test_constructor__pvc_name_matches_regex_and_trims_base_name_with_end_punctuation_for_one_pod_temp_type(self, ):
        punctuation = "--"
        base_name = "test-persistent-storage"
        one_pod_temp = PersistentStorageType.ONE_POD_TEMP
        pvc_name_regex = pvc_name_regex_placeholder.format(
            separator=PersistentStorage.PVC_NAME_SEPARATOR,
            storage_type=one_pod_temp.value,
            id_suffix_length=PersistentStorage.ID_SUFFIX_LENGTH,
        )

        storage = PersistentStorageFactory.create_storage(
            PersistentStorageConfig(
                storage_type=one_pod_temp,
                storage_size="100Gi",
                base_name=base_name + punctuation,
            )
        )

        pvc_name = storage.pvc_name
        match = re.match(pvc_name_regex, pvc_name)

        self.assertIsNotNone(match)
        self.assertEqual(base_name, match.group("name"))  # type: ignore
        self.assertEqual(one_pod_temp.value, match.group("storage_type"))  # type: ignore

    def test_constructor__pvc_name_matches_regex_and_lowers_base_name_capital_letters_for_one_pod(self, ):
        base_name = "test-persistent-storage"
        base_name_upper = base_name.upper()
        one_pod_temp = PersistentStorageType.ONE_POD_TEMP
        pvc_name_regex = pvc_name_regex_placeholder.format(
            separator=PersistentStorage.PVC_NAME_SEPARATOR,
            storage_type=one_pod_temp.value,
            id_suffix_length=PersistentStorage.ID_SUFFIX_LENGTH,
        )

        storage = PersistentStorageFactory.create_storage(
            PersistentStorageConfig(
                storage_type=one_pod_temp,
                storage_size="100Gi",
                base_name=base_name_upper,
            )
        )

        pvc_name = storage.pvc_name
        match = re.match(pvc_name_regex, pvc_name)

        self.assertIsNotNone(match)
        self.assertEqual(base_name, match.group("name"))  # type: ignore
        self.assertEqual(one_pod_temp.value, match.group("storage_type"))  # type: ignore
