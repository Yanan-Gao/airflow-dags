import unittest
from datetime import datetime, timedelta, timezone
from typing import List
from unittest import mock

from airflow.hooks.base import BaseHook
import kubernetes
from kubernetes import config
from kubernetes.client import CoreV1Api
from kubernetes.client.models import (
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimVolumeSource,
    V1Pod,
    V1PodSpec,
    V1Volume,
    V1ObjectMeta,
)

import sys
from ttd.task_service.k8s_connection_helper import aws
from ttd.task_service.persistent_storage.persistent_storage_config import (
    PersistentStorageType,
)
from ttd.ttdenv import TtdEnvFactory

with mock.patch.dict(sys.modules,
                     {"ttd.ttdslack": mock.Mock()}):  # This is needed because otherwise dag_post_to_slack_callback throws an error
    from dags.dataproc.maintenance.task_service_cleaner import (
        get_expired_pvcs,
        expired_pvc_deletion_period,
        get_pvcs_ready_for_deletion,
        get_unmounted_temp_pvcs,
    )

    now = datetime.now(timezone.utc)
    secrets_prefix = "pre-init-"

    time_metadata = {
        "current": now,
        "old": now - timedelta(days=30),
        "older": now - timedelta(days=40),
        "oldest": now - timedelta(days=50),
    }

    class TestTaskServiceCleaningJob(unittest.TestCase):

        def setUp(self) -> None:
            self.mock_env_factory = mock.patch.object(TtdEnvFactory, "get_from_system")
            self.mock_env_factory.return_value = TtdEnvFactory.prod  # type: ignore

        @mock.patch.object(BaseHook, "get_connection")
        @mock.patch.dict(
            "os.environ",
            {
                config.incluster_config.SERVICE_HOST_ENV_NAME: "host",
                config.incluster_config.SERVICE_PORT_ENV_NAME: "port",
            },
        )
        def test_get_config_params(self, mock_get_connection):
            mock_get_connection.return_value.password = "token"

            host, token = aws.cleaner._get_config_params()
            self.assertEqual(host, "https://host:port")
            self.assertEqual(token, "token")

        def test_get_expired_pvcs__returns_empty_with_no_pvcs(self):
            expired_pvcs: List[V1PersistentVolumeClaim] = []
            test_pvcs: List[V1PersistentVolumeClaim] = []

            result = get_expired_pvcs(client=CoreV1Api(), pvcs=test_pvcs)
            self.assertEqual(expired_pvcs, result)

        def test_get_expired_pvcs__returns_empty_with_active_and_to_delete_pvcs(self):
            expired_pvcs: List[V1PersistentVolumeClaim] = []
            test_pvcs: List[V1PersistentVolumeClaim] = [
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name="test-pvc",
                        annotations={
                            "last-used-at": datetime.utcnow().isoformat(),
                            "inactive-days-before-expire": 30,
                        },
                    )
                ),
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name="test-to-delete-pvc",
                        annotations={
                            "last-used-at": (datetime.utcnow() - timedelta(days=30)).isoformat(),
                            "inactive-days-before-expire": 20,
                            "delete-after": (datetime.utcnow() + timedelta(days=5)).isoformat(),
                        },
                    )
                ),
            ]

            result = get_expired_pvcs(client=CoreV1Api(), pvcs=test_pvcs)
            self.assertEqual(expired_pvcs, result)

        def test_get_expired_pvcs__finds_and_patches_expired_pvc_when_active_and_to_delete_pvcs(self, ):
            expired_pvc = V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(
                    name="test-expired-pvc",
                    annotations={
                        "last-used-at": (datetime.utcnow() - timedelta(days=20)).isoformat(),
                        "inactive-days-before-expire": 15,
                    },
                )
            )
            test_pvcs: List[V1PersistentVolumeClaim] = [
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name="test-pvc",
                        annotations={
                            "last-used-at": datetime.utcnow().isoformat(),
                            "inactive-days-before-expire": 30,
                        },
                    )
                ),
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name="test-to-delete-pvc",
                        annotations={
                            "last-used-at": (datetime.utcnow() - timedelta(days=30)).isoformat(),
                            "inactive-days-before-expire": 20,
                            "delete-after": (datetime.utcnow() + timedelta(days=5)).isoformat(),
                        },
                    )
                ),
                expired_pvc,
            ]

            def patch_pvc(self, name, namespace, body, **kwargs):
                body.metadata.annotations["delete-after"] = (datetime.utcnow() + expired_pvc_deletion_period).isoformat()
                return body

            with mock.patch.object(
                    kubernetes.client.CoreV1Api,
                    "patch_namespaced_persistent_volume_claim",
                    patch_pvc,
            ):
                result = get_expired_pvcs(client=CoreV1Api(), pvcs=test_pvcs)
                self.assertTrue(1 == len(result))
                self.assertEqual(expired_pvc, result[0])
                self.assertTrue("delete-after" in result[0].metadata.annotations)

        def test_get_pvcs_ready_for_deletion__returns_empty_with_no_pvcs(self):
            pvcs_ready_for_deletion: List[V1PersistentVolumeClaim] = []
            test_pvcs: List[V1PersistentVolumeClaim] = []

            result = get_pvcs_ready_for_deletion(pvcs=test_pvcs)
            self.assertEqual(pvcs_ready_for_deletion, result)

        def test_get_pvcs_ready_for_deletion__returns_empty_with_mixed_pvcs(self):
            pvcs_ready_for_deletion: List[V1PersistentVolumeClaim] = []
            test_pvcs: List[V1PersistentVolumeClaim] = [
                V1PersistentVolumeClaim(metadata=V1ObjectMeta(
                    name="test-pvc",
                    annotations={},
                )),
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name="test-to-delete-pvc",
                        annotations={
                            "delete-after": (datetime.utcnow() + timedelta(days=5)).isoformat(),
                        },
                    )
                ),
            ]

            result = get_pvcs_ready_for_deletion(pvcs=test_pvcs)
            self.assertEqual(pvcs_ready_for_deletion, result)

        def test_get_pvcs_ready_for_deletion__finds_and_returns_pvc_with_mixed_pvcs(self, ):
            pvcs_ready_for_deletion: List[V1PersistentVolumeClaim] = [
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name="test-to-delete-pvc",
                        annotations={
                            "delete-after": (datetime.utcnow() - timedelta(hours=1)).isoformat(),
                        },
                    )
                ),
            ]
            test_pvcs: List[V1PersistentVolumeClaim] = [
                V1PersistentVolumeClaim(metadata=V1ObjectMeta(
                    name="test-pvc",
                    annotations={},
                )),
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name="test-to-delete-soon-pvc",
                        annotations={
                            "delete-after": (datetime.utcnow() + timedelta(days=5)).isoformat(),
                        },
                    )
                ),
            ]
            test_pvcs.extend(pvcs_ready_for_deletion)

            result = get_pvcs_ready_for_deletion(pvcs=test_pvcs)
            self.assertEqual(pvcs_ready_for_deletion, result)

        def test_get_unmounted_temp_pvcs__returns_empty_with_no_pvcs_and_pods(self):
            unmounted_temp_pvcs: List[V1PersistentVolumeClaim] = []
            test_pvcs: List[V1PersistentVolumeClaim] = []
            pods: List[V1Pod] = []

            result = get_unmounted_temp_pvcs(pvcs=test_pvcs, pods=pods)
            self.assertEqual(unmounted_temp_pvcs, result)

        def test_get_unmounted_temp_pvcs__returns_empty_with_mounted_temp_pod_pvc(self):
            unmounted_temp_pvcs: List[V1PersistentVolumeClaim] = []
            test_pvcs: List[V1PersistentVolumeClaim] = [
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name="test-to-delete-soon-pvc",
                        annotations={
                            "delete-after": (datetime.utcnow() + timedelta(days=5)).isoformat(),
                        },
                        labels={
                            "type": PersistentStorageType.MANY_PODS.value,
                        },
                    )
                ),
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name="test-mounted-temp-pvc",
                        annotations={},
                        labels={
                            "type": PersistentStorageType.ONE_POD_TEMP.value,
                        },
                    )
                ),
            ]
            pods: List[V1Pod] = [
                V1Pod(
                    spec=V1PodSpec(
                        containers=[],
                        volumes=[
                            V1Volume(
                                name="volume",
                                persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name="test-mounted-temp-pvc"),
                            )
                        ],
                    )
                )
            ]

            result = get_unmounted_temp_pvcs(pvcs=test_pvcs, pods=pods)
            self.assertEqual(unmounted_temp_pvcs, result)

        def test_get_unmounted_temp_pvcs__finds_and_returns_pvc_with_mixed_pvcs_and_pods(self, ):
            unmounted_temp_pvc = V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(
                    name="test-unmounted-temp-pvc",
                    annotations={
                        "last-used-at": (datetime.utcnow() - timedelta(days=2)).isoformat(),
                    },
                    labels={
                        "type": PersistentStorageType.ONE_POD_TEMP.value,
                    },
                )
            )
            test_pvcs: List[V1PersistentVolumeClaim] = [
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name="test-to-delete-soon-pvc",
                        annotations={
                            "delete-after": (datetime.utcnow() + timedelta(days=5)).isoformat(),
                        },
                        labels={
                            "type": PersistentStorageType.ONE_POD.value,
                        },
                    )
                ),
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name="test-mounted-temp-pvc",
                        annotations={},
                        labels={
                            "type": PersistentStorageType.ONE_POD_TEMP.value,
                        },
                    )
                ),
                unmounted_temp_pvc,
            ]
            pods: List[V1Pod] = [
                V1Pod(
                    spec=V1PodSpec(
                        containers=[],
                        volumes=[
                            V1Volume(
                                name="volume",
                                persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name="test-mounted-temp-pvc", ),
                            )
                        ],
                    )
                ),
                V1Pod(spec=V1PodSpec(containers=[], ), ),
            ]

            result = get_unmounted_temp_pvcs(pvcs=test_pvcs, pods=pods)
            self.assertEqual([unmounted_temp_pvc], result)

    if __name__ == "__main__":
        unittest.main()
