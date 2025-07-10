resource_assets = [
    {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "annotations": {
                "builders.kserve.tenzing.ttd.io/ingress_initializer": "True",
                "nginx.ingress.kubernetes.io/backend-protocol": "HTTP",
                "nginx.ingress.kubernetes.io/use-regex": "true",
                "nginx.ingress.kubernetes.io/rewrite-target": "/$2",
                "nginx.ingress.kubernetes.io/proxy-body-size": "30m",
                "builders.kserve.tenzing.ttd.io/standard_ingress_builder": "True",
                "builders.kserve.tenzing.ttd.io/client_override_cluster_based_fields": "True",
                "external-dns.alpha.kubernetes.io/aws-weight": "1",
                "external-dns.alpha.kubernetes.io/ttl": "300",
                "builders.kserve.tenzing.ttd.io/standard_ms_endpoint_builder": "True",
            },
            "labels": {
                "app.kubernetes.io/managed-by": "kserve-tenzing"
            },
            "name": "model-serving-tf-mnist-af",
        },
        "spec": {
            "ingressClassName":
            "nginx-internal",
            "rules": [{
                "http": {
                    "paths": [
                        {
                            "backend": {
                                "service": {
                                    "name": "tf-mnist-af-base-af-predictor",
                                    "port": {
                                        "number": 80
                                    },
                                }
                            },
                            "path": "/team/tf-mnist-af/base-af(/|$)(.*)",
                            "pathType": "Prefix",
                        },
                        {
                            "backend": {
                                "service": {
                                    "name": "tf-mnist-af-vpeg-af-predictor",
                                    "port": {
                                        "number": 80
                                    },
                                }
                            },
                            "path": "/team/tf-mnist-af/vpeg-af(/|$)(.*)",
                            "pathType": "Prefix",
                        },
                        {
                            "backend": {
                                "service": {
                                    "name": "tf-mnist-af-vpeg-batch-af-predictor",
                                    "port": {
                                        "number": 80
                                    },
                                }
                            },
                            "path": "/team/tf-mnist-af/vpeg-batch-af(/|$)(.*)",
                            "pathType": "Prefix",
                        },
                    ]
                }
            }],
            "tls": [{
                "secretName": "model-serving-cert-team"
            }],
        },
    },
    {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "annotations": {
                "builders.kserve.tenzing.ttd.io/ingress_initializer": "True",
                "nginx.ingress.kubernetes.io/backend-protocol": "HTTP",
                "nginx.ingress.kubernetes.io/use-regex": "true",
                "nginx.ingress.kubernetes.io/rewrite-target": "/$2",
                "nginx.ingress.kubernetes.io/proxy-body-size": "30m",
                "builders.kserve.tenzing.ttd.io/cluster_specific_ingress_builder": "True",
                "builders.kserve.tenzing.ttd.io/client_override_cluster_based_fields": "True",
                "builders.kserve.tenzing.ttd.io/standard_ms_endpoint_builder": "True",
            },
            "labels": {
                "app.kubernetes.io/managed-by": "kserve-tenzing"
            },
            "name": "model-serving-tf-mnist-af-cluster",
        },
        "spec": {
            "ingressClassName":
            "nginx-internal",
            "rules": [{
                "http": {
                    "paths": [
                        {
                            "backend": {
                                "service": {
                                    "name": "tf-mnist-af-base-af-predictor",
                                    "port": {
                                        "number": 80
                                    },
                                }
                            },
                            "path": "/team/tf-mnist-af/base-af(/|$)(.*)",
                            "pathType": "Prefix",
                        },
                        {
                            "backend": {
                                "service": {
                                    "name": "tf-mnist-af-vpeg-af-predictor",
                                    "port": {
                                        "number": 80
                                    },
                                }
                            },
                            "path": "/team/tf-mnist-af/vpeg-af(/|$)(.*)",
                            "pathType": "Prefix",
                        },
                        {
                            "backend": {
                                "service": {
                                    "name": "tf-mnist-af-vpeg-batch-af-predictor",
                                    "port": {
                                        "number": 80
                                    },
                                }
                            },
                            "path": "/team/tf-mnist-af/vpeg-batch-af(/|$)(.*)",
                            "pathType": "Prefix",
                        },
                    ]
                }
            }],
            "tls": [{
                "secretName": "model-serving-cert-team-cluster"
            }],
        },
    },
    {
        "apiVersion": "serving.kserve.io/v1beta1",
        "kind": "InferenceService",
        "metadata": {
            "annotations": {
                "builders.kserve.tenzing.ttd.io/isvc_initializer": "True",
                "model-serving-endpoints/sourceUri": "s3://thetradedesk-mlplatform-us-east-1/mlops/model_serving/store/aifun/test_tf/vTest",
                "builders.kserve.tenzing.ttd.io/model_source": "True",
                "sumologic.com/include": "True",
                "sumologic.com/sourceCategory": "modelserving-endpoints",
                "sumologic.com/sourceName": "modelserving-endpoints-base-af",
                "builders.kserve.tenzing.ttd.io/sumo_annotation_builder": "True",
                "model-serving-endpoints/team": "team",
                "model-serving-endpoints/modelGroup": "tf-mnist-af",
                "model-serving-endpoints/modelKey": "base-af",
                "builders.kserve.tenzing.ttd.io/tf_isvc_builder": "True",
                "builders.kserve.tenzing.ttd.io/client_override_cluster_based_fields": "True",
                "builders.kserve.tenzing.ttd.io/standard_ms_endpoint_builder": "True",
            },
            "labels": {
                "app.kubernetes.io/managed-by": "kserve-tenzing"
            },
            "name": "tf-mnist-af-base-af",
        },
        "spec": {
            "predictor": {
                "initContainers": [{
                    "args": ["uri", "/mnt/models"],
                    "image": "proxy.docker.adsrvr.org/kserve/storage-initializer:v0.14.1",
                    "imagePullPolicy": "IfNotPresent",
                    "name": "storage-initializer",
                    "resources": {
                        "limits": {
                            "cpu": "500m",
                            "memory": "1Gi"
                        },
                        "requests": {
                            "cpu": "250m",
                            "memory": "512Mi"
                        },
                    },
                    "terminationMessagePath": "/dev/termination-log",
                    "terminationMessagePolicy": "FallbackToLogsOnError",
                    "volumeMounts": [{
                        "mountPath": "/mnt/models",
                        "name": "kserve-provision-location",
                    }],
                }],
                "minReplicas":
                2,
                "model": {
                    "env": [{
                        "name": "TF_CPP_VMODULE",
                        "value": "http_server=1"
                    }],
                    "modelFormat": {
                        "name": "tensorflow"
                    },
                    "resources": {
                        "limits": {
                            "cpu": "250m",
                            "memory": "1Gi"
                        },
                        "requests": {
                            "cpu": "125m",
                            "memory": "500Mi"
                        },
                    },
                    "storageUri": "uri",
                    "volumeMounts": [{
                        "mountPath": "/mnt/models",
                        "name": "kserve-provision-location",
                        "readOnly": True,
                    }],
                },
                "volumes": [{
                    "emptyDir": {},
                    "name": "kserve-provision-location"
                }],
            }
        },
    },
    {
        "apiVersion": "serving.kserve.io/v1beta1",
        "kind": "InferenceService",
        "metadata": {
            "annotations": {
                "builders.kserve.tenzing.ttd.io/isvc_initializer": "True",
                "model-serving-endpoints/sourceUri":
                "s3://thetradedesk-mlplatform-us-east-1/mlops/model_serving/store/aifun/test_tf/vTest/1",
                "builders.kserve.tenzing.ttd.io/model_source": "True",
                "sumologic.com/include": "True",
                "sumologic.com/sourceCategory": "modelserving-endpoints",
                "sumologic.com/sourceName": "modelserving-endpoints-vpeg-af",
                "builders.kserve.tenzing.ttd.io/sumo_annotation_builder": "True",
                "model-serving-endpoints/team": "team",
                "model-serving-endpoints/modelGroup": "tf-mnist-af",
                "model-serving-endpoints/modelKey": "vpeg-af",
                "builders.kserve.tenzing.ttd.io/tf_isvc_builder": "True",
                "builders.kserve.tenzing.ttd.io/client_override_cluster_based_fields": "True",
                "builders.kserve.tenzing.ttd.io/standard_ms_endpoint_builder": "True",
            },
            "labels": {
                "app.kubernetes.io/managed-by": "kserve-tenzing"
            },
            "name": "tf-mnist-af-vpeg-af",
        },
        "spec": {
            "predictor": {
                "initContainers": [{
                    "args": ["url", "/mnt/models"],
                    "image": "proxy.docker.adsrvr.org/kserve/storage-initializer:v0.14.1",
                    "imagePullPolicy": "IfNotPresent",
                    "name": "storage-initializer",
                    "resources": {
                        "limits": {
                            "cpu": "500m",
                            "memory": "1Gi"
                        },
                        "requests": {
                            "cpu": "250m",
                            "memory": "512Mi"
                        },
                    },
                    "terminationMessagePath": "/dev/termination-log",
                    "terminationMessagePolicy": "FallbackToLogsOnError",
                    "volumeMounts": [{
                        "mountPath": "/mnt/models",
                        "name": "kserve-provision-location",
                    }],
                }],
                "minReplicas":
                2,
                "model": {
                    "env": [{
                        "name": "TF_CPP_VMODULE",
                        "value": "http_server=1"
                    }],
                    "modelFormat": {
                        "name": "tensorflow"
                    },
                    "resources": {
                        "limits": {
                            "cpu": "250m",
                            "memory": "1Gi"
                        },
                        "requests": {
                            "cpu": "125m",
                            "memory": "500Mi"
                        },
                    },
                    "storageUri": "url",
                    "volumeMounts": [{
                        "mountPath": "/mnt/models",
                        "name": "kserve-provision-location",
                        "readOnly": True,
                    }],
                },
                "volumes": [{
                    "emptyDir": {},
                    "name": "kserve-provision-location"
                }],
            }
        },
    },
    {
        "apiVersion": "serving.kserve.io/v1beta1",
        "kind": "InferenceService",
        "metadata": {
            "annotations": {
                "builders.kserve.tenzing.ttd.io/isvc_initializer": "True",
                "model-serving-endpoints/sourceUri": "s3://thetradedesk-mlplatform-us-east-1/mlops/model_serving/store/aifun/test_tf/vTest",
                "builders.kserve.tenzing.ttd.io/model_source": "True",
                "sumologic.com/include": "True",
                "sumologic.com/sourceCategory": "modelserving-endpoints",
                "sumologic.com/sourceName": "modelserving-endpoints-vpeg-batch-af",
                "builders.kserve.tenzing.ttd.io/sumo_annotation_builder": "True",
                "model-serving-endpoints/team": "team",
                "model-serving-endpoints/modelGroup": "tf-mnist-af",
                "model-serving-endpoints/modelKey": "vpeg-batch-af",
                "builders.kserve.tenzing.ttd.io/tf_batch_isvc_builder": "True",
                "builders.kserve.tenzing.ttd.io/client_override_cluster_based_fields": "True",
                "builders.kserve.tenzing.ttd.io/standard_ms_endpoint_builder": "True",
            },
            "labels": {
                "app.kubernetes.io/managed-by": "kserve-tenzing"
            },
            "name": "tf-mnist-af-vpeg-batch-af",
        },
        "spec": {
            "predictor": {
                "initContainers": [{
                    "args": ["url", "/mnt/models"],
                    "image": "proxy.docker.adsrvr.org/kserve/storage-initializer:v0.14.1",
                    "imagePullPolicy": "IfNotPresent",
                    "name": "storage-initializer",
                    "resources": {
                        "limits": {
                            "cpu": "500m",
                            "memory": "1Gi"
                        },
                        "requests": {
                            "cpu": "250m",
                            "memory": "512Mi"
                        },
                    },
                    "terminationMessagePath": "/dev/termination-log",
                    "terminationMessagePolicy": "FallbackToLogsOnError",
                    "volumeMounts": [{
                        "mountPath": "/mnt/models",
                        "name": "kserve-provision-location",
                    }],
                }],
                "minReplicas":
                2,
                "model": {
                    "env": [{
                        "name": "TF_CPP_VMODULE",
                        "value": "http_server=1"
                    }],
                    "modelFormat": {
                        "name": "tensorflow-batch"
                    },
                    "resources": {
                        "limits": {
                            "cpu": "250m",
                            "memory": "1Gi"
                        },
                        "requests": {
                            "cpu": "125m",
                            "memory": "500Mi"
                        },
                    },
                    "storageUri":
                    "url",
                    "volumeMounts": [
                        {
                            "mountPath": "/mnt/models",
                            "name": "kserve-provision-location",
                            "readOnly": True,
                        },
                        {
                            "mountPath": "/mnt/config",
                            "name": "tensorflow-batch-config"
                        },
                    ],
                },
                "volumes": [
                    {
                        "emptyDir": {},
                        "name": "kserve-provision-location"
                    },
                    {
                        "configMap": {
                            "name": "tf-mnist-af-vpeg-batch-af-tf-batch-config"
                        },
                        "name": "tensorflow-batch-config",
                    },
                ],
            }
        },
    },
    {
        "apiVersion": "v1",
        "data": {
            "batching_parameters.txt": "max_batch_size { value: 32 }\nbatch_timeout_micros { value: 1000 }\n"
        },
        "kind": "ConfigMap",
        "metadata": {
            "annotations": {
                "builders.kserve.tenzing.ttd.io/configmap_initializer": "True",
                "builders.kserve.tenzing.ttd.io/tensorflow_batching_configmap_builder": "True",
                "builders.kserve.tenzing.ttd.io/standard_ms_endpoint_builder": "True",
            },
            "labels": {
                "app.kubernetes.io/managed-by": "kserve-tenzing"
            },
            "name": "tf-mnist-af-vpeg-batch-af-tf-batch-config",
        },
    },
    {
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {
            "annotations": {
                "builders.kserve.tenzing.ttd.io/pdb_initializer": "True",
                "builders.kserve.tenzing.ttd.io/standard_pdb_builder": "True",
                "builders.kserve.tenzing.ttd.io/standard_ms_endpoint_builder": "True",
            },
            "labels": {
                "app.kubernetes.io/managed-by": "kserve-tenzing"
            },
            "name": "tf-mnist-af-base-af-pdb",
        },
        "spec": {
            "maxUnavailable": 1,
            "selector": {
                "matchLabels": {
                    "app": "isvc.tf-mnist-af-base-af-predictor"
                }
            },
        },
    },
    {
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {
            "annotations": {
                "builders.kserve.tenzing.ttd.io/pdb_initializer": "True",
                "builders.kserve.tenzing.ttd.io/standard_pdb_builder": "True",
                "builders.kserve.tenzing.ttd.io/standard_ms_endpoint_builder": "True",
            },
            "labels": {
                "app.kubernetes.io/managed-by": "kserve-tenzing"
            },
            "name": "tf-mnist-af-vpeg-af-pdb",
        },
        "spec": {
            "maxUnavailable": 1,
            "selector": {
                "matchLabels": {
                    "app": "isvc.tf-mnist-af-vpeg-af-predictor"
                }
            },
        },
    },
    {
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {
            "annotations": {
                "builders.kserve.tenzing.ttd.io/pdb_initializer": "True",
                "builders.kserve.tenzing.ttd.io/standard_pdb_builder": "True",
                "builders.kserve.tenzing.ttd.io/standard_ms_endpoint_builder": "True",
            },
            "labels": {
                "app.kubernetes.io/managed-by": "kserve-tenzing"
            },
            "name": "tf-mnist-af-vpeg-batch-af-pdb",
        },
        "spec": {
            "maxUnavailable": 1,
            "selector": {
                "matchLabels": {
                    "app": "isvc.tf-mnist-af-vpeg-batch-af-predictor"
                }
            },
        },
    },
]
