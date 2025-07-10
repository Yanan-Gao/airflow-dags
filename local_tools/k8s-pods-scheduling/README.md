# Airflow K8s pod scheduling

The following tools will create the required permissions to allow Airflow to create pods in your namespace.

If your production pods are running in the `general-use-production` cluster (or you are running test pods in `general-use-non-production`)
then this tool is not necessary. You just need to use a rolebinding to attach the `airflow2-service-account` to a role in your namespace.

If you want to run pods outside the clusters Airflow is in, you can use this tool to achieve this.

## Prerequisites

You must first ensure you are in the correct Kubernetes cluster.
Use `kubectl config get-contexts` to see the full list available. Use `kubectl config use-context <your-context>`.

You must have admin permissions over the namespace you wish to run pods in. Firstly elevate in JITA, and then use
`kubectl -n <your-namespace> get secrets`. If this command executes successfully you can proceed.

## Creating K8s resources

You can install the chart using the following (run in `local_tools/k8s-pods-scheduling`):
```bash
helm install airflow-pod-scheduling-rbac ./ -n <your-namespace>
```

This will create the RBAC resources to run pods in your namespace/cluster. They will all be named `airflow-pod-scheduling`.

To give the resources an alternative name, you can install with:
```bash
helm install airflow-pod-scheduling-rbac ./ -n <your-namespace> --set name=<your-rbac-resources-name>
```

## Retrieving the token

To retrieve the token and put it into a format Airflow can understand, you can use the following Python tool.
Note you cannot use a remote interpreter to run this script as it requires your personal Kubernetes permissions to work.

You will need to have the `kubernetes` Python package installed.

```bash
python3 ./format_k8s_conn_for_vault.py --namespace "<k8s-namespace>" --cluster "<k8s-cluster>"
```

## Put the token in vault

The script will output a string. Put this as the value of a connection in Vault. There are instructions on this here: 
[Accessing secrets from Vault](https://thetradedesk.gitlab-pages.adsrvr.org/teams/dataproc/docs/airflow/ttd_tooling/secrets_layer/).

## Using the connection

You can just reference the name you chose for you connection when you instantiate `KubernetesPodOperator`:

```python
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client.models import V1LocalObjectReference

KubernetesPodOperator(
    kubernetes_conn_id='<my-conn-id-as-in-vault>',
    image_pull_secrets=[V1LocalObjectReference(name="use-docker")],
    namespace="<my-namespace>",
    image='<my-image>',
    task_id="<my-task-id>"
)

```

There is a demo DAG file located at `src/dags/demo/demo-kubernetes-pod.py`

## Removing the K8s resources

If you wish to remove the Kubernetes resources, run:
```bash
helm uninstall -n task-service airflow-pod-scheduling-rbac
```