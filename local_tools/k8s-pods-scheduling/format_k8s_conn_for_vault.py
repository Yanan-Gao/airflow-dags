import json
from argparse import Namespace, ArgumentParser, ArgumentDefaultsHelpFormatter
from urllib.parse import urlencode

from kubernetes import client, config
from base64 import b64decode

TTD_CLUSTER_CONFIG = 'https://thetradedesk.gitlab-pages.adsrvr.org/teams/kpop/k8s-cluster-addons/ttd'
LOCAL_TEMP_CONFIG = '/tmp/temp_kubeconfig.yaml'


def setup_argument_parser() -> Namespace:
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)

    parser.add_argument('--namespace', type=str, help='The namespace in which to create these resources')
    parser.add_argument('--cluster', type=str, help='The cluster in which the namespace is located')
    parser.add_argument('--name', type=str, default='airflow-pod-scheduling', help='The name of the RBAC resources in K8s')

    args_namespace = parser.parse_args()

    if args_namespace.namespace is None or args_namespace.cluster is None:
        parser.error("You must specify a namespace and cluster")
    return args_namespace


def create_kubeconfig_locally() -> str:
    import requests

    url = TTD_CLUSTER_CONFIG
    response = requests.get(url)
    response.raise_for_status()
    content = response.text

    with open(LOCAL_TEMP_CONFIG, 'w') as file:
        file.write(content)

    return content


def get_token(args: Namespace) -> str:
    config.load_kube_config(context=args.cluster, config_file=LOCAL_TEMP_CONFIG)

    v1 = client.CoreV1Api()

    secret = v1.read_namespaced_secret(name=args.name, namespace=args.namespace)
    token = b64decode(secret.data["token"])

    return token.decode()


def get_cluster_config(args: Namespace, kubeconfig: str):
    from yaml import safe_load

    ttd_config = safe_load(kubeconfig)

    cluster = [cluster for cluster in ttd_config['clusters'] if cluster['name'] == args.cluster]
    ttd_config['clusters'] = cluster

    context = [context for context in ttd_config['contexts'] if context['name'] == args.cluster]
    context[0]['context']['user'] = args.name
    ttd_config['contexts'] = context

    ttd_config['current-context'] = args.cluster
    ttd_config['users'] = [{"name": args.name, "user": {"token": get_token(args)}}]

    return ttd_config


def generate_uri(kubeconfig: str) -> str:
    extra = {
        "kube_config": get_cluster_config(args, kubeconfig),
        "xcom_sidecar_container_resources": json.dumps({
            "limits": {
                "memory": "20Mi"
            },
            "requests": {
                "cpu": "1m",
                "memory": "10Mi"
            },
        }),
        "xcom_sidecar_container_image": "proxy.docker.adsrvr.org/alpine:latest",
    }
    return 'kubernetes:///?' + urlencode(extra)


if __name__ == '__main__':
    args = setup_argument_parser()

    kubeconfig = create_kubeconfig_locally()

    uri = generate_uri(kubeconfig)

    print("Please put the following in Vault:\n")
    print(uri)
