from airflow.operators.python_operator import PythonVirtualenvOperator

from ttd.kubernetes.pod_resources import PodResources
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

KUBERNETES_NAMESPACE = "perf-automation-model-benchmark"
BENCHMARK_DOCKER_IMAGE = "internal.docker.adsrvr.org/ttd/local-prediction-benchmark:aws"
SERVICE_ACCOUNT_NAME = "airflow-model-benchmark"
AWS_ROLE_ARN = "arn:aws:iam::003576902480:role/service.model-benchmark"


def create_kubernetes_benchmark_task(name: str, task_id: str, model_s3_path: str, input_s3_path: str, embedding_length: int = 1) -> OpTask:
    operator = TtdKubernetesPodOperator(
        name=name,
        task_id=task_id,
        kubernetes_conn_id=_get_kubernetes_connection_id(),
        namespace=KUBERNETES_NAMESPACE,
        image_pull_policy="Always",
        dnspolicy='ClusterFirst',
        get_logs=True,
        log_events_on_failure=True,
        random_name_suffix=True,
        image=BENCHMARK_DOCKER_IMAGE,
        cmds=[
            "bash",
            "-cex",
            (
                f"aws s3 cp {model_s3_path} /model/ --recursive --no-progress; "
                f"aws s3 cp {input_s3_path} /input/input.json --no-progress; "
                f"dotnet publish/LocalPredictionBenchmark.dll --filter=*LocalOnnxPrediction* -e briefjson --envVars MODEL_PATH:/model INPUT_PATH:/input/input.json EMBEDDING_LENGTH:{embedding_length}; "
                "mkdir -p /airflow/xcom/; "
                'cat BenchmarkDotNet.Artifacts/results/LocalPredictionBenchmark.LocalOnnxPrediction-report-brief.json | jq ".Benchmarks[0].Statistics.Mean / 1000" > /airflow/xcom/return.json'
            ),
        ],
        service_account_name=SERVICE_ACCOUNT_NAME,
        annotations={
            'iam.amazonaws.com/role': AWS_ROLE_ARN,
            'eks.amazonaws.com/role-arn': AWS_ROLE_ARN
        },
        do_xcom_push=True,
        resources=PodResources(request_cpu="8", request_memory="2G", limit_memory="4G", limit_ephemeral_storage="4G")
    )

    return OpTask(op=operator)


def _get_kubernetes_connection_id() -> str:
    if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
        return "perf-automation-model-benchmark"

    return "perf-automation-model-benchmark-test"


def read_mlflow_tag_task(task_id: str, model_name: str, job_cluster_id: str, tag_name: str) -> OpTask:
    operator = PythonVirtualenvOperator(
        task_id=task_id,
        python_callable=_read_mlflow_tag,
        requirements="ttd-mlflow",
        index_urls=[
            "https://pypi.org/simple", "https://nex.adsrvr.org/repository/pypi/simple",
            "https://nex.adsrvr.org/repository/ttd-pypi-dev/simple"
        ],
        # pylint: disable=use-dict-literal
        op_kwargs=dict(model_name=model_name, job_cluster_id=job_cluster_id, tag_name=tag_name),
    )

    return OpTask(op=operator)


def _read_mlflow_tag(model_name: str, job_cluster_id: str, tag_name: str):
    # pylint: disable=import-outside-toplevel
    from ttd_mlflow import TtdMlflow

    client = TtdMlflow()
    model_versions = client.search_model_versions_by_name_and_job_cluster_id(model_name, job_cluster_id)
    assert (
        len(model_versions) == 1
    ), f"There should be exactly one model version trained under {model_name=} with {job_cluster_id=}, but got {len(model_versions)}"

    tags = model_versions[0].tags
    assert (tag_name in tags), f"Model {model_name} version {model_versions[0]} does not contain a tag with name {tag_name}"

    return tags[tag_name]
