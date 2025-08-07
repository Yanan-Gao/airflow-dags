from ttd.ttdenv import TtdEnv, ProdEnv


class MarquezEndpoints:
    raw: str
    normalized: str

    def __init__(self, raw: str, normalized: str):
        self.raw = raw
        self.normalized = normalized


MARQUEZ_ENDPOINTS = MarquezEndpoints(
    raw="http://marquez.tassadar.svc.cluster.local:80", normalized="http://normalizedmarquez.tassadar.svc.cluster.local:80"
)
AMUNDSEN_AWS_USER_PREFIX = "arn:aws:iam::003576902480:role/service.mlops-amundsen"
K8S_SERVICE_ACC_PREFIX = "amundsen-job"


def ingestion_task_env_str(env: TtdEnv) -> str:
    if env == ProdEnv():
        return 'prod'
    return 'dev'


def get_aws_arn(env: TtdEnv) -> str:
    return f"{AMUNDSEN_AWS_USER_PREFIX}-{ingestion_task_env_str(env)}"


def get_service_account(env: TtdEnv) -> str:
    return f"{K8S_SERVICE_ACC_PREFIX}-{ingestion_task_env_str(env)}"
