from __future__ import annotations

from datetime import timedelta
import base64
import hashlib
import re
import time
from typing import Tuple

from airflow.operators.python import PythonOperator, ShortCircuitOperator

from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ttdenv import TtdEnvFactory


_CONFIG_BUCKET = "thetradedesk-mlplatform-us-east-1"


def _sha256_b64(data: str) -> str:
    return base64.b64encode(hashlib.sha256(data.encode("utf-8")).digest()).decode()


def _render_template(tpl: str, ctx: dict[str, str]) -> str:
    rendered = tpl.format(**ctx)
    unresolved = re.findall(r"{[^{}]+}", rendered)
    if unresolved:
        raise ValueError(f"Unresolved variables in template: {unresolved}")
    return rendered


def _resolve_env(env: str, experiment: str) -> str:
    env = (env or "").lower()
    if env in ("prod", "production", "prodtest"):
        if experiment:
            return "experiment" if env.startswith("prod") else "test"
        return "prod" if env.startswith("prod") else "test"
    if not experiment:
        raise ValueError("experiment_name is required for test env")
    return "test"


def _prepare_runtime_config(
    group: str,
    job: str,
    run_date: str,
    experiment: str,
    timeout: timedelta,
) -> tuple[str, bool]:
    env = _resolve_env(TtdEnvFactory.get_from_system().execution_env, experiment)
    exp_dir = f"{experiment}/" if experiment else ""
    tpl_key = (
        f"s3://{_CONFIG_BUCKET}/configdata/confetti/configs/"
        f"{env}/{exp_dir}{group}/{job}/behavioral_config.yml"
    )

    aws = AwsCloudStorage()
    template = aws.read_key(tpl_key)
    rendered = _render_template(template, {"date": run_date})
    hash_ = _sha256_b64(rendered)

    runtime_base = (
        f"s3://{_CONFIG_BUCKET}/configdata/confetti/runtime-configs/"
        f"{env}/{group}/{job}/{hash_}/"
    )
    cfg_key = runtime_base + "behavioral_config.yml"
    res_key = runtime_base + "result.yml"

    c_bucket, c_path = aws._parse_bucket_and_key(cfg_key, None)
    r_bucket, r_path = aws._parse_bucket_and_key(res_key, None)

    # fast path
    if aws.check_for_key(r_path, r_bucket):
        return runtime_base, True

    # wait if config exists but result not yet ready
    if aws.check_for_key(c_path, c_bucket):
        start = time.time()
        while time.time() - start < timeout.total_seconds():
            if aws.check_for_key(r_path, r_bucket):
                return runtime_base, True
            time.sleep(300)

    aws.load_string(rendered, key=cfg_key, bucket_name=c_bucket, replace=True)
    return runtime_base, False


def make_confetti_tasks(
    *,
    group_name: str,
    job_name: str,
    experiment_name: str = "",
    run_date: str = "{{ ds }}",
    check_timeout: timedelta = timedelta(hours=2),
) -> Tuple[PythonOperator, ShortCircuitOperator]:
    """Return (prepare_op, fast_pass_op) for confetti jobs."""

    def _prep(**context):
        rb, skip = _prepare_runtime_config(
            group_name,
            job_name,
            context.get("ds", run_date),
            experiment_name,
            check_timeout,
        )
        context["ti"].xcom_push(key="runtime_base", value=rb)
        context["ti"].xcom_push(key="skip_job", value=skip)

    prep_op = PythonOperator(
        task_id=f"prepare_confetti_{job_name}",
        python_callable=_prep,
    )

    def _should_run(**context):
        return not context["ti"].xcom_pull(task_ids=prep_op.task_id, key="skip_job")

    gate_op = ShortCircuitOperator(
        task_id=f"confetti_should_run_{job_name}",
        python_callable=_should_run,
    )

    return prep_op, gate_op
