from __future__ import annotations
from datetime import datetime, date
from typing import Any, Dict
import logging
from datetime import timedelta
import base64
import hashlib
import time
# import os
from typing import Tuple
import yaml
import subprocess

from airflow.operators.python import PythonOperator, ShortCircuitOperator

from ttd.tasks.op import OpTask

from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ttdenv import TtdEnvFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_CONFIG_BUCKET = "thetradedesk-mlplatform-us-east-1"


def _sha256_b64(data: str) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(data.encode("utf-8")).digest()).decode()


def _render_template(tpl: str, run_level_variables: dict[str, str]) -> str:
    """Render ``tpl`` as a Jinja template using ``ctx``.

    ``StrictUndefined`` is used so that an informative error is raised if
    required variables are missing. This keeps behaviour similar to the old
    ``str.format`` implementation while allowing full Jinja flexibility.
    """

    from jinja2 import Environment, StrictUndefined

    env = Environment(undefined=StrictUndefined)
    try:
        template = env.from_string(tpl)
        return template.render(**run_level_variables)
    except Exception as exc:
        raise ValueError(f"Failed to render template: {exc}") from exc


def _inject_audience_jar_path(rendered: str, aws: AwsCloudStorage) -> str:
    """Compute audienceJarPath from branch and version and remove those keys.

    Raises ``ValueError`` if the YAML cannot be parsed or required keys are
    missing. Errors from ``AwsCloudStorage`` are propagated so the job fails
    loudly.
    """

    try:
        data = yaml.safe_load(rendered)
    except Exception as exc:  # pragma: no cover - malformed YAML
        raise ValueError(f"Failed to parse Confetti YAML: {exc}") from exc

    if not isinstance(data, dict):  # pragma: no cover - unexpected structure
        raise ValueError("Confetti YAML must be a mapping")

    if "audienceJarBranch" not in data:
        raise ValueError("audienceJarBranch is required in Confetti config")
    if "audienceJarVersion" not in data:
        raise ValueError("audienceJarVersion is required in Confetti config")

    branch = str(data.pop("audienceJarBranch"))
    version = str(data.pop("audienceJarVersion"))

    version_value = version
    if version.lower() == "latest":
        if branch == "master":
            current_key = ("s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/prod/_CURRENT")
        else:
            current_key = (f"s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/mergerequests/{branch}/_CURRENT")
        lines = aws.read_key(current_key).splitlines()
        if not lines or not lines[0].strip():
            raise ValueError(f"No version found in {current_key}")
        version_value = lines[0].strip()

    if branch == "master":
        jar_path = ("s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/snapshots/master/"
                    f"{version_value}/audience.jar")
    else:
        jar_path = ("s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/mergerequests/"
                    f"{branch}/{version_value}/audience.jar")

    data["audienceJarPath"] = jar_path
    return yaml.safe_dump(data, sort_keys=True)
#
#
# def _inject_run_date(rendered: str, run_date: datetime | date | str | None) -> str:
#     """Add ``runDate`` to the config."""
#
#     if run_date is None:
#         return rendered
#
#     try:
#         data = yaml.safe_load(rendered)
#     except Exception as exc:  # pragma: no cover - malformed YAML
#         raise ValueError(f"Failed to parse Confetti YAML: {exc}") from exc
#
#     if not isinstance(data, dict):  # pragma: no cover - unexpected structure
#         raise ValueError("Confetti YAML must be a mapping")
#
#     if isinstance(run_date, str):
#         run_date_obj = datetime.fromisoformat(run_date)
#     elif isinstance(run_date, date) and not isinstance(run_date, datetime):
#         run_date_obj = datetime.combine(run_date, datetime.min.time())
#     else:
#         run_date_obj = run_date
#
#     data["runDate"] = run_date_obj.strftime("%Y-%m-%d")
#     return yaml.safe_dump(data, sort_keys=True)


# ---------------------------------------------------------------------------
# Template‑context helper  # todo support more run level variables.
# ---------------------------------------------------------------------------
def _collect_job_run_level_variables(
        *,  # enforce keyword use – clearer at call‑site
        run_date: str | datetime | date,
        **extra_vars: Any,  # future‑proof: pass any additional items here
) -> dict[str, Any]:
    """
    Build the context dictionary that will be fed into Jinja templates.

    # ``run_date`` may be omitted when executed within an Airflow task. In that
    # case the value is inferred from the ``AIRFLOW_CTX_DATA_INTERVAL_START`` or
    # ``AIRFLOW_CTX_EXECUTION_DATE`` environment variables automatically provided
    # by Airflow. The value is normalised so templates can safely call
    # ``{{ date.strftime('%Y%m%d') }}`` regardless of whether a string
    # (``"YYYY-MM-DD"``), ``datetime.date`` or ``datetime.datetime`` was supplied.
    #
    # Extra keyword arguments are simply forwarded, making it trivial to extend
    # the set of runtime variables later without touching the callers.
    # """
    # if run_date is None:
    #     env_val = os.getenv("AIRFLOW_CTX_DATA_INTERVAL_START") or os.getenv("AIRFLOW_CTX_EXECUTION_DATE")
    #     if not env_val:
    #         raise ValueError("run_date is required and could not be inferred from the environment")
    #     run_date = env_val[:10]

    # Normalise *run_date* → datetime
    if isinstance(run_date, str):
        run_date_obj = datetime.fromisoformat(run_date)
    elif isinstance(run_date, date) and not isinstance(run_date, datetime):
        run_date_obj = datetime.combine(run_date, datetime.min.time())
    else:
        run_date_obj = run_date

    run_level_variables: Dict[str, Any] = {"run_date": run_date_obj}
    run_level_variables.update(extra_vars)
    return run_level_variables


def resolve_env(env: str, experiment: str) -> str:
    env = (env or "").lower()
    if env in ("prod", "production"):
        if experiment:
            return "experiment"
        return "prod"
    else:
        if not experiment:
            raise ValueError("experiment_name is required for test env")
    return "test"


def _template_dir(env: str, experiment: str, group: str, job: str) -> str:
    exp_dir = f"{experiment}/" if experiment else ""
    return f"configdata/confetti/configs/{env}/{exp_dir}{group}/{job}/"


def _render_behavioral_config(
    aws: AwsCloudStorage, tpl_key: str, run_vars: dict[str, Any]
) -> tuple[str, str]:
    template = aws.read_key(tpl_key, bucket_name=_CONFIG_BUCKET)
    rendered = _render_template(template, run_vars)
    rendered = _inject_audience_jar_path(rendered, aws)
    # rendered = _inject_run_date(rendered, run_vars.get("date"))
    jar_path = yaml.safe_load(rendered)["audienceJarPath"]
    return rendered, jar_path


def _runtime_paths(env: str, group: str, job: str, hash_: str, experiment: str) -> tuple[str, str, str, str]:
    base_key = f"configdata/confetti/runtime-configs/{env}/{group}/{job}/{hash_}/"
    runtime_base = f"s3://{_CONFIG_BUCKET}/{base_key}"
    cfg_key = base_key + "behavioral_config.yml"
    success_key = base_key + "_SUCCESS"
    start_key = base_key + "_START"
    return runtime_base, cfg_key, success_key, start_key


def _success_exists(aws: AwsCloudStorage, success_key: str) -> bool:
    return aws.check_for_key(success_key, _CONFIG_BUCKET)


def _wait_for_start_and_success(
    aws: AwsCloudStorage, start_key: str, success_key: str, timeout: timedelta
) -> bool:
    if not aws.check_for_key(start_key, _CONFIG_BUCKET):
        return False
    start = time.time()
    while time.time() - start < timeout.total_seconds():
        if _success_exists(aws, success_key):
            return True
        logger.info(
            "Going to wait for another round for: %s, key %s",
            _CONFIG_BUCKET,
            success_key,
        )
        time.sleep(300)
    return False


def _wait_for_existing_run(aws: AwsCloudStorage, success_key: str, start_key: str, timeout: timedelta) -> bool:
    if _success_exists(aws, success_key):
        return True
    return _wait_for_start_and_success(aws, start_key, success_key, timeout)


def _upload_additional_configs(
    aws: AwsCloudStorage,
    tpl_dir: str,
    runtime_base_key: str,
    run_vars: dict[str, Any],
) -> None:
    for key in aws.list_keys(prefix=tpl_dir, bucket_name=_CONFIG_BUCKET) or []:
        if key.endswith("behavioral_config.yml") or not key.endswith((".yml", ".yaml")):
            continue
        tpl = aws.read_key(key, bucket_name=_CONFIG_BUCKET)
        content = _render_template(tpl, run_vars)
        dest_key = runtime_base_key + key.split("/")[-1]
        logger.info("Uploaded runtime-config to bucket %s, key %s", _CONFIG_BUCKET, dest_key)
        aws.load_string(content, key=dest_key, bucket_name=_CONFIG_BUCKET, replace=True)


def _copy_s3_prefix(aws: AwsCloudStorage, src: str, dst: str) -> None:
    """Recursively copy S3 objects.

    ``src`` and ``dst`` must be full S3 URIs. If ``src`` points to a single
    object the contents are copied to ``dst``. If ``src`` points to a prefix,
    all objects under the prefix are copied into ``dst`` while preserving their
    relative paths.

    Errors from :class:`AwsCloudStorage` bubble up so that the caller can fall
    back to the normal run when copying fails.
    """

    src_bucket, src_prefix = aws._parse_bucket_and_key(src, None)
    dst_bucket, dst_prefix = aws._parse_bucket_and_key(dst, None)

    keys = aws.list_keys(prefix=src_prefix, bucket_name=src_bucket) or []
    if not keys and aws.check_for_key(src_prefix, src_bucket):
        keys = [src_prefix]
    if not keys:
        raise ValueError(f"No objects found at {src}")

    if len(keys) == 1 and not src.endswith("/"):
        aws.copy_file(
            src_key=src_prefix,
            src_bucket_name=src_bucket,
            dst_key=dst_prefix,
            dst_bucket_name=dst_bucket,
        ).get()
    else:
        src_uri = f"s3://{src_bucket}/{src_prefix.rstrip('/')}"
        dst_uri = f"s3://{dst_bucket}/{dst_prefix.rstrip('/')}"
        subprocess.check_call(["aws", "s3", "sync", src_uri, dst_uri])


def _prepare_runtime_config(
        group: str,
        job: str,
        run_date: str,
        experiment: str,
        timeout: timedelta,
        return_jar_path: bool = False,
) -> tuple[str, bool] | tuple[str, bool, str]:
    """Render runtime configs and check for previous results."""
    env = resolve_env(TtdEnvFactory.get_from_system().execution_env, experiment)
    tpl_dir = _template_dir(env, experiment, group, job)
    tpl_key = tpl_dir + "behavioral_config.yml"

    aws = AwsCloudStorage()
    run_vars = _collect_job_run_level_variables(run_date=run_date)
    rendered, jar_path = _render_behavioral_config(aws, tpl_key, run_vars)
    hash_ = _sha256_b64(rendered)

    runtime_base, cfg_key, success_key, start_key = _runtime_paths(env, group, job, hash_, experiment)

    runtime_base_key = cfg_key.rsplit("/", 1)[0] + "/"

    if _wait_for_existing_run(aws, success_key, start_key, timeout):
        return (runtime_base, True, jar_path) if return_jar_path else (runtime_base, True)

    aws.load_string(rendered, key=cfg_key, bucket_name=_CONFIG_BUCKET, replace=True)
    logger.info("Uploaded runtime-config to bucket %s, key %s", _CONFIG_BUCKET, cfg_key)

    _upload_additional_configs(aws, tpl_dir, runtime_base_key, run_vars)

    # write _START key with experiment metadata
    aws.load_string(experiment or "", key=start_key, bucket_name=_CONFIG_BUCKET, replace=True)

    return (runtime_base, False, jar_path) if return_jar_path else (runtime_base, False)


def make_confetti_tasks(
        *,
        group_name: str,
        job_name: str,
        experiment_name: str = "",
        run_date: str = "{{ ds }}",
        check_timeout: timedelta = timedelta(hours=2),
        task_id_prefix: str = "",
) -> Tuple[OpTask, OpTask]:
    """Return (prepare_task, gate_task) for Confetti jobs.

    Both tasks are ``OpTask`` instances wrapping an underlying Airflow
    ``PythonOperator`` and ``ShortCircuitOperator`` so that they can be
    chained with other :class:`BaseTask` objects using ``>>``.

    ``task_id_prefix`` can be used to avoid duplicate task IDs when a
    single DAG instantiates multiple Confetti tasks for the same
    ``job_name``.
    """

    def _prep(**context):
        rb, skip, jar = _prepare_runtime_config(
            group_name,
            job_name,
            context.get("ds", run_date),
            experiment_name,
            check_timeout,
            return_jar_path=True,
        )
        context["ti"].xcom_push(key="confetti_runtime_config_base_path", value=rb)
        context["ti"].xcom_push(key="skip_job", value=skip)
        context["ti"].xcom_push(key="audienceJarPath", value=jar)

    prep_task = OpTask(op=PythonOperator(
        task_id=f"{task_id_prefix}prepare_confetti_{job_name}",
        python_callable=_prep,
    ))

    def _should_run(**context):
        ti = context["ti"]
        skip = ti.xcom_pull(task_ids=prep_task.task_id, key="skip_job")
        if not skip:
            return True

        runtime_base = ti.xcom_pull(task_ids=prep_task.task_id, key="confetti_runtime_config_base_path")

        try:
            aws = AwsCloudStorage()
            run_vars = _collect_job_run_level_variables(run_date=context.get("ds", run_date))
            env = resolve_env(TtdEnvFactory.get_from_system().execution_env, experiment_name)
            tpl_dir = _template_dir(env, experiment_name, group_name, job_name)
            out_tpl_key = tpl_dir + "output_config.yml"
            tpl = aws.read_key(out_tpl_key, bucket_name=_CONFIG_BUCKET)
            rendered = _render_template(tpl, run_vars)
            current_out_path = yaml.safe_load(rendered)["out_path"]

            runtime_out_key = runtime_base.rstrip("/") + "/output_config.yml"
            stored_rendered = aws.read_key(runtime_out_key)
            prev_out_path = yaml.safe_load(stored_rendered)["out_path"]

            if prev_out_path != current_out_path:
                _copy_s3_prefix(aws, prev_out_path, current_out_path)

            return False
        except Exception as exc:  # pragma: no cover - unexpected failure
            logger.exception("Fast pass copy failed: %s", exc)
            return True

    gate_task = OpTask(op=ShortCircuitOperator(
        task_id=f"{task_id_prefix}confetti_should_run_{job_name}",
        python_callable=_should_run,
    ))

    return prep_task, gate_task
