from __future__ import annotations
from datetime import datetime, date
from typing import Any, Dict
import logging
from datetime import timedelta
import base64
import hashlib
import time
from typing import Tuple
import yaml

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

    Currently we normalise *run_date* so templates can safely call
    ``{{ date.strftime('%Y%m%d') }}`` regardless of whether Airflow
    supplied a string (\"YYYY‑MM‑DD\"), a ``datetime.date`` or a
    ``datetime.datetime``.

    Extra keyword arguments are simply forwarded, making it trivial
    to extend the set of runtime variables later without touching the
    callers.
    """
    # Normalise *run_date* → datetime
    if isinstance(run_date, str):
        run_date_obj = datetime.fromisoformat(run_date)
    elif isinstance(run_date, date) and not isinstance(run_date, datetime):
        run_date_obj = datetime.combine(run_date, datetime.min.time())
    else:
        run_date_obj = run_date

    run_level_variables: Dict[str, Any] = {"date": run_date_obj}
    run_level_variables.update(extra_vars)
    return run_level_variables


def _resolve_env(env: str, experiment: str) -> str:
    env = (env or "").lower()
    if env in ("prod", "production"):
        if experiment:
            return "experiment"
        return "prod"
    else:
        if not experiment:
            raise ValueError("experiment_name is required for test env")
    return "test"


def _template_dir(env: str, group: str, job: str, experiment: str) -> str:
    exp_dir = f"{experiment}/" if experiment else ""
    return (
        f"s3://{_CONFIG_BUCKET}/configdata/confetti/configs/"
        f"{env}/{exp_dir}{group}/{job}/"
    )


def _load_render_config(
        aws: AwsCloudStorage,
        tpl_key: str,
        run_date: str,
) -> tuple[str, str, str, dict[str, Any]]:
    template = aws.read_key(tpl_key)
    run_vars = _collect_job_run_level_variables(run_date=run_date)
    rendered = _render_template(template, run_vars)
    rendered = _inject_audience_jar_path(rendered, aws)
    jar_path = yaml.safe_load(rendered)["audienceJarPath"]
    hash_ = _sha256_b64(rendered)
    return rendered, jar_path, hash_, run_vars


def _runtime_paths(
        env: str,
        group: str,
        job: str,
        hash_: str,
        experiment: str,
) -> tuple[str, str, str, str]:
    base = (
        f"s3://{_CONFIG_BUCKET}/configdata/confetti/runtime-configs/"
        f"{env}/{group}/{job}/{hash_}/"
    )
    cfg = base + "behavioral_config.yml"
    success = base + "_SUCCESS"
    start = base + (f"_START_{experiment}" if experiment else "_START")
    return base, cfg, success, start


def _wait_for_success(
        aws: AwsCloudStorage,
        bucket: str,
        success_key: str,
        start_key: str,
        timeout: timedelta,
) -> bool:
    if aws.check_for_key(success_key, bucket):
        return True

    if aws.check_for_key(start_key, bucket):
        start = time.time()
        while time.time() - start < timeout.total_seconds():
            if aws.check_for_key(success_key, bucket):
                return True
            logger.info(
                "Going to wait for another round for: %s, key %s",
                bucket,
                success_key,
            )
            time.sleep(300)
    return False


def _upload_runtime_configs(
        aws: AwsCloudStorage,
        runtime_bucket: str,
        runtime_prefix: str,
        rendered: str,
        tpl_dir: str,
        run_vars: dict[str, Any],
        start_key: str,
) -> None:
    cfg_path = runtime_prefix + "behavioral_config.yml"
    logger.info("Writing runtime-config to bucket %s, key %s", runtime_bucket, cfg_path)
    aws.load_string(rendered, key=cfg_path, bucket_name=runtime_bucket, replace=True)
    logger.info(
        "Finished uploading runtime-config to bucket %s, key %s",
        runtime_bucket,
        cfg_path,
    )

    d_bucket, d_prefix = aws._parse_bucket_and_key(tpl_dir, None)
    for key in aws.list_keys(prefix=d_prefix, bucket_name=d_bucket) or []:
        if key.endswith("behavioral_config.yml") or not key.endswith((".yml", ".yaml")):
            continue
        tpl = aws.read_key(key, bucket_name=d_bucket)
        content = _render_template(tpl, run_vars)
        dest_key = runtime_prefix + key.split("/")[-1]
        aws.load_string(content, key=dest_key, bucket_name=runtime_bucket, replace=True)

    aws.load_string("", key=start_key, bucket_name=runtime_bucket, replace=True)


def _prepare_runtime_config(
        group: str,
        job: str,
        run_date: str,
        experiment: str,
        timeout: timedelta,
        return_jar_path: bool = False,
) -> tuple[str, bool] | tuple[str, bool, str]:
    """Render runtime configs and check for previous results.

    The behavioral config template determines the hash used for the runtime
    path. All YAML templates in the same directory, including ``output_config.yml``,
    are rendered with the run date and uploaded under that path. If a ``_SUCCESS``
    marker already exists, the job is skipped. If a ``_START_<experiment>``
    file exists but ``_SUCCESS`` does not, the call waits for completion up to
    ``timeout``. When new configs are uploaded a ``_START_<experiment>`` file
    is created to mark the run.
    """
    env = _resolve_env(TtdEnvFactory.get_from_system().execution_env, experiment)
    tpl_dir = _template_dir(env, group, job, experiment)
    tpl_key = tpl_dir + "behavioral_config.yml"

    aws = AwsCloudStorage()
    rendered, jar_path, hash_, run_vars = _load_render_config(aws, tpl_key, run_date)

    runtime_base, cfg_key, success_key, start_key = _runtime_paths(
        env,
        group,
        job,
        hash_,
        experiment,
    )
    bucket, prefix = aws._parse_bucket_and_key(runtime_base, None)
    cfg_path = prefix + "behavioral_config.yml"
    success_path = prefix + "_SUCCESS"
    start_path = prefix + (f"_START_{experiment}" if experiment else "_START")

    logger.info("Parsed runtime-config bucket %s, key %s", bucket, cfg_path)

    if _wait_for_success(aws, bucket, success_path, start_path, timeout):
        return (runtime_base, True, jar_path) if return_jar_path else (runtime_base, True)

    _upload_runtime_configs(
        aws,
        bucket,
        prefix,
        rendered,
        tpl_dir,
        run_vars,
        start_path,
    )

    return (runtime_base, False, jar_path) if return_jar_path else (runtime_base, False)


def make_confetti_tasks(
        *,
        group_name: str,
        job_name: str,
        experiment_name: str = "",
        run_date: str = "{{ ds }}",
        check_timeout: timedelta = timedelta(hours=2),
) -> Tuple[OpTask, OpTask]:
    """Return (prepare_task, gate_task) for Confetti jobs.

    Both tasks are ``OpTask`` instances wrapping an underlying Airflow
    ``PythonOperator`` and ``ShortCircuitOperator`` so that they can be
    chained with other :class:`BaseTask` objects using ``>>``.
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
        context["ti"].xcom_push(key="runtime_base", value=rb)
        context["ti"].xcom_push(key="skip_job", value=skip)
        context["ti"].xcom_push(key="audienceJarPath", value=jar)

    prep_task = OpTask(op=PythonOperator(
        task_id=f"prepare_confetti_{job_name}",
        python_callable=_prep,
    ))

    def _should_run(**context):
        return not context["ti"].xcom_pull(task_ids=prep_task.task_id, key="skip_job")

    gate_task = OpTask(op=ShortCircuitOperator(
        task_id=f"confetti_should_run_{job_name}",
        python_callable=_should_run,
    ))

    return prep_task, gate_task
