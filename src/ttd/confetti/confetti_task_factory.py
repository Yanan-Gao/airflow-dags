from __future__ import annotations
from datetime import datetime, date
from typing import Any, Dict
import logging
from datetime import timedelta
import base64
import hashlib
import re
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
    exp_dir = f"{experiment}/" if experiment else ""
    tpl_dir = (
        f"s3://{_CONFIG_BUCKET}/configdata/confetti/configs/"
        f"{env}/{exp_dir}{group}/{job}/"
    )
    tpl_key = tpl_dir + "behavioral_config.yml"

    aws = AwsCloudStorage()
    template = aws.read_key(tpl_key)
    run_level_variables = _collect_job_run_level_variables(run_date=run_date)
    rendered = _render_template(template, run_level_variables)
    rendered = _inject_audience_jar_path(rendered, aws)
    jar_path = yaml.safe_load(rendered)["audienceJarPath"]
    hash_ = _sha256_b64(rendered)

    runtime_base = (
        f"s3://{_CONFIG_BUCKET}/configdata/confetti/runtime-configs/"
        f"{env}/{group}/{job}/{hash_}/"
    )
    cfg_key = runtime_base + "behavioral_config.yml"
    success_key = runtime_base + "_SUCCESS"
    start_key = runtime_base + (f"_START_{experiment}" if experiment else "_START")

    c_bucket, c_path = aws._parse_bucket_and_key(cfg_key, None)
    s_bucket, s_path = aws._parse_bucket_and_key(success_key, None)
    st_bucket, st_path = aws._parse_bucket_and_key(start_key, None)

    logger.info("Parsed runtime-config bucket %s, key %s", c_bucket, c_path)

    # fast path
    if aws.check_for_key(s_path, s_bucket):
        return (runtime_base, True, jar_path) if return_jar_path else (runtime_base, True)

    # wait if another run has started the job but not finished
    if aws.check_for_key(st_path, st_bucket):
        start = time.time()
        while time.time() - start < timeout.total_seconds():
            if aws.check_for_key(s_path, s_bucket):
                return (runtime_base, True, jar_path) if return_jar_path else (runtime_base, True)
            logger.info("Going to wait for another round for: %s, key %s", s_bucket, s_path)
            time.sleep(300)

    logger.info("Writing runtime-config to bucket %s, key %s", c_bucket, c_path)

    aws.load_string(rendered, key=c_path, bucket_name=c_bucket, replace=True)

    logger.info("Finished uploading runtime-config to bucket %s, key %s", c_bucket, c_path)

    # upload the rendered behavioral config
    aws.load_string(rendered, key=cfg_key, bucket_name=c_bucket, replace=True)

    # render and upload other yaml templates in the same directory
    d_bucket, d_prefix = aws._parse_bucket_and_key(tpl_dir, None)
    for key in aws.list_keys(prefix=d_prefix, bucket_name=d_bucket) or []:
        if key.endswith("behavioral_config.yml") or not key.endswith((".yml", ".yaml")):
            continue
        tpl = aws.read_key(key, bucket_name=d_bucket)
        content = _render_template(tpl, run_level_variables)
        dest_key = runtime_base + key.split("/")[-1]
        dest_bucket, _ = aws._parse_bucket_and_key(dest_key, None)
        aws.load_string(content, key=dest_key, bucket_name=dest_bucket, replace=True)

    b_start, _ = aws._parse_bucket_and_key(start_key, None)
    aws.load_string("", key=start_key, bucket_name=b_start, replace=True)

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
        context["ti"].xcom_push(key="confetti_runtime_config_base_path", value=rb)
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
