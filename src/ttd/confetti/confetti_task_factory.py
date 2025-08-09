from __future__ import annotations
from datetime import datetime, date
from typing import Any, Dict, TYPE_CHECKING
import logging
from datetime import timedelta
import base64
import hashlib
import time
# import os
from typing import Tuple

if TYPE_CHECKING:
    from airflow.utils.context import Context
import yaml

from airflow.operators.python import PythonOperator, BranchPythonOperator

from ttd.tasks.op import OpTask

from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ttdenv import TtdEnvFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_CONFIG_BUCKET = "thetradedesk-mlplatform-us-east-1"


def _sha256_b64(data: str) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(data.encode("utf-8")).digest()).decode()


def _render_template(tpl: str, run_level_variables: Dict[str, str]) -> str:
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


def resolve_env(env: str, experiment: str | None) -> str:
    """Return the Confetti environment name for ``env`` and ``experiment``."""

    env = (env or "").lower()
    if env in ("prod", "production"):
        return "experiment" if experiment else "prod"
    return "experiment" if experiment else "test"


def _template_dir(env: str, experiment: str | None, group: str, job: str) -> str:
    exp_dir = f"{experiment}/" if experiment else ""
    return f"configdata/confetti/configs/{env}/{exp_dir}{group}/{job}/"


def _render_identity_config(aws: AwsCloudStorage, tpl_key: str, run_vars: dict[str, Any]) -> tuple[str, str]:
    template = aws.read_key(tpl_key, bucket_name=_CONFIG_BUCKET)
    rendered = _render_template(template, run_vars)
    rendered = _inject_audience_jar_path(rendered, aws)
    # rendered = _inject_run_date(rendered, run_vars.get("date"))
    jar_path = yaml.safe_load(rendered)["audienceJarPath"]
    return rendered, jar_path


def _runtime_paths(env: str, group: str, job: str, hash_: str, experiment: str | None) -> tuple[str, str, str, str]:
    base_key = f"configdata/confetti/runtime-configs/{env}/{group}/{job}/{hash_}/"
    runtime_base = f"s3://{_CONFIG_BUCKET}/{base_key}"
    cfg_key = base_key + "identity_config.yml"
    success_key = base_key + "_SUCCESS"
    start_key = base_key + "_START"
    return runtime_base, cfg_key, success_key, start_key


def _success_exists(aws: AwsCloudStorage, success_key: str) -> bool:
    return aws.check_for_key(success_key, _CONFIG_BUCKET)


def _wait_for_start_and_success(aws: AwsCloudStorage, start_key: str, success_key: str, timeout: timedelta) -> bool:
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
    raise TimeoutError(f"Timed out waiting for Confetti job success key {success_key}")


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
        if key.endswith("identity_config.yml") or not key.endswith((".yml", ".yaml")):
            continue
        tpl = aws.read_key(key, bucket_name=_CONFIG_BUCKET)
        content = _render_template(tpl, run_vars)
        dest_key = runtime_base_key + key.split("/")[-1]
        logger.info("Uploaded runtime-config to bucket %s, key %s", _CONFIG_BUCKET, dest_key)
        aws.load_string(content, key=dest_key, bucket_name=_CONFIG_BUCKET, replace=True)


def _load_execution_config(
    aws: AwsCloudStorage,
    tpl_dir: str,
    run_vars: dict[str, Any],
) -> dict[str, Any]:
    """Load optional execution configuration for a Confetti job."""

    exec_key = tpl_dir + "execution_config.yml"
    try:
        tpl = aws.read_key(exec_key, bucket_name=_CONFIG_BUCKET)
    except Exception:
        return {}

    rendered = _render_template(tpl, run_vars)
    try:
        data = yaml.safe_load(rendered) or {}
    except Exception as exc:  # pragma: no cover - malformed YAML
        raise ValueError(f"Failed to parse execution config: {exc}") from exc

    if not isinstance(data, dict):  # pragma: no cover - unexpected structure
        logger.warning("execution_config.yml is not a mapping, ignoring")
        return {}

    return data


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
        dst_prefix = dst_prefix.rstrip("/")
        src_prefix_no_slash = src_prefix.rstrip("/")
        for key in keys:
            rel = key[len(src_prefix_no_slash) + 1:] if key.startswith(src_prefix_no_slash + "/") else key[len(src_prefix_no_slash):]
            target_key = f"{dst_prefix}/{rel}" if rel else dst_prefix
            aws.copy_file(
                src_key=key,
                src_bucket_name=src_bucket,
                dst_key=target_key,
                dst_bucket_name=dst_bucket,
            ).get()


def _archive_runtime_path(aws: AwsCloudStorage, runtime_base: str) -> None:
    """Move a runtime config directory to the archive location.

    ``runtime_base`` must be a full S3 URI ending with ``/``. The
    contents are copied to ``runtime-configs-archive`` with a UTC
    timestamp and then removed from the original location.
    """

    bucket, prefix = aws._parse_bucket_and_key(runtime_base, None)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    archive_prefix = (prefix.replace("runtime-configs/", "runtime-configs-archive/", 1).rstrip("/") + f"/{timestamp}/")

    logger.info(f"Copying from s3://{bucket}/{prefix} to s3://{bucket}/{archive_prefix}")

    _copy_s3_prefix(
        aws,
        f"s3://{bucket}/{prefix}",
        f"s3://{bucket}/{archive_prefix}",
    )
    logger.info(f"Copied from s3://{bucket}/{prefix} to s3://{bucket}/{archive_prefix}")

    logger.info(f"Removing s3://{bucket}/{prefix}")
    aws.remove_objects(bucket_name=bucket, prefix=prefix)
    logger.info(f"Removed s3://{bucket}/{prefix}")


def make_confetti_post_processing_task(
    job_name: str,
    *,
    prep_task: OpTask,
    cluster_id: str,
    task_id_prefix: str = "",
) -> OpTask:
    """Return an ``OpTask`` that cleans up runtime configs on failure and
    writes a ``_SUCCESS`` marker on success."""

    def _handle_completion(cluster_id: str, **context: Any) -> None:
        from airflow.utils.state import State
        ti = context["ti"]
        # noop for skipped.
        skipped = ti.xcom_pull(task_ids=prep_task.task_id, key="skip_job")
        if skipped:
            return

        runtime_base = ti.xcom_pull(task_ids=prep_task.task_id, key="confetti_runtime_config_base_path")
        aws = AwsCloudStorage()

        dag_run = context.get("dag_run")
        task = context.get("task")
        failed = False
        if dag_run and task:
            for tid in task.upstream_task_ids:
                ti_state = dag_run.get_task_instance(tid).state
                if ti_state in State.failed_states:
                    failed = True
                    break

        if failed:
            _archive_runtime_path(aws, runtime_base)
            return

        experiment_name = ti.xcom_pull(task_ids=prep_task.task_id, key="confetti_experiment_name") or ""
        bucket, prefix = aws._parse_bucket_and_key(runtime_base, None)

        success_key = prefix.rstrip("/") + "/_SUCCESS"
        content = yaml.safe_dump(
            {
                "experimentName": experiment_name,
                "clusterId": cluster_id
            },
            sort_keys=True,
        )
        aws.load_string(content, key=success_key, bucket_name=bucket, replace=True)

    return OpTask(
        op=PythonOperator(
            task_id=f"{task_id_prefix}confetti_post_processing_{job_name}",
            python_callable=_handle_completion,
            op_kwargs={"cluster_id": cluster_id},
            trigger_rule="none_failed_min_one_success",
        )
    )


def _prepare_runtime_config(
    group: str,
    job: str,
    run_date: str,
    experiment: str | None,
    timeout: timedelta,
) -> tuple[str, bool, str]:
    """Render runtime configs and check for previous results."""
    env = resolve_env(TtdEnvFactory.get_from_system().execution_env, experiment)
    tpl_dir = _template_dir(env, experiment, group, job)
    tpl_key = tpl_dir + "identity_config.yml"

    aws = AwsCloudStorage()
    run_vars = _collect_job_run_level_variables(run_date=run_date)
    rendered, jar_path = _render_identity_config(aws, tpl_key, run_vars)
    exec_cfg = _load_execution_config(aws, tpl_dir, run_vars)
    hash_ = _sha256_b64(rendered)

    runtime_base, cfg_key, success_key, start_key = _runtime_paths(env, group, job, hash_, experiment)

    runtime_base_key = cfg_key.rsplit("/", 1)[0] + "/"

    force_run = bool(exec_cfg.get("forceRun"))

    if not force_run and _wait_for_existing_run(aws, success_key, start_key, timeout):
        return (runtime_base, True, jar_path)

    aws.load_string(rendered, key=cfg_key, bucket_name=_CONFIG_BUCKET, replace=True)
    logger.info("Uploaded runtime-config to bucket %s, key %s", _CONFIG_BUCKET, cfg_key)

    _upload_additional_configs(aws, tpl_dir, runtime_base_key, run_vars)

    return (runtime_base, False, jar_path)


def make_confetti_tasks(
    *,
    group_name: str,
    job_name: str,
    experiment_name: str = "",
    run_date: str = "{{ ds }}",
    check_timeout: timedelta = timedelta(hours=2),
    task_id_prefix: str = "",
) -> Tuple[OpTask, OpTask, OpTask]:
    """Return ``(prepare_task, run_task, skip_task)`` for Confetti jobs.

    ``prepare_task`` is a ``BranchPythonOperator`` that sets up runtime
    configuration and branches to either ``run_task`` or ``skip_task``.
    ``run_task`` writes the ``_START`` marker and ``skip_task`` performs the
    fast-pass copy used when the job is skipped. Both are ``PythonOperator``
    instances that can be chained with ``>>``.

    ``task_id_prefix`` can be used to avoid duplicate task IDs when a single
    DAG instantiates multiple Confetti tasks for the same ``job_name``.
    """

    def _prep_and_branch(**context):
        rb, skip, jar = _prepare_runtime_config(
            group_name,
            job_name,
            context.get("ds", run_date),
            experiment_name,
            check_timeout,
        )
        ti = context["ti"]
        ti.xcom_push(key="confetti_runtime_config_base_path", value=rb)
        ti.xcom_push(key="skip_job", value=skip)
        ti.xcom_push(key="audienceJarPath", value=jar)
        ti.xcom_push(key="confetti_experiment_name", value=experiment_name or "")
        return skip_task.task_id if skip else run_task.task_id

    prep_task = OpTask(op=BranchPythonOperator(
        task_id=f"{task_id_prefix}confetti_prepare_{job_name}",
        python_callable=_prep_and_branch,
    ))

    def _mark_start(**context: Any) -> None:
        ti = context["ti"]
        runtime_base = ti.xcom_pull(task_ids=prep_task.task_id, key="confetti_runtime_config_base_path")
        experiment = ti.xcom_pull(task_ids=prep_task.task_id, key="confetti_experiment_name") or ""
        base_key = runtime_base.replace(f"s3://{_CONFIG_BUCKET}/", "")
        start_key = base_key.rstrip("/") + "/_START"
        AwsCloudStorage().load_string(experiment, key=start_key, bucket_name=_CONFIG_BUCKET, replace=True)

    run_task = OpTask(op=PythonOperator(
        task_id=f"{task_id_prefix}confetti_run_{job_name}",
        python_callable=_mark_start,
    ))

    def _fast_pass_copy(**context: "Context") -> None:
        """Copy prior outputs when skipping a run.

        ``PythonOperator`` passes the Airflow context as keyword arguments, hence
        the ``**context`` parameter.
        """

        ti = context["ti"]
        runtime_base = ti.xcom_pull(task_ids=prep_task.task_id, key="confetti_runtime_config_base_path")
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

    skip_task = OpTask(op=PythonOperator(
        task_id=f"{task_id_prefix}confetti_skip_{job_name}",
        python_callable=_fast_pass_copy,
    ))

    return prep_task, run_task, skip_task
