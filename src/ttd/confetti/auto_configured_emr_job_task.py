from __future__ import annotations

import base64
import hashlib
import re
import time
from datetime import timedelta
from typing import Any, Dict, Optional

from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.tasks.chain import ChainOfTasks
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnv, TtdEnvFactory


class AutoConfiguredEmrJobTask(ChainOfTasks):
    """Wrap :class:`EmrJobTask` with automatic configuration logic.

    This task reads a configuration template from S3, renders variables
    like ``{date}``, stores a hashed runtime copy under
    ``configdata/confetti/runtime-configs`` and runs the underlying
    EMR job only when required.
    """

    _CONFIG_BUCKET = "thetradedesk-mlplatform-us-east-1"

    def __init__(
        self,
        group_name: str,
        job_name: str,
        name: str,
        class_name: str,
        experiment_name: str = "",
        run_date: str = "{{ ds }}",
        env: Optional[TtdEnv] = None,
        check_timeout: timedelta = timedelta(hours=2),
        emr_job_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.group_name = group_name
        self.job_name = job_name
        self.experiment_name = experiment_name
        self.run_date = run_date
        self.env = env or TtdEnvFactory.get_from_system()
        self.check_timeout = check_timeout
        self.emr_job_kwargs = emr_job_kwargs or {}

        self._config_task = OpTask()
        self._emr_task = EmrJobTask(name=name, class_name=class_name, **self.emr_job_kwargs)

        super().__init__(task_id=name, tasks=[self._config_task, self._emr_task])

        self._init_config_task()

    # ------------------------------------------------------------------
    def _init_config_task(self) -> None:
        self._config_task.set_op(
            PythonOperator(
                task_id=f"prepare_confetti_config_{self.job_name}",
                python_callable=self._config_callable,
            )
        )

    # ------------------------------------------------------------------
    def _build_config_path(self) -> str:
        exp_dir = f"{self.experiment_name}/" if self.experiment_name else ""
        return (
            f"s3://{self._CONFIG_BUCKET}/configdata/confetti/configs/"
            f"{self.env.execution_env}/"
            f"{exp_dir}{self.group_name}/{self.job_name}/behavioral_config.yml"
        )

    def _render_template(self, content: str, context: Dict[str, Any]) -> str:
        rendered = content.format(**context)
        unresolved = re.findall(r"{[^{}]+}", rendered)
        if unresolved:
            raise ValueError(f"Unresolved variables in template: {unresolved}")
        return rendered

    def _sha256_b64(self, content: str) -> str:
        digest = hashlib.sha256(content.encode("utf-8")).digest()
        return base64.b64encode(digest).decode("utf-8")

    def _build_runtime_base(self, config_hash: str) -> str:
        return (
            f"s3://{self._CONFIG_BUCKET}/configdata/confetti/runtime-configs/"
            f"{self.env.execution_env}/{self.group_name}/{self.job_name}/{config_hash}/"
        )

    def _result_ready_or_wait(self, aws: AwsCloudStorage, runtime_base: str) -> bool:
        """Check for ``result.yml`` when a runtime config already exists.

        The method waits up to ``check_timeout`` (polling every five minutes)
        when ``behavioral_config.yml`` has been uploaded previously.  It returns
        ``True`` when ``result.yml`` is found, otherwise ``False``.
        """

        config_key = runtime_base + "behavioral_config.yml"
        c_bucket, c_key = aws._parse_bucket_and_key(config_key, None)
        if not aws.check_for_key(c_key, c_bucket):
            return False

        result_key = runtime_base + "result.yml"
        r_bucket, r_key = aws._parse_bucket_and_key(result_key, None)
        if aws.check_for_key(r_key, r_bucket):
            return True

        start = time.time()
        while time.time() - start < self.check_timeout.total_seconds():
            if aws.check_for_key(r_key, r_bucket):
                return True
            time.sleep(300)
        return False

    # ------------------------------------------------------------------
    def _config_callable(self, **context) -> None:
        aws = AwsCloudStorage()
        config_path = self._build_config_path()
        template = aws.read_key(config_path)

        run_date = context.get("ds", self.run_date)
        rendered = self._render_template(template, {"date": run_date})

        config_hash = self._sha256_b64(rendered)
        runtime_base = self._build_runtime_base(config_hash)
        config_runtime_key = runtime_base + "behavioral_config.yml"

        # provide runtime path to the underlying EMR task
        self._emr_task.eldorado_config_option_pairs_list.append(
            ("confetti_runtime_config_path", runtime_base)
        )

        # if a previous run exists, possibly skip or wait for its result
        if self._result_ready_or_wait(aws, runtime_base):
            raise AirflowSkipException("Result already present")

        bucket, key = aws._parse_bucket_and_key(config_runtime_key, None)
        if not aws.check_for_key(key, bucket):
            aws.load_string(rendered, key=config_runtime_key, bucket_name=None, replace=True)
        else:
            # config exists but no result after waiting, rewrite to mark this run
            aws.load_string(rendered, key=config_runtime_key, bucket_name=None, replace=True)



