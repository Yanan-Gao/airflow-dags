# from __future__ import annotations
#
# import base64
# import hashlib
# import re
# import time
# from datetime import timedelta
# from typing import Any, Dict, Optional, Sequence, Tuple
#
# from airflow.operators.python import PythonOperator, ShortCircuitOperator
#
# from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
# from ttd.eldorado.aws.emr_job_task import EmrJobTask
# from ttd.tasks.op import OpTask
# from ttd.ttdenv import TtdEnvFactory
#
#
# # ---------------------------------------------------------------------
# # Helper functions (extracted from your old AutoConfiguredEmrJobTask)
# # ---------------------------------------------------------------------
#
# _CONFIG_BUCKET = "thetradedesk-mlplatform-us-east-1"
#
#
# def _sha256_b64(data: str) -> str:
#     return base64.b64encode(hashlib.sha256(data.encode()).digest()).decode()
#
#
# def _render_template(tpl: str, ctx: Dict[str, str]) -> str:
#     rendered = tpl.format(**ctx)
#     unresolved = re.findall(r"{[^{}]+}", rendered)
#     if unresolved:
#         raise ValueError(f"Unresolved variables in template: {unresolved}")
#     return rendered
#
#
# def _resolve_env(env: str, experiment: str) -> str:
#     env = env.lower()
#     if env.startswith("prod"):
#         return "experiment" if experiment else "prod"
#     if not experiment:
#         raise ValueError("experiment_name is required when running in test env")
#     return "test"
#
#
# def _prepare_confetti_runtime(
#     group: str,
#     job: str,
#     run_date: str,
#     experiment: str,
#     timeout: timedelta,
# ) -> tuple[str, bool]:
#     """Return ``(runtime_base, skip)`` – *skip* is True when result.yml exists."""
#     env = _resolve_env(TtdEnvFactory.get_from_system().execution_env, experiment)
#     exp_dir = f"{experiment}/" if experiment else ""
#
#     tpl_key = (
#         f"s3://{_CONFIG_BUCKET}/configdata/confetti/configs/"
#         f"{env}/{exp_dir}{group}/{job}/behavioral_config.yml"
#     )
#     aws = AwsCloudStorage()
#     template = aws.read_key(tpl_key)
#     rendered = _render_template(template, {"date": run_date})
#     hash_ = _sha256_b64(rendered)
#
#     runtime_base = (
#         f"s3://{_CONFIG_BUCKET}/configdata/confetti/runtime-configs/"
#         f"{env}/{group}/{job}/{hash_}/"
#     )
#     cfg_key = runtime_base + "behavioral_config.yml"
#     res_key = runtime_base + "result.yml"
#
#     c_bucket, c_path = aws._parse_bucket_and_key(cfg_key, None)
#     r_bucket, r_path = aws._parse_bucket_and_key(res_key, None)
#
#     # fast path – result already produced
#     if aws.check_for_key(r_path, r_bucket):
#         return runtime_base, True
#
#     # wait if config exists but result not yet ready
#     if aws.check_for_key(c_path, c_bucket):
#         start = time.time()
#         while time.time() - start < timeout.total_seconds():
#             if aws.check_for_key(r_path, r_bucket):
#                 return runtime_base, True
#             time.sleep(300)
#
#     # upload / overwrite runtime config for this run
#     aws.load_string(rendered, key=cfg_key, bucket_name=c_bucket, replace=True)
#     return runtime_base, False
#
#
# # ---------------------------------------------------------------------
# # Public façade
# # ---------------------------------------------------------------------
# class ConfettiEmrJobTask(EmrJobTask):
#     def __init__(self, *, group_name: str, experiment_name: str = "",
#                  run_date: str = "{{ ds }}", check_timeout: timedelta = timedelta(hours=2),
#                  eldorado_config_option_pairs_list: Optional[Sequence[Tuple[str, str]]] = None,
#                  **emr_kwargs):
#
#         # ids
#         job_name = emr_kwargs.get("name") or emr_kwargs.get("task_id") or "emr_job"
#         prep_id  = f"prepare_confetti_config_{job_name}"
#         gate_id  = f"should_run_confetti_{job_name}"
#
#         # helpers --------------------------------------------------------
#         def _prep(**ctx: dict):
#             rb, skip = _prepare_confetti_runtime(
#                 group_name, job_name, ctx["ds"], experiment_name, check_timeout
#             )
#             ctx["ti"].xcom_push(key="runtime_base", value=rb)
#             ctx["ti"].xcom_push(key="skip_job",   value=skip)
#
#         prep_op = PythonOperator(task_id=prep_id,  python_callable=_prep)
#         gate_op = ShortCircuitOperator(
#             task_id=gate_id,
#             python_callable=lambda **c: not c["ti"].xcom_pull(task_ids=prep_id, key="skip_job"),
#         )
#
#         # JVM option -----------------------------------------------------
#         cfg_opt = ("confetti_runtime_config_base_path",
#                    "{{ ti.xcom_pull(task_ids='" + prep_id + "', key='runtime_base') }}")
#
#         user_opts = emr_kwargs.pop("eldorado_config_option_pairs_list", []) or []
#         eld_opts  = list(user_opts) + [cfg_opt]
#         emr_kwargs["eldorado_config_option_pairs_list"] = eld_opts
#
#         # parent ctor ----------------------------------------------------
#         super().__init__(**emr_kwargs)
#
#         # ----------------------------------------------------------------
#         # Guaranteed ordering:
#         # prep  →  gate  →  (add‑step inside the ChainOfTasks)  →  watch
#         # ----------------------------------------------------------------
#         OpTask(op=prep_op) >> OpTask(op=gate_op) >> self.add_job_task
#
#         # the original ‘watch’ task stays downstream of add_job_task
#         self.add_job_task >> self.watch_job_task
#
#         self._confetti_gate = OpTask(op=gate_op)
#
#     def accept(self, visitor):
#         cluster_task = getattr(visitor, "current_cluster", None)
#         if cluster_task:
#             self._confetti_gate >> cluster_task
#         super().accept(visitor)
