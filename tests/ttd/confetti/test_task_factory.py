import sys
import types
import unittest
from datetime import timedelta
from typing import Any, Dict
from unittest.mock import MagicMock, patch, call
import yaml

# provide minimal airflow stubs so imports succeed
fake_airflow: Any = types.ModuleType("airflow")
fake_ops: Any = types.ModuleType("airflow.operators")
fake_py: Any = types.ModuleType("airflow.operators.python")
fake_timetables: Any = types.ModuleType("airflow.timetables")
fake_timetables_base: Any = types.ModuleType("airflow.timetables.base")
fake_timetables_interval: Any = types.ModuleType("airflow.timetables.interval")
fake_security: Any = types.ModuleType("airflow.security")
fake_models: Any = types.ModuleType("airflow.models")
fake_models_dag: Any = types.ModuleType("airflow.models.dag")
fake_settings: Any = types.ModuleType("airflow.settings")
fake_hooks: Any = types.ModuleType("airflow.hooks")
fake_hooks_base: Any = types.ModuleType("airflow.hooks.base")
fake_utils: Any = types.ModuleType("airflow.utils")
fake_utils_trigger: Any = types.ModuleType("airflow.utils.trigger_rule")
fake_utils_state: Any = types.ModuleType("airflow.utils.state")
fake_ops_subdag: Any = types.ModuleType("airflow.operators.subdag")
fake_utils_task_group: Any = types.ModuleType("airflow.utils.task_group")
fake_ttdslack: Any = types.ModuleType("ttd.ttdslack")
fake_pendulum_tz: Any = types.ModuleType("pendulum.tz.timezone")
fake_pendulum_tz.FixedTimezone = type("FixedTimezone", (), {})
fake_pendulum_tz.Timezone = type("Timezone", (), {})


def dummy_slack_cb(*a, **k):
    pass


fake_ttdslack.dag_post_to_slack_callback = dummy_slack_cb


class DummyDAG:

    def __init__(self, *a, **k):
        pass


class DummyTimetable:
    pass


class DummyScheduleInterval:
    pass


class DummyDeltaTimetable:
    pass


class DummyCronTimetable:
    pass


class DummyDagRunInfo:
    pass


class DummyDataInterval:
    pass


class DummyTimeRestriction:
    pass


class DummyBaseHook:

    @staticmethod
    def get_connection(name):
        return types.SimpleNamespace(password="token")


class DummyTaskInstance:
    pass


class DummyDagRun:

    def get_task_instances(self, state=None):
        return []


class DummySubDagOperator:

    def __init__(self, *a, **k):
        pass


class DummyTaskGroup:

    def __init__(self, *a, **k):
        self.prefix_group_id = False

    def add(self, obj):
        pass

    def child_id(self, tid):
        return tid


fake_airflow.DAG = DummyDAG
fake_timetables_base.Timetable = DummyTimetable
fake_models_dag.ScheduleInterval = DummyScheduleInterval
fake_timetables_interval.DeltaDataIntervalTimetable = DummyDeltaTimetable
fake_timetables_interval.CronDataIntervalTimetable = DummyCronTimetable
fake_timetables_base.DagRunInfo = DummyDagRunInfo
fake_timetables_base.DataInterval = DummyDataInterval
fake_timetables_base.TimeRestriction = DummyTimeRestriction
fake_hooks_base.BaseHook = DummyBaseHook
fake_models.TaskInstance = DummyTaskInstance
fake_models.DagRun = DummyDagRun
fake_ops_subdag.SubDagOperator = DummySubDagOperator
fake_utils_task_group.TaskGroup = DummyTaskGroup
fake_utils_trigger.TriggerRule = type("TriggerRule", (), {})
fake_utils_state.TaskInstanceState = type("TaskInstanceState", (), {"FAILED": "failed"})
fake_utils_state.State = type("State", (), {"failed_states": ["failed"]})
fake_security.permissions = types.SimpleNamespace()
fake_settings.TIMEZONE = "UTC"


class _DummyOp:

    def __init__(self, task_id=None, python_callable=None, **_):
        self.task_id = task_id
        self.python_callable = python_callable

    def execute(self, context=None):
        if self.python_callable:
            return self.python_callable(**(context or {}))


fake_py.PythonOperator = _DummyOp
fake_py.ShortCircuitOperator = _DummyOp
fake_ops.python = fake_py
fake_airflow.operators = fake_ops

fake_exc: Any = types.ModuleType("airflow.exceptions")


class DummyAirflowException(Exception):
    pass


fake_exc.AirflowException = DummyAirflowException

fake_s3: Any = types.ModuleType("airflow.providers.amazon.aws.hooks.s3")


class DummyS3Hook:

    def __init__(self, *a, **k):
        pass

    def load_string(self, *a, **k):
        pass

    def load_file_obj(self, *a, **k):
        pass

    def check_for_key(self, *a, **k):
        return False

    def read_key(self, *a, **k):
        return ""

    def parse_s3_url(self, url):
        return ("b", "k")

    def get_conn(self):
        return MagicMock()

    def delete_objects(self, *a, **k):
        pass

    def list_keys(self, *a, **k):
        return []

    def list_prefixes(self, *a, **k):
        return []


fake_s3.S3Hook = DummyS3Hook

sys.modules.setdefault("airflow", fake_airflow)
sys.modules.setdefault("airflow.operators", fake_ops)
sys.modules.setdefault("airflow.operators.python", fake_py)
sys.modules.setdefault("airflow.exceptions", fake_exc)
sys.modules.setdefault("airflow.timetables", fake_timetables)
sys.modules.setdefault("airflow.timetables.base", fake_timetables_base)
sys.modules.setdefault("airflow.timetables.interval", fake_timetables_interval)
sys.modules.setdefault("airflow.security", fake_security)
sys.modules.setdefault("airflow.models", fake_models)
sys.modules.setdefault("airflow.models.dag", fake_models_dag)
sys.modules.setdefault("airflow.settings", fake_settings)
sys.modules.setdefault("airflow.hooks", fake_hooks)
sys.modules.setdefault("airflow.hooks.base", fake_hooks_base)
sys.modules.setdefault("airflow.utils", fake_utils)
sys.modules.setdefault("airflow.utils.trigger_rule", fake_utils_trigger)
sys.modules.setdefault("airflow.utils.state", fake_utils_state)
sys.modules.setdefault("airflow.operators.subdag", fake_ops_subdag)
sys.modules.setdefault("airflow.utils.task_group", fake_utils_task_group)
sys.modules.setdefault("airflow.providers", types.ModuleType("airflow.providers"))
sys.modules.setdefault("airflow.providers.amazon", types.ModuleType("airflow.providers.amazon"))
sys.modules.setdefault("airflow.providers.amazon.aws", types.ModuleType("airflow.providers.amazon.aws"))
sys.modules.setdefault("airflow.providers.amazon.aws.hooks", types.ModuleType("airflow.providers.amazon.aws.hooks"))
sys.modules.setdefault("airflow.providers.amazon.aws.hooks.s3", fake_s3)
sys.modules.setdefault("ttd.ttdslack", fake_ttdslack)
sys.modules.setdefault("pendulum", types.ModuleType("pendulum"))
sys.modules.setdefault("pendulum.tz", types.ModuleType("pendulum.tz"))
sys.modules.setdefault("pendulum.tz.timezone", fake_pendulum_tz)

from ttd.confetti.confetti_task_factory import (  # noqa: E402
    resolve_env,
    _render_template,
    _sha256_b64,
    _inject_audience_jar_path,
    _prepare_runtime_config,
    _copy_s3_prefix,
    make_confetti_tasks,
    make_confetti_post_processing_task,
)
from ttd.tasks.op import OpTask


class ResolveEnvTest(unittest.TestCase):

    def test_prod_without_experiment(self):
        self.assertEqual(resolve_env("prod", ""), "prod")

    def test_prod_with_experiment(self):
        self.assertEqual(resolve_env("prod", "exp"), "experiment")

    def test_non_prod_without_experiment(self):
        self.assertEqual(resolve_env("test", ""), "test")

    def test_non_prod_with_experiment(self):
        self.assertEqual(resolve_env("test", "exp"), "experiment")


class TemplateTest(unittest.TestCase):

    def test_render_and_hash(self):
        tpl = "hello {{ name }}"
        rendered = _render_template(tpl, {"name": "world"})
        self.assertEqual(rendered, "hello world")
        h = _sha256_b64(rendered)
        self.assertEqual(len(h), 44)


class FactoryTest(unittest.TestCase):

    @patch("ttd.confetti.confetti_task_factory.AwsCloudStorage")
    @patch("ttd.confetti.confetti_task_factory.TtdEnvFactory.get_from_system")
    @patch("ttd.confetti.confetti_task_factory._inject_audience_jar_path")
    def test_make_tasks_pushes_xcom(self, mock_inject, mock_get_env, mock_storage):
        mock_get_env.return_value = type("E", (), {"execution_env": "prod"})()
        mock_instance = mock_storage.return_value
        mock_instance.read_key.return_value = "hi {date}"
        mock_instance._parse_bucket_and_key.side_effect = lambda k, b: ("b", k)
        mock_instance.check_for_key.return_value = False
        mock_instance.list_keys.return_value = []
        mock_inject.return_value = "audienceJarPath: bar"

        prep, gate = make_confetti_tasks(group_name="g", job_name="j", run_date="2020-01-01")
        ctx: Dict[str, Any] = {"ds": "2020-01-01", "ti": MagicMock()}
        prep.first_airflow_op().execute(context=ctx)
        ctx["ti"].xcom_pull.return_value = False
        ctx["ti"].xcom_push.assert_any_call(key="confetti_runtime_config_base_path", value=unittest.mock.ANY)
        ctx["ti"].xcom_push.assert_any_call(key="skip_job", value=False)
        ctx["ti"].xcom_push.assert_any_call(key="audienceJarPath", value=unittest.mock.ANY)
        should_run = gate.first_airflow_op().python_callable(ti=ctx["ti"])
        self.assertTrue(should_run)

    @patch("ttd.confetti.confetti_task_factory.AwsCloudStorage")
    @patch("ttd.confetti.confetti_task_factory.TtdEnvFactory.get_from_system")
    @patch("ttd.confetti.confetti_task_factory._inject_audience_jar_path")
    def test_prepare_renders_output_config(self, mock_inject, mock_get_env, mock_storage):
        mock_get_env.return_value = type("E", (), {"execution_env": "prod"})()
        instance = mock_storage.return_value
        def _parse(k, b=None):
            if b is None and str(k).startswith("s3://"):
                no_scheme = str(k)[5:]
                bucket, key_path = no_scheme.split("/", 1)
                return bucket, key_path
            return b or "b", k

        instance._parse_bucket_and_key.side_effect = _parse
        instance.check_for_key.return_value = False
        instance.list_keys.return_value = [
            "p/identity_config.yml",
            "p/output_config.yml",
        ]

        def _read(key, bucket_name=None):
            return "audienceJarBranch: master\naudienceJarVersion: 1" if key.endswith("identity_config.yml") else "hi {date}"

        instance.read_key.side_effect = _read
        mock_inject.return_value = "audienceJarPath: bar"

        _prepare_runtime_config("g", "j", "2020-01-01", "", timedelta(seconds=0))

        keys = [c.kwargs.get("key") for c in instance.load_string.call_args_list]
        # first_call_content = instance.load_string.call_args_list[0].args[0]
        # self.assertIn("runDate: '2020-01-01'", first_call_content)
        self.assertTrue(any(str(k).endswith("output_config.yml") for k in keys))
        out_call = next(c for c in instance.load_string.call_args_list if str(c.kwargs.get("key")).endswith("output_config.yml"))
        self.assertFalse(str(out_call.kwargs.get("key")).startswith("s3://"))
        self.assertTrue(any(str(k).endswith("_START") for k in keys))

    @patch("ttd.confetti.confetti_task_factory.AwsCloudStorage")
    @patch("ttd.confetti.confetti_task_factory.TtdEnvFactory.get_from_system")
    @patch("ttd.confetti.confetti_task_factory._inject_audience_jar_path")
    def test_wait_timeout_fails(self, mock_inject, mock_get_env, mock_storage):
        mock_get_env.return_value = type("E", (), {"execution_env": "prod"})()
        instance = mock_storage.return_value

        instance._parse_bucket_and_key.side_effect = lambda k, b=None: ("b", k)
        instance.list_keys.return_value = []
        instance.read_key.return_value = "audienceJarBranch: master\naudienceJarVersion: 1"

        def _check(key, bucket_name=None):
            return str(key).endswith("_START")

        instance.check_for_key.side_effect = _check
        mock_inject.return_value = "audienceJarPath: bar"

        with self.assertRaises(TimeoutError):
            _prepare_runtime_config("g", "j", "2020-01-01", "", timedelta(seconds=0))

    @patch("ttd.confetti.confetti_task_factory._wait_for_existing_run")
    @patch("ttd.confetti.confetti_task_factory.AwsCloudStorage")
    @patch("ttd.confetti.confetti_task_factory.TtdEnvFactory.get_from_system")
    @patch("ttd.confetti.confetti_task_factory._inject_audience_jar_path")
    def test_force_run_skips_wait(
        self, mock_inject, mock_get_env, mock_storage, mock_wait
    ):
        mock_get_env.return_value = type("E", (), {"execution_env": "prod"})()
        instance = mock_storage.return_value
        instance._parse_bucket_and_key.side_effect = lambda k, b=None: ("b", k)
        instance.list_keys.return_value = []

        def _read(key, bucket_name=None):
            if str(key).endswith("execution_config.yml"):
                return "forceRun: true"
            return "audienceJarBranch: master\naudienceJarVersion: 1"

        instance.read_key.side_effect = _read
        mock_wait.return_value = True
        mock_inject.return_value = "audienceJarPath: bar"

        _, skip, _ = _prepare_runtime_config(
            "g", "j", "2020-01-01", "", timedelta(seconds=0), return_jar_path=True
        )

        self.assertFalse(skip)
        mock_wait.assert_not_called()


class AudienceJarPathTest(unittest.TestCase):

    def test_inject_master_latest(self):
        mock_aws = MagicMock()
        mock_aws.read_key.side_effect = ["123-abc\n456-def"]
        tpl = "audienceJarBranch: master\naudienceJarVersion: latest\nother: v"
        rendered = _inject_audience_jar_path(tpl, mock_aws)
        data = yaml.safe_load(rendered)
        self.assertEqual(
            data["audienceJarPath"],
            "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/snapshots/master/123-abc/audience.jar",
        )
        self.assertNotIn("audienceJarBranch", data)
        self.assertNotIn("audienceJarVersion", data)

    def test_inject_feature_explicit(self):
        mock_aws = MagicMock()
        tpl = ("audienceJarBranch: feature\n"
               "audienceJarVersion: 1.2.3\n"
               "foo: bar\n")
        rendered = _inject_audience_jar_path(tpl, mock_aws)
        data = yaml.safe_load(rendered)
        self.assertEqual(
            data["audienceJarPath"],
            "s3://thetradedesk-mlplatform-us-east-1/libs/audience/jars/mergerequests/feature/1.2.3/audience.jar",
        )
        self.assertNotIn("audienceJarBranch", data)
        self.assertNotIn("audienceJarVersion", data)

    def test_missing_keys_raise(self):
        with self.assertRaisesRegex(ValueError, "audienceJarBranch"):
            _inject_audience_jar_path("audienceJarVersion: 1", MagicMock())
        with self.assertRaisesRegex(ValueError, "audienceJarVersion"):
            _inject_audience_jar_path("audienceJarBranch: m", MagicMock())

    def test_empty_current_file(self):
        mock_aws = MagicMock()
        mock_aws.read_key.return_value = ""
        tpl = "audienceJarBranch: master\naudienceJarVersion: latest"
        with self.assertRaisesRegex(ValueError, "No version"):
            _inject_audience_jar_path(tpl, mock_aws)


class FastPassCopyTest(unittest.TestCase):

    def _parse(self, k, b=None):
        if b is None and str(k).startswith("s3://"):
            no_scheme = str(k)[5:]
            bucket, key_path = no_scheme.split("/", 1)
            return bucket, key_path
        return b or "b", k

    @patch("ttd.confetti.confetti_task_factory.AwsCloudStorage")
    @patch("ttd.confetti.confetti_task_factory.TtdEnvFactory.get_from_system")
    def test_should_run_copies_output_on_fast_pass(self, mock_get_env, mock_storage):
        mock_get_env.return_value = type("E", (), {"execution_env": "prod"})()
        instance = mock_storage.return_value
        instance._parse_bucket_and_key.side_effect = self._parse
        instance.list_keys.return_value = ["p/a"]
        instance.copy_file.return_value = type("S", (), {"get": lambda self: None})()

        def _read(key, bucket_name=None):
            if "runtime-configs" in str(key):
                return "out_path: s3://b/p/"
            return "out_path: s3://b/q/"

        instance.read_key.side_effect = _read

        prep, gate = make_confetti_tasks(group_name="g", job_name="j", run_date="2020-01-01")
        ti = MagicMock()
        ti.xcom_pull.side_effect = lambda task_ids, key: {
            "skip_job": True,
            "confetti_runtime_config_base_path": "s3://b/runtime-configs/run1/",
        }[key]
        should_run = gate.first_airflow_op().python_callable(ti=ti, ds="2020-01-01")
        self.assertFalse(should_run)
        instance.copy_file.assert_called()

    @patch("ttd.confetti.confetti_task_factory.AwsCloudStorage")
    @patch("ttd.confetti.confetti_task_factory.TtdEnvFactory.get_from_system")
    def test_should_run_no_copy_when_same_path(self, mock_get_env, mock_storage):
        mock_get_env.return_value = type("E", (), {"execution_env": "prod"})()
        instance = mock_storage.return_value
        instance._parse_bucket_and_key.side_effect = self._parse
        instance.list_keys.return_value = ["p/a"]
        instance.copy_file.return_value = type("S", (), {"get": lambda self: None})()
        instance.read_key.return_value = "out_path: s3://b/p/"

        prep, gate = make_confetti_tasks(group_name="g", job_name="j", run_date="2020-01-01")
        ti = MagicMock()
        ti.xcom_pull.side_effect = lambda task_ids, key: {
            "skip_job": True,
            "confetti_runtime_config_base_path": "s3://b/runtime-configs/run1/",
        }[key]
        should_run = gate.first_airflow_op().python_callable(ti=ti, ds="2020-01-01")
        self.assertFalse(should_run)
        instance.copy_file.assert_not_called()

    def test_copy_handles_single_file(self):
        aws = MagicMock()
        aws._parse_bucket_and_key.side_effect = self._parse
        aws.list_keys.return_value = []
        aws.check_for_key.return_value = True
        aws.copy_file.return_value = type("S", (), {"get": lambda self: None})()

        _copy_s3_prefix(
            aws,
            "s3://b/p/file.txt",
            "s3://b/q/file.txt",
        )

        aws.copy_file.assert_called_with(
            src_key="p/file.txt",
            src_bucket_name="b",
            dst_key="q/file.txt",
            dst_bucket_name="b",
        )

    def test_copy_handles_prefix(self):
        aws = MagicMock()
        aws._parse_bucket_and_key.side_effect = self._parse
        aws.list_keys.return_value = ["p/a", "p/b"]
        aws.copy_file.return_value = type("S", (), {"get": lambda self: None})()

        _copy_s3_prefix(
            aws,
            "s3://b/p/",
            "s3://b/q/",
        )

        aws.copy_file.assert_has_calls([
            call(
                src_key="p/a",
                src_bucket_name="b",
                dst_key="q/a",
                dst_bucket_name="b",
            ),
            call(
                src_key="p/b",
                src_bucket_name="b",
                dst_key="q/b",
                dst_bucket_name="b",
            ),
        ], any_order=True)


class CleanupTaskTest(unittest.TestCase):

    @patch("ttd.confetti.confetti_task_factory._archive_runtime_path")
    @patch("ttd.confetti.confetti_task_factory.AwsCloudStorage")
    def test_cleanup_archives_on_failure(self, mock_storage, mock_archive):
        prep = OpTask(op=_DummyOp(task_id="prep"))
        cleanup = make_confetti_post_processing_task(
            job_name="j", prep_task=prep, cluster_id="cid", task_id_prefix="p_"
        )

        ti = MagicMock()
        ti.xcom_pull.return_value = "s3://b/run/"
        dag_run = MagicMock()
        dag_run.get_task_instance.return_value = type("T", (), {"state": "failed"})()
        task = MagicMock(upstream_task_ids={"x"})

        cleanup.first_airflow_op().python_callable(ti=ti, dag_run=dag_run, task=task, cluster_id="cid")
        mock_archive.assert_called_once()
        mock_storage.return_value.load_string.assert_not_called()

    @patch("ttd.confetti.confetti_task_factory._archive_runtime_path")
    @patch("ttd.confetti.confetti_task_factory.AwsCloudStorage")
    def test_cleanup_writes_success(self, mock_storage, mock_archive):
        prep = OpTask(op=_DummyOp(task_id="prep"))
        cleanup = make_confetti_post_processing_task(
            job_name="j", prep_task=prep, cluster_id="cid", task_id_prefix="p_"
        )

        ti = MagicMock()
        ti.xcom_pull.return_value = "s3://b/run/"
        dag_run = MagicMock()
        dag_run.get_task_instance.return_value = type("T", (), {"state": "success"})()
        task = MagicMock(upstream_task_ids={"x"})
        instance = mock_storage.return_value
        instance._parse_bucket_and_key.return_value = ("b", "run/")
        instance.read_key.return_value = "exp"

        cleanup.first_airflow_op().python_callable(ti=ti, dag_run=dag_run, task=task, cluster_id="cid")

        mock_archive.assert_not_called()
        instance.load_string.assert_called_once()
