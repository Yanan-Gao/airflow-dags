import sys
import types
import unittest
from unittest.mock import MagicMock, patch
from datetime import timedelta
import yaml

# provide minimal airflow stubs so imports succeed
fake_airflow = types.ModuleType("airflow")
fake_ops = types.ModuleType("airflow.operators")
fake_py = types.ModuleType("airflow.operators.python")
fake_timetables = types.ModuleType("airflow.timetables")
fake_timetables_base = types.ModuleType("airflow.timetables.base")
fake_timetables_interval = types.ModuleType("airflow.timetables.interval")
fake_security = types.ModuleType("airflow.security")
fake_models = types.ModuleType("airflow.models")
fake_models_dag = types.ModuleType("airflow.models.dag")
fake_settings = types.ModuleType("airflow.settings")
fake_hooks = types.ModuleType("airflow.hooks")
fake_hooks_base = types.ModuleType("airflow.hooks.base")
fake_utils = types.ModuleType("airflow.utils")
fake_utils_trigger = types.ModuleType("airflow.utils.trigger_rule")
fake_utils_state = types.ModuleType("airflow.utils.state")
fake_ops_subdag = types.ModuleType("airflow.operators.subdag")
fake_utils_task_group = types.ModuleType("airflow.utils.task_group")
fake_ttdslack = types.ModuleType("ttd.ttdslack")
fake_pendulum_tz = types.ModuleType("pendulum.tz.timezone")
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

fake_exc = types.ModuleType("airflow.exceptions")


class DummyAirflowException(Exception):
    pass


fake_exc.AirflowException = DummyAirflowException

fake_s3 = types.ModuleType("airflow.providers.amazon.aws.hooks.s3")


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
    _resolve_env,
    _render_template,
    _sha256_b64,
    _inject_audience_jar_path,
    _prepare_runtime_config,
    make_confetti_tasks,
)


class ResolveEnvTest(unittest.TestCase):

    def test_prod_without_experiment(self):
        self.assertEqual(_resolve_env("prod", ""), "prod")

    def test_prod_with_experiment(self):
        self.assertEqual(_resolve_env("prod", "exp"), "experiment")

    def test_test_env_requires_experiment(self):
        with self.assertRaises(ValueError):
            _resolve_env("test", "")


class TemplateTest(unittest.TestCase):

    def test_render_and_hash(self):
        tpl = "hello {{ name }}"
        rendered = _render_template(tpl, {"name": "world"})
        self.assertEqual(rendered, "hello world")
        h = _sha256_b64(rendered)
        self.assertEqual(len(h), 43)


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
        ctx = {"ds": "2020-01-01", "ti": MagicMock()}
        prep.first_airflow_op().execute(context=ctx)
        ctx["ti"].xcom_pull.return_value = False
        ctx["ti"].xcom_push.assert_any_call(key="runtime_base", value=unittest.mock.ANY)
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
        instance._parse_bucket_and_key.side_effect = lambda k, b: ("b", k)
        instance.check_for_key.return_value = False
        instance.list_keys.return_value = [
            "p/behavioral_config.yml",
            "p/output_config.yml",
        ]

        def _read(key, bucket_name=None):
            return "audienceJarBranch: master\naudienceJarVersion: 1" if key.endswith("behavioral_config.yml") else "hi {date}"

        instance.read_key.side_effect = _read
        mock_inject.return_value = "audienceJarPath: bar"

        _prepare_runtime_config("g", "j", "2020-01-01", "", timedelta(seconds=0))

        keys = [c.kwargs.get("key") for c in instance.load_string.call_args_list]
        self.assertTrue(any(str(k).endswith("output_config.yml") for k in keys))


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
