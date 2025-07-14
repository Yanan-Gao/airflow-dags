import sys
import types
import unittest
from unittest.mock import MagicMock, patch

# provide minimal airflow stubs so imports succeed
fake_airflow = types.ModuleType("airflow")
fake_ops = types.ModuleType("airflow.operators")
fake_py = types.ModuleType("airflow.operators.python")

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
sys.modules.setdefault("airflow.providers", types.ModuleType("airflow.providers"))
sys.modules.setdefault("airflow.providers.amazon", types.ModuleType("airflow.providers.amazon"))
sys.modules.setdefault("airflow.providers.amazon.aws", types.ModuleType("airflow.providers.amazon.aws"))
sys.modules.setdefault("airflow.providers.amazon.aws.hooks", types.ModuleType("airflow.providers.amazon.aws.hooks"))
sys.modules.setdefault("airflow.providers.amazon.aws.hooks.s3", fake_s3)

from ttd.confetti.confetti_task_factory import (
    _resolve_env,
    _render_template,
    _sha256_b64,
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
        tpl = "hello {name}"
        rendered = _render_template(tpl, {"name": "world"})
        self.assertEqual(rendered, "hello world")
        h = _sha256_b64(rendered)
        self.assertEqual(len(h), 44)

class FactoryTest(unittest.TestCase):
    @patch("ttd.confetti.confetti_task_factory.AwsCloudStorage")
    @patch("ttd.confetti.confetti_task_factory.TtdEnvFactory.get_from_system")
    def test_make_tasks_pushes_xcom(self, mock_get_env, mock_storage):
        mock_get_env.return_value = type("E", (), {"execution_env": "prod"})()
        mock_instance = mock_storage.return_value
        mock_instance.read_key.return_value = "hi {date}"
        mock_instance._parse_bucket_and_key.side_effect = lambda k, b: ("b", k)
        mock_instance.check_for_key.return_value = False

        prep, gate = make_confetti_tasks(group_name="g", job_name="j", run_date="2020-01-01")
        ctx = {"ds": "2020-01-01", "ti": MagicMock()}
        prep.execute(context=ctx)
        ctx["ti"].xcom_pull.return_value = False
        ctx["ti"].xcom_push.assert_any_call(key="runtime_base", value=unittest.mock.ANY)
        ctx["ti"].xcom_push.assert_any_call(key="skip_job", value=False)
        should_run = gate.python_callable(ti=ctx["ti"])
        self.assertTrue(should_run)
