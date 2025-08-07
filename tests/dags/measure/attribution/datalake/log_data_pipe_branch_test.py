import logging
import unittest
from typing import Dict, Any
from unittest.mock import MagicMock

from dags.measure.attribution.datalake.log_datapipe_config import select_task_handler


class LogDataPipeBranchSelectorTest(unittest.TestCase):

    def setUp(self):
        self.run_now_taskid = "run_delta_load"
        self.wait_sensor_taskid = "wait_for_source_data"

        # Mock the Task Instance (ti) and its xcom_pull method
        self.mock_ti = MagicMock()

        # Create the full context dictionary that the handler expects
        self.mock_context: Dict[str, Any] = {"ti": self.mock_ti, "params": {}}
        # Suppress logging warnings from the function to keep test output clean
        logging.disable(logging.WARNING)

    def tearDown(self):
        """Re-enable logging after tests are complete."""
        logging.disable(logging.NOTSET)

    def test_no_delta_watermark_and_no_log_watermark_returns_wait(self):

        def xcom_pull_side_effect(task_ids, key):
            if task_ids == "fetch_delta_load_high_watermark":
                return None  # No delta watermark
            if task_ids == "fetch_hourly_cleanse_watermark":
                return None  # No log watermark
            return None

        self.mock_ti.xcom_pull.side_effect = xcom_pull_side_effect
        self.mock_context["params"]["start_time_override"] = "2025-01-01T00:00:00Z"

        result = select_task_handler(run_now_taskid=self.run_now_taskid, wait_sensor_taskid=self.wait_sensor_taskid, **self.mock_context)

        self.assertEqual(result, self.wait_sensor_taskid)

    def test_no_delta_watermark_but_log_watermark_exists_returns_run(self):

        def xcom_pull_side_effect(task_ids, key):
            if task_ids == "fetch_delta_load_high_watermark":
                return None  # No delta watermark
            if task_ids == "fetch_hourly_cleanse_watermark":
                return "2025-07-29T10:00:00Z"  # Log watermark exists
            return None

        self.mock_ti.xcom_pull.side_effect = xcom_pull_side_effect
        self.mock_context["params"]["start_time_override"] = "2025-01-01T00:00:00Z"

        result = select_task_handler(run_now_taskid=self.run_now_taskid, wait_sensor_taskid=self.wait_sensor_taskid, **self.mock_context)

        self.assertEqual(result, self.run_now_taskid)

    def test_delta_watermark_is_equal_to_log_watermark_returns_wait(self):
        watermark_time = "2025-07-29T11:00:00Z"

        def xcom_pull_side_effect(task_ids, key):
            if task_ids == "fetch_delta_load_high_watermark":
                # The watermark is a dictionary with the relevant key
                return {"IntervalStart": watermark_time}
            if task_ids == "fetch_hourly_cleanse_watermark":
                return watermark_time  # Equal to delta watermark
            return None

        self.mock_ti.xcom_pull.side_effect = xcom_pull_side_effect

        result = select_task_handler(run_now_taskid=self.run_now_taskid, wait_sensor_taskid=self.wait_sensor_taskid, **self.mock_context)

        self.assertEqual(result, self.wait_sensor_taskid)
