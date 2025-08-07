from datetime import date
from unittest import TestCase

from src.dags.forecast.sketches.operators.hardcoded_successful_dow_operator import \
    FindMostRecentSuccessfulDowDatesInPipeline

last_seven_successful_runs_in_airflow1 = [
    date(2024, 9, 9),
    date(2024, 9, 10),
    date(2024, 9, 4),
    date(2024, 9, 5),
    date(2024, 9, 6),
    date(2024, 9, 7),
    date(2024, 9, 1),
]


class TestHardcodedSuccessfulDowOperator(TestCase):

    def test_get_old_runs_if_no_new_successful_runs(self):
        dow_to_datetime = FindMostRecentSuccessfulDowDatesInPipeline.map_to_dow([], last_seven_successful_runs_in_airflow1)

        self.assertEqual(dow_to_datetime[1], "20240909")
        self.assertEqual(dow_to_datetime[2], "20240910")
        self.assertEqual(dow_to_datetime[3], "20240904")
        self.assertEqual(dow_to_datetime[4], "20240905")
        self.assertEqual(dow_to_datetime[5], "20240906")
        self.assertEqual(dow_to_datetime[6], "20240907")
        self.assertEqual(dow_to_datetime[7], "20240901")

    def test_old_runs_dont_overwrite_new_runs(self):
        dow_to_datetime = FindMostRecentSuccessfulDowDatesInPipeline.map_to_dow([date(2024, 9, 20)], last_seven_successful_runs_in_airflow1)

        self.assertEqual(dow_to_datetime[5], "20240920")
