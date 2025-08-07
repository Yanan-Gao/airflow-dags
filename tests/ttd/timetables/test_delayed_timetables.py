import unittest
from datetime import timedelta
import pendulum

from pendulum import datetime

from ttd.timetables.delayed_timetable import ScheduleDelayInfo, DelayedDeltaTimetable, DelayedCronTimetable
from airflow.timetables.base import DagRunInfo
from unittest.mock import Mock
from airflow.timetables.interval import DeltaDataIntervalTimetable, CronDataIntervalTimetable
from airflow.timetables.base import TimeRestriction


class TagUtilsTest(unittest.TestCase):

    def test__ScheduleDelayInfo__deserialize_serialize(self):
        delay_info_dict = [
            ({
                'dag_id': 'testing',
                'delay_max_proportion': 0.1,
                'max_delay': 10,
            }, {
                'dag_id': 'testing',
                'delay_max_proportion': 0.1,
                'max_delay': 10,
            }),
            ({
                'dag_id': 'testing',
                'delay_max_proportion': 0.1,
                'max_delay': None,
            }, {
                'dag_id': 'testing',
                'delay_max_proportion': 0.1,
                'max_delay': None,
            }),
            ({
                'dag_id': 'testing',
                'delay_max_proportion': "0.1",
                'max_delay': "10",
            }, {
                'dag_id': 'testing',
                'delay_max_proportion': 0.1,
                'max_delay': 10,
            }),
            ({
                'dag_id': 'testing',
                'delay_max_proportion': 1,
                'max_delay': 10,
            }, {
                'dag_id': 'testing',
                'delay_max_proportion': 1.0,
                'max_delay': 10,
            }),
        ]
        for data, exp in delay_info_dict:
            self.assertDictEqual(exp, ScheduleDelayInfo.deserialize(data).serialize())

    def test__ScheduleDelayInfo__max_delay(self):
        max_delay_data = [
            (
                ScheduleDelayInfo(dag_id="n/a", delay_max_proportion=0.1, max_delay=timedelta(minutes=15)), timedelta(minutes=60),
                timedelta(minutes=6)
            ),
            (
                ScheduleDelayInfo(dag_id="n/a", delay_max_proportion=0.1, max_delay=timedelta(minutes=15)), timedelta(hours=2),
                timedelta(minutes=12)
            ),
            (
                ScheduleDelayInfo(dag_id="n/a", delay_max_proportion=0.1, max_delay=timedelta(minutes=15)), timedelta(minutes=360),
                timedelta(minutes=15)
            ),
            (ScheduleDelayInfo(dag_id="n/a", delay_max_proportion=0.1, max_delay=None), timedelta(minutes=200), timedelta(minutes=20)),
            (
                ScheduleDelayInfo(dag_id="n/a", delay_max_proportion=1.0, max_delay=timedelta(minutes=15)), timedelta(minutes=20),
                timedelta(minutes=15)
            ),
            (ScheduleDelayInfo(dag_id="n/a", delay_max_proportion=1.0, max_delay=None), timedelta(minutes=200), timedelta(minutes=200)),
            (
                ScheduleDelayInfo(dag_id="n/a", delay_max_proportion=2.0, max_delay=timedelta(minutes=15)), timedelta(minutes=200),
                timedelta(minutes=15)
            ),
            (ScheduleDelayInfo(dag_id="n/a", delay_max_proportion=2.0, max_delay=None), timedelta(minutes=200), timedelta(minutes=400)),
            (ScheduleDelayInfo(dag_id="n/a", delay_max_proportion=0.1, max_delay=None), timedelta(minutes=1), timedelta(seconds=6)),
            (ScheduleDelayInfo(dag_id="n/a", delay_max_proportion=0.1, max_delay=None), timedelta(seconds=3), timedelta(seconds=0)),
        ]
        for spec, data_interval, exp in max_delay_data:
            self.assertEqual(exp, spec._max_delay(data_interval))

    def test__ScheduleDelayInfo__calculate_delayed_info(self):
        spec = ScheduleDelayInfo(dag_id="n/a", delay_max_proportion=0.1, max_delay=timedelta(minutes=10))
        spec._get_delay = Mock(return_value=timedelta(minutes=15))

        runinfo = DagRunInfo.interval(
            start=pendulum.datetime(year=2020, month=1, day=1, hour=0, minute=0, second=0),
            end=pendulum.datetime(year=2020, month=1, day=2, hour=0, minute=0, second=0)
        )
        expected_run_after = pendulum.datetime(year=2020, month=1, day=2, hour=0, minute=15, second=0)
        delayed_info = spec.calculate_delayed_info(runinfo)

        self.assertEqual(expected_run_after, delayed_info.run_after)
        self.assertIs(runinfo.data_interval, delayed_info.data_interval)

        runinfo_short = DagRunInfo.interval(
            start=pendulum.datetime(year=2020, month=1, day=1, hour=0, minute=0, second=0),
            end=pendulum.datetime(year=2020, month=1, day=1, hour=0, minute=5, second=0)
        )

        delayed_info_untouched = spec.calculate_delayed_info(runinfo_short)
        self.assertIs(runinfo_short, delayed_info_untouched)

    start_dates = [
        datetime(2024, 10, 25, 21, 57, 42),
        datetime(2024, 9, 4, 7, 33, 17),
        datetime(2024, 6, 26, 21, 25, 42),
        datetime(2024, 4, 23, 22, 39, 18),
        datetime(2023, 12, 14, 11, 45, 9),
        datetime(2023, 11, 24, 8, 47, 30),
        datetime(2023, 1, 20, 13, 26, 57),
        datetime(2022, 12, 30, 17, 36, 16),
        datetime(2022, 4, 21, 15, 0, 26),
        datetime(2020, 2, 29, 18, 55, 27),  # Leap year date
    ]

    def test__DelayedDeltaTimetable__DI_same(self):
        timedeltas = [
            timedelta(days=1),
            timedelta(days=0, hours=1, minutes=30, seconds=15),
            timedelta(weeks=2),
            timedelta(days=-1),
            timedelta(hours=5, minutes=45),
            timedelta(seconds=10),
            timedelta(milliseconds=500),
            timedelta(microseconds=250000),
            timedelta(days=365),
            timedelta(hours=-3, minutes=-15),
            timedelta(weeks=52, days=1),
            timedelta(days=10, seconds=3600),
            timedelta(days=30),
            timedelta(minutes=0.5),
            timedelta(days=-365),
            timedelta(hours=24),
            timedelta(weeks=-2, days=3),
            timedelta(minutes=60 * 24),
            timedelta(seconds=3600 * 12),
        ]

        for td in timedeltas:
            for start_date in self.start_dates:

                tt = DeltaDataIntervalTimetable(delta=td)
                tt_d = DelayedDeltaTimetable(delta=td, delay=ScheduleDelayInfo("testing_dag", 0.1, timedelta(minutes=15)))
                tt_interval = tt.infer_manual_data_interval(start_date)
                tt_d_interval = tt_d.infer_manual_data_interval(start_date)

                self.assertEqual(tt_interval, tt_d_interval)

                restriction = TimeRestriction(start_date, None, False)

                tt_next_dagrun = tt.next_dagrun_info(last_automated_data_interval=tt_interval, restriction=restriction)
                tt_d_next_dagrun = tt_d.next_dagrun_info(last_automated_data_interval=tt_d_interval, restriction=restriction)

                self.assertEqual(tt_next_dagrun.data_interval, tt_d_next_dagrun.data_interval)  # type: ignore

    def test__DelayedCronTimetable__DI_same(self):
        cron_strings = [
            "* * * * *",  # Every minute
            "0 * * * *",  # At the top of every hour
            "30 8 * * *",  # At 8:30 AM every day
            "0 0 * * 0",  # At midnight on Sunday
            "15 14 1 * *",  # At 2:15 PM on the first day of every month
            "0 9 * * 1-5",  # At 9:00 AM Monday through Friday
            "0 22 * * 1,3,5",  # At 10:00 PM on Monday, Wednesday, and Friday
            "0 3 15 * *",  # At 3:00 AM on the 15th of every month
            "*/5 * * * *",  # Every 5 minutes
            "0 */2 * * *",  # Every 2 hours
            "0 0 1 1 *",  # At midnight on January 1st (New Year)
            "0 12 31 12 *",  # At noon on December 31st
            "30 7 * * 1-5",  # At 7:30 AM Monday through Friday
            "15 6 * * 6,0",  # At 6:15 AM on Saturday and Sunday
            "45 23 28-31 * 5",  # At 11:45 PM on the 28th to the 31st of the month if it's a Friday
            "0 18 1 6 *",  # At 6:00 PM on June 1st
            "30 21 13 * 4",  # At 9:30 PM on the 13th of the month if it's a Thursday
            "0 0 29 2 *",  # At midnight on February 29th (leap years)
            "0 8-18/2 * * 1-5",  # Every 2 hours between 8:00 AM and 6:00 PM Monday through Friday
            "0 22 * * 6#2",  # At 10:00 PM on the second Saturday of the month
        ]

        for cron in cron_strings:
            for start_date in self.start_dates:

                tt = CronDataIntervalTimetable(cron=cron, timezone='UTC')
                tt_d = DelayedCronTimetable(cron=cron, timezone='UTC', delay=ScheduleDelayInfo("testing_dag", 0.1, timedelta(minutes=15)))
                tt_interval = tt.infer_manual_data_interval(run_after=start_date)
                tt_d_interval = tt_d.infer_manual_data_interval(run_after=start_date)

                self.assertEqual(tt_interval, tt_d_interval)

                restriction = TimeRestriction(start_date, None, False)

                tt_next_dagrun = tt.next_dagrun_info(last_automated_data_interval=tt_interval, restriction=restriction)
                tt_d_next_dagrun = tt_d.next_dagrun_info(last_automated_data_interval=tt_d_interval, restriction=restriction)

                # None checks are just for type check
                self.assertIsNotNone(tt_next_dagrun)
                self.assertIsNotNone(tt_d_next_dagrun)
                if tt_next_dagrun and tt_d_next_dagrun:
                    self.assertEqual(tt_next_dagrun.data_interval, tt_d_next_dagrun.data_interval)


if __name__ == "__main__":
    unittest.main()
