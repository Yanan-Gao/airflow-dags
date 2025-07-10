from ttd.timetables.delayed_timetable import DelayedDeltaTimetable, DelayedCronTimetable
from airflow.plugins_manager import AirflowPlugin


class DelayedTimetablePlugin(AirflowPlugin):
    name = "delayed_timetable_plugin"
    timetables = [DelayedDeltaTimetable, DelayedCronTimetable]
