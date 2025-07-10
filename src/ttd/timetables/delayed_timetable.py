from airflow.timetables.interval import DeltaDataIntervalTimetable, CronDataIntervalTimetable
from airflow.models.dag import ScheduleInterval
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from airflow.timetables.base import DagRunInfo, DataInterval
from airflow.timetables.base import TimeRestriction
from pendulum.tz.timezone import FixedTimezone, Timezone
from typing import Any, Union
from airflow.timetables.base import Timetable
from dataclasses import dataclass
import logging
import hashlib

logger = logging.getLogger(__name__)


class ScheduleDelayInfo:

    DATA_INTERVAL_LEN_IGNORE_THRESHOLD = timedelta(minutes=10)

    def __init__(self, dag_id: str, delay_max_proportion: float, max_delay: timedelta | None) -> None:
        self.dag_id = dag_id
        self.delay_max_proportion = delay_max_proportion
        self.max_delay = max_delay

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "ScheduleDelayInfo":
        return ScheduleDelayInfo(
            dag_id=data['dag_id'],
            delay_max_proportion=float(data['delay_max_proportion']),
            max_delay=timedelta(seconds=int(data['max_delay'])) if data['max_delay'] is not None else None,
        )

    def serialize(self) -> dict[str, Any]:
        return {
            'dag_id': self.dag_id,
            'delay_max_proportion': self.delay_max_proportion,
            'max_delay': self.max_delay.total_seconds() if self.max_delay is not None else None
        }

    def _max_delay(self, interval_duration: timedelta) -> timedelta:
        """
        Calculate a maximum delay from the data interval duration and class configuration.
        """

        max_delay = timedelta(seconds=int(interval_duration.total_seconds() * self.delay_max_proportion))
        if self.max_delay is not None:
            max_delay = min(max_delay, self.max_delay)
        return max_delay

    def _get_delay(self, max_delay: timedelta) -> timedelta:
        """
        Use a hash of the dag ID to provide an arbitrary delay within max_delay timeframe.
        """
        hex_signature = hashlib.md5(self.dag_id.encode("utf-8")).hexdigest()
        hash_int = int(hex_signature, 16)
        return timedelta(seconds=(hash_int % max_delay.total_seconds()))

    def calculate_delayed_info(self, run_info: DagRunInfo) -> DagRunInfo:
        super_start = run_info.data_interval.start
        super_end = run_info.data_interval.end

        duration = super_end - super_start

        if duration <= self.DATA_INTERVAL_LEN_IGNORE_THRESHOLD:
            logger.info(f"Skipping delay for dag {self.dag_id} due to short data interval.")
            return run_info

        max_delay = self._max_delay(duration)

        delay = self._get_delay(max_delay)

        run_after = super_end + delay
        logger.info(f"DAG {self.dag_id} running with timetable delay of {str(delay)}. Data interval remains unchanged.")
        return DagRunInfo(run_after=run_after, data_interval=run_info.data_interval)

    def __str__(self) -> str:
        repr = f"Max delay: {self.delay_max_proportion:.0%} of Data Interval. \n"

        if self.max_delay is not None:
            repr += f"Max delay threshold: {self.max_delay}.\n"

        repr += "Data Interval unchanged."
        return repr


class DelayedDeltaTimetable(DeltaDataIntervalTimetable):

    def __init__(self, delta: timedelta, delay: ScheduleDelayInfo) -> None:
        super().__init__(delta)
        self._delay = delay
        self.description = f"Timedelta {self._delta}, {str(self._delay)}"

    @property
    def summary(self) -> str:
        return f"{self._delta} D+"

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        return cls(timedelta(seconds=data["delta"]), delay=ScheduleDelayInfo.deserialize(data["delay"]))

    def serialize(self) -> dict[str, Any]:
        delta = self._delta.total_seconds()
        delay = self._delay.serialize()
        return {"delta": delta, "delay": delay}

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        super_info = super().next_dagrun_info(last_automated_data_interval=last_automated_data_interval, restriction=restriction)
        return self._delay.calculate_delayed_info(super_info) if super_info is not None else None


class DelayedCronTimetable(CronDataIntervalTimetable):

    def __init__(self, cron: str, timezone: str | Timezone | FixedTimezone, delay: ScheduleDelayInfo) -> None:
        super().__init__(cron, timezone)
        self._delay = delay
        self.description += f", {str(self._delay)}"

    @property
    def summary(self):
        return f"{super().summary} D+"

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        from airflow.serialization.serialized_objects import decode_timezone
        return cls(cron=data["cron"], timezone=decode_timezone(data["timezone"]), delay=ScheduleDelayInfo.deserialize(data["delay"]))

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.serialized_objects import encode_timezone
        return {"cron": self._expression, "timezone": encode_timezone(self._timezone), "delay": self._delay.serialize()}

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        super_info = super().next_dagrun_info(last_automated_data_interval=last_automated_data_interval, restriction=restriction)
        return self._delay.calculate_delayed_info(super_info) if super_info is not None else None


DelayedTimetable = Union[DelayedDeltaTimetable, DelayedCronTimetable]


@dataclass
class DelayedIntervalSpec:
    """
    Helper class to wrap a few arguments together to make it extra clear to user that they're creating an interval with
    a delay.
    """
    interval: ScheduleInterval
    delay_max_proportion: float = 0.1
    max_delay: timedelta | None = None


def create_delayed_timetable(
    spec: DelayedIntervalSpec,
    timezone: Timezone | FixedTimezone,
    dag_id: str,
) -> DelayedTimetable:

    delay_spec = ScheduleDelayInfo(dag_id=dag_id, delay_max_proportion=spec.delay_max_proportion, max_delay=spec.max_delay)

    if isinstance(spec.interval, (timedelta, relativedelta)):
        return DelayedDeltaTimetable(spec.interval, delay_spec)
    if isinstance(spec.interval, str):
        if spec.interval not in {"@once", "@continuous"}:
            return DelayedCronTimetable(spec.interval, timezone, delay_spec)
    raise ValueError(f"{spec} is not a valid delayed interval specification.")
