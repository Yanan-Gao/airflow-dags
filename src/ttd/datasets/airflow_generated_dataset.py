from abc import ABC
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from typing import Optional, Callable, List


class AirflowGeneratedDataSet(ABC):
    """
    base class for defining a data set that is generated from airflow.
    :param name: a human-friendly name for this data set
    :param generating_dag_id: the dag id that generates this data set
    :param generating_task_id: (optional) the task id that would indicate this data set is ready. if set to None,
        then the dependency will be the dag as a whole
    """

    def __init__(self, name: str, generating_dag_id: str, generating_task_id: Optional[str]):
        self.name = name
        self.generating_dag_id = generating_dag_id
        self.generating_task_id = generating_task_id

    def _get_wait_for_complete_operator(
        self,
        dag: DAG,
        execution_date_fn: Optional[Callable[[datetime], List[datetime]]] = None,
        execution_delta: Optional[timedelta] = None,
    ):
        """
        private method for getting an ExternalTaskSensor operator to wait for successful execution of an external dag
        :param dag: the dag to create the operator for
        :param execution_date_fn: optional function to determine which execution dates of the external dag to wait on.
            see the documentation for ExternalTaskSensor to learn more about this param
        :param execution_delta: optional timedelta for how far in the past to check for dag execution.
            see the documentation for ExternalTaskSensor to learn more about this param
        """
        return ExternalTaskSensor(
            dag=dag,
            task_id=f"wait_for_complete_{self.name}",
            external_dag_id=self.generating_dag_id,
            external_task_id=self.generating_task_id,
            execution_date_fn=execution_date_fn,
            execution_delta=execution_delta,
            allowed_states=["success"],
            mode="reschedule",
            timeout=60 * 60 * 2,  # timeout is in seconds - wait 2 hours
        )


class HourlyAirflowGeneratedDataSet(AirflowGeneratedDataSet):
    """
    defines a data set that is generated on an hourly basis.
    :param name: a human-friendly name for this data set
    :param generating_dag_id: the dag id that generates this data set
    :param generating_task_id: (optional) the task id that would indicate this data set is ready. if set to None,
        then the dependency will be the dag as a whole
    """

    def __init__(self, name: str, generating_dag_id: str, generating_task_id: Optional[str]):
        super().__init__(
            name=name,
            generating_dag_id=generating_dag_id,
            generating_task_id=generating_task_id,
        )

    @staticmethod
    def _get_24_hour_function_from_delta(execution_delta: Optional[timedelta]):

        def execution_date_fn(timestamp: datetime):
            # move the date into the past
            if execution_delta is not None:
                timestamp -= execution_delta

            # hours 0-23 of the date we now have after applying execution_delta
            return [datetime(timestamp.year, timestamp.month, timestamp.day, hour=hour, tzinfo=timestamp.tzinfo) for hour in range(24)]

        return execution_date_fn

    def get_wait_for_hour_complete_operator(self, dag: DAG, execution_delta: Optional[timedelta] = None):
        """
        gets an operator to wait for a single hour of this data set to complete
        :param dag: the dag for which to create the wait operator
        :param execution_delta: the timedelta compared to the current dag execution date to wait on. for example,
            if you wish to depend on the hour from 2 hours before the current execution date, pass timedelta(hours=2).
            (NOTE positive timedelta indicates the past). Pass None to depend on the same hour as the dag's execution
            date
        """
        return self._get_wait_for_complete_operator(dag=dag, execution_delta=execution_delta)

    def get_wait_for_day_complete_operator(self, dag: DAG, execution_delta: Optional[timedelta] = None):
        """
        gets an operator to wait for a full day of this data set to complete
        :param dag: the dag for which to create the wait operator
        :param execution_delta: the timedelta compared to the current dag execution date to wait on. should be provided
            in days. if None is provided, the operator will wait for all 24 hours of the current execution date. If you
            wish to depend on the previous day, pass timedelta(days=1) (NOTE positive timedelta indicates the past).
        """
        execution_date_fn = self._get_24_hour_function_from_delta(execution_delta)

        return self._get_wait_for_complete_operator(dag=dag, execution_date_fn=execution_date_fn)


class DailyAirflowGeneratedDataSet(AirflowGeneratedDataSet):
    """
    defines a data set that is generated on a daily basis.
    :param name: a human-friendly name for this data set
    :param generating_dag_id: the dag id that generates this data set
    :param generating_task_id: (optional) the task id that would indicate this data set is ready. if set to None,
        then the dependency will be the dag as a whole
    :param execution_timedelta_from_midnight_utc: has to do with when the generating dag is scheduled to run. this is
        important, as airflow cross-dag dependencies rely on exact execution times. if the generating dag runs at exactly
        midnight, pass in None. if the dag runs at 4am UTC for example, pass in timedelta(hours=4)
    """

    def __init__(
        self,
        name: str,
        generating_dag_id: str,
        generating_task_id: Optional[str],
        execution_timedelta_from_midnight_utc: Optional[timedelta],
    ):
        super().__init__(
            name=name,
            generating_dag_id=generating_dag_id,
            generating_task_id=generating_task_id,
        )
        self.execution_timedelta_from_midnight_utc = (execution_timedelta_from_midnight_utc)

    def _get_executing_dag_start_time_fn(self, execution_delta: Optional[timedelta]):

        def execution_date_fn(ts: datetime):
            # move the date into the past
            if execution_delta is not None:
                ts -= execution_delta

            # now get the start of day for the altered date
            ts_start_of_day = datetime(ts.year, ts.month, ts.day, hour=0, tzinfo=ts.tzinfo)

            # now add in the generating dag offset from midnight, and we'll have the proper generating dag start time
            # we wish to depend on
            return [ts_start_of_day + self.execution_timedelta_from_midnight_utc]

        return execution_date_fn

    def get_wait_for_day_complete_operator(self, dag: DAG, execution_delta: Optional[timedelta] = None):
        """
        gets an operator to wait for a single day of this data set to complete
        :param dag: the dag for which to create the wait operator
        :param execution_delta: the timedelta compared to the current dag execution date to wait on. for example,
            if you wish to depend on the hour from 2 days before the current execution date, pass timedelta(days=2).
            (NOTE positive timedelta indicates the past). Pass None to depend on the same hour as the dag's execution
            date
        """
        # we need to calculate
        overall_execution_delta = timedelta(hours=0)

        if execution_delta is not None:
            # add a provided delta to say how many hours the dag should look back in time to get to midnight
            overall_execution_delta += execution_delta

        if self.execution_timedelta_from_midnight_utc is not None:
            # we subtract here. self.execution_timedelta_from_midnight_utc is positive, and the delta we provide
            # to ExternalTaskSensor tells how far back in the past to look. So in order to look further in the future,
            # we need to subtract from the time delta
            overall_execution_delta -= self.execution_timedelta_from_midnight_utc

        return self._get_wait_for_complete_operator(
            dag=dag,
            execution_date_fn=self._get_executing_dag_start_time_fn(execution_delta),
        )
