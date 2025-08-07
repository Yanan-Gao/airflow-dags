from datetime import date, datetime, timedelta
from typing import List, Dict, Optional

import pytz
from airflow import AirflowException
from airflow.models import BaseOperator, DagRun
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from dateutil import parser

utc = pytz.UTC
days_in_a_week = 7
'''
WARNING!!

This operator is a copy of successful_dow_operator.py, but because we've migrated to Airflow2 and can't access the data
of runs in Airflow1, I have hard-coded the last successful run of each weekday into the function.

THIS IS JUST TEMPORARY AND IT IS NOT RECOMMENDED TO USE THIS OPERATOR IN ANY OTHER PIPELINE!
'''
'''
Provided some list of target tasks, this operator finds for each day of week (dow) the
most recent execution date that were successful for all target tasks.
'''


class FindMostRecentSuccessfulDowDatesInPipeline(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        target_tasks: List[str],
        xcom_key: str = "last_good_iso_weekdays",
        cutoff_date: Optional[date] = (datetime.today() - timedelta(days=60)).date(),
        # User-sampled avails are generated with a 1-day offset. Meaning that if the dag is run for 2023-06-10, the
        # datasets for 2023-06-09 will be generated. Since we have already generated a lot of datasets we can not rename
        # the runs, so we have to add a workaround. TODO: should we move logic of calculating successful run in Spark?
        dataset_offset_days=0,
        hardcoded_dates=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        if hardcoded_dates is None:
            hardcoded_dates = []
        self.target_tasks = target_tasks
        self.xcom_key = xcom_key
        self.cutoff_date = cutoff_date
        self.dataset_offset_days = dataset_offset_days
        self.hardcoded_dates = hardcoded_dates

    def execute(self, context) -> str:
        execution_date = context["ds"]
        successful_execution_dates = self.successful_execution_dates_for_tasks(execution_date, dag_id=self.dag_id)
        dow_to_recent_successful_execution_date = self.map_to_dow(successful_execution_dates, self.hardcoded_dates)
        self.check_dow_to_execution(dow_to_recent_successful_execution_date)
        result = ",".join([f'{i}:{dow_to_recent_successful_execution_date[i]}' for i in range(1, days_in_a_week + 1)])
        print(f"DOW results to be used by the weekly cluster: {result}")
        self.xcom_push(context=context, key=self.xcom_key, value=result)
        return result

    def successful_execution_dates_for_tasks(self, execution_date_str: str, dag_id: str) -> List[date]:
        parsed_execution_date = parser.parse(execution_date_str).replace(tzinfo=utc).date()
        if self.cutoff_date is not None and parsed_execution_date < self.cutoff_date:
            raise AirflowException(
                f"Provided cutoff_date of {self.cutoff_date} is after the current execution date of {parsed_execution_date}."
            )
        print(f"provided execution date: {parsed_execution_date}")

        # Per airflow documentation, dag runs are sorted by execution date. The reverse here means we get the most
        # recent execution dates first
        dag_runs: List[DagRun] = DagRun.find(dag_id=dag_id)
        dag_runs.reverse()
        if len(dag_runs) <= 0:
            print("There's no dag runs")
            return []

        print(f"Starting to process {dag_id} executions")
        successful_executions: List[date] = []
        for dag_run in dag_runs:
            dag_run_execution_date = dag_run.execution_date.replace(tzinfo=utc).date()
            if dag_run_execution_date > parsed_execution_date:
                print(f"Found newer execution date - {dag_run_execution_date}, past current execution date so will omit")
                continue
            dataset_date = dag_run_execution_date - timedelta(self.dataset_offset_days)
            # Given the dag_runs are sorted, by the time we get here it means there's no more valid execution dates
            if self.cutoff_date is not None and self.cutoff_date > dag_run_execution_date:
                print(f"Reached cutoff date of {self.cutoff_date} will stop searching for successful tasks")
                break

            if all(dag_run.get_task_instance(task_id=task_to_check) is not None and dag_run.get_task_instance(
                    task_id=task_to_check).state == State.SUCCESS for task_to_check in  # type: ignore
                   self.target_tasks):
                print(f"Found a successful task instance run for date - {dag_run_execution_date} the dataset date: {dataset_date}")
                successful_executions.append(dataset_date)
        return successful_executions

    @staticmethod
    def map_to_dow(successful_execution_dates: List[date], additional_dates: List[date] = []) -> Dict[int, str]:
        if additional_dates is None:
            additional_dates = []
        successful_execution_dates.extend(additional_dates)  # Remove later
        dow_to_datetime: Dict[int, str] = {}
        for execution_date in successful_execution_dates:
            weekday = execution_date.isoweekday()
            if weekday not in dow_to_datetime:
                dow_to_datetime[weekday] = execution_date.strftime("%Y%m%d")
            if len(dow_to_datetime) == days_in_a_week:
                return dow_to_datetime
        return dow_to_datetime

    @staticmethod
    def check_dow_to_execution(dow_to_recent_successful_execution_date):
        print(f"here's the dow to execution date results {dow_to_recent_successful_execution_date}")
        missing_dows = [str(i) for i in range(1, days_in_a_week + 1) if i not in dow_to_recent_successful_execution_date]
        if len(missing_dows) > 0:
            print(f"We are missing the following DOW's {missing_dows}")
            raise AirflowException(f"We're missing DOW's and cannot proceed - specifically missing {missing_dows}")
