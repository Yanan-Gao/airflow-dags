from datetime import datetime, UTC
import logging
from airflow.sensors.python import PythonSensor
from ttd.cloud_provider import CloudProvider, CloudProviders
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from ttd.datasets.dataset import Dataset
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.tasks.op import OpTask
from dags.idnt.statics import RunTimes, Tags
from ttd.el_dorado.v2.base import TtdDag
from ttd.datasets.date_dataset import DateDataset
from typing import Dict, Any, List, Optional, Callable
from datetime import timedelta
from airflow.operators.python import ShortCircuitOperator


class TimeHelpers:

    @staticmethod
    def minutes(m: int) -> int:
        return m * 60

    @staticmethod
    def hours(h: int) -> int:
        return h * 60 * 60


class PathHelpers:
    """
    Utility class for managing experiment runName.

    You can set a custom run name for your experiments by modifying the `EXPERIMENT_RUNNAME` variable.
    This is especially useful for tracking runs related to specific merge requests (MRs).
    The experiment will write to the `env=test` as this will be run from the prodTest airflow environment.

    Recommended format: 'abc-IDNT-1234-ticketName'

    ⚠️ Important: Remember to reset `EXPERIMENT_RUNNAME` to `None` before merging your code!
    """

    EXPERIMENT_RUNNAME: Optional[str] = None

    @classmethod
    def get_test_airflow_run_name(cls) -> str:
        if cls.EXPERIMENT_RUNNAME is not None:
            return f"env=test::{cls.EXPERIMENT_RUNNAME}"
        else:
            return f"""env=test::fromAirflow-{datetime.now(UTC).strftime("%Y-%m-%d")}"""


class DagHelpers:

    @staticmethod
    def identity_dag(
        dag_id: str,
        schedule_interval: str,
        start_date: datetime,
        doc_md: Optional[str] = None,
        tags: Optional[List[str]] = None,
        slack_channel: Optional[str] = None,
        **kwargs
    ) -> TtdDag:
        """Returns a standard dag with boilerplate code for tags, slack channel alerts.

        If you need to pass in more arguments to `TtdDag`, you can still do this via `kwargs`.

        Args:
            dag_id (str): Name of the dag. Convention is snake case. Read `readme` for more info.
            schedule_interval (str): Cron expression for schedule interval.
            start_date (datetime): Date for start of job. Note that this will need to be the
                beginning of the processing period. E.g. if you're running a weekly job that executes
                on Sundays, you'll want to set this to be the Sunday prior to the first desired
                execution date. This is known as `data_interval_start` in airflow. Read more
                here: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html.
            doc_md (Optional[str]): Markdown with DAG's documentation to display in the UI. Usually DAG file's __doc__.
                See an example here: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#dag-task-documentation
                Please treat this as a required parameter: this is optional only because of the existing DAGs.
        Returns:
            TtdDag: Fully qualified TtdDag.
        """
        dag = TtdDag(
            dag_id=dag_id,
            schedule_interval=schedule_interval,
            start_date=start_date,
            slack_channel=slack_channel or Tags.slack_channel(),
            tags=Tags.dag + (tags or []),
            **kwargs
        )
        if doc_md is not None:
            dag.airflow_dag.doc_md = doc_md
        return dag

    @staticmethod
    def _random_offset(name: str) -> int:
        return abs(hash(name)) % 97

    @classmethod
    def check_datasets(
        cls,
        datasets: List[DateDataset],
        ds_date: str = RunTimes.previous_day_last_hour,
        poke_minutes: int = 15,
        timeout_hours: int = 3,
        lookback: int = 0,
        cloud_provider: CloudProvider = CloudProviders.aws,
        suffix: str = "",
    ) -> OpTask:
        """Checks if a list of datasets are ready for a given date. IF dataset is daily check is done
        once if it's hourly all hours are checked for that day.

        This a simple wrapper for DataSetCheckSensor.

        Args:
            dataset (DateGeneratedDataset): Dataset to be checked .
            ds_date (str, optional): Datetime to be checked, Note that this must be a datetime string
                of "%Y-%m-%d %H:%M:%S" format. Use values from `RunTimes` for convenience.
                Defaults to RunTimes.previous_day_last_hour.
            poke_minutes (int, optional): Poking interval in minutes. This is how often the task
                will check for the files. Defaults to 15.
            timeout_hours (int, optional): Timeout in hours. If the data is not ready by this time
                the task will be marked as failed. Defaults to 3.
            lookback (int, optional): How many days of data to check in every dataset.
                If 0 - then only specified date is going to be checked.
            cloud_provider (CloudProvider): Sets the cloud provider for dataset storage. Defaults to AWS (S3) environment.
            suffix (str): Optional suffix to task name to ensure all tasks have unique names.
                Useful for checking multiple hours of same dataset.

        Returns:
            OpTask: A TTD OpTask to check if the data is ready.
        """
        # TODO add docs and highlight that time must be a datetime format
        full_name = "_".join((d.data_name.replace("_", "").replace("/", "_") for d in datasets))

        return OpTask(
            op=DatasetCheckSensor(
                datasets=datasets,
                task_id=f"check_{suffix}_{full_name}",
                ds_date=ds_date,
                poke_interval=TimeHelpers.minutes(poke_minutes) + cls._random_offset(full_name),
                timeout=TimeHelpers.hours(timeout_hours),
                lookback=lookback,
                cloud_provider=cloud_provider
            )
        )

    @classmethod
    def check_datasets_with_lookback(
        cls,
        dataset_name: str,
        datasets: List[DateDataset],
        lookback_days: int = 0,
        poke_minutes: int = 15,
        timeout_hours: int = 3,
        cloud_provider: CloudProvider = CloudProviders.aws,
        suffix: str = ""
    ) -> OpTask:
        """Checks if that all datasets have at-least one days worth of data ready within the lookback (in days) from the execution date of a given DAG.

        Args:
            dataset_name (str): The name of this List of datasets. Distinguishes the name of the final OpTask returned.
            datasets (List[DateGeneratedDataset]): Datasets to be checked.
            dag (DAG): The DAG which this check_dataset will be a part of.
            lookback_days (int, optional): The lookback window in days. From the execution date,
                we search each date to see if the data is ready. If it is ready for a date, we return true.
                Otherwise, we keep searching until we pass the number of lookback_days.
            poke_minutes (int, optional): Poking interval in minutes. This is how often the task
                will check for the files. Defaults to 15.
            timeout_hours (int, optional): Timeout in hours. If the data is not ready by this time
                the task will be marked as failed. Defaults to 3.
            cloud_provider (CloudProvider): Sets the cloud provider for dataset storage. Defaults to AWS (S3) environment.
            suffix (str): Optional suffix to task name to ensure all tasks have unique names.


        Returns:
            OpTask: A TTD OpTask to check if there is a date within the lookback that has data ready.
        """

        def check_all_datasets(**kwargs) -> bool:
            logging.info(f'check_all_datasets {kwargs}.')
            failed_datasets = set()
            for dataset in datasets:
                if not check_data_available_in_lookback(dataset=dataset.with_cloud(cloud_provider), **kwargs):
                    failed_datasets.add(dataset.data)

            if len(failed_datasets) > 0:
                logging.error(f"The following datasets do not have data in the lookback of {lookback_days} days: {failed_datasets}")
                return False

            return True

        def check_data_available_in_lookback(dataset: DateDataset, **kwargs) -> bool:
            ds: str = str(kwargs.get("ds"))
            logging.info(f'kwargs {kwargs}')
            logging.info(f'Check data available for {dataset.data_name} with lookback: {lookback_days} from date {ds}.')
            return dataset.check_recent_data_exist(
                cloud_storage=CloudStorageBuilder(cloud_provider).build(),
                ds_date=datetime.strptime(ds, "%Y-%m-%d"),
                max_lookback=lookback_days
            )

        return OpTask(
            op=PythonSensor(
                task_id=f"check_{suffix}_{dataset_name}",
                python_callable=check_all_datasets,
                poke_interval=TimeHelpers.minutes(poke_minutes) + cls._random_offset(dataset_name),
                timeout=timeout_hours * 60 * 60,
                mode='poke',
            )
        )

    @staticmethod
    def get_next_task_id(task) -> str:
        try:
            task_id = task.first_airflow_op().task_id
        except AttributeError:
            task_id = task.task_id

        return task_id

    @classmethod
    def branch_on_multiple_hour(cls, ds: str, hour_to_index: Callable[[int], int], task_map: Dict[int, str]) -> str:
        """Branch the DAG based on the hour of execution onto a dictionary

        The task_map is a dictionary of integers to clusters, whilst the hour_to_index translates the
        execution hour (ds) into a key of that dicitionary. This lets you branch your DAGs to multiple
        clusters based on the hour of data interval.

        Args:
            ds (str): Datetime of DAG run.
            hour_to_index (Callable[[int], int]): Function to translate each hour to an index of task_map.
            task_map (Dict[int, str]): Task map of integers to tasks. We use this to pick how to branch the DAG.

        Returns:
            str: The task_name of the DAG that should be executed for that hour.
        """
        hour = int(datetime.strptime(ds, "%Y-%m-%d %H:%M:%S").strftime("%H"))
        idx = hour_to_index(hour)
        logging.info(f"Running for {ds} @ hour: {hour:>02} -> idx: {idx}")
        task_id = cls.get_next_task_id(task_map[idx])
        logging.info(f"Task id: {task_id}")
        return task_id

    @classmethod
    def _skip_on_time(cls, intervals_to_run: List[str], datetime_to_string: Callable[[datetime], str], raw_time: str):
        parsed_time = datetime.strptime(raw_time, "%Y-%m-%d %H:%M:%S")
        interval = datetime_to_string(parsed_time)
        logging.info(f"Parsed time {raw_time} into {parsed_time} for instance {interval} vs {intervals_to_run}")
        return interval in intervals_to_run

    @classmethod
    def _skip_task(cls, intervals_to_run: List[str], prefix: str, format: str) -> OpTask:
        if format == "%A":
            skip_type = "days"
        elif format == "%H":
            skip_type = "hours"
        else:
            raise ValueError(f"Invalid formatting: {format}")

        if prefix == "":
            task_prefix = ""
        else:
            task_prefix = f"{prefix}_"

        all_intervals = "_".join(intervals_to_run)

        task = OpTask(
            op=ShortCircuitOperator(
                task_id=f"{task_prefix}skip_unless_{skip_type}_{all_intervals}",
                python_callable=cls._skip_on_time,
                # note that the time must be passed in here for jinja to render
                op_kwargs=dict(
                    intervals_to_run=intervals_to_run,
                    datetime_to_string=lambda dt: dt.strftime(format),
                    raw_time=RunTimes.current_interval_end
                ),
            )
        )
        return task

    @classmethod
    def skip_unless_day(cls, intervals_to_run: List[str], prefix: str = "") -> OpTask:
        """Creates a ShortCircuit OpTask to continue only if the execution day is in the list of days provided.

        Args:
            intervals_to_run (List[str]): List of days when you want processing to happen. In `%A` format.
            prefix (str, optional): Prefix the operator's task id. This is needed for uniqueness if you have multiple skips in 1 dag. Defaults to "".

        Returns:
            OpTask: ShortCircuit operator that will skip processing unless the interval end is on one of the specified days.
        """
        all_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        assert all(interval in all_days for interval in intervals_to_run), f"Some days are incorrect: {intervals_to_run}"
        return cls._skip_task(intervals_to_run, prefix, "%A")

    @classmethod
    def skip_unless_hour(cls, intervals_to_run: List[str], prefix: str = "") -> OpTask:
        """Creates a ShortCircuit OpTask to continue only if the execution hour is in the list of hours provided.

        Args:
            intervals_to_run (List[str]): List of hours when you want processing to happen. In `%H` format, i.e. 00 -> 23.
            prefix (str, optional): Prefix the operator's task id. This is needed for uniqueness if you have multiple skips in 1 dag. Defaults to "".

        Returns:
            OpTask: ShortCircuit operator that will skip processing unless the interval end is on one of the specified hours.
        """
        all_hours = [f"{h:0>2d}" for h in range(0, 24)]
        assert all(interval in all_hours for interval in intervals_to_run), f"Some hours are incorrect: {intervals_to_run}"
        return cls._skip_task(intervals_to_run, prefix, "%H")

    @classmethod
    def get_dataset_transfer_task(
        cls,
        task_name: str,
        dataset: Dataset,
        partitioning_args: Dict[str, Any],
        src_cloud: CloudProvider = CloudProviders.aws,
        dst_cloud: CloudProvider = CloudProviders.azure,
        timeout_hrs: int = 4,
        max_threads=5,
    ) -> DatasetTransferTask:
        """
        Copy data b/w different cloud environments
        Particularly useful for Spark jobs running in Azure where the input datasets need to reside in Azure Storage
        :param task_name: Task Name
        :param dataset: Dataset to be transferred
        :param partitioning_args: Arguments for dataset partitioning (hour, date, etc.)
        :param src_cloud: Source Cloud Environment, Default AWS
        :param dst_cloud: Destination Cloud Environment, Default Azure
        :param timeout_hrs: Timeout in hours, default 4 hours.  If the dataset is not transferred successfully by then, task marked as failed
        :param max_threads: Maximum number of parallel threads to use for the transfer.Helps control concurrency and avoid connection pool exhaustion. Default set to 5.

        :return: DataTransferTask
        """
        return DatasetTransferTask(
            name=task_name,
            dataset=dataset,
            src_cloud_provider=src_cloud,
            dst_cloud_provider=dst_cloud,
            transfer_timeout=timedelta(hours=timeout_hrs),
            partitioning_args=partitioning_args,
            max_threads=max_threads
        )
