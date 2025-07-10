import logging
from datetime import datetime
from typing import Callable, Optional, Sequence, Union

from airflow.exceptions import AirflowNotFoundException
from airflow.sensors.base import BaseSensorOperator

from ttd.cloud_provider import CloudProvider, CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.datasets.dataset import Dataset
from ttd.operators.operator_mixins import OperatorMixins


class DatasetCheckSensor(BaseSensorOperator, OperatorMixins):
    """
    Operator that check if all specified dataset exist for given `ds_date` date.

    :param datasets: Datasets to check data for
    :param ds_date: The date of datasets to check for existence.
            If not provided, then result of InitialiseRunDateOperator is used instead.
    :param ds_date_fn: A lambda function that returns the date of datasets. If not provided, default to using ds_date.
    :param lookback: How many days of data to check in every dataset.
            If 0 - then only specified date is going to be checked.
    :param task_id: Id of the task. In case of `generate_task_id` == True, will be used as a prefix for generated name.
    :param generate_task_id: If True,
            the task_id will be created as product of contraction of all data_name of datasets provided for check.
    :param raise_exception: Raise an exception if data not found
    :param poke_interval: Time in seconds that the job should wait in between each try
    :type poke_interval: int
    :param timeout: Time, in seconds before the task times out and fails.
    :type timeout: int
    :param mode: How the sensor operates.
        Options are: ``{ poke | reschedule }``, default is ``reschedule``.
        When set to ``poke`` the sensor is taking up a worker slot for its
        whole execution time and sleeps between pokes. Use this mode if the
        expected runtime of the sensor is short or if a short poke interval
        is required. Note that the sensor will hold onto a worker slot and
        a pool slot for the duration of the sensor's runtime in this mode.
        When set to ``reschedule`` the sensor task frees the worker slot when
        the criteria is not yet met and it's rescheduled at a later time. Use
        this mode if the time before the criteria is met is expected to be
        quite long. The poke interval should be more than one minute to
        prevent too much load on the scheduler.
    :type mode: str
    :return: PythonOperator for data dependency check
    :rtype: PythonOperator
    """

    ui_color = "#7423ba"
    ui_fgcolor = "#fff"

    template_fields = ["ds_date"]

    def __init__(
        self,
        datasets: Sequence[Dataset],
        ds_date: Optional[Union[datetime, str]] = None,
        ds_date_fn: Optional[Callable[[datetime], datetime]] = None,
        lookback: int = 0,
        task_id: str = "dataset_check",
        generate_task_id: bool = False,
        raise_exception: bool = False,
        poke_interval: int = 60,
        timeout: int = 60 * 60 * 2,
        mode: str = "reschedule",
        cloud_provider: CloudProvider = CloudProviders.aws,
        *args,
        **kwargs,
    ):
        if len(datasets) == 0:
            raise Exception("datasets should be provided")
        if generate_task_id:
            task_id = self.format_ds_name(task_id, datasets)

        if ds_date is not None and ds_date_fn is not None:
            raise ValueError('Only one of `ds_date` or `ds_date_fn` may be provided')

        super().__init__(
            task_id=task_id,
            poke_interval=poke_interval,
            timeout=timeout,
            mode=mode,
            *args,
            **kwargs,
        )
        self.ds_date = ds_date
        self.ds_date_fn = ds_date_fn
        self.raise_exception = raise_exception
        self.datasets = datasets
        self.lookback = lookback
        self.cloud_provider = cloud_provider

    @staticmethod
    def format_ds_name(prefix: str, datasets: Sequence[Dataset]) -> str:
        ds_name = (
            datasets[0].data_name if len(datasets) == 1 else
            "_".join(["-".join([dn2[0:2] for dn1 in ds.data_name.split("/") for dn2 in dn1.split("_")]) for ds in datasets])
        )
        return f"{prefix}_{ds_name}"

    def poke(self, context):
        """
        Check to make sure dependencies for dataset are ready.

        :return: Result of data exist check on all datasets
        :rtype: bool
        """

        if self.ds_date_fn is not None:
            execution_date = context['data_interval_start']
            ds_date = self.ds_date_fn(execution_date)
            self.log.info(f"Run date is provided as execution date transformation: '{execution_date}' -> '{ds_date}'")
        elif isinstance(self.ds_date, str):
            self.log.info(f"Dataset date is provided as string parameter '{self.ds_date}', parsing it to datetime")
            ds_date = datetime.strptime(self.ds_date, "%Y-%m-%d %H:%M:%S")
        else:
            self.log.info(f"Run date is provided as datetime param '{self.ds_date}'")
            ds_date = self.ds_date  # type: ignore
        if ds_date is None:
            raise Exception("No InitialiseRunDateOperator found upstream and ds_date param is None, can't check dataset dependency")

        cloud_storage = CloudStorageBuilder(self.cloud_provider).build()

        # Check every Dataset Dependency (Input Datasets for the Spark Job)
        ready_to_run = True
        for dataset in self.datasets:
            dataset = dataset.with_cloud(cloud_provider=self.cloud_provider)
            if not dataset.check_data_exist(cloud_storage=cloud_storage, lookback=self.lookback, ds_date=ds_date):
                if self.raise_exception:
                    raise AirflowNotFoundException
                ready_to_run = False
                break

        logging.info(f"Dataset Check: {'Ready' if ready_to_run else 'Not ready'}")

        if ready_to_run:
            # If we are ready to run, let's push the templated paths to xcom
            for dataset in self.datasets:
                context["task_instance"].xcom_push(dataset.data_name, dataset.get_read_path(ds_date=ds_date))

        return ready_to_run
