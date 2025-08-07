import logging
from datetime import datetime
from typing import List, Callable, Optional

from airflow import DAG
from airflow.exceptions import AirflowNotFoundException
from airflow.operators.python_operator import PythonOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.decorators import apply_defaults

from ttd.cloud_provider import CloudProvider
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.datasets.dataset import Dataset


def get_datasets_recency_check_callback(
    datasets_input: List[Dataset],
    cloud_provider: CloudProvider,
    recency_start_date: Optional[datetime] = None,
    lookback_days: int = 0,
    raise_exception: bool = True,
    xcom_push: bool = True,
    connection_id: Optional[str] = None
) -> Callable[[str, TaskInstance], bool]:
    """
    Check to make sure dependencies for dataset are recent.

    :param cloud_provider: CloudProvider to use to check existence
    :param recency_start_date:
    :param xcom_push: perform xcom push of found data date
    :type xcom_push: bool
    :param raise_exception: Raise an exception if data not found
    :type raise_exception: bool
    :param datasets_input: Datasets to check data for
    :type datasets_input:  List[DateExternalDataset]
    :param lookback_days: How many days of data to check in every dataset
    :type lookback_days: int
    :param connection_id: Airflow connection id for cloud provider. Default value is None, in this case
        default credentials are used. If you want to add your connection, please reach out DATAPROC.
    :type connection_id: Optional[str]
    :return: Callback function
    :rtype: Any
    """

    def callback(logical_date: str, task_instance: TaskInstance, **kwargs) -> bool:
        """
        Call back function for check_recency_datasets

        :param logical_date: dag logical date  YYYY-MM-DD
        :type logical_date: str
        :param task_instance: the task_instance object
        :type task_instance: TaskInstance
        :return: Result of finding data within recency window
        :rtype: bool
        """
        date_to_use = datetime.strptime(logical_date, "%Y-%m-%d") if recency_start_date is None else recency_start_date
        ready_to_run = True

        cloud_storage = CloudStorageBuilder(cloud_provider).set_conn_id(connection_id).build()

        # Check every Datasets Recency
        for dataset in datasets_input:
            dataset_for_cloud = dataset.with_cloud(cloud_provider=cloud_provider)
            recent_date_maybe = dataset_for_cloud.check_recent_data_exist(
                cloud_storage=cloud_storage, max_lookback=lookback_days, ds_date=date_to_use
            )

            if not recent_date_maybe:
                if raise_exception:
                    raise AirflowNotFoundException

                ready_to_run = False
            else:
                if xcom_push:
                    most_recent_available_date = recent_date_maybe.get()
                    logging.info(f"Xcom pushing {most_recent_available_date} to key {dataset_for_cloud.data_name}")
                    task_instance.xcom_push(key=dataset_for_cloud.data_name, value=most_recent_available_date)

        logging.info(f"Dag Recency Check: {ready_to_run}")

        return ready_to_run

    return callback


class DatasetRecencyOperator(PythonOperator):
    """
    Operator that checks all dataset provided in `datasets_input` list are present within `lookback_days` period
    from `recency_start_date` date.

    :param dag: Dag that is being executed
    :type dag: DAG
    :param datasets_input: Datasets to check data for
    :type datasets_input:  List[DateExternalDataset]
    :param lookback_days: How many days are we allowed to go without finding data.
            0 means that we should check only specified date without looking into history.
            Default: 0.
    :type lookback_days: int
    :param recency_start_date: date to start lookback. Templated `str` or specific `datetime` value. The default is `{{ ds }}`.
    :type recency_start_date: str | datetime
    :param connection_id: Airflow connection id for cloud provider. Default value is None, in this case
        default credentials are used. If you want to add your connection, please reach out DATAPROC.
    :type connection_id: Optional[str]
    :return: PythonOperator for data dependency check
    :rtype: PythonOperator
    """

    ui_color = "#aada8a"

    @apply_defaults
    def __init__(
        self,
        dag: DAG,
        datasets_input: List[Dataset],
        cloud_provider: CloudProvider,
        lookback_days: int = 0,
        recency_start_date: Optional[str | datetime] = '{{ ds }}',
        xcom_push: bool = False,
        task_id: str = 'recency_check',
        connection_id: Optional[str] = None,
        *args,
        **kwargs
    ):
        # Specific, non-templated upper bound.
        recency_start_date_value: Optional[datetime] = None
        if isinstance(recency_start_date, datetime):
            recency_start_date_value = recency_start_date

        super().__init__(
            task_id=task_id,
            python_callable=get_datasets_recency_check_callback(
                datasets_input=datasets_input,
                cloud_provider=cloud_provider,
                lookback_days=lookback_days,
                recency_start_date=recency_start_date_value,
                xcom_push=xcom_push,
                connection_id=connection_id
            ),
            dag=dag,
            op_kwargs=dict(logical_date=recency_start_date),
            provide_context=True
        )
