from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from datasources.sources.avails_datasources import AvailsDatasources

from typing import List, Optional
from ttd.datasets.dataset import Dataset
from datetime import timedelta
from airflow import DAG

from ttd.operators.dataset_check_sensor import DatasetCheckSensor


class HourlyAirflowGeneratedDataSetFromDatasets:

    def __init__(self, datasets: List[Dataset]):
        self.datasets = datasets

    def get_wait_for_hour_complete_operator(self, dag: DAG, execution_delta: Optional[timedelta] = None):
        raise NotImplementedError("HourlyAirflowGeneratedDataSetFromDatasourcess.get_wait_for_hour_complete_operator")

    def get_wait_for_day_complete_operator(self, dag: DAG, execution_delta: Optional[timedelta] = timedelta(days=0)):
        """
        gets an operator to wait for a full day of this data set to complete
        :param dag: the dag for which to create the wait operator
        :param execution_delta: the timedelta compared to the current dag execution date to wait on. should be provided
            in days. if None is provided, the operator will wait for all 24 hours of the current execution date. If you
            wish to depend on the previous day, pass timedelta(days=1) (NOTE positive timedelta indicates the past).
        """

        return DatasetCheckSensor(
            datasets=[d.with_check_type("day") for d in self.datasets],
            poke_interval=60 * 10,  # poke every 10 minutes - more friendly to the scheduler,
            ds_date_fn=lambda execution_date: execution_date - execution_delta,
            generate_task_id=True,
            dag=dag
        )


class CommonDatasources:

    # Deprecated. Please use `ttd.identity_graphs.identity_graphs.IdentityGraphs` instead.
    ttd_graph: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-insights",
        path_prefix="graph",
        data_name="adbrain_legacy",
        date_format="%Y-%m-%d",
        version=None,
    )

    rtb_bidfeedback: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-datapipe-data",
        path_prefix="parquet",
        data_name="rtb_bidfeedback_cleanfile",
        version=4,
        success_file=None,
        env_aware=False,
    )

    rtb_bidfeedback_v5: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-datapipe-data",
        path_prefix="parquet",
        data_name="rtb_bidfeedback_cleanfile",
        version=5,
        success_file=None,
        env_aware=False,
    )

    rtb_bidrequest_v5: HourGeneratedDataset = HourGeneratedDataset(
        bucket="ttd-datapipe-data",
        path_prefix="parquet",
        data_name="rtb_bidrequest_cleanfile",
        version=5,
        success_file=None,
        env_aware=False,
    )

    avails_household_sampled: HourlyAirflowGeneratedDataSetFromDatasets = HourlyAirflowGeneratedDataSetFromDatasets(
        datasets=[AvailsDatasources.househould_sampled_high_sample_avails, AvailsDatasources.househould_sampled_low_sample_avails]
    )

    avails_user_sampled: HourlyAirflowGeneratedDataSetFromDatasets = HourlyAirflowGeneratedDataSetFromDatasets(
        datasets=[AvailsDatasources.user_sampled_high_sample_avails, AvailsDatasources.user_sampled_low_sample_avails]
    )

    avails_open_graph_sampled_iav2: HourlyAirflowGeneratedDataSetFromDatasets = HourlyAirflowGeneratedDataSetFromDatasets(
        datasets=[AvailsDatasources.household_sampled_high_sample_avails_openGraphIav2]
    )

    unsampled_identity_avails_daily_agg: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-avails",
        path_prefix="datasets/withPII",
        data_name="identity-avails-agg-daily-v2-delta",
        version=None,
        date_format="date=%Y-%m-%d",
        success_file=None,
        env_aware=True,
    )
