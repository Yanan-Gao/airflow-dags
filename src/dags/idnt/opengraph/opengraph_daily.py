# DEPRECATED! please use ttdgraph-generator
from airflow import DAG

from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from dags.idnt.identity_helpers import TimeHelpers
from dags.idnt.identity_clusters import IdentityClusters
from dags.idnt.statics import Tags, Executables, RunTimes
from dags.idnt.identity_datasets import IdentityDatasets
from datetime import datetime, timedelta
from ttd.tasks.op import OpTask
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrJobTask
from datasources.datasources import Datasources

opengraph_avails_etl = TtdDag(
    dag_id="opengraph-daily",
    start_date=datetime(2024, 6, 15),
    schedule_interval="0 13 * * *",
    slack_channel=Tags.slack_channel(),
    tags=Tags.dag,
)

check_input_datasets = OpTask(
    op=DatasetCheckSensor(
        datasets=[
            Datasources.rtb_datalake.rtb_bidrequest_v5, Datasources.rtb_datalake.rtb_bidfeedback_v5, IdentityDatasets.avails_daily_agg_v3,
            IdentityDatasets.avails_idless_daily_agg
        ],
        ds_date=RunTimes.previous_day_last_hour,
        poke_interval=TimeHelpers.minutes(15) + 12,  # with offset to poke at slightly different times
        timeout=TimeHelpers.hours(6),
    )
)

cluster = IdentityClusters.get_cluster("opengraph_daily_cluster", opengraph_avails_etl, 7000)

cluster.add_parallel_body_task(
    EmrJobTask(
        name="opengraph_daily_task",
        class_name=f"{Executables.identity_repo_class}.pipelines.OpenGraphDaily",
        eldorado_config_option_pairs_list=IdentityClusters.default_config_pairs(),
        timeout_timedelta=timedelta(hours=4),
        executable_path=Executables.identity_repo_executable,
        configure_cluster_automatically=False
    )
)

opengraph_avails_etl >> check_input_datasets >> cluster

final_dag: DAG = opengraph_avails_etl.airflow_dag
