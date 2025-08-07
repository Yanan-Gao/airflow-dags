from airflow import DAG
from dags.jan.janus_kpis.core import JanusIntermediateKPIDag, CURRENT_HOUR, CURRENT_HOUR_DS_CHECK
from datetime import datetime

from datasources.datasources import Datasources
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.tasks.op import OpTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor

core_fleet_capacity = 640

statsig_br_bf_coalesce_dag: DAG = JanusIntermediateKPIDag(
    metric="statsig-bid-feedback-bid-request-hourly",
    task_class=
    "com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.intermediate.StatsigBidFeedbackBidRequestExplodedJoinedTransformer",
    config_options=[
        ("StatsigBidFeedbackBidRequestExplodedJoinedTransformer.StartTime", CURRENT_HOUR),
        ("StatsigBidFeedbackBidRequestExplodedJoinedTransformer.CoalesceAmount", 1000),
    ],
    spark_options=[("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED"),
                   ("conf", "spark.sql.parquet.int96RebaseModeInWrite=CORRECTED"),
                   ("conf", f"spark.sql.shuffle.partitions={core_fleet_capacity}")],
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R6gd.r6gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R6gd.r6gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
            R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
            R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(96),
        ],
        on_demand_weighted_capacity=core_fleet_capacity
    ),
    max_active_runs=10,
    start_date=datetime(2025, 3, 17, 0),  # Restart data from 3/17
    schedule_interval="0 * * * *",
    core_fleet_capacity=core_fleet_capacity,
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[
                Datasources.rtb_datalake.rtb_bidrequest_v5,
                Datasources.rtb_datalake.rtb_bidfeedback_v5,
            ],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 5,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 8,  # in seconds
        )
    )
).create_dag()
