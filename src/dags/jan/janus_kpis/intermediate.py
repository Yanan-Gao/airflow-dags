from dags.jan.janus_kpis.core import JanusIntermediateKPIDag, CURRENT_HOUR, CURRENT_HOUR_DS_CHECK, CURRENT_DATE
from datetime import datetime, timedelta

from airflow import DAG
from dags.jan.janus_kpis.datasets.intermediate import IntermediateJanusKPIDatasets
from dags.idnt.identity_datasets import IdentityDatasets
from datasources.datasources import Datasources
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.ec2.emr_instance_types.memory_optimized.r6gd import R6gd
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.tasks.op import OpTask
from ttd.operators.dataset_check_sensor import DatasetCheckSensor

brbf_dag: DAG = JanusIntermediateKPIDag(
    metric="bid-request-bid-feedback-hourly",
    task_class="com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.intermediate.BidRequestBidFeedbackHourlyAggregator",
    config_options=[
        ("BidRequestBidFeedbackHourlyAggregator.StartTime", CURRENT_HOUR),
    ],
    max_active_runs=5,
    start_date=datetime(2024, 6, 3, 23, 0),
    schedule_interval="0 * * * *",
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[
                Datasources.rtb_datalake.rtb_bidrequest_v5,
                Datasources.rtb_datalake.rtb_bidfeedback_v5,
            ],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 4,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 8,  # in seconds
        )
    )
).create_dag()

bid_request_variant_dag: DAG = JanusIntermediateKPIDag(
    metric="bid-request-variant-hourly",
    task_class="com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.intermediate.BidRequestVariantHourlyTransformer",
    config_options=[
        ("BidRequestVariantHourlyTransformer.StartTime", CURRENT_HOUR),
    ],
    max_active_runs=5,
    start_date=datetime(2024, 7, 3, 0, 0),
    schedule_interval="0 * * * *",
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[
                Datasources.rtb_datalake.rtb_bidrequest_v5,
            ],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 1,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 8,  # in seconds
        )
    )
).create_dag()

bid_feedback_variant_dag: DAG = JanusIntermediateKPIDag(
    metric="bid-feedback-variant-hourly",
    task_class="com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.intermediate.BidFeedbackVariantHourlyTransformer",
    config_options=[
        ("BidFeedbackVariantHourlyTransformer.StartTime", CURRENT_HOUR),
    ],
    max_active_runs=5,
    start_date=datetime(2024, 7, 3, 0, 0),
    schedule_interval="0 * * * *",
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[
                Datasources.rtb_datalake.rtb_bidfeedback_v5,
                IntermediateJanusKPIDatasets.bid_request_variant,
            ],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 2,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 8,  # in seconds
        )
    )
).create_dag()

click_variant_dag: DAG = JanusIntermediateKPIDag(
    metric="click-variant-hourly",
    task_class="com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.intermediate.ClickVariantHourlyTransformer",
    config_options=[
        ("ClickVariantHourlyTransformer.StartTime", CURRENT_HOUR),
    ],
    max_active_runs=5,
    start_date=datetime(2024, 7, 3, 0, 0),
    schedule_interval="0 * * * *",
    run_only_latest=False,
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[
                Datasources.rtb_datalake.rtb_clicktracker_v5,
                IntermediateJanusKPIDatasets.bid_request_variant,
            ],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 3,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 8,  # in seconds
        )
    )
).create_dag()

attributed_event_dag = JanusIntermediateKPIDag(
    metric="attributed_event_daily",
    task_class="com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.intermediate.AttributionDailyAggregator",
    config_options=[
        ("AttributionDailyAggregator.StartDate", CURRENT_DATE),
    ],
    max_active_runs=5,
    start_date=datetime(2024, 7, 3, 0, 0),
    schedule_interval=timedelta(days=1),
    core_fleet_capacity=256,
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R6gd.r6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
            R6gd.r6gd_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(48),
            R6gd.r6gd_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
            R5d.r5d_16xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(64),
            R5d.r5d_24xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(96),
        ],
        on_demand_weighted_capacity=256
    ),
    spark_options=[
        ("conf", f"spark.sql.shuffle.partitions={256}"),
        ("conf", "spark.driver.maxResultSize=2G"),
    ]
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[
                IdentityDatasets.attributed_event,
                IdentityDatasets.attributed_event_result,
                IntermediateJanusKPIDatasets.bid_feedback_variant.with_check_type("day"),
                IntermediateJanusKPIDatasets.click_variant.with_check_type("day"),
            ],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 5,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 12,  # in seconds
        )
    )
).create_dag()

click_dag: DAG = JanusIntermediateKPIDag(
    metric="click-hourly",
    task_class="com.thetradedesk.ds.libs.janusabtestingkpipipeline.transformers.intermediate.ClickHourlyAggregator",
    config_options=[
        ("ClickHourlyAggregator.StartTime", CURRENT_HOUR),
    ],
    max_active_runs=5,
    start_date=datetime(2024, 11, 22, 0, 0),
    schedule_interval="0 * * * *",
).with_dependency(
    OpTask(
        op=DatasetCheckSensor(
            datasets=[
                Datasources.rtb_datalake.rtb_clicktracker_v5,
                IntermediateJanusKPIDatasets.click_variant,
            ],
            ds_date=CURRENT_HOUR_DS_CHECK,
            poke_interval=60 * 10 + 4,  # in seconds with an arbitrary offset so that the janus dags aren't all poking at the same time
            timeout=60 * 60 * 9,  # in seconds
        )
    )
).create_dag()
