# from airflow import DAG

from datetime import timedelta

from airflow.utils.dates import days_ago

from dags.forecast.utils.team import TEAM_NAME
from dags.forecast.aerospike_set_utils import create_get_inactive_set_version_and_lock_task, \
    create_activate_and_unlock_set_version_task
from dags.forecast.contract_floor_prices.utils.datasets import get_currency_exchange_rate, get_deal_metadata_floor_price, \
    get_private_contract, get_supply_vendor_bidding, get_supply_vendor_deal
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

###########################################
#   job settings
###########################################
job_name = "etl-contract-floor-prices-pipeline"
job_schedule_interval = "0 3 * * *"
jar_path = "s3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar"
job_environment = TtdEnvFactory.get_from_system()
log_uri = f"s3://ttd-forecasting-useast/env={job_environment.dataset_write_env}/contract-floor-prices/logs"
spark_3_emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3  # Ensure spark3 and the correct scala version is used for the spark steps

job_aerospike_host = '{{macros.ttd_extras.resolve_consul_url("aerospike-use-ramv.aerospike.service.useast.consul", port=3000, limit=1)}}'
job_aerospike_namespace = "ttd-ramv"
metadata_set_name = "metadata"
contract_floor_prices_set_root = "cfp"
aerospike_gen_key = "aerospike_gen"
next_set_key_aerospike = "next_set_aerospike"
next_set_base_task_id = "get_next_aerospike_set_"

standard_cluster_tags = {"Team": TEAM_NAME}

export_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M6g.m6g_2xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

export_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[
        M6g.m6gd_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(8),
        M6g.m6gd_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
        M6g.m6gd_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
        M6g.m6g_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(16),
        M6g.m6g_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(32),
    ],
    on_demand_weighted_capacity=128,
)

###########################################
#   DAG setup
###########################################
contract_floor_price_dag = TtdDag(
    dag_id=job_name,
    start_date=days_ago(2),
    schedule_interval=job_schedule_interval,
    retries=1,
    retry_delay=timedelta(minutes=2),
    slack_tags=TEAM_NAME,
    tags=[TEAM_NAME],
    slack_channel='#dev-forecasting-alerts',
    enable_slack_alert=False
)

dag = contract_floor_price_dag.airflow_dag

currency_exchange_rate = get_currency_exchange_rate()
deal_metadata_floor_price = get_deal_metadata_floor_price()
private_contract = get_private_contract()
supply_vendor_bidding = get_supply_vendor_bidding()
supply_vendor_deal = get_supply_vendor_deal()
datasets = [currency_exchange_rate, deal_metadata_floor_price, private_contract, supply_vendor_bidding, supply_vendor_deal]

wait_for_datasets = OpTask(
    op=DatasetCheckSensor(
        task_id="wait_for_datasets",
        datasets=datasets,
        ds_date="{{data_interval_start.to_datetime_string()}}",
        poke_interval=int(timedelta(minutes=5).total_seconds()),
        mode="reschedule",
        timeout=int(timedelta(hours=8).total_seconds()),
    )
)


###########################################
#   Aerospike set logic (functions for get/update next set logic)
###########################################
def get_next_aerospike_set(job_aerospike_set_root: str) -> OpTask:
    return create_get_inactive_set_version_and_lock_task(
        dag=dag,
        task_id=next_set_base_task_id + job_aerospike_set_root,
        aerospike_hosts=job_aerospike_host,
        namespace=job_aerospike_namespace,
        metadata_set_name=metadata_set_name,
        set_key=job_aerospike_set_root,
        inactive_xcom_set_number_key=next_set_key_aerospike,
        aerospike_gen_xcom_key=aerospike_gen_key,
        inactive_xcom_set_key=None
    )


def update_aerospike_track_table(job_aerospike_set_root: str) -> OpTask:
    return create_activate_and_unlock_set_version_task(
        dag=dag,
        task_id="update_aerospike_track_table_" + job_aerospike_set_root,
        aerospike_hosts=job_aerospike_host,
        inactive_get_task_id=next_set_base_task_id + job_aerospike_set_root,
        job_name=job_name,
        namespace=job_aerospike_namespace,
        metadata_set_name=metadata_set_name,
        set_key=job_aerospike_set_root,
        inactive_xcom_set_number_key=next_set_key_aerospike,
        aerospike_gen_xcom_key=aerospike_gen_key
    )


###########################################
#   Export contract floor prices to Aerospike
###########################################
export_cluster = EmrClusterTask(
    name="contract-floor-prices-export-cluster",
    log_uri=log_uri,
    master_fleet_instance_type_configs=export_master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=export_core_fleet_instance_type_configs,
    cluster_tags={
        **standard_cluster_tags, "Process": "Contract-Floor-Prices-Export"
    },
    enable_prometheus_monitoring=True,
    emr_release_label=spark_3_emr_release_label
)


def get_job_aerospike_set_through_xcom(job_aerospike_set_root: str) -> str:
    if job_aerospike_set_root is None:
        return "tst_0"

    xcom_pull_set_number_params = [
        f"dag_id = '{job_name}'", f"task_ids = '{next_set_base_task_id + job_aerospike_set_root}'", f"key = '{next_set_key_aerospike}'"
    ]

    xcom_pull_set_number_call = f"task_instance.xcom_pull({', '.join(xcom_pull_set_number_params)})"

    return job_aerospike_set_root + "_{{" + xcom_pull_set_number_call + "}}"


data_export_step = EmrJobTask(
    cluster_specs=export_cluster.cluster_specs,
    name="ExportContractFloorPrices",
    class_name="com.thetradedesk.etlforecastjobs.universalforecasting.contractfloorprices.ExportContractFloorPricesToAerospikeJob",
    executable_path=jar_path,
    eldorado_config_option_pairs_list=[("aerospikeHosts", job_aerospike_host), ("aerospikeNamespace", job_aerospike_namespace),
                                       ("aerospikeSet", get_job_aerospike_set_through_xcom(contract_floor_prices_set_root)),
                                       ("targetDate", "{{ ds }}")]
)

###########################################
#   Job flow
###########################################
export_cluster.add_parallel_body_task(data_export_step)

contract_floor_price_dag >> wait_for_datasets >> get_next_aerospike_set(contract_floor_prices_set_root) >> export_cluster

export_cluster >> update_aerospike_track_table(contract_floor_prices_set_root)
