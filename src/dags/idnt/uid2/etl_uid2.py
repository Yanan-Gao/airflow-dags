from airflow import DAG
from dags.idnt.identity_helpers import DagHelpers
from datetime import datetime, timedelta, timezone
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
import logging
import boto3
from ttd.tasks.op import OpTask
from airflow.operators.python import ShortCircuitOperator
from dags.idnt.identity_clusters import IdentityClusters
from dags.idnt.statics import Tags, VendorTypes
from dataclasses import dataclass
from typing import Callable, List, Optional
from dags.idnt.vendors.vendor_alerts import LateVendor
from dags.idnt.util.s3_utils import does_folder_have_objects, get_latest_etl_date_from_path

etl_uid2_dag = DagHelpers.identity_dag(
    dag_id="etl-uid2",
    schedule_interval="0 */4 * * *",
    start_date=datetime(2024, 7, 23),
    run_only_latest=True,
    retry_delay=timedelta(minutes=30),
)


@dataclass
class Uid2Vendor:
    """Contains all the information needed as input for Uid2 vendor ETL.

    Attributes:
        name (str): The name of the vendor.
        input_path_prefix (str): The prefix for the input data path.
        date_parse_fun (Callable[[str], str]): A function to parse dates into a format used for data input paths.
        final_partition (int): The number of partitions for the final ETL output.
        cadence_days (int): The expected number of days between each delivery of the input data.
        last_seen_timestamp_threshold_months (int): Data outside this threshold will be ignored in the ETL process.
            Only works for vendors with this explicitly enabled in the Scala code.
    """
    name: str
    input_path_prefix: str
    date_parse_fun: Callable[[str], str]
    final_partition: int
    cadence_days: int
    buffer_days: int = 2
    last_seen_timestamp_threshold_months: int = 15


small_vendor_configs = [
    Uid2Vendor('throtle', 'throtle/import-uid2', lambda x: x, 500, 30, buffer_days=7),
    Uid2Vendor('truedata', 'truedata/from_td/prod/identity_graph/US', lambda x: x, 500, 30, buffer_days=7),
    Uid2Vendor('liveintent', 'LiveIntent', lambda x: f'{x[0:4]}{x[5:7]}{x[8:10]}', 500, 7),
    Uid2Vendor('mediawallah', 'graph/mediawallah.com/uid2', lambda x: x, 200, 7),
    Uid2Vendor('networld', 'graph/networld/uid2', lambda x: x, 50, 7),
    Uid2Vendor('wed', 'wed/uid2', lambda x: x, 100, 7),
    Uid2Vendor('equifax', 'narrative/equifax', lambda x: x, 100, 7),
    Uid2Vendor('pickwell', 'narrative/pickwell', lambda x: x, 100, 7),
    Uid2Vendor('rave', 'narrative/rave', lambda x: x, 100, 7),
    Uid2Vendor('kochava', 'narrative/kochava', lambda x: x, 100, 7),
    Uid2Vendor('digifish', 'Digifish', lambda x: x, 100, 30, buffer_days=7),
    Uid2Vendor('sirdata', 'graph/sirdata/euid', lambda x: x, 50, 7),
    Uid2Vendor('inquiro', 'graph/inquiro/uid2', lambda x: f'date={x}', 100, 30, buffer_days=7),
    Uid2Vendor('happygo', 'graph/happygo/uid2', lambda x: x, 50, 7),
    Uid2Vendor('bobbleai', 'bobbleai/prod_uid2_delivery', lambda x: x, 200, 30, buffer_days=7),
    Uid2Vendor(
        'loyaltyMarketing',
        'loyaltymarketing/uid2',
        lambda x: f'{x[0:4]}{x[5:7]}{x[8:10]}',
        50,
        30,
        buffer_days=7,
        last_seen_timestamp_threshold_months=18
    ),
    Uid2Vendor('rakuteninsight', 'rakuteninsight/uid2', lambda x: x, 100, 7),
]

large_vendor_configs = [Uid2Vendor('lifesight', 'graph/lifesight/uid2', lambda x: f'dt={x}', 1000, 7)]

test_folder = "" if str(Tags.environment()) == "prod" else "/test"
vendor_input_bucket = 'thetradedesk-useast-data-import'
investigation_path = f"s3://ttd-identity/data/vendors{test_folder}/investigateVendorDelivery"
etl_output_bucket = 'thetradedesk-useast-data-import'
etl_output_prefix = f'sxd-etl/uid2{test_folder}'


def get_job(vendor_configs: List[Uid2Vendor], job_size, execution_date, **kwargs):
    late_vendors = set()
    hook = AwsCloudStorage(conn_id='aws_default')
    s3_client = boto3.client('s3', 'us-east-1')

    for vendor_config in vendor_configs:
        vendor_name = vendor_config.name
        logging.info(vendor_name)
        input_prefix = vendor_config.input_path_prefix

        output_path = f's3://{etl_output_bucket}/{etl_output_prefix}/vendor={vendor_name}'
        last_etl_date = get_latest_etl_date_from_path(hook, output_path, execution_date)

        def input_path_generator(d: Optional[datetime] = None) -> str:
            date_suffix = vendor_config.date_parse_fun(d.strftime('%Y-%m-%d')) if d is not None else ""
            return f"s3://{vendor_input_bucket}/{input_prefix}/{date_suffix}"

        latest_input_date = LateVendor.get_latest_input_date(
            current_date=execution_date, last_etl_date=last_etl_date, input_path_generator=input_path_generator
        )

        # If latest input is found
        if latest_input_date is not None:
            expected_output = f'{output_path}/date={latest_input_date.strftime("%Y-%m-%d")}'

            # Investigation folder path
            input_date_formatted = vendor_config.date_parse_fun(latest_input_date.strftime('%Y-%m-%d'))
            investigation_path_with_date = f"{investigation_path}/{vendor_name}/{input_date_formatted}"

            # Check if the input exists in the investigation folder
            if does_folder_have_objects(investigation_path_with_date):
                logging.info(f"Skipping ETL: {vendor_name} data is under investigation in {investigation_path_with_date}")
                continue
            # If the input is not in investigation and hasn't been ETLed yet
            elif does_folder_have_objects(expected_output) is False:
                date = latest_input_date.strftime("%Y-%m-%d")
                path_date_postfix = vendor_config.date_parse_fun(date)
                path_to_check = input_path_generator(latest_input_date)

                # check on files' last modification time, give it 6 hours' cushion
                files = s3_client.list_objects(Bucket=vendor_input_bucket, Prefix=f'{input_prefix}/{path_date_postfix}')
                modified_time = [o['LastModified'].astimezone(timezone.utc) for o in files['Contents']]
                most_recent_time = max(modified_time)

                if datetime.now().astimezone(timezone.utc) - most_recent_time > timedelta(hours=6):
                    logging.info(f'{vendor_name}: find date {date} and data to process at {path_to_check}')

                    kwargs['ti'].xcom_push(key=f'etl-uid2-{job_size}-name', value=vendor_name)
                    kwargs['ti'].xcom_push(key=f'etl-uid2-{job_size}-path', value=path_to_check)
                    kwargs['ti'].xcom_push(key=f'etl-uid2-{job_size}-inPathWithoutDate', value=input_path_generator())
                    kwargs['ti'].xcom_push(key=f'etl-uid2-{job_size}-partition', value=vendor_config.final_partition)
                    kwargs['ti'].xcom_push(key=f'etl-uid2-{job_size}-date', value=date)
                    kwargs['ti'].xcom_push(key=f'etl-uid2-{job_size}-lsThreshold', value=vendor_config.last_seen_timestamp_threshold_months)
                    kwargs['ti'].xcom_push(key=f'etl-uid2-{job_size}-lastETLDate', value=last_etl_date.strftime("%Y-%m-%d"))

                    # Override the retention ratio threshold for specific vendors
                    retention_threshold = "0.0" if vendor_name in ["throtle", "liveintent"] else "0.5"
                    kwargs['ti'].xcom_push(key=f'etl-uid2-{job_size}-retentionRatioThreshold', value=retention_threshold)

                    return True

        # If no input is found or it has all been ETLed, check if the vendor is actually late for delivery
        if LateVendor.is_vendor_late(current_date=execution_date, last_etl_date=last_etl_date, cadence_days=vendor_config.cadence_days,
                                     buffer_days=vendor_config.buffer_days):
            missed_delivery = (last_etl_date + timedelta(days=vendor_config.cadence_days)).date()
            vendor = LateVendor(
                name=vendor_config.name,
                vendor_type=VendorTypes.uid2_type,
                input_path=input_path_generator(None),
                cadence_days=vendor_config.cadence_days,
                last_delivery=last_etl_date.date(),
                missed_delivery=missed_delivery,
                days_late=(execution_date.date() - missed_delivery).days
            )
            late_vendors.add(vendor)

    # run the status on delivery only once a day at midnight
    if execution_date.hour == 0 and len(late_vendors) > 0:
        LateVendor.post_slack_alert(late_vendors, job_size)

    return False


def etl_uid2(dag, vendor_configs, job_size, unit_capacity, timeout_hours, **kwargs):
    get_job_operator = OpTask(
        op=ShortCircuitOperator(
            task_id=f'get_job_{job_size}',
            python_callable=get_job,
            op_kwargs=dict(vendor_configs=vendor_configs, job_size=job_size),
            provide_context=True,
            retries=1
        )
    )

    cluster = IdentityClusters.get_cluster(f"etl_uid2_{job_size}_cluster", etl_uid2_dag, 16 * unit_capacity)

    cluster.add_sequential_body_task(
        IdentityClusters.task(
            class_name="jobs.identity.VendorPreETLCriticalAlerts",
            timeout_hours=timeout_hours,
            runDate=f'{{{{ task_instance.xcom_pull(task_ids="get_job_{job_size}", key="etl-uid2-{job_size}-date") }}}}',
            eldorado_configs=[
                ("VendorName", f'{{{{ task_instance.xcom_pull(task_ids="get_job_{job_size}", key="etl-uid2-{job_size}-name") }}}}'),
                (
                    "InPathWithoutDate",
                    f'{{{{ task_instance.xcom_pull(task_ids="get_job_{job_size}", key="etl-uid2-{job_size}-inPathWithoutDate") }}}}'
                ),
                ("PercentChangeThreshold", 30),
                ("LastETLDate", f'{{{{ task_instance.xcom_pull(task_ids="get_job_{job_size}", key="etl-uid2-{job_size}-lastETLDate") }}}}'),
                ("InvestigateDeliveryPath", investigation_path),
            ],
        )
    )

    cluster.add_sequential_body_task(
        IdentityClusters.task(
            class_name="jobs.identity.uid2.vendors.Uid2ETL",
            timeout_hours=timeout_hours,
            runDate=f'{{{{ task_instance.xcom_pull(task_ids="get_job_{job_size}", key="etl-uid2-{job_size}-date") }}}}',
            eldorado_configs=[
                ("VendorName", f'{{{{ task_instance.xcom_pull(task_ids="get_job_{job_size}", key="etl-uid2-{job_size}-name") }}}}'),
                ("InPath", f'{{{{ task_instance.xcom_pull(task_ids="get_job_{job_size}", key="etl-uid2-{job_size}-path") }}}}'),
                ("OutPartitions", f'{{{{ task_instance.xcom_pull(task_ids="get_job_{job_size}", key="etl-uid2-{job_size}-partition") }}}}'),
                (
                    "RetentionRatioThreshold",
                    f'{{{{ task_instance.xcom_pull(task_ids="get_job_{job_size}", key="etl-uid2-{job_size}-retentionRatioThreshold") }}}}'
                ),
                ("InvestigateDeliveryPath", investigation_path),
                (
                    "jobs.identity.uid2.vendors.Uid2ETL.LastSeenTimestampThresholdMonth",
                    f'{{{{ task_instance.xcom_pull(task_ids="get_job_{job_size}", key="etl-uid2-{job_size}-lsThreshold") }}}}'
                ),
                ("jobs.identity.uid2.vendors.Uid2ETL.OutPath", f's3://{etl_output_bucket}/{etl_output_prefix}'),
            ],
        )
    )

    dag >> get_job_operator >> cluster


etl_uid2(etl_uid2_dag, small_vendor_configs, "small", 20, 1)
etl_uid2(etl_uid2_dag, large_vendor_configs, "large", 288, 3)

final_dag: DAG = etl_uid2_dag.airflow_dag
