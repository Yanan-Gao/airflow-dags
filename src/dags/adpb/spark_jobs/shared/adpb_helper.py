import logging
from datetime import datetime

from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage

from ttd.ttdenv import TtdEnvFactory
from ttd.slack import slack_groups
from ttd.interop.logworkflow_callables import ExternalGateOpen

job_environment = TtdEnvFactory.get_from_system()

# Variables
team_ownership = slack_groups.hpc
dmp_root = 'ttd-datamarketplace'

received_counts_dataset_version = 5
received_counts_write_path = f"counts/{job_environment.dataset_write_env}/targetingdatareceivedcounts/v={received_counts_dataset_version}/"
received_counts_azure_write_path = f"counts/{job_environment.dataset_write_env}/targetingdatareceivedcountsazure/v={received_counts_dataset_version}/"

received_counts_by_id_type_dataset_version = 1
received_counts_by_id_type_write_path = f"counts/prod/targetingdatareceivedcountsbyuiidtype/v={received_counts_by_id_type_dataset_version}/"
received_counts_by_id_type_azure_write_path = f"counts/prod/targetingdatareceivedcountsbyuiidtypeazure/v={received_counts_by_id_type_dataset_version}/"

targeting_data_staging_s3 = "s3://thetradedesk-useast-qubole/warehouse/thetradedesk.db/sib-daily-sketches-targeting-data-ids-staging-hmh/"
targeting_data_staging_azure = "wasbs://ttd-datamarketplace@eastusttdlogs.blob.core.windows.net/counts/prod/sib-daily-sketches-targeting-data-ids-staging-hmh/"
targeting_data_final_s3 = "s3://thetradedesk-useast-qubole/warehouse/thetradedesk.db/sib-daily-sketches-targeting-data-ids/"
targeting_data_final_azure = "wasbs://ttd-datamarketplace@eastusttdlogs.blob.core.windows.net/counts/prod/sib-daily-sketches-targeting-data-ids/"
ttd_counts_azure = "wasbs://ttd-counts@ttdexportdata.blob.core.windows.net/counts/"

job_datetime_format: str = "%Y-%m-%dT%H:00:00"
TaskBatchGrain_Daily = 100002  # dbo.fn_Enum_TaskBatchGrain_Daily()
logworkflow_connection = 'lwdb'
logworkflow_sandbox_connection = 'sandbox-lwdb'
logworkflow_db = 'LogWorkflow'

# Aerospike Variables
cold_storage_address = '{{macros.ttd_extras.resolve_consul_url("ttd-coldstorage-onprem.aerospike.service.vaf.consul", port=4333)}}'
china_cold_storage_address = '{{macros.ttd_extras.resolve_consul_url("ttd-coldstorage-cn.aerospike.service.cn4.consul", port=3000, limit=1)}}'
china_cold_storage_namespace = 'ttd-coldstorage-cn'
counts_aerospike_namespace = 'ttd-sgsk-hmh'
aws_counts_aerospike_address = '{{macros.ttd_extras.resolve_consul_url("aerospike-use-sgsk-hmh.aerospike.service.useast.consul", port=3000, limit=1)}}'
azure_counts_aerospike_address = '{{macros.ttd_extras.resolve_consul_url("aerospike-va9-sgsk-hmh.aerospike.service.va9.consul", port=4333, limit=1)}}'
counts_aerospike_ttl = '-1'

vad_hc_address = '{{macros.ttd_extras.resolve_consul_url("aerospike-vad-aip.aerospike.service.vad.consul", port=3000, limit=10)}}'


# Lal Aerospike Variable
class LalAerospikeConstant:
    aerospike_address = "{{ macros.ttd_extras.resolve_consul_url('ttd-lal.aerospike.service.useast.consul', port=3000, limit=1) }}"
    aerospike_namespace = "ttd-lal"
    aerospike_set = "lal"
    job_emr_managed_master_security_group = "sg-008678553e48f48a3"
    job_emr_managed_slave_security_group = "sg-02fa4e26912fd6530"
    job_service_access_security_group = "sg-0b0581bc37bcac50a"
    job_ec2_subnet_id = "subnet-09a5010de5ff59ee5"
    ttl_in_days = 21
    ttl = 86400 * ttl_in_days  # 21 days


# XD variables
xd_graph_lookback_days = 12
xd_graph_bucket = 'thetradedesk-useast-data-import'
xd_graph_prefix_iav2 = 'sxd-etl/universal/iav2graph/'
xd_graph_date_key_iav2 = 'xd_graph_date_iav2'

# Pipeline Cadences
received_counts_cadence_in_hours = 6
targeting_data_user_extra_duration_in_hours = 3  # TDU runs after the ColdStorageScan job, so goes beyond the 6 hour duration


# Checks whether to push to SQL DB
def is_push_to_sql_enabled(**kwargs):
    return kwargs['push_to_sql_enabled']


# Checks whether to run Vertica Load
def is_vertica_load_enabled(**kwargs):
    return kwargs['verticaload_enabled']


# Gets vertica load objects for specified path
def get_vertica_load_object_list(**kwargs):
    hook = AwsCloudStorage(conn_id='aws_default')
    run_date = kwargs['datetime']
    date_str = datetime.strptime(run_date, "%Y-%m-%dT%H:00:00").strftime("%Y%m%d")
    hour_str = datetime.strptime(run_date, "%Y-%m-%dT%H:00:00").strftime("%-H")
    s3_prefix = f"{kwargs['s3_prefix']}date={date_str}/hour={hour_str}/"
    log_type_id = kwargs['log_type_id']
    keys = hook.list_keys(prefix=s3_prefix, bucket_name=dmp_root)
    if keys is None or len(keys) == 0:
        raise Exception(f'Expected non-zero number of files for VerticaLoad log type {log_type_id} at s3 path {s3_prefix}')
    logging.info(f'Found {len(keys)} files for log type {log_type_id}')
    object_list = []
    for key in keys:
        object_list.append((log_type_id, key, date_str, 1, 0, 1, 1, 0))  # CloudServiceId 1 == AWS, DataDomain 1 == TTD_RestOfWorld

    return object_list


def open_lwdb_gate_daily(gating_type, job_date_str: str, **kwargs):
    job_date = datetime.strptime(job_date_str, job_datetime_format)
    log_start_time = job_date.strftime('%Y-%m-%d')
    ExternalGateOpen(
        mssql_conn_id=logworkflow_connection if job_environment == TtdEnvFactory.prod else logworkflow_sandbox_connection,
        sproc_arguments={
            'gatingType': gating_type,
            'grain': TaskBatchGrain_Daily,
            'dateTimeToOpen': log_start_time
        }
    )
