"""
Runs sprocs in provisioning to categorize 1pd as converters

Job Details:
    - Runs daily
"""
from datetime import datetime

from dags.hpc.converter_categorization.helper import ConverterCategorizationSprocConfig, \
    get_data_type_id_task, ConverterDataTypeId, get_purchase_task
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import hpc

dag_name = '1pd-converter-categorization'
start_date = datetime(2025, 2, 24)

# Prod variable
schedule_interval = "0 10 * * *"
# see https://airflow.adsrvr.org/connection/list/ access restricted to admin
mssql_conn_id = 'ttd_hpc_airflow'  # provdb-int.adsrvr.org
batch_size = 25000
test_only = 0
delay = 1800  # 30 mins

# Test variable
# schedule_interval = None
# mssql_conn_id = 'sandbox_ttd_hpc_airflow'  # intsb-prov-lst.adsrvr.org
# batch_size = 10000
# test_only = 1
# delay = 0.1

###########################################
# DAG Setup
###########################################

dag = TtdDag(
    dag_id=dag_name,
    start_date=start_date,
    schedule_interval=schedule_interval,
    run_only_latest=True,
    slack_channel=hpc.alarm_channel,
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/FwA1G',
    tags=[hpc.jira_team],
)
adag = dag.airflow_dag

sproc_config = ConverterCategorizationSprocConfig(
    mssql_conn_id=mssql_conn_id,
    batch_size=batch_size,
    test_only=test_only,
    delay=delay,
)

tracking_tag_categorized_by_offlinedataprovider_table_task = get_purchase_task(
    ConverterDataTypeId.TrackingTag, sproc_config, is_categorized_by_keywords=0
)
crm_data_first_task, crm_data_last_task = get_data_type_id_task(ConverterDataTypeId.CrmData, sproc_config)
imported_advertiser_data_with_base_bid_first_task, imported_advertiser_data_with_base_bid_last_task = get_data_type_id_task(
    ConverterDataTypeId.ImportedAdvertiserDataWithBaseBid, sproc_config
)
imported_advertiser_data_first_task, imported_advertiser_data_last_task = get_data_type_id_task(
    ConverterDataTypeId.ImportedAdvertiserData, sproc_config
)
tracking_tag_first_task, tracking_tag_last_task = get_data_type_id_task(ConverterDataTypeId.TrackingTag, sproc_config)
ecommerce_catalog_list_first_task, ecommerce_catalog_list_last_task = get_data_type_id_task(
    ConverterDataTypeId.EcommerceCatalogList, sproc_config
)

dag >> tracking_tag_categorized_by_offlinedataprovider_table_task >> crm_data_first_task
crm_data_last_task >> imported_advertiser_data_with_base_bid_first_task
imported_advertiser_data_with_base_bid_last_task >> imported_advertiser_data_first_task
imported_advertiser_data_last_task >> tracking_tag_first_task
tracking_tag_last_task >> ecommerce_catalog_list_first_task
