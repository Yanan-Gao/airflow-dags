from datetime import timedelta, datetime
from typing import Dict
import logging
import pymssql
from timeit import default_timer as timer
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from ttd.el_dorado.v2.base import TtdDag
from ttd.tasks.op import OpTask

batch_lines = 1000

conn_params = {
    'provdb_repo_up': {
        'database': 'Provisioning',
        'appname': 'Scheduling events mover',
        'read_only': False,
        'as_dict': False,
        'timeout': 10
    },
    'lwdb_repo_up': {
        'database': 'LogWorkflow',
        'appname': 'Scheduling events mover',
        'read_only': False,
        'as_dict': False,
        'timeout': 10
    },
    'ttdglobal_repo_up': {
        'database': 'TTDGlobal',
        'appname': 'Scheduling events mover',
        'read_only': False,
        'as_dict': False,
        'timeout': 10
    }
}

dpsr_event_mover = 'dpsr-event-mover'

default_args = {
    'owner': 'DPRPTS',
}


def get_db_connection(conn_name):
    if conn_name not in conn_params:
        raise ValueError(f"Unknown connection name: {conn_name}")
    conn_info = BaseHook.get_connection(conn_name)
    conn_params[conn_name]['host'] = conn_info.host
    conn_params[conn_name]['port'] = conn_info.port
    conn_params[conn_name]['user'] = conn_info.login
    conn_params[conn_name]['password'] = conn_info.password
    conn_params[conn_name]['database'] = conn_info.schema
    return conn_params[conn_name]


lwdb_drop_temp_table = "drop table if exists #copiedEventsDAG;"
lwdb_create_temp_table = """
create table #copiedEventsDAG (
    ReportSchedulingEventTypeId tinyint,
    EventData nvarchar(50),
    ReportProviderSourceId bigint,
    CloudServiceId tinyint,
    DataDomainId tinyint,
    InsertionTime datetime,
    Repeats smallint
)
"""

lwdb_populate_temp_table = """
insert into #copiedEventsDAG
select top {top}
    ReportSchedulingEventTypeId,
    EventData,
    ReportProviderSourceId,
    CloudServiceId,
    DataDomainId,
    min(InsertionTime) as InsertionTime,
    count(*) as Repeats
from LogWorkflow.rptsched.OutgoingReportSchedulingEvent
group by
    ReportSchedulingEventTypeId,
    EventData,
    ReportProviderSourceId,
    CloudServiceId,
    DataDomainId
""".format(top=batch_lines)

lwdb_read_from_temp_table = """
select
    ReportSchedulingEventTypeId,
    EventData,
    ReportProviderSourceId,
    CloudServiceId,
    DataDomainId,
    InsertionTime,
    Repeats
from #copiedEventsDAG
"""
provdb_drop_temp_table = "drop table if exists #copiedEventsDAG;"
provdb_create_temp_table = """
create table #copiedEventsDAG (
    ReportSchedulingEventTypeId tinyint,
    EventData nvarchar(50),
    ReportProviderSourceId bigint,
    CloudServiceId tinyint,
    DataDomainId tinyint,
    InsertionTime datetime
)
"""
provdb_drop_res_table = "drop table if exists #affectedEvents;"
provdb_create_res_table = """
create table #affectedEvents (
    ReportSchedulingEventTypeId tinyint,
    ReportProviderSourceId bigint,
    DataDomainId tinyint
)
"""
provdb_populate_temp_table = """
insert into #copiedEventsDAG (
    ReportSchedulingEventTypeId,
    EventData,
    ReportProviderSourceId,
    CloudServiceId,
    DataDomainId,
    InsertionTime
)
values (%d, %s, %d, %d, %d, %s)
"""
provdb_update_existing_events = """
update Provisioning.rptsched.ReportSchedulingEvent
set InsertionTime = c.InsertionTime
from Provisioning.rptsched.ReportSchedulingEvent rse
inner join #copiedEventsDAG c
on  rse.ReportSchedulingEventTypeId = c.ReportSchedulingEventTypeId
    and rse.EventData = c.EventData
    and rse.ReportProviderSourceId = c.ReportProviderSourceId
    and rse.CloudServiceId = c.CloudServiceId
    -- The previous line may need to be removed once events dependency on
    -- CloudServiceId is deemed useless
    and rse.DataDomainId = c.DataDomainId
"""
provdb_insert_new_events = """
insert into Provisioning.rptsched.ReportSchedulingEvent
(
    ReportSchedulingEventTypeId,
    EventData,
    ReportProviderSourceId,
    CloudServiceId,
    DataDomainId,
    InsertionTime
)
output
    inserted.ReportSchedulingEventTypeId,
    inserted.ReportProviderSourceId,
    inserted.DataDomainId
into #affectedEvents
select
    ReportSchedulingEventTypeId,
    EventData,
    ReportProviderSourceId,
    CloudServiceId,
    DataDomainId,
    min(InsertionTime)
from #copiedEventsDAG c
where not exists
(
    select top 1 1
    from Provisioning.rptsched.ReportSchedulingEvent rse
    where
        rse.ReportSchedulingEventTypeId = c.ReportSchedulingEventTypeId
        and rse.EventData = c.EventData
        and rse.ReportProviderSourceId = c.ReportProviderSourceId
        and rse.CloudServiceId = c.CloudServiceId
        -- The previous line may need to be removed once events dependency on
        -- CloudServiceId is deemed useless
        and rse.DataDomainId = c.DataDomainId
)
group by
    ReportSchedulingEventTypeId,
    EventData,
    ReportProviderSourceId,
    CloudServiceId,
    DataDomainId
"""
provdb_read_res_table = """
select
    ReportSchedulingEventTypeId,
    ReportProviderSourceId,
    DataDomainId
from #affectedEvents
"""
lwdb_clean_processed_events = """
delete oe from rptsched.OutgoingReportSchedulingEvent oe
inner join #copiedEventsDAG ce
    on ce.ReportSchedulingEventTypeId = oe.ReportSchedulingEventTypeId
    and ce.EventData = oe.EventData
    and ce.ReportProviderSourceId = oe.ReportProviderSourceId
    and ce.CloudServiceId = oe.CloudServiceId
    -- The previous line may need to be removed once events dependency on
    -- CloudServiceId is deemed useless
    and ce.DataDomainId = oe.DataDomainId
"""
lwdb_get_vertica_cluster = """
select
    ReportProviderSourceId,
    VerticaClusterId,
    IsEnabled,
    IsReadingAllowed
from dbo.VerticaCluster
"""
provdb_update_report_provider_sources = """
;with rps_update as (
{update_lines_prov}
)
merge into rptsched.ReportProviderSource rps
using rps_update
on rps.ReportProviderSourceId = rps_update.ReportProviderSourceId
when matched then update
    set Enabled = rps_update.Enabled;
"""
ttdglobal_update_vertica_clusters = """
;with vc_update as (
{update_lines_ttdglobal}
)
merge into TTDGlobal.dbo.VerticaCluster vc
using vc_update
on vc.VerticaClusterId = vc_update.VerticaClusterId
when matched then update set
    IsEnabled = vc_update.IsEnabled,
    IsReadingAllowed = vc_update.IsReadingAllowed;
"""


def transfer_events():
    logging.info("trying to connect for events transfering")
    provdb_conn_info = get_db_connection('provdb_repo_up')
    lwdb_conn_info = get_db_connection('lwdb_repo_up')
    at_source: Dict[str, int] = {}
    inserted: Dict[str, int] = {}
    repeat_counts: Dict[str, int] = {}

    total = 0
    load_start = timer()
    with pymssql.connect(**provdb_conn_info) as conn_prov:
        with pymssql.connect(**lwdb_conn_info) as conn_lwf:
            with conn_prov.cursor() as cursor_prov:
                with conn_lwf.cursor() as cursor_lwf:
                    # First four steps make sure that empty temporary tables exist on both LWDB and ProvDB sides (drop and create)
                    logging.debug('Executing lwdb_drop_temp_table')
                    exec_start = timer()
                    cursor_lwf.execute(lwdb_drop_temp_table)
                    exec_end = timer()
                    logging.info('Executed lwdb_drop_temp_table in {} seconds'.format(exec_end - exec_start))

                    logging.debug('Executing lwdb_create_temp_table')
                    exec_start = timer()
                    cursor_lwf.execute(lwdb_create_temp_table)
                    exec_end = timer()
                    logging.info('Executed lwdb_create_temp_table in {} seconds'.format(exec_end - exec_start))

                    logging.debug('Executing provdb_drop_temp_table')
                    exec_start = timer()
                    cursor_prov.execute(provdb_drop_temp_table)
                    exec_end = timer()
                    logging.info('Executed provdb_drop_temp_table in {} seconds'.format(exec_end - exec_start))

                    logging.debug('Executing provdb_create_temp_table')
                    exec_start = timer()
                    cursor_prov.execute(provdb_create_temp_table)
                    exec_end = timer()
                    logging.info('Executed provdb_create_temp_table in {} seconds'.format(exec_end - exec_start))

                    # Two steps to create the auxiliary table where we store the records about events inserted
                    logging.debug('Executing provdb_drop_res_table')
                    exec_start = timer()
                    cursor_prov.execute(provdb_drop_res_table)
                    exec_end = timer()
                    logging.info('Executed provdb_drop_res_table in {} seconds'.format(exec_end - exec_start))

                    logging.debug('Executing provdb_create_res_table')
                    exec_start = timer()
                    cursor_prov.execute(provdb_create_res_table)
                    exec_end = timer()
                    logging.info('Executed provdb_create_res_table in {} seconds'.format(exec_end - exec_start))

                    # Make a snapshot of unique events waiting to be copied in a temporary table
                    logging.debug('Executing lwdb_populate_temp_table')
                    exec_start = timer()
                    cursor_lwf.execute(lwdb_populate_temp_table)
                    exec_end = timer()
                    logging.info('Executed lwdb_populate_temp_table in {} seconds'.format(exec_end - exec_start))

                    # Read from temporary table at LWDB and insert into temporary table at ProvDB
                    logging.debug('Executing lwdb_read_from_temp_table')
                    exec_start = timer()
                    cursor_lwf.execute(lwdb_read_from_temp_table)
                    prev_ts = timer()
                    total = 0
                    res_list = cursor_lwf.fetchmany(batch_lines)
                    try:
                        while len(res_list) > 0:
                            dst_list = []

                            # rse_item will contain
                            # [0] ReportSchedulingEventTypeId
                            # [1] EventData
                            # [2] ReportProviderSourceId
                            # [3] CloudServiceId
                            # [4] DataDomainId
                            # [5] min(InsertionTime) as InsertionTime
                            # [6] count(*) as Repeats
                            for res_item in res_list:
                                elem_key = f'{res_item[0]}.{res_item[2]}.{res_item[4]}'
                                # We only report on repeats to monitor some vague bug in LogWorkflow
                                if res_item[6] > 1:
                                    if elem_key in repeat_counts:
                                        repeat_counts[elem_key] += res_item[6]
                                    else:
                                        repeat_counts[elem_key] = res_item[6]
                                if elem_key in at_source:
                                    at_source[elem_key] += 1
                                else:
                                    at_source[elem_key] = 1
                                dst_list.append(res_item[:-1:])
                            cursor_prov.executemany(provdb_populate_temp_table, dst_list)
                            now_ts = timer()
                            total += len(res_list)
                            logging.debug(
                                'Processed next {} rows in {} seconds with total so far {}'
                                .format(len(res_list), (now_ts - prev_ts), total)
                            )
                            prev_ts = now_ts
                            res_list = cursor_lwf.fetchmany(batch_lines)
                    except pymssql.Error as qerr:
                        logging.error(f'Error occured while sending {total} lines')
                        raise
                    exec_end = timer()
                    logging.info('Executed events upload in {} seconds with total {} lines inserted'.format(exec_end - exec_start, total))

                    # Update time for events already present in ProvDB persistent table to record duplicates
                    logging.debug('Executing provdb_update_existing_events')
                    exec_start = timer()
                    cursor_prov.execute(provdb_update_existing_events)
                    exec_end = timer()
                    logging.info(
                        'Executed provdb_update_existing_events in {} seconds with result {}'
                        .format(exec_end - exec_start, cursor_prov.rowcount)
                    )

                    # Insert new events from temporary table to persistent table at ProvDB
                    logging.debug('Executing provdb_insert_new_events')
                    exec_start = timer()
                    cursor_prov.execute(provdb_insert_new_events)
                    exec_end = timer()
                    logging.info(
                        'Executed provdb_insert_new_events in {} seconds with result {}'
                        .format(exec_end - exec_start, cursor_prov.rowcount)
                    )
                    # Commit inserts and updates at ProvDB
                    conn_prov.commit()

                    # Read records about events inserted at the previous step from auxiliary table
                    logging.debug('Executing provdb_read_res_table')
                    exec_start = timer()
                    cursor_prov.execute(provdb_read_res_table)
                    prev_ts = timer()
                    total = 0
                    res_list = cursor_prov.fetchmany(batch_lines)
                    try:
                        while len(res_list) > 0:
                            # res_item will contain information on inserted events (with possible duplicates)
                            # [0] ReportSchedulingEventTypeId
                            # [1] ReportProviderSourceId
                            # [2] DataDomainId
                            for res_item in res_list:
                                elem_key = f'{res_item[0]}.{res_item[1]}.{res_item[2]}'
                                if elem_key in inserted:
                                    inserted[elem_key] += 1
                                else:
                                    inserted[elem_key] = 1
                            now_ts = timer()
                            total += len(res_list)
                            logging.debug(
                                'Processed next {} rows in {} seconds with total so far {}'
                                .format(len(res_list), (now_ts - prev_ts), total)
                            )
                            prev_ts = now_ts
                            res_list = cursor_prov.fetchmany(batch_lines)
                    except pymssql.Error as qerr:
                        logging.error(f'Error occured while reading {total} lines')
                        raise
                    exec_end = timer()
                    logging.info('Executed results accounting in {} seconds with total {} lines read'.format(exec_end - exec_start, total))

                    # Clear from persistent table at LWDB events that were captured in snapshot previously
                    logging.debug('Executing lwdb_clean_processed_events')
                    exec_start = timer()
                    cursor_lwf.execute(lwdb_clean_processed_events)
                    exec_end = timer()
                    logging.info(
                        'Executed lwdb_clean_processed_events in {} seconds with result {}'
                        .format(exec_end - exec_start, cursor_lwf.rowcount)
                    )
                    # Commit deletion at LWDB
                    conn_lwf.commit()

    load_end = timer()
    logging.info('Loaded {} lines in {} seconds'.format(total, (load_end - load_start)))
    logging.info(f'Got from source {at_source}, repeats {repeat_counts}, inserted {inserted}')


def transfer_clusters_statuses():
    logging.info("trying to connect for Vertica cluster statuses update")
    provdb_conn_info = get_db_connection('provdb_repo_up')
    lwdb_conn_info = get_db_connection('lwdb_repo_up')
    ttdglobal_conn_info = get_db_connection('ttdglobal_repo_up')

    # Read into memory the cluster statuses from LWDB, forming records to insert on the fly
    provdb_updates = ''
    ttdglobal_updates = ''
    with pymssql.connect(**lwdb_conn_info) as conn_lwf:
        with conn_lwf.cursor() as cursor_lwf:
            logging.debug('Executing lwdb_get_vertica_cluster')
            exec_start = timer()
            cursor_lwf.execute(lwdb_get_vertica_cluster)
            status_list = cursor_lwf.fetchmany(batch_lines)
            while len(status_list) > 0:
                # status_item will contain
                # [0] ReportProviderSourceId
                # [1] VerticaClusterId
                # [2] IsEnabled
                # [3] IsReadingAllowed
                for status_item in status_list:
                    if status_item[2]:
                        enabled = 1
                    else:
                        enabled = 0
                    if status_item[3]:
                        reading_allowed = 1
                    else:
                        reading_allowed = 0
                    if status_item[0] is not None:
                        if len(provdb_updates) > 0:
                            provdb_updates += "\nunion all\n"
                        provdb_updates += f"select {status_item[0]} as ReportProviderSourceId, {reading_allowed} as Enabled"
                    if status_item[1] is not None:
                        if len(ttdglobal_updates) > 0:
                            ttdglobal_updates += "\nunion all\n"
                        ttdglobal_updates += f"select '{status_item[1]}' as VerticaClusterId, {enabled} as IsEnabled, {reading_allowed} as IsReadingAllowed"
                status_list = cursor_lwf.fetchmany(batch_lines)
            exec_end = timer()
            logging.info('Executed lwdb_get_vertica_cluster in {} seconds'.format(exec_end - exec_start))

    # Update cluster statuses at ProvDB
    with pymssql.connect(**provdb_conn_info) as conn_prov:
        with conn_prov.cursor() as cursor_prov:
            logging.debug('Executing ')
            exec_start = timer()
            sql = provdb_update_report_provider_sources.format(update_lines_prov=provdb_updates)
            logging.debug(f"provdb is {sql}")
            cursor_prov.execute(sql)
            exec_end = timer()
            logging.info('Executed provdb_update_report_provider_sources in {} seconds'.format(exec_end - exec_start))
            # Commit updates at ProvDB
            conn_prov.commit()

    # Update cluster statuses at TTDGlobal
    with pymssql.connect(**ttdglobal_conn_info) as conn_ttdg:
        with conn_ttdg.cursor() as cursor_ttdg:
            logging.debug('Executing ')
            exec_start = timer()
            sql = ttdglobal_update_vertica_clusters.format(update_lines_ttdglobal=ttdglobal_updates)
            logging.debug(f"ttdg is {sql}")
            cursor_ttdg.execute(sql)
            exec_end = timer()
            logging.info('Executed ttdglobal_update_vertica_clusters in {} seconds'.format(exec_end - exec_start))
            # Commit updates at TTDGlobal
            conn_ttdg.commit()


dag = TtdDag(
    dag_id=dpsr_event_mover,
    default_args=default_args,
    dagrun_timeout=timedelta(seconds=150),
    schedule_interval='*/3 * * * *',  # Resolution sproc works on the same cadence
    max_active_runs=1,
    run_only_latest=True,  # transforms into Airflow.DAG.catchup=False
    depends_on_past=True,
    start_date=datetime(2024, 9, 2, 0, 0, 0),
    tags=['DPRPTS', 'DPSRInfra', 'ReportUpdater'],
)

# DAG Task Flow

move_events = OpTask(op=PythonOperator(task_id="do_move_events", execution_timeout=timedelta(seconds=20), python_callable=transfer_events))
update_vcs = OpTask(
    op=PythonOperator(
        task_id="do_update_vertica_clusters", execution_timeout=timedelta(seconds=20), python_callable=transfer_clusters_statuses
    )
)

adag = dag.airflow_dag

dag >> move_events
dag >> update_vcs
