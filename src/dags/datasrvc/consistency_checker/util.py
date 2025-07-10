import os
import datetime

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mssql_hook import MsSqlHook
from dags.datasrvc.consistency_checker.constants import lwdb_conn_id

import vertica_python


def read_query_file(query_config, owner):
    # Construct the full path to the SQL script
    dag_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

    sql_script_path = os.path.join(dag_dir, owner.lower(), 'consistency_checker', 'queries', query_config)

    # Read the SQL script file
    with open(sql_script_path, 'r') as file:
        query = file.read()
        return query


def open_external_gate(gating_type_Id, grain, datetime_to_open, task_variant_id):
    hook = MsSqlHook(mssql_conn_id=lwdb_conn_id, schema='LogWorkflow')
    conn = hook.get_conn()
    conn.autocommit(True)
    cursor = conn.cursor(as_dict=False)
    sql = f"""
        exec [WorkflowEngine].[prc_OpenExternalGate]
            @gatingType = {gating_type_Id},
            @grain = {grain},
            @dateTimeToOpen = '{datetime_to_open.strftime("%Y-%m-%d %H:%M")}',
            @taskVariantId = {task_variant_id if task_variant_id is not None else 'null'},
            @failOnEmptyRowUpdate = 0
    """

    print("open external gate query:", sql)

    cursor.execute(sql)


def open_external_gate_callback(gating_type_id, grain):

    def execute(dag_id, dag_start_date, cluster_info):
        open_external_gate(gating_type_id, grain, dag_start_date, cluster_info.task_variant)

    return execute


def open_cross_cluster_external_gate_callback(gating_type_id, grain):

    def execute(dag_id, dag_start_date):
        open_external_gate(gating_type_id, grain, dag_start_date, None)

    return execute


def print_attribution_data_element_debugging_info(dag_id, dag_start_date, cluster_info):

    print("Logging more debugging info")
    conn = BaseHook.get_connection(cluster_info.conn_id)
    conn_info = {'host': conn.host, 'port': conn.port, 'user': conn.login, 'password': conn.password, 'database': conn.schema}
    conn = vertica_python.connect(**conn_info)

    startDate = dag_start_date
    endDate = dag_start_date + datetime.timedelta(hours=1)

    query = f"""
         SELECT AE.ReportHourUtc, AE.epoch as AE_EPOCH, AEDE.epoch as AEDE_EPOCH, AER.epoch as AER_EPOCH,
               (sum(CASE WHEN ((AER.AttributionMethodId = 2) AND (AER.CampaignReportingColumnId = 1)) THEN (AER.AttributedCount * AER.ConversionMultiplier) ELSE 0::numeric(18,0) END))::int AS Touch1Count,
               (sum(CASE WHEN ((AER.AttributionMethodId = 2) AND (AER.CampaignReportingColumnId = 2)) THEN (AER.AttributedCount * AER.ConversionMultiplier) ELSE 0::numeric(18,0) END))::int AS Touch2Count,
               count(1) as RowCount,
               count(distinct AE.LogFileId) as LogFileIdCount,
               count(distinct AE.ConversionTrackerLogFileId) as ConversionTrackerLogFileIdCount
        FROM ((ttdprotected.AttributedEvent AE JOIN ttdprotected.AttributedEventDataElement AEDE USING (ReportHourUtc, AttributedEventLogFileId, AttributedEventIntId1, AttributedEventIntId2, ConversionTrackerLogFileId, ConversionTrackerIntId1, ConversionTrackerIntId2)) JOIN ttdprotected.AttributedEventResult AER/*+skip_projs('ttdprotected.AttributedEventResult')*/ USING (ReportHourUtc, AttributedEventLogFileId, AttributedEventIntId1, AttributedEventIntId2, ConversionTrackerLogFileId, ConversionTrackerIntId1, ConversionTrackerIntId2))
        WHERE AE.ReportHourUtc >= '{startDate.strftime("%Y-%m-%d %H:%M")}' and AE.ReportHourUtc < '{endDate.strftime("%Y-%m-%d %H:%M")}' and AE.LateDataProviderId is null
        GROUP BY AE.ReportHourUtc, AE.epoch, AEDE.epoch, AER.epoch
    """

    cur = conn.cursor('dict')
    cur.execute(query)
    res = cur.fetchall()
    conn.close()
    print(res)


def disable_vertica_cluster(dag_id, dag_start_date, cluster_info):

    clusters_to_disable = [cluster_info.name]
    if cluster_info.name == 'USEast01' or cluster_info.name == 'USWest01':
        sc09 = cluster_info.name + 'sc09'
        clusters_to_disable.append(sc09)

    print("Going to disable vertica clusters: ", clusters_to_disable)

    hook = MsSqlHook(mssql_conn_id=lwdb_conn_id, schema='LogWorkflow')
    conn = hook.get_conn()
    conn.autocommit(True)
    cur = conn.cursor(as_dict=True)

    for cluster_to_disable in clusters_to_disable:
        sql = f"""
            select VerticaClusterId, IsEnabled, IsReadingAllowed, IsWritingAllowed, IsAttributionAllowed from dbo.VerticaCluster where VerticaClusterId = '{cluster_to_disable}'
        """
        cur.execute(sql)
        res = cur.fetchall()
        print(f"Before disabling, cluster {cluster_to_disable} status is: ", res)

        sql = f"""
            exec dbo.prc_DisableVerticaCluster @verticaClusterId = '{cluster_to_disable}', @commit = 1
        """
        cur.execute(sql)
        print(
            f"Cluster {cluster_to_disable} is disabled now. Once the issue is fixed, we need to restore the cluster to the previous status."
        )

    cur.close()
    conn.close()
