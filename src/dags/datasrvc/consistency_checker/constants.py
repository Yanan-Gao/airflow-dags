datasrvc_consistency_checker_dag_failure_tsg = 'https://atlassian.thetradedesk.com/confluence/display/EN/DataService+Consistency+Checker+DAG+Failure+TSG'
lwdb_conn_id = 'lwdb'
snowflake_conn_id = 'snowflake'

lwdb_grain_hourly = 100001
lwdb_grain_daily = 100002

# lwdb external gating type name and id map
lwdb_external_gating_type_map = {
    'ExternalPerfAndPlatformHoulyConsistencyCheckerSingleCluster': 2000501,
    'ExternalPerfAndPlatformHoulyConsistencyCheckerCrossCluster': 2000502
}
