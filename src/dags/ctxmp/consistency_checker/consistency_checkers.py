from dags.datasrvc.consistency_checker.consistency_checker_dag import ConsistencyCheckerDag
from dags.datasrvc.consistency_checker.constants import datasrvc_consistency_checker_dag_failure_tsg
from dags.datasrvc.materialized_gating.materialized_gating_types import MATERIALIZED_GATING_TYPES
from dags.ctxmp.consistency_checker.validator import category_element_validate

ctxmp_team = 'CTXMP'
ctxmp_alarms_slack_channel = '#scrum-contextual-marketplace-alarms'


def register_dag(consistency_checker_dag):
    globals()[consistency_checker_dag.airflow_dag.dag_id] = consistency_checker_dag.airflow_dag


register_dag(
    ConsistencyCheckerDag(
        dag_id='checker-raw-category-element-hourly',
        owner=ctxmp_team,
        schedule_interval='0 * * * *',
        dependency_gates=[MATERIALIZED_GATING_TYPES['VerticaMergeIntoCategoryElementReport']],
        query_config='raw-data-category-hourly.sql',
        sources={'raw', 'perf_report'},
        cross_cluster_enabled=True,
        cross_cluster_dependency_gates=[
            MATERIALIZED_GATING_TYPES['ImportPlatformCategoryElementReportFromAzure'],
            MATERIALIZED_GATING_TYPES['ImportPlatformCategoryElementReportFromChina']
        ],
        cross_cluster_sources={'perf_report'},
        slack_channel=ctxmp_alarms_slack_channel,
        tsg=datasrvc_consistency_checker_dag_failure_tsg,
        validator=category_element_validate
    )
)
