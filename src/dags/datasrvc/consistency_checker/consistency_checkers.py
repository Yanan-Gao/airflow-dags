from dags.datasrvc.consistency_checker.consistency_checker_dag import ConsistencyCheckerDag
from dags.datasrvc.consistency_checker.constants import datasrvc_consistency_checker_dag_failure_tsg, lwdb_grain_hourly, lwdb_external_gating_type_map
from dags.datasrvc.consistency_checker.validator import raw_data_cloud_tenant_validate
from dags.datasrvc.materialized_gating.materialized_gating_types import MATERIALIZED_GATING_TYPES
from dags.datasrvc.consistency_checker.util import open_external_gate_callback, open_cross_cluster_external_gate_callback, print_attribution_data_element_debugging_info

datasrvc_team = 'DATASRVC'
datasrvc_alarms_slack_channel = '#scrum-data-services-alarms'


def register_dag(consistency_checker_dag):
    globals()[consistency_checker_dag.airflow_dag.dag_id] = consistency_checker_dag.airflow_dag


register_dag(
    ConsistencyCheckerDag(
        dag_id='checker-perf-platform-hourly',
        owner=datasrvc_team,
        schedule_interval='0 * * * *',
        dependency_gates=
        [MATERIALIZED_GATING_TYPES['VerticaMergeIntoPerformanceReport'], MATERIALIZED_GATING_TYPES['VerticaMergeIntoRTBPlatformReport']],
        query_config='perf-platform-hourly.sql',
        sources={'raw', 'perf_report', 'super_report'},
        on_consistency_success=[
            open_external_gate_callback(
                lwdb_external_gating_type_map['ExternalPerfAndPlatformHoulyConsistencyCheckerSingleCluster'], lwdb_grain_hourly
            )
        ],
        cross_cluster_enabled=True,
        cross_cluster_dependency_gates=[
            MATERIALIZED_GATING_TYPES['ImportPerformanceReportFromAzure'], MATERIALIZED_GATING_TYPES['ImportPlatformReportFromAzure'],
            MATERIALIZED_GATING_TYPES['ImportPerformanceReportFromChina'], MATERIALIZED_GATING_TYPES['ImportPlatformReportFromChina']
        ],
        cross_cluster_sources={'perf_report', 'super_report'},
        on_cross_cluster_consistency_success=[
            open_cross_cluster_external_gate_callback(
                lwdb_external_gating_type_map['ExternalPerfAndPlatformHoulyConsistencyCheckerCrossCluster'], lwdb_grain_hourly
            )
        ],
        slack_channel=datasrvc_alarms_slack_channel,
        tsg=datasrvc_consistency_checker_dag_failure_tsg
    )
)

register_dag(
    ConsistencyCheckerDag(
        dag_id='checker-perf-data-element-hourly',
        owner=datasrvc_team,
        schedule_interval='0 * * * *',
        dependency_gates=[MATERIALIZED_GATING_TYPES['VerticaMergeIntoDataElementReport']],
        query_config='perf-data-element-hourly.sql',
        sources={'raw', 'perf_report'},
        cross_cluster_enabled=True,
        cross_cluster_dependency_gates=[
            MATERIALIZED_GATING_TYPES['ImportPlatformDataElementReportFromAzure'],
            MATERIALIZED_GATING_TYPES['ImportPlatformDataElementReportFromChina']
        ],
        cross_cluster_sources={'perf_report'},
        slack_channel=datasrvc_alarms_slack_channel,
        tsg=datasrvc_consistency_checker_dag_failure_tsg,
        on_consistency_failure=[print_attribution_data_element_debugging_info],
        retry_on_inconsistent_result=True
    )
)

register_dag(
    ConsistencyCheckerDag(
        dag_id='checker-perf-fee-feature-hourly',
        owner=datasrvc_team,
        schedule_interval='0 * * * *',
        dependency_gates=[MATERIALIZED_GATING_TYPES['VerticaMergeIntoFeeFeaturesReport']],
        query_config='perf-fee-feature-hourly.sql',
        sources={'raw', 'perf_report'},
        cross_cluster_enabled=True,
        cross_cluster_dependency_gates=[
            MATERIALIZED_GATING_TYPES['ImportPlatformFeeFeatureReportFromAzure'],
            MATERIALIZED_GATING_TYPES['ImportPlatformFeeFeatureReportFromChina']
        ],
        cross_cluster_sources={'perf_report'},
        slack_channel=datasrvc_alarms_slack_channel,
        tsg=datasrvc_consistency_checker_dag_failure_tsg
    )
)

register_dag(
    ConsistencyCheckerDag(
        dag_id='checker-cumulative-perf-daily',
        owner=datasrvc_team,
        schedule_interval='0 0 * * *',
        dependency_gates=[MATERIALIZED_GATING_TYPES['VerticaMergeIntoCumulativePerformanceReport']],
        query_config='cumulative-perf-daily.sql',
        sources={'raw', 'perf_report'},
        cross_cluster_enabled=True,
        cross_cluster_sources={'perf_report'},
        slack_channel=datasrvc_alarms_slack_channel,
        tsg=datasrvc_consistency_checker_dag_failure_tsg
    )
)

register_dag(
    ConsistencyCheckerDag(
        dag_id='checker-raw-data-cloud-tenant-hourly',
        owner=datasrvc_team,
        schedule_interval='0 * * * *',
        dependency_gates=[
            MATERIALIZED_GATING_TYPES['VerticaLoadBidRequest'], MATERIALIZED_GATING_TYPES['VerticaLoadBidFeedback'],
            MATERIALIZED_GATING_TYPES['VerticaLoadClickTracker'], MATERIALIZED_GATING_TYPES['VerticaLoadConversionTracker'],
            MATERIALIZED_GATING_TYPES['VerticaLoadVideoEvent']
        ],
        query_config='raw-data-cloud-tenant-hourly.sql',
        validator=raw_data_cloud_tenant_validate,
        slack_channel=datasrvc_alarms_slack_channel,
        tsg=datasrvc_consistency_checker_dag_failure_tsg
    )
)

register_dag(
    ConsistencyCheckerDag(
        dag_id='checker-perf-platform-late-data-hourly',
        owner=datasrvc_team,
        schedule_interval='0 * * * *',
        dependency_gates=[
            MATERIALIZED_GATING_TYPES['VerticaLateDataMergeIntoPerformanceReport'],
            MATERIALIZED_GATING_TYPES['VerticaLateDataMergeIntoPlatformReport']
        ],
        query_config='perf-platform-late-data-hourly.sql',
        clusters=['USEast01', 'USWest01'],
        sources={'raw', 'perf_report', 'plat_report'},
        cross_cluster_enabled=True,
        cross_cluster_sources={'perf_report', 'plat_report'},
        cross_cluster_exported_data_domains=set(),
        sensor_poke_timeout=2 * 24 * 60 * 60,  # timeout 2 days
        slack_channel=datasrvc_alarms_slack_channel,
        tsg=datasrvc_consistency_checker_dag_failure_tsg
    )
)

register_dag(
    ConsistencyCheckerDag(
        dag_id='checker-perf-platform-ias-late-data-daily',
        owner=datasrvc_team,
        schedule_interval='0 0 * * *',
        dependency_gates=[
            MATERIALIZED_GATING_TYPES['IntegralVerticaMergeIntoPerformanceReport'],
            MATERIALIZED_GATING_TYPES['IntegralVerticaMergeIntoPlatformReport'],
            MATERIALIZED_GATING_TYPES['VerticaViewabilityMergeIntoPlatformReport'],
            MATERIALIZED_GATING_TYPES['VerticaViewabilityCopyIntoPerformanceReport']
        ],
        query_config='perf-platform-ias-late-data-daily.sql',
        clusters=['USEast01', 'USWest01', 'USEast03', 'USWest03'],
        sources={'raw', 'perf_report', 'plat_report'},
        cross_cluster_enabled=True,
        cross_cluster_dependency_gates=[
            MATERIALIZED_GATING_TYPES['ImportIntegralPerformanceReportFromAzure'],
            MATERIALIZED_GATING_TYPES['ImportIntegralPlatformReportFromAzure']
        ],
        cross_cluster_sources={'perf_report', 'plat_report'},
        cross_cluster_exported_data_domains=set(['Walmart_US']),
        sensor_poke_interval=30 * 60,  # poke every 30 minute
        sensor_poke_timeout=5 * 24 * 60 * 60,  # timeout 5 days
        slack_channel=datasrvc_alarms_slack_channel,
        tsg=datasrvc_consistency_checker_dag_failure_tsg
    )
)
