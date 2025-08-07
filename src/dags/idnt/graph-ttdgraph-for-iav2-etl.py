from airflow import DAG

from datetime import datetime, timedelta
from ttd.el_dorado.v2.emr import EmrJobTask
from ttd.ttdenv import TtdEnvFactory

from dags.idnt.feature_flags import FeatureFlags
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_datasets import IdentityDatasets
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.statics import Executables, RunTimes

env = TtdEnvFactory.get_from_system()
read_env = TtdEnvFactory.get_from_system().dataset_read_env
test_folder = '' if env.dataset_write_env == 'prod' else '/test'

ttdgraph_for_iav2_etl = DagHelpers.identity_dag(
    dag_id='graph-ttdgraph-for-iav2-etl',
    # We do not ETL the vertica/dataserver load for Sunday run. Only the first stage
    schedule_interval='0 22 * * SUN',
    start_date=datetime(2024, 8, 11),
    run_only_latest=False
)

adbrain_graph_name = 'adbrain_legacy'
abrain_household_graph_name = 'adbrain_household_legacy'

raw_adbrain_variants = [IdentityDatasets.ttdgraph_v1]
if FeatureFlags.enable_cookieless_graph_generation:
    cookieless = "cookieless"
    cookieless_raw_graph_name = 'cookielessttdgraph4iav2'
    cookieless_etl_graph_name = 'cookielessnextgen4iav2'
    # TODO(leonid.vasilev): Moving under the feature as is, but the production pipeline should not
    # depend on the cookieless one.
    raw_adbrain_variants.append(IdentityDatasets.cookieless_adbrain)

check_input_graphs = DagHelpers.check_datasets(raw_adbrain_variants)

etl_cluster = IdentityClusters.set_step_concurrency(
    IdentityClusters.get_cluster(
        "etl_cluster", ttdgraph_for_iav2_etl, 3500, ComputeType.STORAGE, use_delta=False, emr_release_label=Executables.emr_version_6
    )
)

etl_cluster.add_parallel_body_task(
    EmrJobTask(
        name=f"{adbrain_graph_name}_etl",
        class_name=Executables.etl_driver_class,
        eldorado_config_option_pairs_list=[
            ('config.resource', 'application.conf'),
            ('INPUT_PATH', f's3://ttd-identity/data/prod/graph/{read_env}/{adbrain_graph_name}/person_relabeller'),
            ('OUTPUT_PATH', f's3://thetradedesk-useast-data-import/sxd-etl{test_folder}/universal/{adbrain_graph_name}'),
            ('NUM_PARTITIONS', '800'),
            ('METRICS_PATH', f's3://thetradedesk-useast-data-import/sxd-etl{test_folder}/metrics/'),
            ('GATEWAY_ADDRESS', 'prom-push-gateway.adsrvr.org:80'),
            ('LOCAL', 'false'),
            ('HOUSEHOLD_OUTPUT_PATH', f's3://thetradedesk-useast-data-import/sxd-etl{test_folder}/universal/{abrain_household_graph_name}'),
            # just because vendor name will show on grafana dashboards, where we need to use adbrain for nextgen
            ('VENDOR_NAME', 'adbrain'),
            ('runDate', RunTimes.previous_full_day)
        ],
        timeout_timedelta=timedelta(hours=6),
        executable_path=Executables.etl_repo_executable,
        configure_cluster_automatically=False,
        action_on_failure="CONTINUE",
    )
)

if FeatureFlags.enable_cookieless_graph_generation:
    etl_cluster.add_parallel_body_task(
        EmrJobTask(
            name=f"{cookieless_raw_graph_name}_etl",
            class_name=Executables.etl_driver_class,
            eldorado_config_option_pairs_list=[
                ('config.resource', 'application.conf'),
                ('INPUT_PATH', f's3://ttd-identity/data/prod/graph/{read_env}/{cookieless_raw_graph_name}/person_relabeller'),
                ('OUTPUT_PATH', f's3://thetradedesk-useast-data-import/sxd-etl{test_folder}/universal/{cookieless_etl_graph_name}'),
                ('NUM_PARTITIONS', '800'),
                ('METRICS_PATH', f's3://thetradedesk-useast-data-import/sxd-etl{test_folder}/metrics/'),
                ('GATEWAY_ADDRESS', 'prom-push-gateway.adsrvr.org:80'),
                ('LOCAL', 'false'),
                (
                    'HOUSEHOLD_OUTPUT_PATH',
                    f's3://thetradedesk-useast-data-import/sxd-etl{test_folder}/universal/{cookieless_etl_graph_name}_household'
                ),
                # just because vendor name will show on grafana dashboards, where we need to use adbrain for nextgen
                ('VENDOR_NAME', 'adbrain'),
                ('runDate', RunTimes.previous_full_day)
            ],
            timeout_timedelta=timedelta(hours=6),
            executable_path=Executables.etl_repo_executable,
            configure_cluster_automatically=False,
            action_on_failure="CONTINUE",
        )
    )

ttdgraph_for_iav2_etl >> check_input_graphs >> etl_cluster

final_dag: DAG = ttdgraph_for_iav2_etl.airflow_dag
