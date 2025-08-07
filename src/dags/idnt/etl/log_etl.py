"""
### Log ETL
ETL process for AttributedEvent, AttributedEventResult, ClickTracker, ConversionTracker, EventTracker, and VideoEvent logs.
"""

from datetime import datetime, timedelta
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.statics import Tags, Executables
from ttd.eldorado.aws.emr_job_task import EmrJobTask

dag = DagHelpers.identity_dag(
    dag_id='log-etl',
    schedule_interval="0 9 * * *",
    start_date=datetime(2024, 8, 8),
    retries=2,
    retry_delay=timedelta(hours=1),
)

cluster = IdentityClusters.get_cluster(
    "LogEtlCluster", dag, 1200, ComputeType.STORAGE, use_delta=False, emr_release_label=Executables.emr_version_6
)

step_configs = [
    ('AttributedEvent', 'attributedevent', 500, True),
    ('AttributedEventResult', 'attributedevent', 500, True),
    ('ClickTracker', 'clicktracker', 500, False),
    ('ConversionTracker', 'conversiontracker', 1000, False),
    ('EventTracker', 'eventtracker', 1000, False),
    ('VideoEvent', 'videoevent', 1000, False),
]

test_folder = "" if Tags.environment() == "prod" else f"{Tags.environment()}-"
for (name, folder, partitions, partition_by_attributedevent) in step_configs:
    cluster.add_sequential_body_task(
        EmrJobTask(
            name=name,
            class_name='com.thetradedesk.etl.misc.LogEtl',
            eldorado_config_option_pairs_list=IdentityClusters.default_config_pairs(
                [('COUNTRY_CODE_TRANSLATION_PATH', 's3://ttd-identity/data-governance/countrycodes.csv'),
                 ('OUTPUT_PARTITIONS', f'{partitions}'),
                 ('OUTPUT_PARTITIONING_COLUMN', 'AttributedEventIntId1' if partition_by_attributedevent else ''),
                 ('SCHEMA_PATH', f'/schemas/ttd_{name.lower()}.csv'),
                 ('INPUT_PATH', f's3://thetradedesk-useast-logs-2/{folder}/verticaload/ttd_{name.lower()}/'),
                 ('OUTPUT_PATH', f's3://ttd-identity/datapipeline/sources/firstpartydata_v2/{test_folder}{name.lower()}/')],
                runDate_arg='RUN_DATE',
            ),
            timeout_timedelta=timedelta(hours=12),
            executable_path=Executables.etl_repo_executable,
        )
    )

dag >> cluster
final_dag = dag.airflow_dag
