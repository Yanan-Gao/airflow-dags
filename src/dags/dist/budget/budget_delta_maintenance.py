from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Tuple

import logging
from dags.dist.budget.alerting.budget_slack_alert_processor import PySparkSqlTask
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.general_purpose.m7a import M7a
from ttd.ec2.emr_instance_types.memory_optimized.r7a import R7a
from ttd.slack.slack_groups import dist
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator

logger = logging.getLogger(__name__)

dag_name = "budget-delta-tableshousekeeping"

# Environment
env = TtdEnvFactory.get_from_system()

version = '2'
s3_bucket = 'ttd-budget-calculation-lake'
s3_prefix = f's3://ttd-budget-calculation-lake/env={env.dataset_write_env}/'
retention_hours = 24 * 2


def discover_delta_tables() -> List[Tuple[str, str, List[str]]]:
    return [(
        'virtual_throttle_metric_delta',
        "s3://ttd-budget-calculation-lake/env=prod/virtual-aggregate-pacing-statistics/v=2/metric=throttle_metric_delta",
        ['ReportDate', 'CampaignId']
    ),
            (
                'virtual_throttle_metric_campaign_delta',
                "s3://ttd-budget-calculation-lake/env=prod/virtual-aggregate-pacing-statistics/v=2/metric=throttle_metric_campaign_delta",
                ['ReportDate', 'CampaignId']
            ),
            (
                'virtual_hard_min_throttle_delta',
                "s3://ttd-budget-calculation-lake/env=prod/virtual-aggregate-pacing-statistics/v=2/metric=hard_min_throttle_delta",
                ['ReportDate', 'CampaignId']
            ),
            (
                'throttle_metric_delta',
                "s3://ttd-budget-calculation-lake/env=prod/aggregate-pacing-hourly-statistics/v=2/metric=throttle_metric_delta",
                ['ReportUntilHourUTC', 'CampaignId']
            ),
            (
                'hard_min_throttle_delta',
                "s3://ttd-budget-calculation-lake/env=prod/aggregate-pacing-hourly-statistics/v=2/metric=hard_min_throttle_delta",
                ['ReportUntilHourUTC', 'CampaignId']
            ),
            (
                'throttle_metric_campaign_delta',
                "s3://ttd-budget-calculation-lake/env=prod/aggregate-pacing-hourly-statistics/v=2/metric=throttle_metric_campaign_delta",
                ['ReportUntilHourUTC', 'CampaignId']
            )]


###############################################################################
# DAG
###############################################################################

# The top-level dag
maintenance_delta_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2025, 1, 10),
    schedule_interval='0 0 * * *',
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/FoALC',
    retries=3,
    max_active_runs=1,
    retry_delay=timedelta(minutes=5),
    slack_channel="#taskforce-budget-metrics-alarms",
    slack_alert_only_for_prod=True,
    tags=["DIST"],
)

dag = maintenance_delta_dag.airflow_dag

###############################################################################
# clusters
###############################################################################

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M7a.m7a_8xlarge().with_fleet_weighted_capacity(1)],
    on_demand_weighted_capacity=1,
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R7a.r7a_4xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=5
)

maintenance_cluster = EmrClusterTask(
    name=dag_name,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    cluster_tags={"Team": dist.jira_team},
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5_2,
)

###############################################################################
# Spark Configuration
###############################################################################

spark_options_list = [
    ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
    ("conf", "spark.kryoserializer.buffer.max=1g"),
    ("conf", "spark.sql.adaptive.enabled=true"),
    ("conf", "spark.sql.adaptive.skewJoin.enabled=true"),
    (
        "conf",
        "spark.jars=/usr/share/aws/delta/lib/delta-spark.jar,/usr/share/aws/delta/lib/delta-storage.jar,/usr/share/aws/delta/lib/delta-storage-s3-dynamodb.jar"
    ),
    ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
    ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("conf", "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"),
    ("conf", "spark.databricks.delta.autoCompact.enabled=true"),
    ("conf", "spark.databricks.delta.optimizeWrite.enabled=true"),
    ("conf", "spark.databricks.delta.vacuum.parallelDelete.enabled=true"),
    ('conf', 'spark.dynamicAllocation.enabled=true'),
    ('conf', 'spark.dynamicAllocation.minExecutors=3'),
    ("conf", "spark.executor.cores=4"),
    ('conf', 'spark.shuffle.service.enabled=true'),
]

###############################################################################
# Task Generation
###############################################################################

# Discover tables at DAG definition time
delta_tables = discover_delta_tables()
table_groups = defaultdict(list)

# Generate tasks for each discovered table
for table, path, cluster_by in delta_tables:
    logger.info(f'Creating maintenance tasks for table: {table}')

    vacuum_sql = f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS;"

    cluster_cols = ", ".join(f"`{col}`" for col in cluster_by)
    optimize_sql = f"OPTIMIZE  delta.`{path}`;"
    optimize_zorder_sql = f"""
    set spark.databricks.delta.optimize.zorder.checkStatsCollection.enabled = false;
    OPTIMIZE  delta.`{path}` ZORDER BY ({cluster_cols});"""

    vacuum_task = PySparkSqlTask(
        name=f"vacuum_{table}",
        sql_query=vacuum_sql,
        additional_args_option_pairs_list=spark_options_list,
        timeout_timedelta=timedelta(hours=10),
    )

    optimize_task = PySparkSqlTask(
        name=f"optimize_{table}",
        sql_query=optimize_sql,
        additional_args_option_pairs_list=spark_options_list,
        timeout_timedelta=timedelta(hours=10),
    )

    optimize_zorder_task = PySparkSqlTask(
        name=f"optimize_zorder_{table}",
        sql_query=optimize_zorder_sql,
        additional_args_option_pairs_list=spark_options_list,
        timeout_timedelta=timedelta(hours=10),
    )

    vacuum_task >> optimize_task >> optimize_zorder_task
    table_groups[table].extend([vacuum_task, optimize_task, optimize_zorder_task])

# Add tasks to cluster
for table, tasks in table_groups.items():
    for task in tasks:
        maintenance_cluster.add_parallel_body_task(task)

###############################################################################
# DAG Dependencies
###############################################################################
final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

# Set up DAG dependencies
maintenance_delta_dag >> maintenance_cluster >> final_dag_status_step
