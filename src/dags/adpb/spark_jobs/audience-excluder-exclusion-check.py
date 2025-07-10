import logging
import math
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

dag_name = 'adpb-audience-excluder-exclusion-check'
owner = ADPB.team

audience_excluder_exclusion_check_job_name = "Audience_Excluder_Exclusion_Check"

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"
job_environment = TtdEnvFactory.get_from_system()
job_start_date = datetime(2024, 8, 9, 6, 0, 0)
job_schedule_interval_in_hours = 6
job_schedule_interval = timedelta(hours=job_schedule_interval_in_hours)
spark_timeout_delta = timedelta(hours=24)
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_2

batch_size = 24 / job_schedule_interval_in_hours  # number of batches each day

cluster_tags = {
    'Team': owner.jira_team,
}

spark_options_list = [("executor-memory", "18G"), ("executor-cores", "5"), ("conf", "spark.driver.memory=40G"),
                      ("conf", "spark.driver.cores=12"), ("conf", "spark.sql.shuffle.partitions=3000"),
                      ("conf", "spark.driver.maxResultSize=32G"), ("conf", "spark.yarn.maxAppAttempts=1"),
                      ("conf", "spark.sql.files.ignoreCorruptFiles=true"),
                      ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC")]

# Setup DAG
audience_excluder_exclusion_check_dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=owner.alarm_channel,
    slack_tags=owner.sub_team,
    tags=["Monitoring", owner.jira_team],
    enable_slack_alert=True
)

dag = audience_excluder_exclusion_check_dag.airflow_dag


def generate_batch_id(ts_nodash, **kwargs):
    """
    Generate batch_id according to dag run hour in day
    """
    ts = datetime.strptime(ts_nodash, '%Y%m%dT%H%M%S')
    _batch_id = math.floor(ts.hour / job_schedule_interval_in_hours)
    logging.info(f'Setting batch_id: {_batch_id} (of {batch_size}) at hour: {ts.hour}')
    kwargs['task_instance'].xcom_push(key='batch_id', value=_batch_id)
    return _batch_id


generate_batch_id_task = OpTask(
    op=PythonOperator(
        task_id='generate_batch_id',
        python_callable=generate_batch_id,
        op_kwargs=dict(ts_nodash='{{ ts_nodash }}'),
        provide_context=True,
        do_xcom_push=True
    )
)

batch_id_macro = f'{{{{ task_instance.xcom_pull(dag_id="{dag_name}", ' \
           f'task_ids="{generate_batch_id_task.first_airflow_op().task_id}") }}}}'

audience_excluder_exclusion_check_cluster = EmrClusterTask(
    name="Audience_Excluder_Exclusion_Check",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            M5.m5_4xlarge().with_ebs_size_gb(128).with_fleet_weighted_capacity(1),
            M5.m5_8xlarge().with_ebs_size_gb(256).with_fleet_weighted_capacity(2),
        ],
        on_demand_weighted_capacity=4,
    ),
    emr_release_label=emr_release_label,
    cluster_tags=cluster_tags,
    enable_prometheus_monitoring=True,
    environment=job_environment
)

audience_excluder_exclusion_check_step = EmrJobTask(
    name=audience_excluder_exclusion_check_job_name,
    class_name="jobs.audienceexcluder.AudienceExcluderExclusionCheck",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_options_list,
    eldorado_config_option_pairs_list=[
        ("datetime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
        ('batchId', batch_id_macro),
        ('batchSize', batch_size),
        ('generatePrometheusMetrics', 'true'),
        ('audienceCountsServiceAddress', "https://batch-int.audience-count.adsrvr.org:9443/counts/"),
        ('maxCountsRequestTries', 10),
        ('requestParallelism', 100),
        ('failureThresholdPercentage', 5)  # 5% of ACS requests allowed to fail
    ],
    timeout_timedelta=timedelta(hours=6),
    cluster_specs=audience_excluder_exclusion_check_cluster.cluster_specs,
    configure_cluster_automatically=True
)

audience_excluder_exclusion_check_cluster.add_parallel_body_task(audience_excluder_exclusion_check_step)

final_check = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

audience_excluder_exclusion_check_dag >> generate_batch_id_task >> audience_excluder_exclusion_check_cluster >> final_check
