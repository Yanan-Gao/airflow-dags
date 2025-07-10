from airflow import DAG
from datetime import datetime, timedelta

from dags.cmo.utils.pipeline_config import PipelineConfig
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import CMO
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from ttd.kubernetes.pod_resources import PodResources
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
import json

####################################################################################################################
# Configs
####################################################################################################################
job_name = "omnichannel-bidfactor-calculation"
start_date = datetime(2024, 12, 5, 0, 0)
job_schedule_interval = timedelta(days=1)

ttd_dag = TtdDag(
    dag_id=job_name,
    start_date=start_date,
    schedule_interval=job_schedule_interval,
    retries=1,
    retry_delay=timedelta(hours=1),
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    enable_slack_alert=True,
    tags=["cmo"]
)

dag: DAG = ttd_dag.airflow_dag
config = PipelineConfig()
env = TtdEnvFactory.get_from_system().dataset_write_env

####################################################################################################################
# Steps
####################################################################################################################

configurations_v2 = json.dumps([{
    "path_optimization_type": 1,
    "bidfactor_scale_min": 1.0,
    "bidfactor_scale_max": 3.0
}, {
    "path_optimization_type": 1,
    "bidfactor_scale_min": 2.5,
    "bidfactor_scale_max": 3.5,
    "omnichannel_group_id": 200
}, {
    "path_optimization_type": 1,
    "omnichannel_group_id": 200,
    "janus_experiment_name": "exp-200001",
    "bidfactor_scale_min": 2.0,
    "bidfactor_scale_max": 5.0
}, {
    "path_optimization_type": 1,
    "bidfactor_scale_min": 2.5,
    "bidfactor_scale_max": 3.5,
    "omnichannel_group_id": 1039
}, {
    "path_optimization_type": 1,
    "omnichannel_group_id": 1039,
    "janus_experiment_name": "exp-200005",
    "bidfactor_scale_min": 2.0,
    "bidfactor_scale_max": 5.0
}, *[{
    "path_optimization_type": 1,
    "omnichannel_group_id": group_id,
    "janus_experiment_name": "exp-220002",
    "bidfactor_scale_min": 1.0,
    "bidfactor_scale_max": 3.0
} for group_id in [823, 806, 472, 745, 291, 992, 965]], *[{
    "path_optimization_type": 1,
    "omnichannel_group_id": group_id,
    "bidfactor_scale_min": 1.0,
    "bidfactor_scale_max": 2.0
} for group_id in [823, 806, 472, 745, 291, 992, 965]], *[{
    "path_optimization_type": 1,
    "omnichannel_group_id": group_id,
    "janus_experiment_name": "exp-220003",
    "bidfactor_scale_min": 1.0,
    "bidfactor_scale_max": 1.5
} for group_id in [612, 237, 834, 837, 469, 855, 993, 991, 967, 798, 523]], *[{
    "path_optimization_type": 1,
    "omnichannel_group_id": group_id,
    "bidfactor_scale_min": 1.0,
    "bidfactor_scale_max": 2.0
} for group_id in [612, 237, 834, 837, 469, 855, 993, 991, 967, 798, 523]]])

calculate_bidfactors_v2 = OpTask(
    op=TtdKubernetesPodOperator(
        namespace='channel-optimizations',
        image='production.docker.adsrvr.org/ttd/cmo/omnichannel-bidfactor-generator:3004177',
        name='omnichannel-bidfactor-calculation_v2',
        task_id='omnichannel-bidfactor-calculation_v2',
        dnspolicy='ClusterFirst',
        get_logs=True,
        is_delete_operator_pod=True,
        dag=dag,
        service_account_name='channel-optimizations',
        startup_timeout_seconds=800,
        log_events_on_failure=True,
        cmds=["python3"],
        arguments=[
            "main.py", "--input_path", "s3://ttd-ctv/prod/OmnichannelView/ForecastPathToConversion/MergedPath/v=1/", "--output_path",
            f"s3://ttd-omnichannel-optimizations/env={env}/omnichannel-path-optimization/v=2/date={{{{ logical_date.strftime(\"%Y%m%d\") }}}}",
            "--configurations", configurations_v2
        ],
        annotations={
            'sumologic.com/include': 'true',
            'sumologic.com/sourceCategory': job_name
        },
        env_vars=dict(PROMETHEUS_ENV=config.env.execution_env),
        resources=PodResources(limit_ephemeral_storage='500M', request_memory='1G', limit_memory='2G', request_cpu='1')
    )
)

ttd_dag >> [calculate_bidfactors_v2]
