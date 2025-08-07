from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m7g import M7g
from ttd.ec2.emr_instance_types.memory_optimized.r7g import R7g
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"

TaskBatchGrain_Daily = 100002  # dbo.fn_Enum_TaskBatchGrain_Daily()
logworkflow_connection = 'lwdb'
gating_type_id = 2000513  # dbo.fn_Enum_GatingType_ImportAudiencePredictorGlobalGeneratedParameters()


def _get_time_slot(dt: datetime):
    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt


def _open_lwdb_gate(**context):
    dt = _get_time_slot(context['data_interval_start'])
    log_start_time = dt.strftime('%Y-%m-%d %H:00:00')
    ExternalGateOpen(
        mssql_conn_id=logworkflow_connection,
        sproc_arguments={
            'gatingType': gating_type_id,
            'grain': TaskBatchGrain_Daily,
            'dateTimeToOpen': log_start_time
        }
    )


audience_predictor_global_generated_parameters_generation = TtdDag(
    dag_id="adpb-audience-predictor-global-generated-parameters-generation",
    start_date=datetime(2024, 11, 18, 12, 0, 0),
    schedule_interval=timedelta(hours=24),
    slack_channel="#scrum-adpb-alerts",
    slack_tags=ADPB.team.sub_team,
    enable_slack_alert=True
)

cluster = EmrClusterTask(
    name="AudiencePredictorGlobalGeneratedParametersGeneration_Cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M7g.m7g_4xlarge().with_ebs_size_gb(64).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    cluster_tags={
        "Team": ADPB.team.jira_team,
    },
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R7g.r7g_8xlarge().with_ebs_size_gb(64).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
    ),
    enable_prometheus_monitoring=True,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_5
)

pfs_step = EmrJobTask(
    cluster_specs=cluster.cluster_specs,
    name="AudiencePredictorGlobalGeneratedParametersGeneratorStep",
    class_name="jobs.audiencepredictorglobalgeneratedparameters.AudiencePredictorGlobalGeneratedParametersGeneratorJob",
    eldorado_config_option_pairs_list=[("date", "{{ data_interval_start.strftime(\"%Y-%m-%d\") }}")],
    executable_path=jar_path
)
cluster.add_sequential_body_task(pfs_step)

open_gate = OpTask(
    op=PythonOperator(
        task_id='open_lwdb_gate',
        python_callable=_open_lwdb_gate,
        provide_context=True,
        dag=audience_predictor_global_generated_parameters_generation.airflow_dag,
    )
)

check = OpTask(op=FinalDagStatusCheckOperator(dag=audience_predictor_global_generated_parameters_generation.airflow_dag))

audience_predictor_global_generated_parameters_generation >> cluster >> open_gate >> check

dag = audience_predictor_global_generated_parameters_generation.airflow_dag
