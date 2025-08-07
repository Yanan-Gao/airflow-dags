from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

from ttd.kubernetes.pod_resources import PodResources
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.sensors.external_task import ExternalTaskSensor
from ttd.el_dorado.v2.base import TtdDag
from ttd.interop.logworkflow_callables import ExternalGateOpen
from ttd.interop.logworkflow_constants import LogWorkflowTaskBatchGrain
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.interop.logworkflow_constants import LogWorkflowGatingType


def create_dag(
    dag_id,
    dag_start_date,
    dependency_task_variant,
    vertica_url,
    data_domain_id,
    job_output_root,
    prometheus_job,
    data_mover_gating_type,
    job_secrets=[],
    job_config=[
        "--packages",
        "org.apache.hadoop:hadoop-aws:3.2.1,com.amazonaws:aws-java-sdk:1.12.515",
        "--exclude-packages",
        "com.amazonaws:aws-java-sdk-bundle",
        "--conf",
        "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
    ],
) -> DAG:
    report_date = '{{ (data_interval_end - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}'
    run_id = '{{ run_id }}'

    if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
        # Production variables
        dag_slack_channel = '#scrum-measurement-up-alarms'

        k8s_namespace = 'measure-prod'
        k8s_conn_id = "airflow-2-pod-scheduling-rbac-conn-prod"
        k8s_service_docker_image = 'production.docker.adsrvr.org/ttd-measurement/frequencyreport-guidance:latest'

        data_mover_logworkflow_connection = 'lwdb'
        active_profile = 'prod'
    else:
        # Test variables
        dag_slack_channel = None

        k8s_namespace = 'measure-dev'
        k8s_conn_id = "airflow-2-pod-scheduling-rbac-conn-dev"
        k8s_service_docker_image = 'dev.docker.adsrvr.org/ttd-measurement/frequencyreport-guidance:latest'

        data_mover_logworkflow_connection = 'sandbox-lwdb'
        active_profile = 'prodtest'

    dag_schedule_interval = "0 22 * * *"  # make sure this matches previous_check's execution_delta

    job_command_line_arguments = [
        f'--spring.profiles.active={active_profile}',
        '--spring.config.import=configtree:/etc/frequencyreport-guidance/',
        f'--application.runId={run_id}',
        f'--application.reportDate={report_date}',
        f'--application.attribution.dataPath={job_output_root}/attribution-frequency',
        f'--application.lateAttribution.dataPath={job_output_root}/late-attribution-frequency',
        f'--application.report.reportPath={job_output_root}/report',
        f'--application.report.dataDomainId={data_domain_id}',
        f'--application.sanityCheck.resultPath={job_output_root}/sanity-check',
        f'--application.vertica.vertdb.url={vertica_url}',
        f'--application.prometheus.job={prometheus_job}',
    ]

    arguments = job_config + ["--conf", "spark.driver.memory=31G", "/guidance-all.jar"] + job_command_line_arguments

    secrets = job_secrets + [
        Secret(
            deploy_type='volume',
            deploy_target='/etc/frequencyreport-guidance/application.vertica.vertdb',
            secret='ttd-measure-svc-vertica-ro-account'
        ),
    ]

    frequencyreport_guidance_dag = TtdDag(
        dag_id=dag_id,
        start_date=dag_start_date,
        schedule_interval=dag_schedule_interval,
        # depends_on_past=True,
        max_active_runs=8,
        retries=1,
        retry_delay=timedelta(minutes=30),
        slack_channel=dag_slack_channel,
        tags=['measurement', 'frequency-guidance'],
    )
    dag = frequencyreport_guidance_dag.airflow_dag

    # check previous status
    previous_check = OpTask(
        op=ExternalTaskSensor(
            dag=dag,
            task_id="previous_check",
            external_dag_id=dag.dag_id,
            external_task_id=None,  # wait for dag
            execution_delta=timedelta(days=1),  # make sure this matches dag_schedule_interval
            check_existence=True,
            mode="reschedule"
        )
    )

    # gating
    gating_query = f"""
    declare @reportLookBackDays int = 28;
    declare @reportDate date = '{report_date}';
    declare @startTime date = dateadd(day, 1 - @reportLookBackDays, @reportDate);
    declare @endTime date = dateadd(day, 1, @reportDate);

    with task as (select lf.LogTypeId, t.TaskId, lf.LogStartTime
                  from dbo.Task t
                           join dbo.LogFileTask lft on t.TaskId = lft.TaskId
                           join dbo.LogFile lf on lft.LogFileId = lf.LogFileId
                  where 1 = 1
                    and lft.TaskVariantId in ({dependency_task_variant})
                    and lft.LogFileTaskStatusId not in (5, -- dbo.fn_Enum_LogFileTaskStatus_Completed(),
                                                        6 -- dbo.fn_Enum_LogFileTaskStatus_Ignored()
                      )
    )
       , imp as (select t.TaskId
                 from task t
                 where LogStartTime >= @startTime
                   and LogStartTime < @endTime
                   and t.TaskId = 1104 -- dbo.fn_Enum_Task_VerticaMergeIntoFreqReport()
    )
       , attr as (select t.TaskId
                  from task t
                  where convert(date, t.LogStartTime) = @reportDate
                    and t.TaskId = 1008 -- dbo.fn_Enum_Task_SingleLogAttribution()
    )
       , lateImpAttr as (select t.TaskId
                         from task t
                         where convert(date, t.LogStartTime) = @reportDate
                           and t.TaskId = 1098 -- dbo.fn_Enum_Task_VerticaLateImpressionAttribution()
    )
       , attrEvent as (select t.TaskId
                       from task t
                       where convert(date, t.LogStartTime) = @reportDate
                         and t.TaskId = 1166 -- dbo.fn_Enum_Task_HourlyVerticaSanityCheckAttributedEvent() -- HourlyVerticaLoadGateAttributedEvent
    )
       , a as (select * from imp union all select * from attr union all select * from lateImpAttr union all select * from attrEvent)
       , b as (select top 1 * from a)

    select iif(count(1) > 0, 0, 1) as Completed  -- 0 for not completed, none 0 for completed
    from b
    ;
    """

    dependency_sensor = OpTask(
        op=SqlSensor(
            task_id="dependency_sensor",
            conn_id='lwdb',
            sql=gating_query,
            poke_interval=60 * 30,  # 30 minutes
            timeout=60 * 60 * 12,  # 12 hours
            mode="reschedule"
        )
    )

    k8s_pod = OpTask(
        op=TtdKubernetesPodOperator(
            namespace=k8s_namespace,
            kubernetes_conn_id=k8s_conn_id,
            image=k8s_service_docker_image,
            image_pull_policy="Always",
            name="frequencyreport-guidance-service",
            task_id="frequencyreport-guidance-service",
            dnspolicy='ClusterFirst',
            get_logs=True,
            is_delete_operator_pod=True,
            dag=dag,
            startup_timeout_seconds=600,
            execution_timeout=timedelta(hours=8),
            service_account_name='frequencyreport-guidance',
            log_events_on_failure=True,
            resources=PodResources(
                request_cpu="4000m",
                # slightly higher than spark.driver.memory to allow overhead for JVM and other processes
                request_memory="32G",
                request_ephemeral_storage="50G",
                limit_cpu="8000m",
                limit_memory="32G",
                limit_ephemeral_storage="100G",
            ),
            secrets=secrets,
            arguments=arguments
        )
    )

    frequencyreport_guidance_dag >> previous_check >> dependency_sensor >> k8s_pod

    if data_mover_gating_type is not None:
        # open data mover gate
        open_datamover_gate = OpTask(
            op=PythonOperator(
                dag=dag,
                python_callable=ExternalGateOpen,
                provide_context=True,
                op_kwargs={
                    'mssql_conn_id': data_mover_logworkflow_connection,
                    'sproc_arguments': {
                        'gatingType': data_mover_gating_type,
                        'grain': LogWorkflowTaskBatchGrain.daily,
                        'dateTimeToOpen': report_date
                    }
                },
                task_id="open_datamover_gate",
                trigger_rule="none_failed_or_skipped"
            )
        )
        k8s_pod >> open_datamover_gate

    return dag


# Rest of world
if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    job_output_root = 's3://ttd-identity/data/prod/regular/frequency-report-guidance'
else:
    job_output_root = 's3://ttd-identity/data/test/regular/frequency-report-guidance'

# airflow dag
dag = create_dag(
    dag_id='frequencyreport-guidance',
    dag_start_date=datetime(2024, 8, 13),
    dependency_task_variant=5,  # dbo.fn_enum_TaskVariant_VerticaUSEast01()
    vertica_url='jdbc:vertica://etl.useast01.vertica.adsrvr.org:5433/theTradeDesk',
    # dependency_task_variant=9,  # dbo.fn_enum_TaskVariant_VerticaUSWest01()
    # vertica_url='jdbc:vertica://uswest01.vertica.adsrvr.org:5433/theTradeDesk',
    data_domain_id=1,  # fn_Enum_DataDomainId_TTD_RoW()
    job_output_root=job_output_root,
    prometheus_job='frequencyreport_guidance',
    data_mover_gating_type=LogWorkflowGatingType.frequency_report_guidance
)

# walmart
if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    job_output_root_walmart = 's3://ttd-identity/data/prod/regular/frequency-report-guidance-walmart'
else:
    job_output_root_walmart = 's3://ttd-identity/data/test/regular/frequency-report-guidance-walmart'

# airflow dag
dag_walmart = create_dag(
    dag_id='frequencyreport-guidance-walmart',
    dag_start_date=datetime(2024, 8, 22),
    dependency_task_variant=24,  # dbo.fn_enum_TaskVariant_VerticaUSEast03()
    vertica_url='jdbc:vertica://useast03.vertica.adsrvr.org:5433/theTradeDesk',
    data_domain_id=2,  # fn_Enum_DataDomainId_Walmart_US()
    job_output_root=job_output_root_walmart,
    prometheus_job='frequencyreport_guidance_walmart',
    data_mover_gating_type=LogWorkflowGatingType.frequency_report_guidance_walmart
)
