from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.general_purpose.m5a import M5a
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask
from ttd.ttdslack import dag_post_to_slack_callback

from datetime import datetime, timedelta
from math import floor

import jinja2
import logging
from ttd.interop.logworkflow_callables import ExternalGateOpen

# We could pass this in as part of dag_run.conf as well; but would require variable such as application_data_url be defined at run time
env = "prod"

# A dictionary to change between "dev" and "prod" parameters; the switching is done through setting `env`
# The thinking behind passing `dev_user` as part of `dag_run.conf` is that one doesnt need to do a `kubectl cp` to change where the DAG writes to; but simply pass the username when triggering a run, to see the output of the DAG
DICT_CONFIG = {
    "dev": {
        "version": "1.1.56-SNAPSHOT",
        "s3_folder": "dev/{{dag_run.conf.dev_user}}/application/radarDemoInsights",
        "app_s3_folder": "application/radar",
        "application_data_url": "s3://ttd-datprd-us-east-1/dev/{{dag_run.conf.dev_user}}/application/radarDemoInsights/data",
    },
    "prod": {
        "version": "2.0.45",
        "s3_folder": "application/demographic-insights",
        "app_s3_folder": "application/radar",
        "application_data_url": "s3://ttd-datprd-us-east-1/application/radar/data",
    }
}

prometheus_push_gateway_address = "prom-push-gateway.adsrvr.org:80"
compute_hours = 72

application_data_url = DICT_CONFIG[env]["application_data_url"]
backstage_url = "s3://ttd-datprd-us-east-1/backstage"
name = "demographic-insights"
group_id = "com.thetradedesk.radar"
version = DICT_CONFIG[env]["version"]

start_date = datetime(2024, 8, 25, 19, 0)
schedule_interval = "0 * * * *"
timeout_hours = 36
slack_channel = "@emma.cohn"

s3_bucket = "ttd-datprd-us-east-1"
s3_folder = DICT_CONFIG[env]["s3_folder"]
app_s3_folder = DICT_CONFIG[env]["app_s3_folder"]

tags = {"Environment": env, "Job": name, "Resource": "EMR", "process": "Radar", "Source": "Airflow", "Team": "DATPRD"}

main_ebs_configuration = {
    'EbsOptimized': True,
    'EbsBlockDeviceConfigs': [{
        'VolumeSpecification': {
            'VolumeType': 'io1',
            'SizeInGB': 512,
            'Iops': 3000
        },
        'VolumesPerInstance': 1
    }]
}

main_master_machine_vmem = 185  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
main_master_machine_vcpu = 48
main_core_machine_vmem = 24  # yarn.nodemanager.resource.memory-mb Amount of physical memory, in MB, that can be allocated for containers. It means the amount of memory YARN can utilize on this node and therefore this property should be lower than the total memory of that machine.
main_core_machine_vcpu_granularity = 2
main_core_machine_vcpu = 8 * main_core_machine_vcpu_granularity
main_driver_memory_overhead = 0.25
main_executor_memory_overhead = 0.25
main_driver_count = 1
main_core_machine_count = 100
main_executor_count_per_machine = 1
main_driver_core_count = int(floor(main_master_machine_vcpu / main_driver_count))
main_driver_memory = str(int(floor(main_master_machine_vmem / (main_driver_count * (1 + main_driver_memory_overhead))))) + "G"
main_executor_count = main_core_machine_count * main_executor_count_per_machine
main_executor_core_count = int(floor(main_core_machine_vcpu / main_executor_count_per_machine))
main_executor_memory = str(
    int(floor(main_core_machine_vmem / (main_executor_count_per_machine * (1 + main_executor_memory_overhead))))
) + "G"

log_level = "TRACE"

instance_timestamp = datetime.utcnow()

job_user_profile_end_date = instance_timestamp.strftime("%Y-%m-%d")
# TODO: this is not consistent with lookBackDays with ingestion - this needs to be changed together with change in radar repo
job_instance_s3_folder = s3_folder + "/instance/" + instance_timestamp.isoformat()
job_application_version = version
job_k8s_in_cluster = True
job_k8s_cluster_context = None
job_k8s_namespace = "airflow"

job_demo_insights_docker_image_name = "dev.docker.adsrvr.org/com.thetradedesk.radar-demoInsights:" + job_application_version

job_emr_cluster_name = "Data Products - Demo Insights @ " + instance_timestamp.isoformat()

concurrent_train_cluster_params_sets = 3

####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
dag: TtdDag = TtdDag(
    dag_id=name,
    tags=["DATPRD"],
    default_args={
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": None,
        "owner": "DATPRD",
        "retries": 2,
        'in_cluster': True,
    },
    max_active_runs=1,
    run_only_latest=True,
    start_date=start_date,
    schedule_interval=schedule_interval,
    dagrun_timeout=timedelta(hours=timeout_hours),
    slack_channel="#scrum-data-products-alarms",
    retry_delay=timedelta(minutes=10),
    on_failure_callback=dag_post_to_slack_callback(dag_name=name, step_name="", slack_channel=slack_channel),
    slack_alert_only_for_prod=True
)

instance_types = [
    M6g.m6g_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(128),
    M5a.m5a_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(128),
    M5.m5_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1).with_ebs_size_gb(128)
]

subnet_ids = [
    "subnet-f2cb63aa",  # 1A
    "subnet-7a86170c",  # 1D
    "subnet-62cd6b48"  # 1E
]

ec2_key_name = "emma.cohn2"

cluster_configs = [{
    "Classification": "yarn-site",
    "Properties": {
        "yarn.nodemanager.vmem-check-enabled": "false",
        "yarn.nodemanager.pmem-check-enabled": "false",
        "yarn.nodemanager.resource.cpu-vcores": str(main_core_machine_vcpu)
    }
}, {
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxRetries": "5",
        "fs.s3.sleepTimeSeconds": "10"
    }
}]

create_cluster = EmrClusterTask(
    name="DATPRD-Demographic-Insights",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(instance_types=instance_types, on_demand_weighted_capacity=1),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(instance_types=instance_types, on_demand_weighted_capacity=main_executor_count),
    cluster_tags=tags,
    ec2_subnet_ids=subnet_ids,
    ec2_key_name=ec2_key_name,
    log_uri="s3://" + s3_bucket + "/" + s3_folder + "/log/emr",
    additional_application_configurations=cluster_configs,
    cluster_auto_termination_idle_timeout_seconds=300
)

time_to_use = '{{ logical_date.subtract(hours=1).strftime("%Y-%m-%dT%H:%M:%S+00:00") }}'
date_to_use = '{{ logical_date.subtract(hours=1).strftime("%Y-%m-%d") }}'
hour_str = '{{ logical_date.subtract(hours=1).strftime("%H") }}'


def add_emr_step(
    task_id, spark_args, driver_core_count, driver_memory, executor_count, executor_core_count, executor_core_count_per_task,
    executor_memory, artifact_id, command_name, command_args
):

    spark_arguments = [
        ("conf", "spark.driver.memory=" + driver_memory), ("conf", "spark.driver.cores=" + str(driver_core_count)),
        ("conf", "spark.driver.maxResultSize=32G"), ("conf", "spark.executor.instances=" + str(executor_count)),
        ("conf", "spark.executor.cores=" + str(executor_core_count)), ("conf", "spark.executor.memory=" + str(executor_memory)),
        ("conf", "spark.executor.extraJavaOptions=-Dcom.amazonaws.sdk.retryMode=STANDARD -Dcom.amazonaws.sdk.maxAttempts=15000"),
        ("conf", "spark.task.cpus=" + str(executor_core_count_per_task)), ("conf", "spark.dynamicAllocation.enabled=false"),
        ("conf", "spark.sql.files.ignoreCorruptFiles=false"),
        ("conf", "spark.default.parallelism=" + str(executor_count * executor_core_count)),
        ("conf", "spark.sql.shuffle.partitions=" + str(executor_count * executor_core_count))
    ] + spark_args

    return EmrJobTask(
        name=task_id,
        cluster_specs=create_cluster.cluster_specs,
        class_name=group_id + "." + artifact_id + ".Main",
        executable_path="s3://" + s3_bucket + "/" + app_s3_folder + "/bin/" + artifact_id + "/" + version + "/" + group_id + "-" +
        artifact_id + "-" + version + "-all.jar",
        timeout_timedelta=timedelta(minutes=20),
        configure_cluster_automatically=False,
        deploy_mode="client",
        additional_args_option_pairs_list=spark_arguments,
        command_line_arguments=[
            command_name, "--dagTimestamp", time_to_use, "--prometheusPushGatewayAddress", prometheus_push_gateway_address,
            "--computationHours",
            str(compute_hours)
        ] + command_args
    )


artifact_id = "demoInsights"

aggregate_demo_insights_step = add_emr_step(
    task_id="aggregate_demo_insights",
    spark_args=[],
    driver_core_count=main_driver_core_count,
    driver_memory=main_driver_memory,
    executor_count=main_executor_count,
    executor_core_count=main_executor_core_count,
    executor_core_count_per_task=main_core_machine_vcpu_granularity,
    executor_memory=main_executor_memory,
    artifact_id="demoInsights",
    command_name="aggregate-hourly-demo-insights",
    command_args=[
        "--logLevel",
        log_level,
        "--applicationDataUrl",
        application_data_url,
        "--referenceDate",
        date_to_use,
        "--backstageUrl",
        backstage_url,
        "--env",
        env,
    ]
)

create_cluster.add_sequential_body_task(aggregate_demo_insights_step)

# data mover code starts here
logworkflow_connection = "lwdb"
lwdb_conn_id = logworkflow_connection
job_name = 'demo-insights-hourly-datamover-import'
aws_conn_id = 'aws_default'
TaskBatchGrain_Hourly = 100001
s3_parquet_location = 'thetradedesk-useast-vertica-parquet-export/DemographicInsights'
gating_type_id = 2000073


def _gate_is_unopened(mssql_conn_id: str, sproc_arguments: dict):
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id, schema='LogWorkflow')
    conn = hook.get_conn()
    conn.autocommit(False)
    cursor = conn.cursor(as_dict=False)
    execute_sproc = jinja2.Template(
        """
        declare @logDate datetime2
        select count( * ) as gateIsUnopened -- if > 0, this value is true
        from LogFileTask
        where TaskId = 1000097
        and TaskVariantId = 5
        and LogFileTaskStatusId = 1
        and LogStartTime = CAST ( '{{ sproc_arguments['logDate'] }}' as datetime2)
        """
    ).render(sproc_arguments=sproc_arguments)
    logging.info(execute_sproc)
    try:
        cursor.execute(execute_sproc)
        gate_is_unopened = cursor.fetchone()[0]
        print("gateIsUnopened value = " + str(gate_is_unopened))
        print("sproc args = " + str(sproc_arguments))
        logging.info(gate_is_unopened)
        return gate_is_unopened > 0

    except Exception as ex:
        logging.warn("failed on checking if gate previously opened at:  %s", sproc_arguments['logDate'])
        raise ValueError('sproc call failed')


def _open_lwdb_gate(**context):
    for hours_to_subtract in range(compute_hours):
        pendDT = context['logical_date'].replace(minute=0, second=0, microsecond=0)
        log_time = pendDT.subtract(hours=25 + hours_to_subtract)

        log_start_time = log_time.strftime('%Y-%m-%d %H:00:00')
        # check if the hour has already been processed
        gate_should_open = _gate_is_unopened(mssql_conn_id=lwdb_conn_id, sproc_arguments={'logDate': log_start_time})
        if (gate_should_open):
            print("Opening gate for datetime " + log_start_time)
            ExternalGateOpen(
                mssql_conn_id=lwdb_conn_id,
                sproc_arguments={
                    'gatingType': gating_type_id,
                    'grain': TaskBatchGrain_Hourly,
                    'dateTimeToOpen': log_start_time
                }
            )
        else:
            print("Skipping this hour because Gate has already opened for  " + log_start_time)


def get_open_gate_task() -> OpTask:
    return OpTask(
        task_id="open_lwdb_gate", op=PythonOperator(task_id='open-lwdb-gate', python_callable=_open_lwdb_gate, provide_context=True)
    )


def get_finalizer() -> OpTask:
    return OpTask(task_id="finalizer_new", op=BashOperator(task_id="finalizer-new", bash_command="echo 'finalizer'"))


check = OpTask(op=FinalDagStatusCheckOperator(dag=dag.airflow_dag))

dag >> create_cluster
create_cluster >> get_finalizer() >> get_open_gate_task() >> check

adag = dag.airflow_dag
