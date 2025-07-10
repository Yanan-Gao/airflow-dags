import copy
import logging
from datetime import timedelta, datetime, date

from airflow.exceptions import AirflowSensorTimeout
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from dags.adpb.spark_jobs.shared.adpb_helper import LalAerospikeConstant
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.cloud_provider import CloudProviders
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from dags.adpb.datasets.datasets import all_model_results_rolling

from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.constants import DataTypeId
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.slack.slack_groups import ADPB
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory

dag_name = "adpb-sibv2-publish-lal-rolling"
owner = ADPB.team
slack_tags = owner.sub_team
cluster_tags = {"Team": owner.jira_team}

# Job Config
job_start_date = datetime(2024, 8, 29, 6, 0)
job_schedule_interval = timedelta(hours=12)
job_environment = TtdEnvFactory.get_from_system()

jar_path = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-adpb-assembly.jar"
emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_3

env_str = "prod" if job_environment == TtdEnvFactory.prod else "test"
execution_date_time = "{{ data_interval_end.strftime(\"%Y-%m-%dT%H:00:00\") }}"
run_date_str = "{{ data_interval_end.strftime(\"%Y%m%d\") }}"
run_hour_str = "{{ data_interval_end.strftime(\"%H\") }}"

allowed_data_types_for_lal = [DataTypeId.CampaignSeedData.value]

# Publish time check
is_test_run_key = 'is_test_run'
check_publish_time_task_id = 'check_publish_time'
is_test_run_value = "{{ task_instance.xcom_pull(dag_id='" + dag_name + "', task_ids='" + check_publish_time_task_id + "', key='" + is_test_run_key + "') }}"
result_publish_expiration_days = 1

# Aerospike configs
model_results_bucket = "ttd-identity"
all_model_results_key = f"datapipeline/{env_str}/models/lal/all_model_results_rolling/v=2/isxd=false/date={run_date_str}/hour={run_hour_str}"
all_model_results_path = f"s3://{model_results_bucket}/{all_model_results_key}"
refreshed_model_results_key = f"datapipeline/{env_str}/models/lal/model_results_for_aerospike_rolling/v=1/date={run_date_str}/hour={run_hour_str}"
refreshed_model_results_path = f"s3://{model_results_bucket}/{refreshed_model_results_key}"
aerospike_address = LalAerospikeConstant.aerospike_address
aerospike_namespace = LalAerospikeConstant.aerospike_namespace
aerospike_set = LalAerospikeConstant.aerospike_set
ttl = LalAerospikeConstant.ttl
max_map_size = 200000  # how big can our maps be in aerospike

additional_application_configurations = [{"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}}]

sibv2_publish_lal_dag = TtdDag(
    dag_id=dag_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=owner.alarm_channel,
    slack_tags=slack_tags,
    tags=[owner.jira_team],
    retries=0,
    run_only_latest=False,
)


def check_recent_daily_publish_date(**kwargs):
    run_date_str_arg = kwargs['run_date_str']
    current_run_date = datetime.strptime(run_date_str_arg, '%Y%m%d').date()
    hook = AwsCloudStorage(conn_id='aws_default')

    check_recent_date = Datasources.lal.daily_model_results_for_aerospike(version=1).check_recent_data_exist(hook, date.today(), 7)

    if check_recent_date:
        latest_publish_date = check_recent_date.get()
        if current_run_date + timedelta(days=result_publish_expiration_days) < latest_publish_date:
            logging.warning(f'Current run date: {current_run_date} is behind latest publish date: {latest_publish_date}')
            kwargs['task_instance'].xcom_push(key=is_test_run_key, value=str(True).lower())
        else:
            kwargs['task_instance'].xcom_push(key=is_test_run_key, value=str(False).lower())
    else:
        raise ValueError('Could not find aerospike publish date in last 7 days')


check_recent_daily_publish_date_op_task = OpTask(
    op=PythonOperator(
        task_id=check_publish_time_task_id,
        provide_context=True,
        op_kwargs={'run_date_str': run_date_str},
        python_callable=check_recent_daily_publish_date,
        dag=sibv2_publish_lal_dag.airflow_dag
    )
)

rolling_lal_metrics_cluster = EmrClusterTask(
    name="Rolling-LAL-Metrics",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[M5.m5_2xlarge().with_ebs_size_gb(32).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5d.r5d_4xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R5d.r5d_8xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5d.r5d_12xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R5.r5_8xlarge().with_ebs_size_gb(1024).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5.r5_12xlarge().with_ebs_size_gb(1536).with_max_ondemand_price().with_fleet_weighted_capacity(3)
        ],
        on_demand_weighted_capacity=12
    ),
    additional_application_configurations=copy.deepcopy(additional_application_configurations),
    cluster_tags=cluster_tags,
    emr_release_label=emr_release_label,
    enable_prometheus_monitoring=True,
    environment=job_environment
)

spark_config_options = [("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                        ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

lal_metrics_step = EmrJobTask(
    name="Rolling-LAL-Metrics",
    class_name="jobs.lal.RollingLalMetricsGenerator",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_config_options,
    eldorado_config_option_pairs_list=[("dateTime", execution_date_time)],
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=rolling_lal_metrics_cluster.cluster_specs,
)

rolling_lal_metrics_cluster.add_parallel_body_task(lal_metrics_step)

lal_push_to_aerospike_spark_options = [("executor-memory", "70G"), ("executor-cores", "5"),
                                       ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
                                       ("conf", "spark.driver.memory=40G"), ("conf", "spark.driver.maxResultSize=6G"),
                                       ("conf", "spark.sql.shuffle.partitions=3000"),
                                       ("conf", "spark.sql.parquet.int96RebaseModeInRead=CORRECTED")]

job_ec2_subnet_id = LalAerospikeConstant.job_ec2_subnet_id
job_emr_managed_master_security_group = LalAerospikeConstant.job_emr_managed_master_security_group
job_emr_managed_slave_security_group = LalAerospikeConstant.job_emr_managed_slave_security_group
job_service_access_security_group = LalAerospikeConstant.job_service_access_security_group

rolling_lal_push_aerospike_cluster = EmrClusterTask(
    name="Push-Aerospike-Cluster",
    master_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[R5.r5_8xlarge().with_ebs_size_gb(512).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    ),
    core_fleet_instance_type_configs=EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_4xlarge().with_fleet_weighted_capacity(1),
            R5.r5_8xlarge().with_fleet_weighted_capacity(2),
            R5.r5_12xlarge().with_fleet_weighted_capacity(3)
        ],
        on_demand_weighted_capacity=18,
    ),
    cluster_tags=cluster_tags,
    emr_release_label=emr_release_label,
    enable_prometheus_monitoring=True,
    ec2_subnet_ids=[job_ec2_subnet_id],  # us-east-1d
    emr_managed_master_security_group=job_emr_managed_master_security_group,
    emr_managed_slave_security_group=job_emr_managed_slave_security_group,
    service_access_security_group=job_service_access_security_group,
    environment=job_environment,
)

prod_push_to_aerospike_spark_step = EmrJobTask(
    name="Push-Aerospike",
    class_name="jobs.lal.PublishToAerospikeLal",
    executable_path=jar_path,
    additional_args_option_pairs_list=lal_push_to_aerospike_spark_options,
    eldorado_config_option_pairs_list=[("modelResultsPath", refreshed_model_results_path), ("aerospikeAddress", aerospike_address),
                                       ("aerospikeNamespace", aerospike_namespace), ("ttl", ttl), ("aerospikeSet", aerospike_set),
                                       ("maxMapSize", max_map_size), ("isTestRun", is_test_run_value), ("metricsPrefix", "rolling_lal_")],
    timeout_timedelta=timedelta(hours=1),  # usually takes minutes
    cluster_specs=rolling_lal_push_aerospike_cluster.cluster_specs,
)

lal_generate_rolling_refresh_aerospike_step_options = [("dateTime", execution_date_time), ("maxMapSize", max_map_size),
                                                       ("inputLalPath", "models/lal/all_model_results_rolling/v=2/isxd=false")]

lal_generate_rolling_refresh_aerospike_step = EmrJobTask(
    name="LAL-Generate-Rolling-Refresh-LalModels-InAerospike",
    class_name="jobs.lal.RollingRefreshLalAerospike",
    executable_path=jar_path,
    additional_args_option_pairs_list=spark_config_options,
    eldorado_config_option_pairs_list=lal_generate_rolling_refresh_aerospike_step_options,
    timeout_timedelta=timedelta(hours=2),
    cluster_specs=rolling_lal_push_aerospike_cluster.cluster_specs,
)

rolling_lal_push_aerospike_cluster.add_sequential_body_task(lal_generate_rolling_refresh_aerospike_step)
rolling_lal_push_aerospike_cluster.add_sequential_body_task(prod_push_to_aerospike_spark_step)


def skip_downstream_on_timeout(context):
    exception = context['exception']
    if isinstance(exception, AirflowSensorTimeout):
        context['task_instance'].set_state("skipped")
        logging.info("Sensor skipped on timeout")


# Check if rolling lal model results are ready
check_rolling_device_lal_model_results_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id='check_rolling_lal_model_results',
        datasets=[all_model_results_rolling.with_check_type("hour")],
        cloud_provider=CloudProviders.aws,
        dag=sibv2_publish_lal_dag.airflow_dag,
        ds_date='{{data_interval_end.to_datetime_string()}}',
        poke_interval=60 * 10,
        timeout=60 * 60 * 6,
        on_failure_callback=skip_downstream_on_timeout
    ),
)

final_dag_status = OpTask(
    op=FinalDagStatusCheckOperator(dag=sibv2_publish_lal_dag.airflow_dag, trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED)
)

lal_generate_rolling_refresh_aerospike_step >> prod_push_to_aerospike_spark_step

sibv2_publish_lal_dag >> check_rolling_device_lal_model_results_sensor
check_rolling_device_lal_model_results_sensor >> check_recent_daily_publish_date_op_task >> rolling_lal_push_aerospike_cluster >> final_dag_status
check_rolling_device_lal_model_results_sensor >> rolling_lal_metrics_cluster >> final_dag_status

airflow_dag = sibv2_publish_lal_dag.airflow_dag
