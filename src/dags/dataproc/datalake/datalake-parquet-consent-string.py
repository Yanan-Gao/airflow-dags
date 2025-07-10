import logging

from os import path

import json

import fastavro
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from pendulum import DateTime

# from fastavro.repository import AbstractSchemaRepository, SchemaRepositoryError

from datasources.datasources import Datasources
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.cloud_provider import CloudProvider, CloudProviders
from ttd.ec2.cluster_params import ClusterCalcDefaults
from ttd.ec2.emr_instance_types.compute_optimized.c7g import C7g
from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from datetime import datetime
from ttd.eldorado.base import TtdDag
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.operators.dataset_check_sensor import DatasetCheckSensor
from ttd.slack.slack_groups import dataproc, AIFUN
from ttd.tasks.op import OpTask

# class SchemaRepo(AbstractSchemaRepository):
#     def __init__(self, repo_path: str):
#         self.repo_path = repo_path
#         self.file_ext = 'avsc'
#
#     def load(self, name):
#         root, ext = path.splitext(name)
#         if ext == f".{self.file_ext}":
#             root, ext = path.splitext(root)
#         file_name = root if ext == '' else ext[1:]
#
#         file_path = path.join(self.repo_path, f"{file_name}.{self.file_ext}")
#         try:
#             with open(file_path) as schema_file:
#                 return json.load(schema_file)
#         except IOError as error:
#             raise SchemaRepositoryError(
#                 f"Failed to load '{name}' schema",
#             ) from error
#         except json.decoder.JSONDecodeError as error:
#             raise SchemaRepositoryError(
#                 f"Failed to parse '{name}' schema",
#             ) from error


class CloudJobConfig:

    def __init__(
        self,
        provider: CloudProvider,
        capacity: int,
        base_disk_space: int = 75,
        output_files: int = 200,
    ):
        self.provider = provider
        self.capacity = capacity
        self.base_disk_space = base_disk_space
        self.output_files = output_files


job_schedule_interval = "0 * * * *"
job_start_date = datetime(2024, 6, 20, 7, 00)

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]
additional_args = [("conf", "spark.driver.maxResultSize=2g")]

aws_jar: str = "s3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-dataproc-assembly.jar"


def list_keys_in_cloud(data_interval_start: DateTime, bucket: str):
    hook = S3Hook(aws_conn_id='aws_default', verify=None)
    prefix = f"parquet/bidrequest_gdpr_consent_string/v=2/date={data_interval_start.format('YMMDD')}/hour={data_interval_start.format('HH')}"
    logging.info(f"Path: {bucket}/{prefix}")
    return hook.list_keys(bucket, prefix)


def send_notification_to_kafka(data_interval_start: DateTime, **kwargs):
    bucket = "ttd-datapipe-data"
    cloud_keys_list = list_keys_in_cloud(data_interval_start, bucket)

    logging.info(f"Keys from cloud, {len(cloud_keys_list)} items (printing top 10): {cloud_keys_list[0:5]}")

    bootstrap_servers = Variable.get("TTD__DATAPIPE_KAFKA__BOOTSTRAP_SERVERS")
    schema_registry_url = Variable.get("TTD__DATAPIPE_KAFKA__SCHEMA_REGISTRY_URL")
    schema_registry_conf = {"url": schema_registry_url}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    schemas_path = "src/dags/dataproc/datalake/data-schemas/Logging"
    # repo = SchemaRepo(f"{schemas_path}/parquet")
    value_schema = fastavro.schema.load_schema(
        path.join(schemas_path, "parquet", "com.thetradedesk.streaming.records.parquet.ParquetWriterSinkControlRecord.avsc")
    )
    avro_serializer = AvroSerializer(schema_registry_client, json.dumps(value_schema))

    producer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer
    }
    avro_producer = SerializingProducer(producer_conf)

    topic_old = "parquet_writer_sink_consentstringv3_ctrl"
    topic_old_schema = {"topic": "bidrequest_gdpr_consent_string", "version": 1, "schemaRegistry": schema_registry_url}
    for cloud_key in cloud_keys_list:
        key = cloud_key

        value_old = {
            "EventType": "FilePush",
            "EventTime": datetime.utcnow(),
            "S3Bucket": bucket,
            "S3Key": cloud_key,
            "FileSchema": topic_old_schema,
            "ParquetFileSchema": topic_old_schema,
            "CheckpointId": 0,
            "ParquetWriterFolderCompleteMetadata": None
        }
        avro_producer.produce(topic=topic_old, key=key, value=value_old)

    avro_producer.flush()


def create_dag(job_config: CloudJobConfig):
    name = f"datalake-consentstring2-{job_config.provider.__str__()}"

    datalake_dag = TtdDag(
        dag_id=name,
        start_date=job_start_date,
        schedule_interval=job_schedule_interval,
        tags=["Datalake", dataproc.jira_team],
        max_active_runs=8,
        default_args={"owner": dataproc.jira_team},
        enable_slack_alert=False,
        teams_allowed_to_access=[AIFUN.team.jira_team]
    )

    check_hour_exists = OpTask(
        op=DatasetCheckSensor(
            datasets=[Datasources.rtb_datalake.rtb_bidrequest_v5],
            ds_date='{{data_interval_start.to_datetime_string()}}',
            poke_interval=60 * 10,  # in seconds
            timeout=24 * 60 * 60  # in seconds
        )
    )

    cluster = EmrClusterTask(
        name=f"{name}",
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[M6g.m6g_xlarge().with_ebs_size_gb(20).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
        ),
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[
                C7g.c7g_2xlarge().with_ebs_size_gb(job_config.base_disk_space * 2).with_fleet_weighted_capacity(2),
                C7g.c7g_4xlarge().with_ebs_size_gb(job_config.base_disk_space * 4).with_fleet_weighted_capacity(4),
                C7g.c7g_8xlarge().with_ebs_size_gb(job_config.base_disk_space * 8).with_fleet_weighted_capacity(8),
            ],
            spot_weighted_capacity=job_config.capacity
        ),
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
        enable_prometheus_monitoring=True,
        cluster_auto_terminates=True,
        cluster_tags={
            "Cloud": job_config.provider.__str__(),
            "Team": dataproc.jira_team,
        },
    )

    step = EmrJobTask(
        name="ExtractConsentString",
        executable_path=aws_jar,
        class_name="jobs.dataproc.datalake.DatalakeConsentStringFeedJob",
        configure_cluster_automatically=True,
        eldorado_config_option_pairs_list=java_settings_list +
        [("dataTime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
         ("ttd.azure.enable", "true" if job_config.provider == CloudProviders.azure else "false"), ("azure.key", "azure-account-key"),
         ("ttd.defaultcloudprovider", job_config.provider.__str__()), ("coalesceSize", job_config.output_files)],
        additional_args_option_pairs_list=additional_args,
        cluster_calc_defaults=ClusterCalcDefaults(memory_tolerance=0.9970),
    )

    cluster.add_parallel_body_task(step)

    send_notification_op = OpTask(
        op=PythonOperator(task_id=send_notification_to_kafka.__name__, python_callable=send_notification_to_kafka, provide_context=True)
    )

    datalake_dag >> check_hour_exists >> cluster >> send_notification_op

    return datalake_dag


cloud_configs = [
    CloudJobConfig(CloudProviders.aws, capacity=16, output_files=200),
    # We don't generate consent string for Walmart yet
    # CloudJobConfig(CloudProviders.azure, capacity=5, output_files=200)
]

for config in cloud_configs:
    dag = create_dag(config)
    globals()[dag.airflow_dag.dag_id] = dag.airflow_dag
