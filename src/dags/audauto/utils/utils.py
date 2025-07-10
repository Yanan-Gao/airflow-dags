import copy
from datetime import datetime, timedelta
import calendar
from typing import Optional, List

from ttd.ec2.emr_instance_type import EmrInstanceType
from ttd.ec2.emr_instance_types.compute_optimized.c4 import C4
from ttd.ec2.emr_instance_types.compute_optimized.c5 import C5
from ttd.ec2.emr_instance_types.general_purpose.m4 import M4
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.datasets.hour_dataset import HourDataset
from ttd.datasets.date_external_dataset import DateExternalDataset
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.dataset import SUCCESS
from ttd.docker import DockerEmrClusterTask, DockerRunEmrTask, DockerCommandBuilder, PySparkEmrTask
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.eldorado.script_bootstrap_action import ScriptBootstrapAction
from ttd.slack.slack_groups import AUDAUTO
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.ttdenv import TtdEnvFactory
import re
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from airflow.operators.python_operator import PythonOperator
from airflow.models import TaskInstance

# Normally we set schemaPolicy to DefaultUseFirstFileSchema,
# but during backfill, we set it to StrictCaseClassSchema
schemaPolicy = [("schemaPolicy", "StrictCaseClassSchema")]

cluster_tags = {
    "Team": AUDAUTO.team.jira_team,
}

schedule_interval = '0 10 * * *'

cpa_roas_dataset_list = [
    "AdGroupPolicyDataset",
    "AdGroupPolicyMappingDataset",
    "ArrayDailyOfflineScoringDataset",
    "ArrayOutOfSampleAttributionDataset",
    "ArraySampledImpressionForIsotonicRegDataset",
    "ArrayUserDataIncCsvForModelTrainingDatasetLastTouch",
    "ArrayUserDataCsvForModelTrainingDatasetLastTouch",
    "ArrayUserDataCsvForModelTrainingDatasetClick",
    "ArrayUserDataIncCsvForModelTrainingDatasetClick",
    "CvrForScalingDataset",
    "DailyAttributionDataset",
    "DailyAttributionEventsDataset",
    "DailyBidFeedbackDataset",
    "DailyBidRequestDataset",
    "DailyBidsImpressionsDataset",
    "DailyBidsImpressionsFullDataset",
    "DailyClickDataset",
    "DailyConversionDataset",
    "DailyExchangeRateDataset",
    "DailyNegativeSampledBidRequestDataSet",
    "DailyOfflineScoringDataset",
    "DailyPositiveBidRequestDataset",
    "DailyPositiveCountSummaryDataset",
    "DataCsvForModelTrainingDatasetClick",
    "DataCsvForModelTrainingDatasetLastTouch",
    "DataForModelTrainingDataset",
    "DataIncCsvForModelTrainingDatasetClick",
    "DataIncCsvForModelTrainingDatasetLastTouch",
    "DataIncForModelTrainingDataset",
    "ImpressionForIsotonicRegDataset",
    "ImpressionPlacementIdDataset",
    "IntermediateTrainDataBalancedDataset",
    "IntermediateTrainDataWithFeatureDataset",
    "IntermediateValidationDataForModelTrainingDataset",
    "InternalDailyOfflineScoringDataset",
    "MetadataDataset",
    "OfflineScoredImpressionDataset",
    "OldDailyOfflineScoringDataset",
    "OldOutOfSampleAttributionDataset",
    "OnlineLogsDiscrepancyDataset",
    "OutOfSampleAttributionDataset",
    "OutOfSampleAttributionDatasetDeprecated",
    "ROASTemporaryIntermediateTrainDataWithFeatureDataset",
    "ROASTemporaryIntermediateValidationDataForModelTrainingDataset",
    "SampledImpressionForIsotonicRegDataset",
    "TrainSetFeatureMappingDataset",
    "UserDataCsvForModelTrainingDataset",
    "UserDataCsvForModelTrainingDatasetClick",
    "UserDataCsvForModelTrainingDatasetLastTouch",
    "UserDataIncCsvForModelTrainingDataset",
    "UserDataIncCsvForModelTrainingDatasetClick",
    "UserDataIncCsvForModelTrainingDatasetLastTouch",
    "ValidationDataForModelTrainingDataset",
    "WatchListDataForEvalDataset",
    "WatchListDataset",
    "WatchListOosDataForEvalDataset",
]


def get_experiment_setting(expriment):
    ret = [(f"ttd.{x}.experiment", expriment) for x in cpa_roas_dataset_list]
    return ret


def get_spark_options_list(num_partitions):
    spark_options_list = [
        ("executor-memory", "100G"),
        ("executor-cores", "16"),
        ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
        ("conf", "spark.driver.memory=100G"),
        ("conf", "spark.driver.cores=15"),
        ("conf", "spark.sql.shuffle.partitions=%s" % num_partitions),
        ("conf", "spark.default.parallelism=%s" % num_partitions),
        ("conf", "spark.driver.maxResultSize=50G"),
        ("conf", "spark.dynamicAllocation.enabled=true"),
        ("conf", "spark.memory.fraction=0.7"),
        ("conf", "spark.memory.storageFraction=0.25"),
        ("conf", "spark.sql.legacy.parquet.int96RebaseModeInRead=CORRECTED"),
    ]
    return spark_options_list


def get_application_configuration():
    application_configuration = [{
        "Classification": "emrfs-site",
        "Properties": {
            "fs.s3.maxConnections": "1000",
            "fs.s3.maxRetries": "100",
            "fs.s3.sleepTimeSeconds": "15",
        },
    }, {
        "Classification": "spark-hive-site",
        "Properties": {
            "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        }
    }]
    return application_configuration


geronimo_date_part_format = 'year=%Y/month=%m/day=%d'  # takes the date passed in from sensor and convert to date partitions.
geronimo_hour_part_format = 'hourPart={hour:d}'


# hour_dataset takes version, environment into consideration in constructing file path.
# However, it concatnates with bucket/path_prefix/env/data_name/version.
# bidimpression data are stored in this format.
# s3://thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1/prod/bidsimpressions/year=2024/month=08/day=21/hourPart=3/_SUCCESS
def get_geronimo_etl_dataset():
    geronimo_etl_dataset = HourDataset(
        bucket="thetradedesk-mlplatform-us-east-1",
        path_prefix="features/data/koav4/v=1/prod",
        data_name='bidsimpressions',
        version=None,
        date_format=geronimo_date_part_format,
        hour_format=geronimo_hour_part_format,
        env_aware=False,
        check_type="hour"
    )
    return geronimo_etl_dataset


last_hour_partition = 23
# takes the date passed in from sensor and convert to success file name.
# example file: _SUCCESS-sx-20240101-23
# example full path: s3://ttd-datapipe-data/parquet/rtb_conversiontracker_cleanfile/v=5/_SUCCESS-sx-20240101-23
# because the date part is generated/parsed by the data sensor class
# and DateExternalDataset that's most closely serving our purpose
# and DateExternalDataset determines the path of the success file as
# f"{self.path_prefix}{data_name_str}" + version_str + date_str. <- date_str is passed in by data sensor class.
# so we pass in the date format as the format that would transform into the successfule name.
conversion_date_part_format = f'_SUCCESS-sx-%Y%m%d-{last_hour_partition}'


def get_conversion_tracker_dataset():
    conversion_tracker_dataset = DateExternalDataset(
        bucket="ttd-datapipe-data",
        path_prefix="parquet",
        data_name="rtb_conversiontracker_cleanfile",
        version=5,
        date_format=conversion_date_part_format,
        success_file="",  # it's important to supply the empty string as success_file
        # o/w the path is treated as a directory to check instead of treated as a file.
    )
    return conversion_tracker_dataset


def get_attribute_event_dataset():
    attribute_event_dataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/sources/firstpartydata_v2",
        data_name="attributedevent",
        date_format="date=%Y-%m-%d",
        version=None,
        env_aware=False,
        success_file=SUCCESS,
    )
    return attribute_event_dataset


def get_attribute_event_result_dataset():
    return DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline/sources/firstpartydata_v2",
        data_name="attributedeventresult",
        date_format="date=%Y-%m-%d",
        version=None,
        env_aware=False,
        success_file=SUCCESS,
    )


incremental_train_key = "incremental_train"


def get_incremental_train_template_scala():
    incremental_train_template_scala = (f'{{{{ task_instance.xcom_pull(key="{incremental_train_key}") }}}}')
    return incremental_train_template_scala


def update_xcom_incremental_train_info(execution_date_str, **kwargs):
    task_instance = kwargs["task_instance"]
    execution_date = datetime.strptime(execution_date_str, "%Y%m%d")
    incremental_train = execution_date.weekday() != calendar.SUNDAY
    task_instance.xcom_push(key="incremental_train", value=str(incremental_train).lower())


def get_update_xcom_incremental_train_task():
    update_xcom_incremental_train_task = PythonOperator(
        task_id=update_xcom_incremental_train_info.__name__,
        python_callable=update_xcom_incremental_train_info,
        op_kwargs={"execution_date_str": "{{ ds_nodash }}"},
        provide_context=True,
    )
    return update_xcom_incremental_train_task


def create_emr_cluster(
    name,
    capacity,
    master_ebs_gb: int = 300,
    core_ebs_per_x: int = 32,
    bootstrap_script_actions: Optional[List[ScriptBootstrapAction]] = None,
    emr_release_label: str = AwsEmrVersions.AWS_EMR_SPARK_3_2
):
    kongming_etl_cluster = EmrClusterTask(
        name=name,
        master_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[R5.r5_4xlarge().with_ebs_size_gb(master_ebs_gb).with_fleet_weighted_capacity(1)],
            on_demand_weighted_capacity=1,
        ),
        cluster_tags=cluster_tags,
        core_fleet_instance_type_configs=EmrFleetInstanceTypes(
            instance_types=[
                R5.r5_8xlarge().with_ebs_size_gb(8 * core_ebs_per_x).with_max_ondemand_price().with_fleet_weighted_capacity(1),
                R5.r5_16xlarge().with_ebs_size_gb(16 * core_ebs_per_x).with_max_ondemand_price().with_fleet_weighted_capacity(2),
                R5.r5_24xlarge().with_ebs_size_gb(24 * core_ebs_per_x).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            ],
            on_demand_weighted_capacity=capacity,
        ),
        emr_release_label=emr_release_label,
        additional_application_configurations=copy.deepcopy(get_application_configuration()),
        enable_prometheus_monitoring=True,
        additional_application=["Hadoop"],  # include Hadoop so that we can use command s3-dist-cp
        bootstrap_script_actions=bootstrap_script_actions,
    )
    return kongming_etl_cluster


def create_emr_spark_job(name, class_name, jar, spark_options_list, job_setting_list, emr, timeout=timedelta(hours=3)):
    t = EmrJobTask(
        name=name,
        class_name=class_name,
        additional_args_option_pairs_list=copy.deepcopy(spark_options_list),
        eldorado_config_option_pairs_list=copy.deepcopy(job_setting_list),
        executable_path=jar,
        timeout_timedelta=timeout
    )
    emr.add_parallel_body_task(t)
    return t


def create_docker_job(name, image_name, image_tag, docker_registry, exec_lang, app_path, exec_args, emr, timeout=timedelta(hours=3)):
    task = DockerRunEmrTask(
        name=name,
        docker_run_command=DockerCommandBuilder(
            docker_registry=docker_registry,
            docker_image_name=image_name,
            docker_image_tag=image_tag,
            execution_language=exec_lang,
            path_to_app=app_path,
            additional_parameters=[
                "--gpus all",
                "--ipc=host",
                "-e TF_GPU_THREAD_MODE=gpu_private",
                "-e NVIDIA_DISABLE_REQUIRE=1",
            ],
            additional_execution_parameters=exec_args,
        ).build_command(),
        timeout_timedelta=timeout,
    )
    emr.add_parallel_body_task(task)
    return task


def create_pyspark_docker_job(
    name, image_name, image_tag, docker_registry, spark_options_list, exec_lang, app_path, exec_args, emr, timeout=timedelta(hours=3)
):
    task = PySparkEmrTask(
        name=name,
        entry_point_path=app_path,
        docker_registry=docker_registry,
        image_name=image_name,
        image_tag=image_tag,
        additional_args_option_pairs_list=spark_options_list,
        command_line_arguments=exec_args,
        timeout_timedelta=timeout,
        python_distribution=exec_lang,
    )
    emr.add_parallel_body_task(task)
    return task


def get_env_vars():
    # Route errors to test channel in test environment
    if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
        return "#dev-perf-auto-alerts-cpa-roas", AUDAUTO.team.sub_team, True
    else:
        return "#scrum-perf-automation-alerts-testing", None, True


def create_single_master_instance_docker_cluster(
    cluster_name: str,
    docker_registry: str,
    docker_image_name: str,
    docker_image_tag: str,
    instance_type: EmrInstanceType,
    entrypoint_in_image: str = None,
):
    master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[instance_type.with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )
    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            M4.m4_large().with_fleet_weighted_capacity(1),
            C4.c4_large().with_fleet_weighted_capacity(1),
            C5.c5_xlarge().with_fleet_weighted_capacity(1),
        ],
        on_demand_weighted_capacity=1,
    )

    return DockerEmrClusterTask(
        name=cluster_name,
        image_name=docker_image_name,
        image_tag=docker_image_tag,
        docker_registry=docker_registry,
        entrypoint_in_image=entrypoint_in_image,
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        cluster_tags=cluster_tags,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
        enable_prometheus_monitoring=True,
        additional_application_configurations=copy.deepcopy(get_application_configuration()),
    )


def create_calculation_docker_cluster(
    cluster_name: str,
    docker_registry: str,
    docker_image_name: str,
    docker_image_tag: str,
    master_instance_type: EmrInstanceType,
    weighted_capacity: int,
    entrypoint_in_image: str,
    master_ebs_gb: int = 512,
    core_ebs_per_x: int = 64,
):
    master_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[master_instance_type.with_ebs_size_gb(master_ebs_gb).with_fleet_weighted_capacity(1)],
        on_demand_weighted_capacity=1,
    )
    core_fleet_instance_type_configs = EmrFleetInstanceTypes(
        instance_types=[
            R5.r5_4xlarge().with_ebs_size_gb(4 * core_ebs_per_x).with_max_ondemand_price().with_fleet_weighted_capacity(1),
            R5.r5_8xlarge().with_ebs_size_gb(8 * core_ebs_per_x).with_max_ondemand_price().with_fleet_weighted_capacity(2),
            R5.r5_12xlarge().with_ebs_size_gb(12 * core_ebs_per_x).with_max_ondemand_price().with_fleet_weighted_capacity(3),
            R5.r5_16xlarge().with_ebs_size_gb(16 * core_ebs_per_x).with_max_ondemand_price().with_fleet_weighted_capacity(4),
            R5.r5_24xlarge().with_ebs_size_gb(24 * core_ebs_per_x).with_max_ondemand_price().with_fleet_weighted_capacity(6),
        ],
        on_demand_weighted_capacity=weighted_capacity,
    )

    return DockerEmrClusterTask(
        name=cluster_name,
        image_name=docker_image_name,
        image_tag=docker_image_tag,
        docker_registry=docker_registry,
        entrypoint_in_image=entrypoint_in_image,
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        cluster_tags=cluster_tags,
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_3,
        enable_prometheus_monitoring=True,
        additional_application_configurations=copy.deepcopy(get_application_configuration()),
    )


def extract_bucket_and_key(s3_path):
    match = re.match(r'^s3://([^/]+)/(.*)$', s3_path)
    if match:
        bucket_name = match.group(1)
        object_key = match.group(2)
        return bucket_name, object_key
    else:
        raise ValueError("Invalid S3 path format")


def list_s3_object(s3_path):
    bucket_name, prefix = extract_bucket_and_key(s3_path)
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    object_keys = []

    # Loop through all pages
    while response.get('IsTruncated'):  # Continue until all pages are retrieved
        # Append the object keys from the current page
        object_keys.extend(["s3://" + bucket_name + "/" + obj['Key'] for obj in response.get('Contents', [])])

        # If the response is truncated, get the next page
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=response.get('NextContinuationToken'))

    # Don't forget the last page if it's not truncated
    object_keys.extend(["s3://" + bucket_name + "/" + obj['Key'] for obj in response.get('Contents', [])])

    print(f"Got {len(object_keys)} under {s3_path}")
    return object_keys


def latest_file_timestamp_in_s3_prefix(s3_path):
    bucket_name, prefix = extract_bucket_and_key(s3_path)
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    last_modified = None

    # Loop through all pages
    while response.get('IsTruncated'):  # Continue until all pages are retrieved
        # get the latest LastModified
        last_modified = max(f['LastModified'] for f in response.get('Contents', []))

        # If the response is truncated, get the next page
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=response.get('NextContinuationToken'))

    # Don't forget the last page if it's not truncated
    if len(response.get('Contents', [])) >= 1:
        last_modified = max(f['LastModified'] for f in response.get('Contents', []))
    return last_modified


def delete_s3_objects_with_prefix(s3_path):
    """
    Deletes all objects in an S3 bucket with the specified prefix.
    """
    print(f"Going to clean up {s3_path}")
    bucket_name, prefix = extract_bucket_and_key(s3_path)
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')

    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    # Collect all object keys to delete
    for page in page_iterator:
        print("deleting with pagine")
        if 'Contents' in page:
            object_keys = []
            for obj in page['Contents']:
                object_keys.append({'Key': obj['Key']})

            # If there are objects to delete, perform the delete operation
            if object_keys:
                response = s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': object_keys})
                print(f"Deleted {len(object_keys)} objects with prefix '{prefix}' from bucket '{bucket_name}'.")
            else:
                print(f"No objects found with prefix '{prefix}' in bucket '{bucket_name}'.")


def extract_file_date_version(task_instance: TaskInstance, **kwargs):

    cutoff_date_str = kwargs["cutoff_date"]

    cutoff_date = datetime.strptime(cutoff_date_str, "%Y%m%d").date()
    # prefix = kwargs["prefix"]
    source_bucket, source_key = extract_bucket_and_key(kwargs['source'])

    # Initialize boto3 S3 client and get the file contents.
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=source_bucket, Key=source_key)
    content = response["Body"].read().decode("utf-8")

    # Split the file into lines and filter out empty ones.
    date_lines = [line.strip() for line in content.splitlines() if line.strip()]

    # Parse each line into a datetime using the known format.
    valid_dates = []
    for line in date_lines:
        try:
            file_dt = datetime.strptime(line, "%Y%m%d%H%M%S")
            if file_dt.date() < cutoff_date:
                valid_dates.append((file_dt, line))
        except Exception as e:
            # Skip any lines that do not parse.
            continue

    if not valid_dates:
        raise ValueError(f"No date in the file is smaller than cutoff_date {cutoff_date_str}")

    selected = max(valid_dates, key=lambda x: x[0])
    print("Selected date string:", selected[1])
    task_instance.xcom_push(key="previous_version", value=selected[1])


def copy_s3_object(**kwargs):
    # Extract the parameters from the context
    source_bucket, source_key = extract_bucket_and_key(kwargs['source'])
    destination_bucket, destination_key = extract_bucket_and_key(kwargs['destination'])

    # Initialize the S3 client
    s3_client = boto3.client('s3')

    try:
        # Copy the object
        s3_client.copy_object(CopySource={'Bucket': source_bucket, 'Key': source_key}, Bucket=destination_bucket, Key=destination_key)
        print(f"Copied {source_bucket}/{source_key} to {destination_bucket}/{destination_key}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Error in credentials: {e}")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise


def copy_s3_dir(**kwargs):

    # Extract bucket and prefix from each using your helper function.
    source_bucket, source_prefix = extract_bucket_and_key(kwargs['source'])
    destination_bucket, destination_prefix = extract_bucket_and_key(kwargs['destination'])

    # Initialize the S3 client.
    s3_client = boto3.client('s3')

    # Use a paginator to list all objects under the source prefix.
    paginator = s3_client.get_paginator('list_objects_v2')
    try:
        for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                # Compute the relative key by stripping the source prefix.
                relative_key = key[len(source_prefix):] if key.startswith(source_prefix) else key
                # Create the new destination key.
                new_key = destination_prefix + relative_key
                s3_client.copy_object(CopySource={'Bucket': source_bucket, 'Key': key}, Bucket=destination_bucket, Key=new_key)
                print(f"Copied {source_bucket}/{key} to {destination_bucket}/{new_key}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Error in credentials: {e}")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
