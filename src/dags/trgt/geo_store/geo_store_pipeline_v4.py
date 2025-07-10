from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.slack.slack_groups import targeting
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.ec2.ec2_subnet import EmrSubnets
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from typing import List, Tuple

import dns.resolver
import socket
import boto3
import pymssql
import copy

# use branch jar initially
job_jar = "s3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/uberjars/latest/com/thetradedesk/geostore/spark/processing/geostoresparkprocessing-assembly.jar"
# if test, use ttd-geo-test
geo_bucket = "ttd-geo"
# if test, use ttd-geo-test
geo_store_bucket = "thetradedesk-useast-geostore"
geo_store_generated_data_prefix = "GeoStoreNg/GeoStoreGeneratedData"
output_root = f"s3://{geo_bucket}/{geo_store_generated_data_prefix}"
active_running_jobs = 1
job_name = "geo-store-pipeline-v4"
date = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='get_value_prepared_task', key='date') }}"
hour = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='get_value_prepared_task', key='hour') }}"
warm_geo_target_path = f"s3a://ttd-geo/GeoStoreNg/ExportedWarmGeoTargets/date={date}/hour={hour}"
full_partial_output = f"{output_root}/{date}/{hour}/S2CellToFullAndPartialMatches"
diffcache_output = f"{output_root}/{date}/{hour}/DiffCacheData"
aerospike_data_output = f"{output_root}/{date}/{hour}/AerospikeData"
small_geos_output_prefix = f"{output_root}/{date}/{hour}/SmallGeo"
partitioned_data_output_prefix = f"{output_root}/{date}/{hour}/PartitionedCellIdToTargets"
forced_trunk_levels_path = "s3a://ttd-geo/GeoStoreNg/GeoStoreForcedTrunkLevels"
polygon_cell_mappings_path = "s3a://ttd-geo/GeoStoreNg/PolygonCellMappings"
num_files = 5
HDFSRootPath = "hdfs:///user/hadoop/output-temp-dir"

num_cores_expanding_cluster = 480
num_partition_expanding_cluster = 960

num_cores_converting_cluster = 64
num_partition_converting_cluster = 128

num_cores_partitioning_cluster = 160
num_partition_partitioning_cluster = 320

job_schedule_interval = "0 */3 * * *"
aerospike_namespace = "ttd-geo"
aerospike_set = "reg"

de2_host = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='get_value_prepared_task', key='de2_host') }}"
ie1_host = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='get_value_prepared_task', key='ie1_host') }}"
sg2_host = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='get_value_prepared_task', key='sg2_host') }}"
jp1_host = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='get_value_prepared_task', key='jp1_host') }}"
ca4_host = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='get_value_prepared_task', key='ca4_host') }}"
ca2_host = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='get_value_prepared_task', key='ca2_host') }}"
ny1_host = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='get_value_prepared_task', key='ny1_host') }}"
va6_host = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='get_value_prepared_task', key='va6_host') }}"
vad_host = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='get_value_prepared_task', key='vad_host') }}"
vae_host = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='get_value_prepared_task', key='vae_host') }}"
vam_host = "{{ task_instance.xcom_pull(dag_id='" + job_name + "', task_ids='get_value_prepared_task', key='vam_host') }}"

aerospike_cluster_service_names = [("sg2", "ttd-geo.aerospike.service.sg2.consul"), ("jp1", "ttd-geo.aerospike.service.jp1.consul"),
                                   ("de2", "ttd-geo.aerospike.service.de2.consul"), ("ie1", "ttd-geo.aerospike.service.ie1.consul"),
                                   ("ca2", "ttd-geo.aerospike.service.ca2.consul"), ("ca4", "ttd-geo.aerospike.service.ca4.consul"),
                                   ("ny1", "ttd-geo.aerospike.service.ny1.consul"), ("va6", "ttd-geo.aerospike.service.va6.consul"),
                                   ("vae", "ttd-geo.aerospike.service.vae.consul"), ("vad", "ttd-geo.aerospike.service.vad.consul"),
                                   ("vam", "ttd-geo.aerospike.service.vam.consul")]

aerospike_clusters = [("sg2", sg2_host, "ap-southeast-1", aerospike_namespace), ("jp1", jp1_host, "ap-northeast-1", aerospike_namespace),
                      ("de2", de2_host, "eu-central-1", aerospike_namespace), ("ca4", ca4_host, "us-east-1", aerospike_namespace),
                      ("ca2", ca2_host, "us-east-1", aerospike_namespace), ("ny1", ny1_host, "us-east-1", aerospike_namespace),
                      ("ie1", ie1_host, "eu-west-1", aerospike_namespace), ("va6", va6_host, "us-east-1", aerospike_namespace),
                      ("vad", vad_host, "us-east-1", aerospike_namespace), ("vae", vae_host, "us-east-1", aerospike_namespace),
                      ("vam", vam_host, "us-east-1", aerospike_namespace)]

# This is a test example
aerospike_clusters_test = [("sg2", sg2_host, "ap-southeast-1", "ttd-geo-test"),
                           ("cit-test3", "10.100.190.225:3000", "us-east-1", "ttd-test")]

cluster_kwargs_by_region = {
    "us-east-1": {
        "enable_prometheus_monitoring": True,
        "enable_spark_history_server_stats": False,
    },
    "us-west-2": {
        "emr_managed_master_security_group": "sg-0bf03a9cbbaeb0494",
        "emr_managed_slave_security_group": "sg-0dfc2e6a823862dbf",
        "ec2_subnet_ids": EmrSubnets.PrivateUSWest2.all(),
        "pass_ec2_key_name": False,
        "service_access_security_group": "sg-0ccb4ca554f6e1165",
        "enable_prometheus_monitoring": True,
        "enable_spark_history_server_stats": False,
    },
    "ap-northeast-1": {
        "emr_managed_master_security_group": "sg-02cd06e673800a7d4",
        "emr_managed_slave_security_group": "sg-0a9b18bb4c0fa5577",
        "ec2_subnet_ids": EmrSubnets.PrivateAPNortheast1.all(),
        "pass_ec2_key_name": False,
        "service_access_security_group": "sg-0644d2eafc6dd2a8d",
        "enable_prometheus_monitoring": True,
        "enable_spark_history_server_stats": False,
    },
    "ap-southeast-1": {
        "emr_managed_master_security_group": "sg-014b895831026416d",
        "emr_managed_slave_security_group": "sg-03149058ce1479ab2",
        "ec2_subnet_ids": EmrSubnets.PrivateAPSoutheast1.all(),
        "pass_ec2_key_name": False,
        "service_access_security_group": "sg-008e3e75c75f7885d",
        "enable_prometheus_monitoring": True,
        "enable_spark_history_server_stats": False,
    },
    "eu-west-1": {
        "emr_managed_master_security_group": "sg-081d59c2ec2e9ef68",
        "emr_managed_slave_security_group": "sg-0ff0115d48152d67a",
        "ec2_subnet_ids": EmrSubnets.PrivateEUWest1.all(),
        "pass_ec2_key_name": False,
        "service_access_security_group": "sg-06a23349af478630b",
        "enable_prometheus_monitoring": True,
        "enable_spark_history_server_stats": False,
    },
    "eu-central-1": {
        "emr_managed_master_security_group": "sg-0a905c2e9d0b35fb8",
        "emr_managed_slave_security_group": "sg-054551f0756205dc8",
        "ec2_subnet_ids": EmrSubnets.PrivateEUCentral1.all(),
        "pass_ec2_key_name": False,
        "service_access_security_group": "sg-09a4d1b6a8145bd39",
        "enable_prometheus_monitoring": False,
        "enable_spark_history_server_stats": False,
    },
}

dag = TtdDag(
    dag_id=job_name,
    start_date=datetime(2024, 10, 17),
    schedule_interval=job_schedule_interval,
    max_active_runs=active_running_jobs,
    slack_channel="#scrum-targeting-alarms",
    slack_alert_only_for_prod=True,
    tags=[targeting.jira_team]
)

java_settings_list = [("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "4096")]


def create_cluster(cluster_label, master_fleet_instance_type_configs, core_fleet_instance_type_configs, aws_region, kwargs):
    return EmrClusterTask(
        name=f"{cluster_label}_{aws_region}",
        master_fleet_instance_type_configs=master_fleet_instance_type_configs,
        core_fleet_instance_type_configs=core_fleet_instance_type_configs,
        cluster_tags={"Team": targeting.jira_team},
        emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2,
        region_name=aws_region,
        **kwargs
    )


# get value prepared task
def get_ip_addresses(dns_name):
    try:
        ip_addresses = socket.gethostbyname_ex(dns_name)[2]
        print(ip_addresses)
        return ip_addresses
    except socket.gaierror as e:
        print(f"Failed to resolve IPs: {e}")
        return []


def get_dns_ips(dns_name, port):
    try:
        result = dns.resolver.resolve(dns_name, 'A')  # 'A' record for IPv4 addresses
        ips = [f"{ip.address}:{port}" for ip in result]  # type: ignore
        aerospike_host = ",".join(ips)  # Aerospike host format
        print(aerospike_host)
        return aerospike_host
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.Timeout) as e:
        print(f"Failed to resolve IPs: {e}")
        return []


def get_value_prepared(**context):
    # Get the latest hour and date
    bucket = 'ttd-geo'
    key = 'GeoStoreNg/ExportedWarmGeoTargets/LatestExportRecord'

    s3 = boto3.client("s3")
    latest_file_contents = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode().strip()

    # Extract date and hour from the row in the latest file
    date = latest_file_contents.split('date=')[1].split('/')[0]
    hour = latest_file_contents.split('hour=')[1].split('/')[0]
    context['task_instance'].xcom_push(key='date', value=date)
    context['task_instance'].xcom_push(key='hour', value=hour)
    print(f"get date {date} and hour {hour} from {bucket}/{key}")

    for (cluster, name) in aerospike_cluster_service_names:
        # Resolve IPs by DNS and compose them with port
        host = get_dns_ips(name, 3000)
        context['task_instance'].xcom_push(key=f'{cluster}_host', value=host)


get_value_prepared_task = OpTask(
    op=PythonOperator(task_id='get_value_prepared_task', python_callable=get_value_prepared, provide_context=True)
)

# process geo targets and partition data
partition_cluster_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5d.r5d_2xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

partition_cluster_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5d.r5d_8xlarge()], on_demand_weighted_capacity=num_cores_partitioning_cluster
)

partition_cluster_additional_args_option_pairs_list = [
    ("conf", "spark.driver.memory=64g"),
    ("conf", "spark.driver.cores=8"),
    ("conf", "spark.executor.cores=15"),
    ("conf", "spark.executor.memory=100g"),
    ("conf", "spark.executor.memoryOverhead=10g"),
    ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", f"spark.sql.shuffle.partitions={num_partition_converting_cluster}"),
    ("conf", f"spark.default.parallelism={num_partition_converting_cluster}"),
    ("conf", "spark.memory.fraction=0.9"),
]

partition_cluster_task = create_cluster(
    'process_geo_targets_and_partition_s2_cells', partition_cluster_master_fleet_instance_type_configs,
    partition_cluster_core_fleet_instance_type_configs, "us-east-1", cluster_kwargs_by_region["us-east-1"]
)

# Step 1: filter out small geo targets
generate_valid_warm_geo_target_data_task = EmrJobTask(
    cluster_specs=partition_cluster_task.cluster_specs,
    name="generate_valid_warm_geo_target_data",
    class_name="com.thetradedesk.jobs.GenerateValidWarmGeoTargetData",
    executable_path=job_jar,
    eldorado_config_option_pairs_list=java_settings_list + [("stateShapeCellLevel", 17), ("usStateMinimumRadiusInMeters", 914.4),
                                                            ("defaultMinimumRadiusInMeters", 100),
                                                            ("warmGeoTargetsPath", warm_geo_target_path),
                                                            ("segmentShapePathPrefix", "s3a://ttd-geo/GeoStoreNg/ConfigMinimumAreaShapes/"),
                                                            ("segmentShapeFiles", "washington,newyork,connecticut,nevada"),
                                                            ("validWarmGeoTargetsPath", HDFSRootPath + "/ValidWarmGeoTargets"),
                                                            ("smallGeoPathPrefix", small_geos_output_prefix),
                                                            ("geoStorePolygonCellMappingsPath", polygon_cell_mappings_path)],
    additional_args_option_pairs_list=partition_cluster_additional_args_option_pairs_list +
    [('conf', 'spark.kryoserializer.buffer.max=512m')]
)

# Step 2: partition S2 cells
partition_s2_cells_task = EmrJobTask(
    cluster_specs=partition_cluster_task.cluster_specs,
    name="partition_s2_cells",
    class_name="com.thetradedesk.jobs.PartitionS2Cells",
    executable_path=job_jar,
    eldorado_config_option_pairs_list=java_settings_list + [("sensitivePlacesBucketName", "thetradedesk-useast-data-import"),
                                                            ("sensitivePlacesPrefix", "factual/integrated_places/"),
                                                            ("sensitivePlacesPrivacyRadiusInMeters", 50),
                                                            ("sensitivePlacesPrivacyBrandPrefix", "ttd-blk-"),
                                                            ("SensitivePlacesMinimumProcessedRatio", 0.01),
                                                            ("sensitivePlacesOutputBucketName", geo_bucket), ("geoStoreNg", "GeoStoreNg"),
                                                            ("sensitivePlacesOutput", "SensitivePlacesOutput"),
                                                            ("validWarmGeoTargetsPath", HDFSRootPath + "/ValidWarmGeoTargets"),
                                                            ("trunkLevel", 10), ("levelBegin", 10), ("numFiles", num_files),
                                                            ("partitionedCellIdToTargetsPrefix", partitioned_data_output_prefix),
                                                            ("geoStoreForcedTrunkLevelsPath", forced_trunk_levels_path)],
    additional_args_option_pairs_list=partition_cluster_additional_args_option_pairs_list
)

partition_cluster_task.add_sequential_body_task(generate_valid_warm_geo_target_data_task)
partition_cluster_task.add_sequential_body_task(partition_s2_cells_task)

# process partitioned data
process_partitioned_data_cluster_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5d.r5d_2xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

process_partitioned_data_cluster_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5d.r5d_8xlarge()], on_demand_weighted_capacity=num_cores_expanding_cluster
)

process_partitioned_data_cluster_additional_args_option_pairs_list = [
    ("conf", "spark.driver.memory=64g"),
    ("conf", "spark.driver.cores=8"),
    ("conf", "spark.executor.cores=15"),
    ("conf", "spark.executor.memory=100g"),
    ("conf", "spark.executor.memoryOverhead=10g"),
    ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", f"spark.sql.shuffle.partitions={num_partition_expanding_cluster}"),
    ("conf", f"spark.default.parallelism={num_partition_expanding_cluster}"),
    ("conf", "spark.memory.fraction=0.9"),
]


def create_expand_s2_cells_task(cluster: EmrClusterTask, fileIndex: int):
    return EmrJobTask(
        cluster_specs=cluster.cluster_specs,
        name="expand_s2_cells",
        class_name="com.thetradedesk.jobs.ExpandS2CellsV3",
        executable_path=job_jar,
        eldorado_config_option_pairs_list=java_settings_list +
        [("partitionedCellIdToTargetsPath", f"{partitioned_data_output_prefix}/FileIndex={fileIndex}"),
         ("geoStoreS3S2CellToFullAndPartialMatchesAtLvlPath", f"{full_partial_output}/FileIndex={fileIndex}"),
         ("geoStoreDiffCacheDataAtLvlPath", f"{diffcache_output}/FileIndex={fileIndex}"), ("trunkLevel", 10), ("maxLeafTargetCount", 500),
         ("partitionLevel", 5), ("levelBegin", 10), ("levelEnd", 16), ("geoCacheEnv", "HDFS"), ("hdfsRootPath", HDFSRootPath),
         ("partialMaxPerBin", 1000), ("numFiles", num_files), ("geoStoreForcedTrunkLevelsPath", forced_trunk_levels_path),
         ("geoStorePolygonCellMappingsPath", polygon_cell_mappings_path)],
        additional_args_option_pairs_list=copy.deepcopy(process_partitioned_data_cluster_additional_args_option_pairs_list)
    )


def create_convert_aerospike_data_task(cluster: EmrClusterTask, fileIndex: int):
    return EmrJobTask(
        cluster_specs=cluster.cluster_specs,
        name="generate_aerospike_data_task",
        class_name="com.thetradedesk.jobs.GenerateAerospikeData",
        executable_path=job_jar,
        eldorado_config_option_pairs_list=java_settings_list +
        [("geoStoreS3S2CellToFullAndPartialMatchesAtLvlPath", f"{full_partial_output}/FileIndex={fileIndex}/*"),
         ("geoStoreS3AerospikeDataPath", f"{aerospike_data_output}/FileIndex={fileIndex}")],
        additional_args_option_pairs_list=copy.deepcopy(process_partitioned_data_cluster_additional_args_option_pairs_list)
    )


def generate_process_partitioned_data_clusters(num_files: int):
    clusters = []
    for i in range(num_files):
        cluster_task = create_cluster(
            f'expand-s2-cells-generate-diffcache-convert-aerospike-data-for-partition-index-{i}',
            process_partitioned_data_cluster_master_fleet_instance_type_configs,
            process_partitioned_data_cluster_core_fleet_instance_type_configs, "us-east-1", cluster_kwargs_by_region["us-east-1"]
        )
        cluster_task.add_sequential_body_task(create_expand_s2_cells_task(cluster_task, i))
        cluster_task.add_sequential_body_task(create_convert_aerospike_data_task(cluster_task, i))
        clusters.append(cluster_task)

    return clusters


process_partitioned_data_clusters = generate_process_partitioned_data_clusters(num_files)

# push Aerospike task
push_cluster_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

push_cluster_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[M5.m5_2xlarge().with_max_ondemand_price().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=8
)


def create_push_task(aws_region, host, namespace):
    return EmrJobTask(
        name="push_to_aerospike",
        class_name="com.thetradedesk.jobs.WriteGeoDataToAerospike",
        executable_path=job_jar,
        eldorado_config_option_pairs_list=[("geoStoreS3AerospikeDataPath", f"{aerospike_data_output}/*"),
                                           ("aerospikeRetryMaxAttempts", 200), ("aerospikeRetryInitialMs", 50),
                                           ("aerospikeTransactionRate", 1500), ("aerospikeHosts", host), ("aerospikeNamespace", namespace),
                                           ("aerospikeSet", aerospike_set), ("useAerospikeTestCluster", "false")],
        additional_args_option_pairs_list=[
            ("conf", "spark.executor.cores=5"),
            ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
        ],
        region_name=aws_region
    )


def generate_push_clusters(aerospike_clusters: List[Tuple[str, str, str, str]]):
    clusters = []
    for clusterName, host, aws_region, namespace in aerospike_clusters:
        cluster_task = create_cluster(
            f'push-data-to-aerospike-cluster-{clusterName}', push_cluster_master_fleet_instance_type_configs,
            push_cluster_core_fleet_instance_type_configs, aws_region, cluster_kwargs_by_region[aws_region]
        )

        job_task = create_push_task(aws_region, host, namespace)
        cluster_task.add_parallel_body_task(job_task)
        clusters.append(cluster_task)

    return clusters


push_aerospike_clusters = generate_push_clusters(aerospike_clusters)

# convert and push diffcache
convert_and_push_diffcache_cluster_master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5d.r5d_2xlarge().with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

convert_and_push_diffcache_cluster_core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[R5d.r5d_8xlarge()], on_demand_weighted_capacity=num_cores_converting_cluster
)

convert_and_push_diffcache_cluster_additional_args_option_pairs_list = [
    ("conf", "spark.driver.memory=64g"),
    ("conf", "spark.driver.cores=8"),
    ("conf", "spark.executor.cores=15"),
    ("conf", "spark.executor.memory=100g"),
    ("conf", "spark.executor.memoryOverhead=10g"),
    ("conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
    ("conf", "spark.executor.extraJavaOptions=-server -XX:+UseParallelGC"),
    ("conf", f"spark.sql.shuffle.partitions={num_partition_converting_cluster}"),
    ("conf", f"spark.default.parallelism={num_partition_converting_cluster}"),
    ("conf", "spark.memory.fraction=0.9"),
]

convert_and_push_diffcache_cluster_task = create_cluster(
    'convert-and-push-diffcache-index', convert_and_push_diffcache_cluster_master_fleet_instance_type_configs,
    convert_and_push_diffcache_cluster_core_fleet_instance_type_configs, "us-east-1", cluster_kwargs_by_region["us-east-1"]
)

# Step 1: convert diffcache data to protobuf, and upload it to S3
write_diffcache_data_task = EmrJobTask(
    cluster_specs=convert_and_push_diffcache_cluster_task.cluster_specs,
    name="convert_upload_diffcache_data",
    class_name="com.thetradedesk.jobs.WriteGeoStoreDiffCacheData",
    executable_path=job_jar,
    eldorado_config_option_pairs_list=java_settings_list + [("geoStoreDiffCacheDataAtLvlPath", f"{diffcache_output}/*/*"),
                                                            ("geoStoreBucket", geo_store_bucket), ("geoStorePrefix", "diffcache2")],
    additional_args_option_pairs_list=convert_and_push_diffcache_cluster_additional_args_option_pairs_list
)

convert_and_push_diffcache_cluster_task.add_sequential_body_task(write_diffcache_data_task)


# update diffcache index db
def get_db_connection(conn_name):
    conn_info = BaseHook.get_connection(conn_name)
    server = conn_info.host
    user = conn_info.login
    password = conn_info.password
    database = conn_info.schema
    return pymssql.connect(server=server, user=user, password=password, database=database)


def update_diffcache_index_db(conn_id, **kwargs):
    # fetch the version
    aws_cloud_storage = AwsCloudStorage(conn_id='aws_default')
    keys = aws_cloud_storage.list_keys(bucket_name='thetradedesk-useast-geostore', prefix='diffcache2')
    file_names = [int(key.split('/')[-1].split('.')[0]) for key in keys if key.split('/')[-1].split('.')[0].isdigit()]
    max_file_version = max(file_names)
    print(f'The max version of DiffCache index is {max_file_version}')

    # update in db
    conn = get_db_connection(conn_id)
    cursor = conn.cursor()

    sprocCall = (
        "DECLARE @updateTime DATETIME = GETUTCDATE(); "
        f"DECLARE @insertVersion BIGINT = {max_file_version}; "
        "DECLARE @maxDbVersion BIGINT; "
        "SELECT @maxDbVersion = MAX(Version) FROM dbo.GeoSparkStatus; "
        "IF (@insertVersion > @maxDbVersion) "
        "BEGIN "
        "    INSERT INTO dbo.GeoSparkStatus(Version, Time) VALUES (@insertVersion, @updateTime); "
        "END "
        "ELSE "
        "BEGIN "
        "    PRINT 'Insert version is not greater than the max version in db'; "
        "END"
    )

    # Execute the stored procedure call
    cursor.execute(sprocCall)
    conn.commit()
    conn.close()


update_diffcache_index_db = OpTask(
    op=PythonOperator(
        task_id='update_diffcache_index_db',
        python_callable=update_diffcache_index_db,
        op_kwargs={'conn_id': 'ttd_geo_provdb'},
        provide_context=True
    )
)


def set_latest_job_done(**context):
    latest_prefix_key = f"{geo_store_generated_data_prefix}/LatestPrefix.txt"
    s3 = boto3.resource("s3")
    current_date = context['task_instance'].xcom_pull(task_ids='get_value_prepared_task', key='date')
    current_hour = context['task_instance'].xcom_pull(task_ids='get_value_prepared_task', key='hour')
    content = f"{current_date}/{current_hour}"
    s3.Object(geo_bucket, latest_prefix_key).put(Body=content)

    print(f"set {current_date}/{current_hour} to {geo_bucket}/{latest_prefix_key}")


set_latest_job_done_task = OpTask(
    op=PythonOperator(
        task_id="set_latest_job_done_task",
        python_callable=set_latest_job_done,
        provide_context=True,
    )
)

# Check all steps
check_all_steps_succeeded = OpTask(op=FinalDagStatusCheckOperator(dag=dag.airflow_dag, name="all_steps_success_check"))

# Dependency
dag >> get_value_prepared_task >> partition_cluster_task

for task in process_partitioned_data_clusters:
    partition_cluster_task >> task
for task in process_partitioned_data_clusters:
    task >> convert_and_push_diffcache_cluster_task

for task in push_aerospike_clusters:
    convert_and_push_diffcache_cluster_task >> task
for task in push_aerospike_clusters:
    task >> update_diffcache_index_db

update_diffcache_index_db >> set_latest_job_done_task
set_latest_job_done_task >> check_all_steps_succeeded
# Airflow only recognizes top-level dag objects, so extract the underlying dag we generated.
geo_store_dag = dag.airflow_dag
