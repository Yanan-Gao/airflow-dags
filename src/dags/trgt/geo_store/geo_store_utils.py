from ttd.ec2.ec2_subnet import EmrSubnets
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.ttdenv import TtdEnvFactory
from airflow.hooks.base import BaseHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime
import dns.resolver
import boto3
import pymssql
import io
import re
from urllib.parse import urlparse
from airflow.models import Variable

if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
    # use the release path for airflow prod
    job_jar_prefix = 's3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/uberjars/latest/com/thetradedesk/geostore/spark/processing'
else:
    # use snapshot path for airflow prod test
    job_jar_prefix = 's3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/snapshot/uberjars/latest/com/thetradedesk/geostore/spark/processing'

default_jar_name = 'geostoresparkprocessing-assembly.jar'
geostore_jar_name_key = 'geostore_jar_name'

update_small_geo_by_general_pipeline = False


class GeoStoreUtils:
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

    aws_cloud_storage = AwsCloudStorage()

    delta_pipeline_aerospike_namespace = "ttd-geo"
    delta_pipeline_aerospike_set = "reg"
    delta_pipeline_aerospike_cluster_service_names = {
        "sg2": "ttd-geo.aerospike.service.sg2.consul",
        "jp1": "ttd-geo.aerospike.service.jp1.consul",
        "de2": "ttd-geo.aerospike.service.de2.consul",
        "ie1": "ttd-geo.aerospike.service.ie1.consul",
        "ca2": "ttd-geo.aerospike.service.ca2.consul",
        "ny1": "ttd-geo.aerospike.service.ny1.consul",
        "va6": "ttd-geo.aerospike.service.va6.consul",
        "vae": "ttd-geo.aerospike.service.vae.consul",
        "vad": "ttd-geo.aerospike.service.vad.consul",
        "vam": "ttd-geo.aerospike.service.vam.consul",
        "wa2": "ttd-geo.aerospike.service.wa2.consul",
        "vat": "ttd-geo.aerospike.service.vat.consul"
    }

    general_polygon__pipeline_aerospike_set = "reg"
    if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
        general_polygon_pipeline_aerospike_namespace = "ttd-geo"
        # prod test doesn't push to aerospike for now, should coordinate with delta aerospike service names in future
        general_polygon_pipeline_aerospike_cluster_service_names = {}
    else:
        # prod test airflow should push to aerospike prod test cluster and namespace
        general_polygon_pipeline_aerospike_namespace = "ttd-geo-test"
        general_polygon_pipeline_aerospike_cluster_service_names = {'sg2': 'aerospike-sg2-prodtest-geo.aerospike.service.sg2.consul'}

    # Test one cluster
    # aerospike_namespace = "ttd-geo-test"
    # aerospike_set = "reg"
    # aerospike_cluster_service_names = {"sg2": "ttd-geo.aerospike.service.sg2.consul"}

    aerospike_clusters_regions = {
        "sg2": "ap-southeast-1",
        "jp1": "ap-northeast-1",
        "de2": "eu-central-1",
        "ie1": "eu-west-1",
        "ca2": "us-east-1",
        "ny1": "us-east-1",
        "va6": "us-east-1",
        "vae": "us-east-1",
        "vad": "us-east-1",
        "vam": "us-east-1",
        "wa2": "us-west-2",
        "vat": "us-east-1",
    }

    @staticmethod
    def get_dns_ips(dns_name, port=3000):
        try:
            result = dns.resolver.resolve(dns_name, 'A')  # 'A' record for IPv4 addresses
            ips = [f"{ip.address}:{port}" for ip in result]  # type: ignore
            aerospike_host = ",".join(ips)  # Aerospike host format
            print(aerospike_host)
            return aerospike_host
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.Timeout) as e:
            print(f"Failed to resolve IPs: {e}")
            return []

    @staticmethod
    def get_max_number(bucket, prefix):
        print(f"Looking at the bucket: {bucket}, prefix: {prefix}")

        s3 = boto3.client('s3')
        paginator = s3.get_paginator('list_objects_v2')
        result_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

        csv_files = []
        for result in result_iterator:
            for obj in result.get('Contents', []):
                file_key = obj['Key']
                file_name = file_key.split('/')[-1]
                file_number = file_name.split('.')[0]

                if file_number.isdigit():
                    csv_files.append(int(file_number))
        if csv_files:
            max_file_number = max(csv_files)
            return f"{max_file_number}"
        else:
            return None

    @staticmethod
    def get_max_two(bucket, prefix, strip_prefix, date_format):
        print(f"Looking at the bucket: {bucket}, prefix: {prefix}")
        s3 = boto3.client('s3')
        result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
        date_prefixes = [obj['Prefix'] for obj in result.get('CommonPrefixes', []) if strip_prefix in obj['Prefix']]
        if not date_prefixes:
            return None

        sorted_prefixes = sorted(
            date_prefixes,
            key=lambda p: datetime.strptime(p.replace(prefix, '').strip('/').replace(strip_prefix, ''), date_format),
            reverse=True
        )
        top_two = [p.replace(prefix, '').strip('/') for p in sorted_prefixes[:2]]
        return top_two

    @staticmethod
    def get_max_two_hour_paths(bucket, prefix, date_strip_prefix, date_format, hour_strip_prefix, hour_format):
        print(f"Looking at the bucket: {bucket}, prefix: {prefix}")
        s3 = boto3.client('s3')

        max_two_dates = GeoStoreUtils.get_max_two(bucket, prefix, date_strip_prefix, date_format)
        max_two_hours_day1 = GeoStoreUtils.get_max_two(bucket, f'{prefix}{max_two_dates[0]}/', hour_strip_prefix, hour_format)
        max_two_hour_paths = [f"{bucket}/{prefix}{max_two_dates[0]}/{path}" for path in max_two_hours_day1]

        if len(max_two_hour_paths) == 2:
            return max_two_hour_paths
        else:
            max_two_hours_day2 = GeoStoreUtils.get_max_two(bucket, f'{prefix}{max_two_dates[1]}/', hour_strip_prefix, hour_format)
            max_two_hour_paths_day2 = [f"{bucket}/{prefix}{max_two_dates[1]}/{path}" for path in max_two_hours_day2]
            max_two_hour_paths.append(max_two_hour_paths_day2[0])
            print(max_two_hour_paths)
            return max_two_hour_paths

    @staticmethod
    def get_all_file_names(bucket, prefix):
        print(f"Looking at the bucket: {bucket}, prefix: {prefix}")
        s3 = boto3.client('s3')
        result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
        file_names = [
            obj['Prefix'].replace(prefix, '').strip('/') for obj in result.get('CommonPrefixes', []) if 'FileIndex=' in obj['Prefix']
        ]
        print(file_names)
        return file_names

    @staticmethod
    def get_db_connection(conn_name):
        conn_info = BaseHook.get_connection(conn_name)
        server = conn_info.host
        user = conn_info.login
        password = conn_info.password
        database = conn_info.schema
        return pymssql.connect(server=server, user=user, password=password, database=database)

    @staticmethod
    def update_diffcache_index_db(conn_id):
        if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
            max_file_version = GeoStoreUtils.get_max_number('thetradedesk-useast-geostore', 'diffcache2')
            print(f'The max version of DiffCache index is {max_file_version}')
            # update in db
            conn = GeoStoreUtils.get_db_connection(conn_id)
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
        else:
            print("Skipping update as environment is not the prod")
            return

    @staticmethod
    def update_diffcache_3_index_db(conn_id):
        if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
            max_file_version = GeoStoreUtils.get_max_number('thetradedesk-useast-geostore', 'diffcache3')
            print(f'The max version of DiffCache index is {max_file_version}')
            # update in db
            conn = GeoStoreUtils.get_db_connection(conn_id)
            cursor = conn.cursor()

            sprocCall = (
                "DECLARE @updateTime DATETIME = GETUTCDATE(); "
                f"DECLARE @insertVersion BIGINT = {max_file_version}; "
                "DECLARE @maxDbVersion BIGINT; "
                "SELECT @maxDbVersion = MAX(Version) FROM dbo.GeoSpark3Status; "
                "IF (@insertVersion > @maxDbVersion) "
                "BEGIN "
                "    INSERT INTO dbo.GeoSpark3Status(Version, Time) VALUES (@insertVersion, @updateTime); "
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
        else:
            print("Skipping update as environment is not the prod")
            return

    @staticmethod
    def update_high_freq_diff_cache_index_db(conn_id, insert_version):
        if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
            conn = GeoStoreUtils.get_db_connection(conn_id)
            cursor = conn.cursor()

            sprocCall = (
                "DECLARE @updateTime DATETIME = GETUTCDATE(); "
                f"DECLARE @insertVersion BIGINT = {insert_version}; "
                "DECLARE @maxDbVersion BIGINT; "
                "SELECT @maxDbVersion = MAX(Version) FROM dbo.GeoHighFreqTaskServiceStatus; "
                "IF (@insertVersion > @maxDbVersion) "
                "BEGIN "
                "    INSERT INTO dbo.GeoHighFreqTaskServiceStatus(Version, Time) VALUES (@insertVersion, @updateTime); "
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
        else:
            print("Skipping update as environment is not the prod")
            return

    @staticmethod
    def update_small_geo_db(conn_id, path):
        if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
            import pandas as pd

            s3 = boto3.client('s3')
            if path.startswith("s3://"):
                stripped_path = path[5:]  # Remove 's3://'
                bucket, _, prefix = stripped_path.partition('/')
                print(f"Bucket: {bucket}")
                print(f"Prefix: {prefix}")
                response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
                parquet_files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".parquet")]

                if not parquet_files:
                    print("No Parquet files found!")
                    return

                # Read Parquet files into a DataFrame
                dataframes = []
                for file_key in parquet_files:
                    obj = s3.get_object(Bucket=bucket, Key=file_key)
                    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
                    dataframes.append(df)

                # Combine all DataFrames
                if dataframes:
                    df = pd.concat(dataframes, ignore_index=True)
                else:
                    print("No dataframes were created!")
                    return

                targeting_data_ids = ','.join(map(str, df['TargetingDataId'].tolist()))
                filter_types = ','.join(map(str, df['FilterType'].tolist()))

                conn = GeoStoreUtils.get_db_connection(conn_id)
                cursor = conn.cursor()
                execute_sproc = """
                    DECLARE @targetingDataIds nvarchar(max), @filterTypes nvarchar(max);
                    SET NOCOUNT ON;
                    SET @targetingDataIds = %s;
                    SET @filterTypes = %s;
                    EXEC dbo.prc_UpdateSmallGeoTargetingDataV2 @targetingDataIds = @targetingDataIds, @filterTypes = @filterTypes;
                """
                cursor.execute(execute_sproc, (targeting_data_ids, filter_types))
                conn.commit()
                conn.close()
        else:
            print("Skipping update as environment is not the prod")
            return

    @staticmethod
    def push_values_to_xcom(config, **context):
        # override the JAR name by airflow variable in order to save the effort of code change when doing prod test
        geostore_jar_name = Variable.get('GEOSTORE_JAR_NAME', default_var=default_jar_name)
        context['task_instance'].xcom_push(key=geostore_jar_name_key, value=geostore_jar_name)

        # Get last successful delta build paths
        s3 = boto3.client("s3")

        if not (config.is_full_build.lower() == 'true'):
            latest_file_contents = s3.get_object(Bucket=config.geo_bucket, Key=config.delta_last_log)["Body"].read().decode().strip()

            for line in latest_file_contents.splitlines():
                if line.strip():
                    # Split on the first ':' to get key and value
                    if ':' in line:
                        key, value = line.split(':', 1)
                        context['task_instance'].xcom_push(key=key, value=value)
                    else:
                        print(f"Warning: Line is malformed and will be skipped: {line}")
        else:
            print("Current build is a full build")

        # Get current build paths
        # TODO: this code will be replaced by reading the latest path from databahn
        ##########################
        # eg: ['ttd-geo-test/env=test/test/date=20141011/time=010000', 'ttd-geo-test/env=test/test/date=20141010/time=123456']
        max_two_hour_paths_targeting_data_geo_target = GeoStoreUtils.get_max_two_hour_paths(
            config.cdc_bucket, config.targeting_data_geo_target_prefix, 'date=', '%Y%m%d', 'time=', '%H%M%S'
        )

        context['task_instance'].xcom_push(
            key=config.current_targeting_data_geo_target_path_key, value=f's3://{max_two_hour_paths_targeting_data_geo_target[1]}'
        )

        max_two_hour_paths_geo_target = GeoStoreUtils.get_max_two_hour_paths(
            config.cdc_bucket, config.geo_target_prefix, 'date=', '%Y%m%d', 'time=', '%H%M%S'
        )
        context['task_instance'].xcom_push(key=config.current_geo_target_path_key, value=f's3://{max_two_hour_paths_geo_target[1]}')
        # CDC data is delayed, which caused two delta runs try to save the data into the same path.
        # Let's use the current date and time
        now = datetime.now()
        date_str = now.strftime("%Y%m%d")
        time_str = now.strftime("%H%M%S")
        current_prefix = f'{config.geo_bucket}/{config.geo_store_generated_data_prefix}/date={date_str}/time={time_str}'
        context['task_instance'].xcom_push(key=config.geo_store_current_prefix_key, value=f's3://{current_prefix}')

        max_geo_targeting_data = GeoStoreUtils.get_max_number(config.geo_targeting_data_bucket, config.geo_targeting_data_prefix)
        context['task_instance'].xcom_push(
            key=config.current_geo_targeting_data_path_key,
            value=f's3://{config.geo_targeting_data_bucket}/{config.geo_targeting_data_prefix}{max_geo_targeting_data}.csv'
        )

        sensitive_source_custom_max_date_str = GeoStoreUtils.get_max_two(
            config.sensitive_source_bucket, config.sensitive_source_prefix, 'dt=', '%Y-%m-%d'
        )[0]  # dt=2014-10-02
        context['task_instance'].xcom_push(
            key=config.sensitive_places_source_path_key,
            value=f's3://{config.sensitive_source_bucket}/{config.sensitive_source_prefix}{sensitive_source_custom_max_date_str}'
        )
        sensitive_source_date = sensitive_source_custom_max_date_str.replace('dt=', '')  # 2014-10-02
        context['task_instance'].xcom_push(
            key=config.sensitive_places_source_output_path_key,
            value=f's3://{config.sensitive_output_bucket}/{config.sensitive_output_prefix}{sensitive_source_date}'
        )
        # processed_sensitive_places_last_used_path_key
        built_sensitive_max_date_str = GeoStoreUtils.get_max_two(
            config.sensitive_output_bucket, config.sensitive_output_prefix, '', '%Y-%m-%d'
        )[0]  # 2014-09-02
        # The latest file in the output is last successful build we used
        context['task_instance'].xcom_push(
            key=config.built_sensitive_places_path_key,
            value=f's3://{config.sensitive_output_bucket}/{config.sensitive_output_prefix}{built_sensitive_max_date_str}'
        )
        # If two dates are the same, no need to build
        context['task_instance'].xcom_push(
            key=config.need_build_monthly_sensitive_places_key, value={sensitive_source_date != built_sensitive_max_date_str}
        )

        aerospike_service_names = GeoStoreUtils.general_polygon_pipeline_aerospike_cluster_service_names if hasattr(
            config, 'is_general_polygon'
        ) else GeoStoreUtils.delta_pipeline_aerospike_cluster_service_names
        # Get Aerospike clusters info
        for cluster, service_name in aerospike_service_names.items():
            # Resolve IPs by DNS and compose them with port
            host = GeoStoreUtils.get_dns_ips(service_name)
            context['task_instance'].xcom_push(key=f'{cluster}', value=host)

    @staticmethod
    def record_hourly_job_path(push_values_to_xcom_task_id, config, **context):
        geo_targeting_data_path = context['task_instance'].xcom_pull(
            task_ids=push_values_to_xcom_task_id, key=config.current_geo_targeting_data_path_key
        )
        targeting_data_geo_target_path = context['task_instance'].xcom_pull(
            task_ids=push_values_to_xcom_task_id, key=config.current_targeting_data_geo_target_path_key
        )
        geo_target_path = context['task_instance'].xcom_pull(task_ids=push_values_to_xcom_task_id, key=config.current_geo_target_path_key)
        geo_store_prefix = context['task_instance'].xcom_pull(task_ids=push_values_to_xcom_task_id, key=config.geo_store_current_prefix_key)
        last_included_sensitive_places_path = context['task_instance'].xcom_pull(
            task_ids=push_values_to_xcom_task_id, key=config.sensitive_places_source_output_path_key
        )

        buffer = io.StringIO()
        buffer.write(f'LastSuccessfulGeoTargetingDataPath:{geo_targeting_data_path}\n')
        buffer.write(f'LastSuccessfulTargetingDataGeoTargetPath:{targeting_data_geo_target_path}\n')
        buffer.write(f'LastSuccessfulGeoTargetPath:{geo_target_path}\n')
        buffer.write(f'LastSuccessfulGeoStorePrefix:{geo_store_prefix}\n')
        buffer.write(f'LastIncludedSensitivePlacesPath:{last_included_sensitive_places_path}\n')

        buffer_contents = buffer.getvalue()
        GeoStoreUtils.write_to_s3_file(config.geo_bucket, config.delta_last_log, buffer_contents)
        buffer.close()

        match = re.search(r'(date=\d+/time=\d+)', geo_store_prefix)
        date_hour_path = match.group(1) if match else "No match found"
        GeoStoreUtils.write_to_s3_file(config.geo_bucket, config.latest_full_geo_tiles_log, date_hour_path)

    @staticmethod
    def write_to_s3_file(bucket, key, content):
        s3 = boto3.resource("s3")
        s3.Object(bucket, key).put(Body=content)

    @staticmethod
    def push_values_from_s3_to_xcom(**context):
        s3 = boto3.client("s3")
        latest_file_contents = s3.get_object(
            Bucket="ttd-geo-test", Key="env=test/GeoStoreNg/DeltaProcessingLog/ManualTriggerFullPaths.txt"
        )["Body"].read().decode().strip()

        for line in latest_file_contents.splitlines():
            if line.strip():
                # Split on the first ':' to get key and value
                if ':' in line:
                    key, value = line.split(':', 1)
                    context['task_instance'].xcom_push(key=key, value=value)
                else:
                    print(f"Warning: Line is malformed and will be skipped: {line}")

    @staticmethod
    def pull_geo_targeting_data_ids_and_brands_to_s3(geo_bucket, output_base_path, **context):
        sql_connection = 'provisioning_replica'  # provdb-bi.adsrvr.org
        db_name = 'Provisioning'
        sql_hook = MsSqlHook(mssql_conn_id=sql_connection, schema=db_name)

        # first param: the modified targets look back days; 1
        # second param: 0 means include all Adsquare data, not only the active ones
        geo_targeting_data = sql_hook.get_pandas_df("exec dbo.prc_GetGeoTargetingDataIdsAndBrands 7, 1")
        output_str = geo_targeting_data.to_csv()

        aws_storage = AwsCloudStorage(conn_id='aws_default')
        aws_storage.load_string(
            string_data=output_str,
            key=output_base_path + datetime.now().strftime("%Y%m%d%H") + '.csv',
            bucket_name=geo_bucket,
            replace=True
        )

    @staticmethod
    def check_if_exists_delta_data_task(path, bucket, **context):
        # if there is no delta data, stop building the whole pipeline
        prefix = path[len(f"s3://{bucket}/"):]
        file_names = GeoStoreUtils.get_all_file_names(bucket, prefix)
        files_num = len(file_names)
        context['task_instance'].xcom_push(key="PartitionFileNames", value=file_names)
        if files_num == 0:
            print("Did not find delta partitions. Stop building the delta pipeline.")
            return False
        print(f"Found {files_num} delta partitions. Keep building the delta pipeline.")
        return True

    @staticmethod
    def get_all_paths(bucket, prefix, **context):
        print(f"Looking at the bucket: {bucket}, prefix: {prefix}")
        s3 = boto3.client('s3')
        result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
        paths = [f"s3://{bucket}/{obj['Prefix']}" for obj in result.get('CommonPrefixes', [])]
        paths_str = ",".join(paths)
        context['task_instance'].xcom_push(key="paths", value=paths_str)

    @staticmethod
    def load_csv_by_prefix(s3_path):
        import pandas as pd

        parsed = urlparse(s3_path)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip('/')
        if not prefix.endswith('/'):
            prefix += '/'
        s3 = boto3.client('s3')

        # List objects under the prefix
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        # Find the first .csv file
        csv_key = None
        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith(".csv"):
                csv_key = key
                break

        if not csv_key:
            raise FileNotFoundError("No CSV file found in the specified prefix")

        # Read and parse the CSV
        csv_obj = s3.get_object(Bucket=bucket, Key=csv_key)
        body = csv_obj['Body'].read().decode('utf-8')

        return pd.read_csv(io.StringIO(body))

    @staticmethod
    def check_cdc_data_task(last_path, current_path, threshold, **context):
        # Load CSV files
        last_df = GeoStoreUtils.load_csv_by_prefix(last_path)
        current_df = GeoStoreUtils.load_csv_by_prefix(current_path)
        # Extract values from the single-row dataframes
        last_cdc_targeting_data_geo_target_count = current_df["LastTargetingDataGeoTargetCount"].iloc[0]
        current_cdc_targeting_data_geo_target_count = current_df["CurrentTargetingDataGeoTargetCount"].iloc[0]
        last_cdc_geo_target_count = last_df["CurrentValidLatestGeoTargetCount"].iloc[0]
        current_cdc_geo_target_count = current_df["CurrentValidLatestGeoTargetCount"].iloc[0]

        print(f'Last TargetingDataGeoTarget count: {last_cdc_targeting_data_geo_target_count}')
        print(f'Current TargetingDataGeoTarget count: {current_cdc_targeting_data_geo_target_count}')
        print(f'Last valid GeoTarget count: {last_cdc_geo_target_count}')
        print(f'Current valid GeoTarget count: {current_cdc_geo_target_count}')

        # Calculate drop percentages
        if last_cdc_targeting_data_geo_target_count > current_cdc_targeting_data_geo_target_count:
            mapping_table_dropped_percent = ((last_cdc_targeting_data_geo_target_count - current_cdc_targeting_data_geo_target_count) /
                                             last_cdc_targeting_data_geo_target_count)
        else:
            mapping_table_dropped_percent = 0

        if last_cdc_geo_target_count > current_cdc_geo_target_count:
            targeting_table_dropped_percent = ((last_cdc_geo_target_count - current_cdc_geo_target_count) / last_cdc_geo_target_count)
        else:
            targeting_table_dropped_percent = 0

        print(
            f"TargetingDataGeoTarget is dropped by {mapping_table_dropped_percent:.2%}. "
            f"GeoTarget is dropped by {targeting_table_dropped_percent:.2%}. Threshold is {threshold:.2%}"
        )

        return mapping_table_dropped_percent < threshold and targeting_table_dropped_percent < threshold
