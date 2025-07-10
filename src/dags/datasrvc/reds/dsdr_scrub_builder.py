"""
Helper class to build some common step shared for reds/exfeeds
"""

import json
import logging
from datetime import datetime, timezone, timedelta

from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from prometheus_client import Gauge

from dags.datasrvc.reds.dsdr_request import get_agg_prefix, fetch
from dags.datasrvc.reds.exposurefeed_dsdr import ExposureFeedDsdr
from dags.datasrvc.reds.redsfeed import RedsFeed
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.cloud_storages.cloud_storage import CloudStorage
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.el_dorado.v2.hdi import HDIJobTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.ttdprometheus import get_or_register_gauge, push_all
import os
import tempfile

user_id_columns = ['tdid', 'combinedidentifier', 'originalid', 'deviceadvertisingid']
divide_workitem = True
aws_default = 'aws_default'

dsdr_scrubber_application_configuration = [{
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.maxConnections": "5000"
    }
}, {
    "Classification": "core-site",
    "Properties": {
        "fs.s3a.connection.maximum": "5000",
        "fs.s3a.threads.max": "50"
    }
}, {
    "Classification": "capacity-scheduler",
    "Properties": {
        "yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
    }
}, {
    "Classification": "spark",
    "Properties": {
        "maximizeResourceAllocation": "true"
    }
}, {
    "Classification": "spark-defaults",
    "Properties": {
        "spark.dynamicAllocation.enabled": "true",
        "spark.executor.instances": "0"
    }
}]


class DsdrStepBuilder:

    def __init__(
        self,
        job_name,
        dag,
        env_prefix,
        dsdr_bucket,
        parent_bucket,
        backup_bucket,
        workitem_meta_bucket,
        jar_bucket,
        dsdr_root,
        feed_class,
        feed_query_conn_id,
        biz_mode,
        scrub_partitions,
        storage_conn_id='aws_default',
        cloud_provider='aws',
        lookback_days=str(99),
        enable_backup='true',
        parallelism=str(10),
        timeout_in_mins=str(20),
        work_item_override_bucket=None,
        test_mode='false',
        prod_test_folder_prefix="env=prod"
    ):
        self.job_name = job_name
        self.dag = dag
        self.env_prefix = env_prefix
        self.dsdr_bucket = dsdr_bucket
        self.parent_bucket = parent_bucket
        self.backup_bucket = backup_bucket
        self.workitem_meta_bucket = workitem_meta_bucket
        self.jar_bucket = jar_bucket
        self.dsdr_root = dsdr_root
        self.storage_conn_id = storage_conn_id
        self.feed_class = feed_class
        self.feed_query_conn_id = feed_query_conn_id
        self.work_item_override_bucket = work_item_override_bucket
        self.biz_mode = biz_mode
        self.test_mode = test_mode
        self.scrub_partitions = scrub_partitions
        self.lookback_days = lookback_days
        self.cloud_provider = cloud_provider
        self.enable_backup = enable_backup
        self.parallelism = parallelism
        self.timeout_in_mins = timeout_in_mins
        self.prod_test_folder_prefix = prod_test_folder_prefix

    def step_get_dsdr_id_graph(self):
        return PythonOperator(
            task_id='get_dsdr_id_graph', python_callable=_get_dsdr_id_graph, op_args=[self], provide_context=True, dag=self.dag
        )

    def step_get_available_feeds(self):
        return PythonOperator(
            task_id='get_available_feeds', python_callable=_get_available_feeds, op_args=[self], provide_context=True, dag=self.dag
        )

    def step_shortcircuit_feed_check(self):
        return ShortCircuitOperator(
            task_id='shortcircuit_feed_check', python_callable=_shortcircuit_feed_check, op_args=[self], provide_context=True, dag=self.dag
        )

    def step_prepare_work_item(self):
        return PythonOperator(
            task_id='prepare_work_item',
            python_callable=_prepare_work_item,
            op_args=[self],
            provide_context=True,
            dag=self.dag,
        )

    def step_dsdr_scrub(self, cluster):
        worker_output_command = f'{{{{ task_instance.xcom_pull(dag_id="{self.job_name}", task_ids="prepare_work_item", key="workitem_output") }}}}'
        worker_input_command = f'{{{{ task_instance.xcom_pull(dag_id="{self.job_name}", task_ids="prepare_work_item", key="workitem_input") }}}}'
        identity_graph_command = f'{{{{ task_instance.xcom_pull(dag_id="{self.job_name}", task_ids="prepare_work_item", key="id_graph_key") }}}}'
        job_space_url_command = f'{{{{ task_instance.xcom_pull(dag_id="{self.job_name}", task_ids="prepare_work_item", key="job_space_url") }}}}'
        backup_prefix_command = f'dsdr-backup/{{{{ task_instance.xcom_pull(dag_id="{self.job_name}", task_ids="prepare_work_item", key="logical_date_formatted") }}}}/'

        return EmrJobTask(
            timeout_timedelta=timedelta(hours=48),
            name=f'{self.env_prefix}{self.biz_mode}_dsdr_scrub_step',
            class_name='com.thetradedesk.dsdr.BatchJobApp',
            executable_path=f's3://{self.jar_bucket}/reds-scrubbing/uber-REDS_S3_File_Scrubbing-2.0-SNAPSHOT.jar',
            command_line_arguments=[
                '-p', self.parent_bucket, '-b', self.backup_bucket, '-d', self.parent_bucket, '-m', self.workitem_meta_bucket, '-cloud',
                self.cloud_provider, '-bizMode', self.biz_mode, '-lookBackDays', self.lookback_days, '-parallelism', self.parallelism,
                '-partitions', self.scrub_partitions, '-timeoutInMinutes', self.timeout_in_mins, '-enableBackup', self.enable_backup,
                '-testMode', self.test_mode, '-outputKey', worker_output_command, '-workItemKey', worker_input_command, '-identityGraphKey',
                identity_graph_command, '-backupPrefix', backup_prefix_command, '-jobSpaceUrl', job_space_url_command, '-divideWorkItem',
                str(divide_workitem).lower()
            ]
        )

    def get_hdi_job(self, cluster_task):
        worker_output_command = f'{{{{ task_instance.xcom_pull(dag_id="{self.job_name}", task_ids="prepare_work_item", key="workitem_output") }}}}'
        worker_input_command = f'{{{{ task_instance.xcom_pull(dag_id="{self.job_name}", task_ids="prepare_work_item", key="workitem_input") }}}}'
        identity_graph_command = f'{{{{ task_instance.xcom_pull(dag_id="{self.job_name}", task_ids="prepare_work_item", key="id_graph_key") }}}}'
        job_space_url_command = f'{{{{ task_instance.xcom_pull(dag_id="{self.job_name}", task_ids="prepare_work_item", key="job_space_url") }}}}'
        backup_prefix_command = f'dsdr-backup/{{{{ task_instance.xcom_pull(dag_id="{self.job_name}", task_ids="prepare_work_item", key="logical_date_formatted") }}}}/'
        run_time_command = f'{{{{ task_instance.xcom_pull(dag_id="{self.job_name}", task_ids="prepare_work_item", key="run_time_formatted") }}}}'

        return HDIJobTask(
            name='dsdr_scrub',
            class_name='com.thetradedesk.dsdr.BatchJobApp',
            jar_path=
            "abfs://ttd-build-artefacts@ttdartefacts.dfs.core.windows.net/datasrvc-reds-feed/manually_uploaded/uber-REDS_S3_File_Scrubbing-2.0-SNAPSHOT.jar",
            eldorado_config_option_pairs_list=[("aggType", "hour"), ("logLevel", "WARN"), ("runtime", run_time_command),
                                               ("ttd.defaultcloudprovider", "azure"),
                                               ("azure.key", "eastusttdreds-client-id,eastusttdreds-client-key")],
            configure_cluster_automatically=True,
            command_line_arguments=[
                '-p',
                get_azure_container(self.parent_bucket), '-b',
                get_azure_container(self.backup_bucket), '-d',
                get_azure_container(self.parent_bucket), '-m',
                get_azure_container(self.workitem_meta_bucket), '-cloud', self.cloud_provider, '-bizMode', self.biz_mode, '-lookBackDays',
                self.lookback_days, '-parallelism', self.parallelism, '-partitions', self.scrub_partitions, '-timeoutInMinutes',
                self.timeout_in_mins, '-enableBackup', self.enable_backup, '-testMode', self.test_mode, '-outputKey', worker_output_command,
                '-workItemKey', worker_input_command, '-identityGraphKey', identity_graph_command, '-backupPrefix', backup_prefix_command,
                '-jobSpaceUrl', job_space_url_command, '-divideWorkItem', 'true'
            ]
        )

    def step_validate_dsdr_scrub_output(self):
        return PythonOperator(
            task_id='validate_dsdr_scrub_output',
            python_callable=_validate_dsdr_scrub_output,
            op_args=[self],
            provide_context=True,
            dag=self.dag,
        )

    def get_storage_name(self):
        if self.cloud_provider == 'aws':
            return 's3'
        elif self.cloud_provider == 'azure':
            return 'azure'
        else:
            return None

    def get_cloud_provider(self):
        if self.cloud_provider == 'aws':
            return CloudProviders.aws
        elif self.cloud_provider == 'azure':
            return CloudProviders.azure
        else:
            return None


def get_azure_container(bucket_name: str) -> str:
    return bucket_name.split('@')[0]


def _prepare_work_item(builder: DsdrStepBuilder, **context):
    dt = context['logical_date']
    print(f'{builder.job_name}: Execution date: {dt}')

    serialized_feeds = context['ti'].xcom_pull(task_ids='get_available_feeds', key='scrubbing_feeds')

    if builder.feed_class == ExposureFeedDsdr:
        feeds = [ExposureFeedDsdr.from_dict(serialized_feed) for serialized_feed in serialized_feeds]
    else:
        feeds = [RedsFeed.from_dict(serialized_feed) for serialized_feed in serialized_feeds]

    configs = []

    # filter the feed by checking if any userId columns exist
    if builder.work_item_override_bucket:
        print(f'Overriding the parent bucket to {builder.work_item_override_bucket}')
    if not feeds:
        raise AirflowFailException("No feeds to process due to deserialization issue")
    for feed in feeds:
        config = feed.to_dsdr_work_item(builder.work_item_override_bucket)
        print(f'config: {config}')
        configs.append(config)
        print(
            f'{builder.job_name}: Added scrub work item for FeedId={feed.feed_id}, FeedStatus={feed.status}, '
            f'EnabledDate={feed.enable_date}, '
            f'PartnerId={feed.partner_id}, AdvertiserId={feed.advertiser_id}'
        )

    if not configs:
        raise AirflowFailException("No work item to process")

    # write work items into s3 json file
    execution_date_str = dt.strftime('%Y-%m-%dT%H-%M-%S')
    now_dt = datetime.now(timezone.utc)
    now = now_dt.strftime('%Y-%m-%dT%H-%M-%S')
    job_space = f'{builder.dsdr_root}/{dt.year}-{dt.month}/{execution_date_str}/{now}'
    work_item_data = '\n'.join(configs)
    work_item_key = f'{job_space}/work_item.json'
    print(f'work_item_key: {work_item_key}')
    out = work_item_key.replace('.json', '.result.json')

    storage_hook = CloudStorageBuilder(builder.get_cloud_provider()).set_conn_id(builder.storage_conn_id).build()
    _write_file(builder.workitem_meta_bucket, work_item_key, work_item_data, storage_hook, builder.cloud_provider)
    logging.info(f'{len(configs)} work items written to {work_item_key}')

    # write id graph into s3 json file
    id_graph = context['ti'].xcom_pull(task_ids='get_dsdr_id_graph', key='id_graph')
    id_graph_key = work_item_key.replace('.json', '.id.json')
    id_data = json.dumps(id_graph)
    _write_file(builder.workitem_meta_bucket, id_graph_key, id_data, storage_hook, builder.cloud_provider)
    logging.info(f'DSDR id graph written to {id_graph_key}')

    # push keys to context
    context['ti'].xcom_push(key='workitem_input', value=work_item_key)
    context['ti'].xcom_push(key='workitem_output', value=out)
    context['ti'].xcom_push(key='id_graph_key', value=id_graph_key)
    context['ti'].xcom_push(key='job_space_url', value=job_space)
    context['ti'].xcom_push(key='logical_date_formatted', value=dt.strftime('%Y%m%d%H%M'))
    context['ti'].xcom_push(key='run_time_formatted', value=dt.strftime('%Y-%m-%dT%H:%M:00'))


def _write_file(bucket: str, key: str, data: str, storage: CloudStorage, cloud_provider: str):
    if cloud_provider == 'aws':
        storage.put_object(bucket, key, data)
        return
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_file.write(data)
        temp_file_path = temp_file.name
        storage.upload_file(key=key, file_path=temp_file_path, bucket_name=bucket)


def _get_dsdr_id_graph(builder: DsdrStepBuilder, **context):
    conf = context['dag_run'].conf or {}
    execution_date_override_str = conf.get('targetStartDate')
    dt = context['logical_date']

    # If a start date override is provided, log it and parse it
    if execution_date_override_str:
        logging.info(f'Using start date {execution_date_override_str} manually from job configuration')
        dt = datetime.strptime(execution_date_override_str, '%Y-%m-%d')

    prefix = get_agg_prefix(dt, builder.prod_test_folder_prefix)
    logging.info(f'Reading DSDR requests from prefix {prefix}')
    s3_hook = AwsCloudStorage(conn_id=aws_default)
    id_graph, dsdr_request_ids = fetch(s3_hook, builder.dsdr_bucket, prefix)
    if not id_graph:
        logging.info(f'Fetched empty dsdr request from bucket {builder.parent_bucket}, prefix: {prefix}')
    context['ti'].xcom_push(key='id_graph', value=id_graph)
    context['ti'].xcom_push(key='num_of_dsdr_requests', value=len(dsdr_request_ids))


def _get_available_feeds(builder: DsdrStepBuilder, **context):
    id_graph = context['ti'].xcom_pull(task_ids='get_dsdr_id_graph', key='id_graph')
    logging.info(f'Id graph: {json.dumps(id_graph)}')

    if not id_graph:
        logging.info("Id graph is empty. Skip scrubbing")
        context['ti'].xcom_push(key='scrubbing_feeds', value=[])
        return

    sql_hook = MsSqlHook(mssql_conn_id=builder.feed_query_conn_id)
    conn = sql_hook.get_conn()
    all_feeds = builder.feed_class.all_feeds_with_columns(conn)
    logging.info(f'Total {len(all_feeds)} feeds fetched')
    if not all_feeds:
        raise AirflowFailException('No feed fetched for scrubbing')

    scrubable_feeds = [feed for feed in all_feeds if feed.dsdr_scrubbable(id_graph, user_id_columns, builder.get_storage_name())]
    if not scrubable_feeds:
        logging.info('No feed is scrubbable')
        context['ti'].xcom_push(key='scrubbing_feeds', value=[])
        return

    feed_id_list = [str(feed.feed_id) for feed in scrubable_feeds]
    logging.info(f'Total {len(scrubable_feeds)} feeds are scrubbable: {",".join(feed_id_list)} in cloud {builder.get_storage_name()}')

    conf = context['dag_run'].conf or {}
    feed_white_list_str = conf.get('feedWhiteList', '')
    feed_white_list = feed_white_list_str.split(",") if feed_white_list_str else []
    if feed_white_list:
        logging.info(f'Applying white list: {feed_white_list_str}')

    feeds_to_scrub = [feed.to_dict() for feed in scrubable_feeds if not feed_white_list or str(feed.feed_id) in feed_white_list]

    if not feeds_to_scrub:
        logging.info('No feed is scrubbable')
    else:
        logging.info(f'Total {len(feeds_to_scrub)} feeds in {builder.get_storage_name()} will be scrubbed')
    context['ti'].xcom_push(key='scrubbing_feeds', value=feeds_to_scrub)


def _shortcircuit_feed_check(builder: DsdrStepBuilder, **context):
    feeds = context['ti'].xcom_pull(task_ids='get_available_feeds', key='scrubbing_feeds')
    return feeds and len(feeds) > 0


def _get_cluster_duration(dag_run, cloud_provider):
    task_instances = dag_run.get_task_instances()

    logging.info(f"Total tasks in this DAG run: {len(task_instances)}")

    task_id = 'monitor_startup_exfeed_dsdr_scrub_cluster'

    for task_instance in task_instances:
        logging.info(
            f"Task ID: {task_instance.task_id}, State: {task_instance.state}, "
            f"Duration: {task_instance.duration}, Start Date: {task_instance.start_date}, "
            f"End Date: {task_instance.end_date}"
        )
        if 'monitor_startup' in task_instance.task_id:
            # currently dev is: monitor_startup_dev-exfeed_dsdr_scrub_cluster
            task_id = task_instance.task_id
    if cloud_provider == 'aws':
        # this is the monitoring job inside the spark run: it is the longest and includes waiting for
        # job to run
        return dag_run.get_task_instance(task_id).duration
    else:
        # reds has the same name for dev and prod
        return dag_run.get_task_instance('redscluster_add_task_dsdr_scrub').duration


def _validate_dsdr_scrub_output(builder: DsdrStepBuilder, **context):
    out = context['ti'].xcom_pull(dag_id=builder.job_name, task_ids='prepare_work_item', key='workitem_output')
    dt = context['logical_date']
    number_of_dsdr_requests = context['ti'].xcom_pull(dag_id=builder.job_name, task_ids='get_dsdr_id_graph', key='num_of_dsdr_requests')
    number_of_dsdr_requests_processed: Gauge = get_or_register_gauge(
        job=builder.job_name,
        name="number_of_dsdr_requests_processed",
        description="Number of DSDR requests processed",
        labels=["biz_mode", "product", "cloud_provider"]
    )
    number_of_dsdr_requests_processed.labels(
        biz_mode=builder.biz_mode, product='dsdr', cloud_provider=builder.cloud_provider
    ).set(number_of_dsdr_requests)

    # update dsdr spark job duration
    dag_run = context['dag_run']

    spark_job_duration = _get_cluster_duration(dag_run, builder.cloud_provider)
    spark_job_duration_in_min = round(spark_job_duration / 60, 2)
    logging.info(f'run_spark_job duration: {spark_job_duration_in_min} mins')

    dsdr_spark_job_duration_in_min: Gauge = get_or_register_gauge(
        job=builder.job_name,
        name="dsdr_spark_job_duration_in_min",
        description="DSDR spark job duration",
        labels=["biz_mode", "product", "cloud_provider"]
    )
    dsdr_spark_job_duration_in_min.labels(
        biz_mode=builder.biz_mode, product='dsdr', cloud_provider=builder.cloud_provider
    ).set(spark_job_duration_in_min)

    logging.info(f'Validating output for {out}')
    # S3 connection
    validate_work_item_result(builder, out, dt)


def validate_work_item_result(builder, result_output, execution_date):
    storage_hook = CloudStorageBuilder(builder.get_cloud_provider()).set_conn_id(builder.storage_conn_id).build()
    file_name = {result_output.split('/')[-1]}
    tmp_file = f'/tmp/dsdr-result/{file_name}'
    os.makedirs(os.path.dirname(tmp_file), exist_ok=True)
    storage_hook.download_file(tmp_file, result_output, builder.workitem_meta_bucket)

    results = []
    with open(tmp_file, 'r') as file:
        for line in file:
            results.append(json.loads(line))

    failures = [feed for feed in results if not feed.get('completed', False) or feed.get('failedKeys', 0)]
    successes = [feed for feed in results if feed.get('completed', False) and not feed.get('failedKeys', 0)]
    num_of_processed_keys = sum(feed.get('processedKeys', 0) for feed in results)
    num_of_scrubbed_keys = sum(feed.get('scrubbedKeys', 0) for feed in results)
    logging.info(f'Successfully run {builder.biz_mode} DSDR scrub task for {len(successes)} feed work items')
    # Register the gauges
    number_of_succeeded_feed: Gauge = get_or_register_gauge(
        job=builder.job_name,
        name="number_of_succeeded_feed",
        description="Number of feed successfully processed",
        labels=["biz_mode", "product", "cloud_provider"]
    )
    number_of_failed_feed: Gauge = get_or_register_gauge(
        job=builder.job_name,
        name="number_of_failed_feed",
        description="Number of feed failed to process",
        labels=["biz_mode", "product", "cloud_provider"]
    )
    number_of_files_successfully_processed: Gauge = get_or_register_gauge(
        job=builder.job_name,
        name="number_of_files_successfully_processed",
        description="Number of files successfully processed",
        labels=["biz_mode", "product", "cloud_provider"]
    )
    number_of_scrubbed_files: Gauge = get_or_register_gauge(
        job=builder.job_name,
        name="number_of_scrubbed_files",
        description="Number of files scrubbed",
        labels=["biz_mode", "product", "cloud_provider"]
    )
    number_of_succeeded_feed.labels(biz_mode=builder.biz_mode, product='dsdr', cloud_provider=builder.cloud_provider).set(len(successes))
    number_of_failed_feed.labels(biz_mode=builder.biz_mode, product='dsdr', cloud_provider=builder.cloud_provider).set(len(failures))
    number_of_scrubbed_files.labels(
        biz_mode=builder.biz_mode, product='dsdr', cloud_provider=builder.cloud_provider
    ).set(num_of_scrubbed_keys)
    number_of_files_successfully_processed.labels(
        biz_mode=builder.biz_mode, product='dsdr', cloud_provider=builder.cloud_provider
    ).set(num_of_processed_keys)
    push_all(builder.job_name)
    # raise exception if there are any failures
    if failures:
        failed_ids = [feed.get('feedId') for feed in failures]
        print(
            f'To retry the failed feed: rerun the airflow job with: '
            f'{{"feedWhiteList": "{",".join(failed_ids)}", "targetStartDate": "{execution_date.strftime("%Y-%m-%d")}"}}'
        )
        count = len(failures)
        for i, feed in enumerate(failures, start=1):
            print(
                f'({i}/{count}) Failed to run DSDR task for feedId = {feed.get("feedId", "Unknown")} '
                f'prefix={feed.get("prefix", "Unknown")} parentBucket={feed.get("parentBucket", "Unknown")} '
                f'failedKeys={feed.get("failedKeys", "Unknown")}: \n'
                f'firstExceptionMessage={feed.get("exceptionDetails", [{}])[0].get("firstErrorMessage", "Unknown")}'
            )
        raise AirflowFailException(f'Failed to run DSDR scrubbing task for {count} feeds')
