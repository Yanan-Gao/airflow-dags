from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from typing import Union
from airflow.exceptions import AirflowFailException
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import DagRunState
from airflow.providers.cncf.kubernetes.secret import Secret
from ttd.eldorado.base import TtdDag
from ttd.kubernetes.pod_resources import PodResources
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from ttd.operators.ttd_kubernetes_pod_operator import TtdKubernetesPodOperator


class Utils:
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
    DATE_FORMAT = "%Y-%m-%d"

    @staticmethod
    def get_application_dir() -> str:
        if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
            return "s3://ttd-identity/data/prod/regular/disney-id-integration"
        else:
            return "s3://ttd-identity/data/test/regular/disney-id-integration"

    @staticmethod
    def get_job_dir(job) -> str:
        return f"{Utils.get_application_dir()}/{job}/target_date={{{{ get_target_date(data_interval_end) }}}}/execution_time={{{{ get_execution_time_str(data_interval_end) }}}}"

    @staticmethod
    def get_nebula_conversion_logs_dir() -> str:
        return "s3://thetradedesk-useast-logs-2/nebulaidconversion/collected"

    @staticmethod
    def get_uid2_audience_logs_dir() -> str:
        return "s3://thetradedesk-useast-logs-2/uid2audience/collected"

    @staticmethod
    def get_uid2_avails_dir() -> str:
        return "s3://ttd-identity/data/prod/events/avails-idnt-uid2/v1"

    @staticmethod
    def get_bidrequest_dir() -> str:
        return "s3://ttd-identity/deltaData/openGraph/daily/bidRequest"

    @staticmethod
    def get_uid2_graphs_dir() -> str:
        return "s3://thetradedesk-useast-data-import/sxd-etl/uid2"

    @staticmethod
    def get_execution_time(data_interval_end) -> datetime:
        return data_interval_end.astimezone(tz=timezone.utc).replace(tzinfo=None)

    @staticmethod
    def get_execution_time_str(data_interval_end) -> str:
        return Utils.get_execution_time(data_interval_end).strftime(Utils.TIMESTAMP_FORMAT)

    @staticmethod
    def create_dag(dag_id, start_date, schedule_interval, retry_delay) -> TtdDag:
        if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
            slack_channel = "#scrum-identity-applications-alarms"
        else:
            slack_channel = "#lha-test-bot-alerts"
        dag = TtdDag(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            max_active_runs=1,
            retries=1,
            retry_delay=retry_delay,
            slack_channel=slack_channel,
            tags=["identity", "disney-id-integration"],
            run_only_latest=True,
        )
        dag.airflow_dag.user_defined_macros = {
            "get_execution_time_str": Utils.get_execution_time_str,
        }
        return dag

    @staticmethod
    def create_pod_operator(
        dag,
        name,
        task_id,
        sumologic_source_category,
        prometheus_job,
        execution_timeout,
        arguments,
        env_vars={},
        container_resources=PodResources(
            request_cpu="1000m",
            request_memory="1G",
            request_ephemeral_storage="2G",
            limit_cpu="2000m",
            limit_memory="3G",
            limit_ephemeral_storage="5G",
        )
    ) -> OpTask:
        if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
            k8s_namespace = "ttd-identity-prod"
            k8s_conn_id = "airflow-2-pod-scheduling-rbac-conn-prod"
            disneyidintegration_image = "internal.docker.adsrvr.org/ttd/disneyidintegration:latest"
            env_vars["TTD_DISNEY_ID_INTEGRATION_ENVIRONMENT"] = "ProductionOnly"
        else:
            k8s_namespace = "ttd-identity-dev"
            k8s_conn_id = "airflow-2-pod-scheduling-rbac-conn-dev"
            disneyidintegration_image = "dev.docker.adsrvr.org/ttd/disneyidintegration:latest"
            env_vars["TTD_DISNEY_ID_INTEGRATION_ENVIRONMENT"] = "ProdTest"

        return OpTask(
            op=TtdKubernetesPodOperator(
                dag=dag,
                name=name,
                task_id=task_id,
                namespace=k8s_namespace,
                kubernetes_conn_id=k8s_conn_id,
                image=disneyidintegration_image,
                image_pull_policy="Always",
                dnspolicy="ClusterFirst",
                get_logs=True,
                log_events_on_failure=True,
                is_delete_operator_pod=True,
                startup_timeout_seconds=1000,  # We need to timeout since occasionally, logs are unavailable immediately
                execution_timeout=execution_timeout,
                service_account_name="disney-id-integration",
                annotations={
                    "sumologic.com/include": "true",
                    "sumologic.com/sourceCategory": sumologic_source_category
                },
                do_xcom_push=True,
                resources=container_resources,
                env_vars={
                    **env_vars,
                    "TTD_AppTelemetry__Metrics__PrometheousJob": prometheus_job,
                },
                secrets=[
                    Secret(deploy_type="env", deploy_target="TTD_Snowflake__User", secret="disney-service", key="TTD_Snowflake__User"),
                    Secret(
                        deploy_type="env", deploy_target="TTD_Snowflake__Password", secret="disney-service", key="TTD_Snowflake__Password"
                    ),
                    Secret(
                        deploy_type='env',
                        deploy_target='TTD_Snowflake__SharedHash',
                        secret='disney-service',
                        key='TTD_Snowflake__SharedHash'
                    ),
                ],
                arguments=arguments,
            )
        )

    @staticmethod
    def get_task_result_from_previous_success_run(dag_run, task_id):
        pre_dag_run = DagRun.get_previous_dagrun(dag_run, state=DagRunState.SUCCESS)
        pre_task_instance = pre_dag_run.get_task_instance(task_id=task_id) if pre_dag_run is not None else None
        last_result = pre_task_instance.xcom_pull(key="return_value", task_ids=task_id) if pre_task_instance is not None else None
        return last_result

    @staticmethod
    def get_process_files_begin_timestamp(dag_run, task_id, result_field, max_look_back_time) -> datetime:
        # change to utc and to naive datetime
        max_look_back_time = max_look_back_time.astimezone(tz=timezone.utc).replace(tzinfo=None)

        last_result = Utils.get_task_result_from_previous_success_run(dag_run, task_id)
        last_modified_time = last_result[result_field] if last_result is not None and result_field in last_result.keys() else None
        last_modified_time = datetime.strptime(last_modified_time, Utils.TIMESTAMP_FORMAT) if last_modified_time is not None else None

        if last_modified_time is None or last_modified_time < max_look_back_time:
            last_modified_time = max_look_back_time
        return last_modified_time

    @staticmethod
    def get_load_mappings_files_begin_timestamp(dag_run, task_id, max_look_back_time) -> datetime:
        return Utils.get_process_files_begin_timestamp(dag_run, task_id, "SourceFileLatestModifiedTime", max_look_back_time)

    @staticmethod
    def get_collect_files_begin_timestamp(dag_run, task_id, max_look_back_time) -> datetime:
        return Utils.get_process_files_begin_timestamp(dag_run, task_id, "LogFileLatestModifiedTime", max_look_back_time)

    # from legacy DAGs, will be removed after migrating to new ones
    @staticmethod
    def utcnow_to_target_month() -> str:
        return datetime.utcnow().date().replace(day=1).strftime("%Y-%m-%d")


# from legacy DAGs, will be removed after migrating to new ones
class TimestampHelpers:

    @staticmethod
    def epoch_to_human_readable_timestamp(epoch_timestamp: Union[int, str]) -> str:
        epoch_timestamp = int(epoch_timestamp)
        current_time = datetime.utcfromtimestamp(epoch_timestamp)
        return current_time.strftime("%Y-%m-%dT%H-%M-%S")

    @staticmethod
    def utcnow_to_target_month() -> str:
        return datetime.utcnow().date().replace(day=1).strftime("%Y-%m-%d")

    @staticmethod
    def extract_epoch_timestamp_from_task_instance(task_instance: TaskInstance) -> int:
        epoch_timestamp = task_instance.xcom_pull(
            task_ids='update_processed_timestamp', key='processed_timestamp', include_prior_dates=True
        )
        if epoch_timestamp is None:
            return 0
        else:
            return int(float(epoch_timestamp))

    @staticmethod
    def generate_prefix_for_past_hours(base_path, lookup_hours, current_time):
        for x in reversed(range(lookup_hours)):
            new_date = current_time - timedelta(hours=x)
            yield f"{base_path}/{new_date.year}/{new_date.month:02d}/{new_date.day:02d}/{new_date.hour:02d}"


class CutoverHelpers:
    # keys
    RUN_DATETIME_KEY = "run_datetime"  # when the DAG runs, can be configured in dag_run
    TARGET_DATETIME_KEY = "target_datetime"  # target month, day should always be 1 and hour, minute and second should always be 0. can be configured in dag_run
    CUTOFF_DATETIME_KEY = "cutoff_datetime"  # the start time of date to load, can be configured in dag_run
    MAPPING_FOR_NEXT_MONTH_KEY = "mapping_for_next_month"  # boolean indicates if it's for cut over
    CUTOVER_DATETIME_KEY = "cutover_datetime"  # the end time of data loaded last time. it's the new cutoff datetime if it's not the first day of month
    DEDUP_BY_TARGET_MONTH_KEY = "dedup_by_target_month"  # boolean for if dedup by target month

    DATETIME_FORMAT = "%Y-%m-%dT%H-%M-%S"
    TARGET_MONTH_FORMAT = "%Y-%m-%d"
    CUTOVER_THRESHOLD_DAY_FOR_CURRENT_MONTH = 20  # if run day is equal or less than this, the run is for current month

    @staticmethod
    def update_xcom_datetime_info(save_cutover_datetime_task_id, full_lookback_days, **kwargs):
        task_instance = kwargs['task_instance']
        dag_run = kwargs['dag_run']

        if dag_run.run_id.startswith('manual'):
            if dag_run.conf is None or \
                    CutoverHelpers.RUN_DATETIME_KEY not in dag_run.conf or \
                    CutoverHelpers.TARGET_DATETIME_KEY not in dag_run.conf or \
                    CutoverHelpers.CUTOFF_DATETIME_KEY not in dag_run.conf or \
                    CutoverHelpers.MAPPING_FOR_NEXT_MONTH_KEY not in dag_run.conf or \
                    CutoverHelpers.DEDUP_BY_TARGET_MONTH_KEY not in dag_run.conf:
                print(
                    f"""
                    Manually trigger this DAG need to provide all these parameters:

                    {CutoverHelpers.RUN_DATETIME_KEY}: the end datetime of UID2s (excluded) which will be transformed and imported, in format {CutoverHelpers.DATETIME_FORMAT}
                    {CutoverHelpers.CUTOFF_DATETIME_KEY}: the begin datetime of UID2s (included) which will be transformed and imported, in format {CutoverHelpers.DATETIME_FORMAT}
                    {CutoverHelpers.TARGET_DATETIME_KEY}: the target month, in format {CutoverHelpers.DATETIME_FORMAT}, day, hour, minute and second are ignored
                    {CutoverHelpers.MAPPING_FOR_NEXT_MONTH_KEY}: boolean, indicates if use cut over mapping function (for next month)
                    {CutoverHelpers.DEDUP_BY_TARGET_MONTH_KEY}: boolean, indicates if dedup uid2s by targeting month

                    For example:
                    {{
                        "{CutoverHelpers.RUN_DATETIME_KEY}": "2023-10-11T05-00-00",
                        "{CutoverHelpers.CUTOFF_DATETIME_KEY}": "2023-09-11T00-00-00",
                        "{CutoverHelpers.TARGET_DATETIME_KEY}": "2023-11-01T00-00-00",
                        "{CutoverHelpers.MAPPING_FOR_NEXT_MONTH_KEY}": true,
                        "{CutoverHelpers.DEDUP_BY_TARGET_MONTH_KEY}": true
                    }}
                    """
                )

                raise AirflowFailException()

            run_datetime = datetime.strptime(dag_run.conf[CutoverHelpers.RUN_DATETIME_KEY], CutoverHelpers.DATETIME_FORMAT)
            target_datetime = datetime.strptime(dag_run.conf[CutoverHelpers.TARGET_DATETIME_KEY], CutoverHelpers.DATETIME_FORMAT)
            target_datetime = datetime(target_datetime.year, target_datetime.month, 1)
            cutoff_datetime = datetime.strptime(dag_run.conf[CutoverHelpers.CUTOFF_DATETIME_KEY], CutoverHelpers.DATETIME_FORMAT)
            mapping_for_next_month = dag_run.conf[CutoverHelpers.MAPPING_FOR_NEXT_MONTH_KEY]
            dedup_by_target_month = dag_run.conf[CutoverHelpers.DEDUP_BY_TARGET_MONTH_KEY]
        else:
            pre_dag_run = DagRun.get_previous_dagrun(dag_run, state=DagRunState.SUCCESS)
            pre_task_instance = pre_dag_run.get_task_instance(task_id=save_cutover_datetime_task_id) if pre_dag_run is not None else None
            print(pre_dag_run)
            print(pre_task_instance)
            last_target_datetime_str = pre_task_instance.xcom_pull(
                key=CutoverHelpers.TARGET_DATETIME_KEY, task_ids=save_cutover_datetime_task_id
            ) if pre_task_instance is not None else None
            last_cutover_datetime_str = pre_task_instance.xcom_pull(
                key=CutoverHelpers.CUTOVER_DATETIME_KEY, task_ids=save_cutover_datetime_task_id
            ) if pre_task_instance is not None else None

            last_target_datetime = datetime.strptime(
                last_target_datetime_str, CutoverHelpers.DATETIME_FORMAT
            ) if last_target_datetime_str is not None else None
            last_cutover_datetime = datetime.strptime(
                last_cutover_datetime_str, CutoverHelpers.DATETIME_FORMAT
            ) if last_cutover_datetime_str is not None else None

            # run_datetime
            run_datetime = kwargs['data_interval_end']

            # target_datetime
            if run_datetime.day <= CutoverHelpers.CUTOVER_THRESHOLD_DAY_FOR_CURRENT_MONTH:
                target_datetime = datetime(run_datetime.year, run_datetime.month, 1)
                mapping_for_next_month = False
            else:
                target_datetime = datetime(run_datetime.year, run_datetime.month, 1) + relativedelta(months=1)
                mapping_for_next_month = True

            # cutoff_datetime
            if last_cutover_datetime is None or \
                    target_datetime != last_target_datetime or \
                    run_datetime.year != last_cutover_datetime.year or \
                    run_datetime.month != last_cutover_datetime.month:
                # no cutover before or the first in month, use lookback days
                cutoff_datetime = run_datetime - timedelta(days=full_lookback_days)
            else:
                # if we already have cutover before, continue it
                cutoff_datetime = last_cutover_datetime - timedelta(days=1)  # 1 day overlap

            # dedup_by_target_month
            # only set it to false when
            # - it targets current month (refresh)
            # - no previous run in same month
            if not mapping_for_next_month and \
                    (
                            last_cutover_datetime is None or
                            (run_datetime.year != last_cutover_datetime.year or run_datetime.month != last_cutover_datetime.month)
                    ):
                dedup_by_target_month = False
            else:
                dedup_by_target_month = True

            # for debugging
            print(f"last_target_datetime: {last_target_datetime}")
            print(f"last_cutover_datetime: {last_cutover_datetime}")

        task_instance.xcom_push(key=CutoverHelpers.RUN_DATETIME_KEY, value=run_datetime.strftime(CutoverHelpers.DATETIME_FORMAT))
        task_instance.xcom_push(key=CutoverHelpers.TARGET_DATETIME_KEY, value=target_datetime.strftime(CutoverHelpers.DATETIME_FORMAT))
        task_instance.xcom_push(key=CutoverHelpers.CUTOFF_DATETIME_KEY, value=cutoff_datetime.strftime(CutoverHelpers.DATETIME_FORMAT))
        task_instance.xcom_push(key=CutoverHelpers.MAPPING_FOR_NEXT_MONTH_KEY, value=mapping_for_next_month)
        task_instance.xcom_push(key=CutoverHelpers.DEDUP_BY_TARGET_MONTH_KEY, value=dedup_by_target_month)

        # for debugging
        print(f"run_datetime: {run_datetime}")
        print(f"target_datetime: {target_datetime}")
        print(f"cutoff_datetime: {cutoff_datetime}")
        print(f"mapping_for_next_month: {mapping_for_next_month}")
        print(f"dedup_by_target_month: {dedup_by_target_month}")

    @staticmethod
    def get_target_month(task_instance: TaskInstance):
        target_datetime = task_instance.xcom_pull(key=CutoverHelpers.TARGET_DATETIME_KEY)
        target_month = datetime.strptime(target_datetime, CutoverHelpers.DATETIME_FORMAT)
        return target_month.strftime(CutoverHelpers.TARGET_MONTH_FORMAT)

    @staticmethod
    def get_mapping_for_next_month(task_instance: TaskInstance):
        return task_instance.xcom_pull(key=CutoverHelpers.MAPPING_FOR_NEXT_MONTH_KEY)

    @staticmethod
    def get_dedup_by_target_month(task_instance: TaskInstance):
        return task_instance.xcom_pull(key=CutoverHelpers.DEDUP_BY_TARGET_MONTH_KEY)

    @staticmethod
    def save_cutover_info(**kwargs):
        task_instance = kwargs['task_instance']

        cutover_datetime = task_instance.xcom_pull(key=CutoverHelpers.RUN_DATETIME_KEY)
        target_datetime = task_instance.xcom_pull(key=CutoverHelpers.TARGET_DATETIME_KEY)

        task_instance.xcom_push(key=CutoverHelpers.TARGET_DATETIME_KEY, value=target_datetime)
        task_instance.xcom_push(key=CutoverHelpers.CUTOVER_DATETIME_KEY, value=cutover_datetime)

        print(f"saving target_datetime: {target_datetime}")
        print(f"saving cutover_datetime: {cutover_datetime}")
