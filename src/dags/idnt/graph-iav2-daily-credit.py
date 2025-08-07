"""
Performs crediting of cross device revenue via Identity Alliance v2 to contributing graph vendors

We get impressions from both AWS/Azure, hence we got 2 different DAGs to process them

Once the output credit summary is generated/moved to S3, the Datamover task transfers the data to Vertica for reporting
"""
from airflow import DAG
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.statics import RunTimes, Tags, Executables
from datetime import datetime
from ttd.eldorado.hdiversion import HDIClusterVersions
from ttd.hdinsight.hdi_vm_config import HDIVMConfig
from ttd.tasks.op import OpTask
from ttd.cloud_provider import CloudProvider, CloudProviders
from ttd.el_dorado.v2.hdi import HDIClusterTask, HDIJobTask
from datetime import timedelta
from ttd.azure_vm.hdi_instance_types import HDIInstanceTypes
from airflow.operators.python_operator import PythonOperator
from ttd.interop.logworkflow_callables import ExternalGateOpen
from airflow.providers.common.sql.sensors.sql import SqlSensor
from datasources.sources.rtb_datalake_datasource import RtbDatalakeDatasource
from dags.idnt.identity_datasets import IdentityDatasets
from ttd.ttdenv import TtdEnvFactory
from ttd.constants import Day
from airflow.exceptions import AirflowFailException
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder
from ttd.data_transfer.dataset_transfer_task import DatasetTransferTask
from dateutil import parser
import logging
from ttd.tasks.chain import ChainOfTasks
from typing import List

dag_id = "graph-iav2-daily-credit"
dag = DagHelpers.identity_dag(
    dag_id=dag_id,
    start_date=datetime(2025, 4, 1),
    run_only_latest=False,
    schedule_interval="50 23 * * *",
    dag_tsg="https://atlassian.thetradedesk.com/confluence/pages/viewpage.action?pageId=241299257",
    doc_md=__doc__,
    # Allow crediting job be able to catch up
    max_active_runs=5,
)


def create_cluster(cloud_provider: CloudProvider, cluster_name: str):
    if cloud_provider == CloudProviders.aws:
        return IdentityClusters.get_cluster(cluster_name, "iav2-daily-credit", NUM_CORES_AWS, ComputeType.STORAGE, cpu_bounds=(64, 2048))
    elif cloud_provider == CloudProviders.azure:
        return HDIClusterTask(
            name=cluster_name,
            cluster_tags=Tags.cluster,
            vm_config=HDIVMConfig(
                headnode_type=HDIInstanceTypes.Standard_D14_v2(),
                workernode_type=HDIInstanceTypes.Standard_D14_v2(),
                num_workernode=180,
                disks_per_node=1
            ),
            cluster_version=HDIClusterVersions.AzureHdiSpark33,
            enable_openlineage=False
        )


def create_credit_step(cloud_provider, cluster, job_name):
    job_class = 'jobs.identity.alliance.v2.IAv2DailyCredit'
    if cloud_provider == CloudProviders.aws:
        return IdentityClusters.task(
            job_class,
            timeout_hours=6,
            runDate_arg="date",
            eldorado_configs=[("azure.key", "azure-account-key"), ("ttd.azure.enable", "false"),
                              ("ttd.BidFeedbackDataSetV5.cloudprovider", cloud_provider.__str__()), ("NumWeeksToSearch", "2"),
                              ("NumLocalPartitions", str(NUM_CORES_AWS * 2)),
                              ("jobs.identity.alliance.v2.GraphMetrics.IAVendorMetricsCrediting.StickinessOverrides", ""),
                              ("jobs.identity.alliance.v2.GraphMetrics.IAVendorMetricsCrediting.StabilityOverrides", ""),
                              ("jobs.identity.alliance.v2.GraphMetrics.IAVendorMetricsCrediting.PrecisionOverrides", "id5->GLOBAL=0.757"),
                              ("jobs.identity.alliance.v2.GraphMetrics.IAVendorMetricsCrediting.UniquenessOverrides", "")],
        )
    elif cloud_provider == CloudProviders.azure:
        return HDIJobTask(
            name=job_name,
            class_name=job_class,
            cluster_specs=cluster.cluster_specs,
            eldorado_config_option_pairs_list=[
                ("date", JOB_RUN_DATE), ("ttd.azure.enable", "true"),
                ("ttd.BidFeedbackDataSetV5.cloudprovider", CloudProviders.azure.__str__()),
                ("ttd.IAV2PersonGraph.cloudprovider", CloudProviders.azure.__str__()),
                ("ttd.IAV2HHGraph.cloudprovider", CloudProviders.azure.__str__()),
                ("ttd.OriginalIdOverallAggDataset.cloudprovider", CloudProviders.azure.__str__()),
                ("ttd.AvailsIdOverallAggDataset.cloudprovider", CloudProviders.azure.__str__()), ("NumWeeksToSearch", "2"),
                ("ttd.defaultcloudprovider", "azure"), ("ttd.s3.access.enable", "true"), ("openlineage.enable", "false"),
                ("azure.key", "eastusttdlogs,ttdexportdata"),
                ("jobs.identity.alliance.v2.GraphMetrics.IAVendorMetricsCrediting.StickinessOverrides", ""),
                ("jobs.identity.alliance.v2.GraphMetrics.IAVendorMetricsCrediting.StabilityOverrides", ""),
                ("jobs.identity.alliance.v2.GraphMetrics.IAVendorMetricsCrediting.PrecisionOverrides", "id5->GLOBAL=0.757"),
                ("jobs.identity.alliance.v2.GraphMetrics.IAVendorMetricsCrediting.UniquenessOverrides", "")
            ],
            additional_args_option_pairs_list=[('conf', 'spark.executor.extraJavaOptions=-server -XX:+UseParallelGC')],
            configure_cluster_automatically=True,
            jar_path=Executables.identity_repo_executable_azure,
            command_line_arguments=['--version']
        )


def create_credit_task_group_per_env(cloud_provider: CloudProvider) -> OpTask:

    cloud_provider_str = cloud_provider.__str__()
    job_name = f'{dag_id}-{cloud_provider_str}'
    cluster_name = f'{job_name}-cluster'

    # Operator which checks if each hour success file exists for the run date
    check_incoming_bidfeedback_data_exists = DagHelpers.check_datasets([RtbDatalakeDatasource.rtb_bidfeedback_v5.with_check_type('day')],
                                                                       cloud_provider=cloud_provider,
                                                                       suffix=cloud_provider_str)

    # need to make sure the utility dataset in previous Friday is ready
    last_friday_template = f'{{{{ task_instance.xcom_pull(dag_id="{dag_id}", task_ids="get_last_friday_date_{cloud_provider_str}") }}}}'

    check_incoming_bidfeedback_consistency = get_bidfeedback_consistency_check_task(cloud_provider_str)

    check_utility_datasets_complete = DagHelpers.check_datasets([
        IdentityDatasets.avails_id_overall_agg, IdentityDatasets.original_ids_overall_agg
    ],
                                                                ds_date=last_friday_template,
                                                                cloud_provider=cloud_provider,
                                                                suffix=cloud_provider_str)

    check_iav2_graphs_etl_complete = DagHelpers.check_datasets_with_lookback(
        dataset_name="iav2_graph_etl",
        datasets=[IdentityDatasets.get_post_etl_graph("iav2graph"),
                  IdentityDatasets.get_post_etl_graph("iav2graph_household")],
        lookback_days=14,
        cloud_provider=cloud_provider,
        suffix=cloud_provider_str
    )

    cluster = create_cluster(cloud_provider=cloud_provider, cluster_name=cluster_name)

    iav2_credit_step = create_credit_step(cloud_provider=cloud_provider, cluster=cluster, job_name=job_name)

    cluster.add_parallel_body_task(iav2_credit_step)

    iav2_credit_vertica_import = get_vertica_import_task(cloud_provider)

    ordered_crediting_tasks = [
        check_incoming_bidfeedback_data_exists,
        get_last_friday_date_task(cloud_provider_str), check_incoming_bidfeedback_consistency, check_utility_datasets_complete,
        check_iav2_graphs_etl_complete, cluster
    ]

    if cloud_provider == CloudProviders.azure:
        ordered_crediting_tasks += get_copy_and_verify_credit_summary_from_azure_to_s3() + [
            get_iav2_credit_vertica_import_aws_dependency_check_task()
        ]

    return ChainOfTasks(
        task_id=f"crediting-{cloud_provider_str}", tasks=ordered_crediting_tasks + [iav2_credit_vertica_import]
    ).as_taskgroup(f"crediting-{cloud_provider_str}-task")


def get_vertica_import_task(cloud_provider) -> OpTask:
    # enable gate for Datamover to publish results into Vertica
    return OpTask(
        op=PythonOperator(
            python_callable=ExternalGateOpen,
            provide_context=True,
            op_kwargs={
                'mssql_conn_id': LOGWORKFLOW_CONNECTION if str(TTD_ENV) == 'prod' else LOGWORKFLOW_SANDBOX_CONNECTION,
                'sproc_arguments': {
                    'gatingType': 10092 if cloud_provider == CloudProviders.azure else 10088,
                    # dbo.fn_Enum_GatingType_IAv2DataElementReportAzure() or dbo.fn_Enum_GatingType_IAv2DataElementReport() depending cloud_provider
                    'grain': 100002,  # dbo.fn_Enum_TaskBatchGrain_Daily()
                    'dateTimeToOpen': JOB_RUN_DATE
                }
            },
            task_id=f'iav2_credit_vertica_import_{cloud_provider.__str__()}',
        )
    )


def get_last_friday_date_task(cloud_env: str) -> OpTask:
    """
    It's fetching last Friday beginning Sunday, else for Fri/Sat it results in Friday from previous week
    This is done to match the utility date being used to the expected latest graph date in crediting which is generated on Sat
    """

    def get_last_friday_date(run_date) -> datetime:
        date = datetime.strptime(run_date, "%Y-%m-%d")
        day_of_week_to_check = [date - timedelta(days=x) for x in range(2, 9)]  # check on 7 days in the past
        last_friday = next(d for d in day_of_week_to_check if Day(d.weekday()) == Day.FRIDAY)
        return last_friday

    # fetches the date utility datasets are generated
    return OpTask(
        op=PythonOperator(
            task_id=f'get_last_friday_date_{cloud_env}',
            python_callable=get_last_friday_date,
            op_kwargs=dict(run_date=JOB_RUN_DATE),
            do_xcom_push=True
        )
    )


def get_bidfeedback_consistency_check_task(cloud_env: str) -> OpTask:

    consistency_check_query = (
        f"""
            declare @BidfeedbackSourcesCount int --Checks that all 3 sources of Bidfeedback are loaded (USEast01, USWest01, and Datalake)
            declare @BidfeedbackBetweenSourcesCheckCount int --Consistency Check b/w these sources gets triggered after all 3 sources have been loaded. A record present only if the status is inconsistent
            declare @IsConsistent bit
            select @BidfeedbackSourcesCount = count(*) from chk.ConsistencyCheck where CheckName = 'DailyDatalakeConsistencyCheck' and Interval = '{JOB_RUN_DATE}' and ConsistencyCheckStatusId = 5 and SourceName in ('Datalake', 'USEast01', 'USWest01')
            select @BidfeedbackBetweenSourcesCheckCount = count(*) from chk.ConsistencyCheck where CheckName = 'DatalakeConsistencyCheck.BetweenSources' and SourceName = 'DailyDatalakeConsistencyCheck' and Interval = '{JOB_RUN_DATE}' and ConsistencyCheckStatusId = 4 --(Status => Inconsistent)
            if (@BidfeedbackSourcesCount = 3 and @BidfeedbackBetweenSourcesCheckCount = 0) set @IsConsistent = 1 else set @IsConsistent = 0
            select @IsConsistent
        """
    )

    # checks that Bidfeedback dataset is consistent with the truth set in Vertica
    return OpTask(
        op=SqlSensor(
            task_id=f"check_incoming_bidfeedback_consistency_{cloud_env}",
            conn_id='ttd_idnt_alliance',  # TTDGlobal connection
            sql=consistency_check_query,
            mode='reschedule',
            timeout=60 * 60 * 3,  # timeout after 3 hrs
            poke_interval=60 * 5  # poke every 5 minutes
        )
    )


def get_iav2_credit_vertica_import_aws_dependency_check_task() -> OpTask:
    """
    The schedule for weekly/monthly Vertica reports for crediting depends on the execution of both AWS/Azure Crediting
    Hence creating an internal dependency between AWS & Azure final import to Vertica tasks ensures that check to make sure both are completed can be done using a single task (azure or aws)
    """
    gating_query = (
        f"""
            declare @taskVariantsCompletedCount int
            declare @IsCompleted bit
            select @taskVariantsCompletedCount = count(*) from dbo.LogFileTask
            where TaskId = dbo.fn_enum_Task_IAv2DataElementReport() and LogStartTime = '{JOB_RUN_DATE}' and LogFileTaskStatusId = 5 --fn_Enum_LogFileTaskStatus_Completed
            select @IsCompleted = case when @taskVariantsCompletedCount = 2 then 1 else 0 end
            select IsCompleted = @IsCompleted
        """
    )

    return OpTask(
        op=SqlSensor(
            task_id="check_dependency_iav2_credit_vertica_import_aws",
            conn_id=LOGWORKFLOW_CONNECTION if str(TTD_ENV) == 'prod' else LOGWORKFLOW_SANDBOX_CONNECTION,
            sql=gating_query,
            mode='reschedule',
            timeout=60 * 60 * 2,  # timeout after 2 hrs
            poke_interval=60 * 5  # poke every 5 minutes
        )
    )


def get_copy_and_verify_credit_summary_from_azure_to_s3() -> List[OpTask]:
    logger = logging.getLogger(__name__)

    def list_file_names(bucket, prefix, cloud_hook):
        if not cloud_hook.check_for_prefix(prefix, bucket):
            logger.info("data folder doesn't exist for : " + prefix + " in bucket " + bucket)
            return []

        files = cloud_hook.list_keys(prefix, bucket)
        file_names = [f.split('/')[-1] for f in files]
        return file_names

    def compare_data_across_cloud_after_copy(run_date, **kwargs):

        azure_credit_summary_dataset = IdentityDatasets.iav2_credit_summary_azure
        full_path_prefix = azure_credit_summary_dataset._get_full_key(parser.parse(run_date).date()) + "/"

        credit_summary_azure = list_file_names(
            azure_credit_summary_dataset.azure_bucket, full_path_prefix,
            CloudStorageBuilder(CloudProviders.azure).build()
        )
        credit_summary_azure_copied_to_s3 = list_file_names(
            azure_credit_summary_dataset.bucket, full_path_prefix,
            CloudStorageBuilder(CloudProviders.aws).build()
        )

        credit_summary_azure.sort()
        credit_summary_azure_copied_to_s3.sort()

        if len(credit_summary_azure) == 0:
            raise AirflowFailException("Credit Summary in Azure is empty")
        elif credit_summary_azure != credit_summary_azure_copied_to_s3:
            logger.info(f"SOURCE: CREDIT SUMMARY IN AZURE: {credit_summary_azure}")
            logger.info(f"DESTINATION: COPIED CREDIT SUMMARY IN S3: {credit_summary_azure_copied_to_s3}")
            raise AirflowFailException("Credit Summary in Azure and copied to S3 don't match")

        logger.info("Credit Summary in Azure and copied to S3 match perfectly")

    # For Azure need to copy credit summary data set to load to Vertica
    copy_credit_summary_to_s3 = DatasetTransferTask(
        name='copy_credit_summary_to_s3',
        dataset=IdentityDatasets.iav2_credit_summary_azure,
        src_cloud_provider=CloudProviders.azure,
        dst_cloud_provider=CloudProviders.aws,
        partitioning_args=IdentityDatasets.iav2_credit_summary_azure.get_partitioning_args(ds_date=JOB_RUN_DATE)
    )

    verify_credit_summary_azure_copied_to_s3 = OpTask(
        op=PythonOperator(
            python_callable=compare_data_across_cloud_after_copy,
            op_kwargs={'run_date': JOB_RUN_DATE},
            task_id='verify_iav2_credit_summary_azure_copied_to_s3'
        )
    )
    return [copy_credit_summary_to_s3, verify_credit_summary_azure_copied_to_s3]


NUM_CORES_AWS = 6000
LOGWORKFLOW_CONNECTION = 'lwdb'
LOGWORKFLOW_SANDBOX_CONNECTION = 'sandbox-lwdb'
TTD_ENV = TtdEnvFactory.get_from_system()
JOB_RUN_DATE = RunTimes.previous_full_day

dag >> create_credit_task_group_per_env(CloudProviders.aws)
dag >> create_credit_task_group_per_env(CloudProviders.azure)

final_dag: DAG = dag.airflow_dag
