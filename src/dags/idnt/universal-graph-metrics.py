"""
### Universal Graph Metrics DAG

This DAG checks for new releases of post-ETL cross device graphs and calculates metrics such as stickiness, stability and avails coverage for [the Grafana dashboard](https://dash.adsrvr.org/d/mnwC3PgMz/universal-graph-metrics?orgId=1).
"""
from airflow import DAG
from airflow.operators.python_operator import ShortCircuitOperator
from dags.idnt.statics import Executables
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_helpers import DagHelpers
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
from typing import Optional, List, Tuple
from ttd.eldorado.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.tasks.op import OpTask
from ttd.tasks.python import PythonTask
from ttd.operators.ttd_emr_add_steps_operator import TtdEmrAddStepsOperator
from ttd.sensors.ttd_emr_step_sensor import TtdEmrStepSensor
from ttd.ttdenv import TtdEnvFactory
from ttd.jinja.xcom_return_value import XComReturnValue
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage

job_name = 'universal-graph-metrics'

dag: TtdDag = DagHelpers.identity_dag(
    dag_id=job_name,
    start_date=datetime(2024, 6, 18, 6, 0),
    schedule_interval="0 6 * * *",
    doc_md=__doc__,
    run_only_latest=True,
)
airflow_dag: DAG = dag.airflow_dag

log = logging.getLogger(job_name)
ttd_env = TtdEnvFactory.get_from_system().execution_env

DEFAULT_REGION_NAME = "us-east-1"
DEFAULT_AWS_CONN_ID = "aws_default"

vendor_metrics_subdir = 'prod/' if ttd_env == 'prod' else 'test/'
metrics_subdir = 'genericmetrics/' if ttd_env == 'prod' else 'test/'
push_to_prometheus = 'true' if ttd_env == 'prod' else 'false'


@dataclass
class VendorConfig:
    vendorName: str
    bucket: str
    graphBasePrefix: str
    groupingLevel: str
    useFeedbackOverlapInDeviceTypeCount: bool
    locationToReportMetricsBackToVendor: Optional[str] = None
    useRegionalizedMetrics: bool = False
    enableRegionalizedReceiptsSchema: bool = False


vendorConfigs = [
    # ia partners from outside
    VendorConfig(
        'tapad_na', 'thetradedesk-useast-data-import', 'sxd-etl/universal/tapad/na/', 'personId', True,
        f's3://thetradedesk-useast-data-import/tapad/delivery/na/metrics/{vendor_metrics_subdir}'
    ),
    VendorConfig(
        'tapad_apac', 'thetradedesk-useast-data-import', 'sxd-etl/universal/tapad/apac/', 'personId', True,
        f's3://thetradedesk-useast-data-import/tapad/delivery/apac/metrics/{vendor_metrics_subdir}'
    ),
    VendorConfig(
        'tapad_eur', 'thetradedesk-useast-data-import', 'sxd-etl/universal/tapad/eur/', 'personId', True,
        f's3://thetradedesk-eu-central-data-import/tapad/delivery/eu/metrics/{vendor_metrics_subdir}'
    ),
    VendorConfig(
        'tapad', 'thetradedesk-useast-data-import',
        'sxd-etl/universal/tapad/na/,sxd-etl/universal/tapad/apac/,sxd-etl/universal/tapad/eur/', 'personId', True,
        f's3://thetradedesk-useast-data-import/tapad/delivery/combined/metrics/{vendor_metrics_subdir}', True, True
    ),
    VendorConfig(
        'identitylink', 'thetradedesk-useast-data-import', 'sxd-etl/universal/identitylink/', 'personId', True,
        f's3://thetradedesk-fileshare/liveramp-idlink/metrics/{vendor_metrics_subdir}', True
    ),
    VendorConfig(
        'id5-iav2', 'thetradedesk-useast-data-import', 'sxd-etl/universal/id5-iav2/', 'personId', True,
        f's3://thetradedesk-useast-data-import/graph/id5.io/metrics/{vendor_metrics_subdir}', True
    ),
    VendorConfig(
        'emetriq-iav2', 'thetradedesk-useast-data-import', 'sxd-etl/universal/emetriq-iav2/', 'personId', True,
        f's3://thetradedesk-useast-data-import/emetriq/metrics/{vendor_metrics_subdir}', True
    ),

    # og + ab graph, both standalone and for iav2graph
    VendorConfig('nextgen_person', 'thetradedesk-useast-data-import', 'sxd-etl/universal/nextgen/', 'personId', False),
    VendorConfig('nextgen_hh', 'thetradedesk-useast-data-import', 'sxd-etl/universal/nextgen_household/', 'householdId', False),

    # iav2graph based on og + ab
    VendorConfig('iav2', 'thetradedesk-useast-data-import', 'sxd-etl/universal/iav2graph/', 'personId', False),
    VendorConfig('iav2_hh', 'thetradedesk-useast-data-import', 'sxd-etl/universal/iav2graph_household/', 'householdId', False),
    VendorConfig('iav2predat', 'thetradedesk-useast-data-import', 'sxd-etl/universal/pre-dat-graphs/iav2graph/', 'personId', False),
    VendorConfig(
        'iav2predat_hh', 'thetradedesk-useast-data-import', 'sxd-etl/universal/pre-dat-graphs/iav2graph_household/', 'householdId', False
    ),

    # Full OpenGraph.
    VendorConfig('opengraph_person', 'thetradedesk-useast-data-import', 'sxd-etl/universal/opengraph/', 'personId', False),

    # IAv2 based on full OpenGraph.
    VendorConfig(
        'iav2_opengraph_predat', 'thetradedesk-useast-data-import', 'sxd-etl/universal/pre-dat-graphs/iav2graph_with_full_opengraph/',
        'personId', False
    ),
    VendorConfig(
        'iav2_opengraph_predat_hh', 'thetradedesk-useast-data-import',
        'sxd-etl/universal/pre-dat-graphs/iav2graph_with_full_opengraph_household/', 'householdId', False
    ),

    # legacy adbrain standalone and legacy iav2graph
    VendorConfig(
        'adbrain_legacy_hh', 'thetradedesk-useast-data-import', 'sxd-etl/universal/adbrain_household_legacy/', 'householdId', False
    ),
    VendorConfig('adbrain_legacy_person', 'thetradedesk-useast-data-import', 'sxd-etl/universal/adbrain_legacy/', 'personId', False),
    VendorConfig('adbrain_legacy_eur_person', 'thetradedesk-useast-data-import', 'sxd-etl/universal/adbrain_legacy/', 'personId', False),
    VendorConfig('iav2_legacy', 'thetradedesk-useast-data-import', 'sxd-etl/universal/iav2graph_legacy/', 'personId', False),
    VendorConfig(
        'iav2_legacy_hh', 'thetradedesk-useast-data-import', 'sxd-etl/universal/iav2graph_household_legacy/', 'householdId', False
    ),

    # deterministic vendors
    VendorConfig('truedata', 'thetradedesk-useast-data-import', 'sxd-etl/universal/truedata/', 'personId', True),
    VendorConfig('liveintent', 'thetradedesk-useast-data-import', 'sxd-etl/universal/liveintent/', 'personId', True),
    VendorConfig('throtle', 'thetradedesk-useast-data-import', 'sxd-etl/universal/throtle/', 'personId', True),
]


# compare most recent date in graph path and metric path
def check_work_exists_callable(**kwargs):
    params = get_works_parameter()
    return len(params) != 0


def convert_work_to_emr_steps_callable(**kwargs):
    # boto3-style EMR steps.
    steps: list[dict] = []
    # TODO: Get rid of the second call to get_works_parameter. It does a bunch of AWS API call that are not necessary because
    # we get all information the first time in check_work_exists_callable. Save the result of the first call to XCom or cache the
    # params instead.
    params = get_works_parameter()

    # get most recent det data path
    detDataS3Bucket = 'thetradedesk-useast-data-import'
    detDataPrefix = 'sxd-etl/deterministic/uts_graph/devices/v=1/'
    datesFoundSorted = get_sorted_date_subfolders(detDataS3Bucket, detDataPrefix)
    hook = AwsCloudStorage(conn_id=DEFAULT_AWS_CONN_ID)
    pathToUse: str
    for date in datesFoundSorted:
        pathToUse = f"{detDataPrefix}{date.strftime('%Y-%m-%d')}/"
        if hook.check_for_key(key=pathToUse + "_SUCCESS", bucket_name=detDataS3Bucket):
            log.info(f"find the recent deterministic path: {pathToUse} in bucket {detDataS3Bucket}")
            break

    for vendor in params.keys():
        (
            mostRecentGraphDate, mostRecentGraphPathString, prevGraphPathString, mostRecentGraphMetricsPath, groupingLevel,
            useFeedbackOverlapInDeviceTypeCount, locationToReportMetricsBackToVendor, useRegionalizedMetrics,
            enableRegionalizedReceiptsSchema
        ) = params[vendor]

        flag: str = str(useFeedbackOverlapInDeviceTypeCount).lower()
        stats_generator_step_config = get_emr_step_config(
            name=f'Stats generator for {vendor}',
            class_name='jobs.identity.GraphStatsGenerator',
            executable_location=Executables.identity_repo_executable,
            eldorado_config_option_pairs_list=[("vendorName", vendor), ("testGraphPath", mostRecentGraphPathString),
                                               ("previousGraphPath", prevGraphPathString),
                                               ("detDataPath", f"s3://{detDataS3Bucket}/{pathToUse}"), ("groupingLevel", groupingLevel),
                                               ("useFeedbackInDeviceTypeCount", flag), ("metricsSavePath", mostRecentGraphMetricsPath),
                                               ("pushMetricsToPrometheus", push_to_prometheus), ("ttd.env", ttd_env)],
        )

        log.info(
            f"add step - vendor: {vendor}, prev: {prevGraphPathString}, test: {mostRecentGraphPathString}, metric: {mostRecentGraphMetricsPath}"
        )
        steps.append(stats_generator_step_config)

        if locationToReportMetricsBackToVendor is not None:
            etlMetricsPath = mostRecentGraphPathString.replace("success", "stats")
            metrics_collector_step_config = get_emr_step_config(
                name=f'Metrics collector for {vendor}',
                class_name='jobs.identity.metrics.GraphMetricsCollector',
                executable_location=Executables.identity_repo_executable,
                eldorado_config_option_pairs_list=[
                    (
                        "GraphVendor", vendor.split("_")[0].split("-")[0]
                    ),  # vendor.split("_") essentially to treat all tapad subgraphs as tapad, also conform id5/emetriq naming to regionalized metrics
                    ("LatestGraphVendorDate", mostRecentGraphDate),
                    ("UseRegionalizedMetrics", str(useRegionalizedMetrics).lower()),
                    ("EnableRegionalizedReceiptsSchema", str(enableRegionalizedReceiptsSchema).lower()),
                    ("EtlMetricsPath", etlMetricsPath),
                    ("UniversalMetricsPath", mostRecentGraphMetricsPath),
                    ("OutputPath", locationToReportMetricsBackToVendor),
                    ("ttd.env", ttd_env)
                ],
                additional_args_option_pairs_list=[
                    ("conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"),
                    ("conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog")
                ],
            )

            log.info(f"add step - vendor: {vendor} metrics output path: {locationToReportMetricsBackToVendor}")

            steps.append(metrics_collector_step_config)

    return steps


def get_regionalized_vendor_metrics_ready(vendor_name: str, mostRecentVendorGraphDate: str) -> bool:

    # TODO: expand the check to all regions specific to vendor
    regionalizedVendorMetricsBasePrefix = "iaVendorGraphs/creditMetrics/"
    regionalizedVendorMetricsPrefixWithPartitions = regionalizedVendorMetricsBasePrefix + f"dayDate={mostRecentVendorGraphDate}/" + "region=GLOBAL/" + f"graph={vendor_name}/"

    # not checking on uniqueness as there is some wiggle room to read previous week's results in case iav2 graph pipeline doesn't executes in time
    metricTypes = ["stability", "precision"]

    hook = AwsCloudStorage(conn_id=DEFAULT_AWS_CONN_ID)

    return all([
        bool(
            hook.list_keys(bucket_name="ttd-identity", prefix=regionalizedVendorMetricsPrefixWithPartitions + f"metricType={metricType}/")
        ) for metricType in metricTypes
    ])


def get_works_parameter():
    params = {}
    hook = AwsCloudStorage(conn_id=DEFAULT_AWS_CONN_ID)
    for vendorConfig in vendorConfigs:
        graphBasePrefixes = vendorConfig.graphBasePrefix.split(",")
        maxDateStrings = []
        mostRecentGraphPathsPrefix = []
        prevGraphPaths = []
        mostRecentLocationToReportMetricsBackToVendor: Optional[str] = None

        for graphBasePrefix in graphBasePrefixes:
            datesFoundSorted = get_sorted_date_subfolders(vendorConfig.bucket, graphBasePrefix)
            if len(datesFoundSorted) < 2:
                log.warning(f'The {graphBasePrefix} prefix does not contain two deliveries to calculate the metrics from')
                continue

            maxDateString = datesFoundSorted[0].strftime('%Y-%m-%d')
            prevDateString = datesFoundSorted[1].strftime('%Y-%m-%d')
            maxDateStrings.append(maxDateString)
            log.debug(f"{graphBasePrefix}:{maxDateString}")
            mostRecentGraphPathsPrefix.append(f"{graphBasePrefix}{maxDateString}/success/")
            prevGraphPaths.append(f"s3://{vendorConfig.bucket}/{graphBasePrefix}{prevDateString}/success/")

        if not maxDateStrings:
            log.warning(f'Skip {vendorConfig.vendorName} because it does not have two deliveries to calculate the metrics from.')
            continue

        # use the max data so we could compute metrics each time we get a new subgraph
        dateString = max(maxDateStrings)

        metricsBasePathBucket = "ttd-identity"
        mostRecentGraphMetricsPathPrefix = f'graphmetrics/{metrics_subdir}{vendorConfig.vendorName}/{dateString}.csv'
        mostRecentGraphMetricsPath = f"s3://{metricsBasePathBucket}/{mostRecentGraphMetricsPathPrefix}"

        if vendorConfig.locationToReportMetricsBackToVendor is not None:
            mostRecentLocationToReportMetricsBackToVendor = vendorConfig.locationToReportMetricsBackToVendor + dateString

        if hook.check_for_key(key=mostRecentGraphMetricsPathPrefix,
                              bucket_name=metricsBasePathBucket):  # metrics have already been calculated
            log.info("metrics already calculated for graph at " + mostRecentGraphMetricsPath)
            continue

        if vendorConfig.locationToReportMetricsBackToVendor is not None and (not get_regionalized_vendor_metrics_ready(
                vendorConfig.vendorName.split("_")[0].split("-")[0], dateString)):
            log.info(f"regionalized metrics for {vendorConfig.vendorName} for date {dateString} is not ready")
            continue

        # check graph paths
        passETLCheck = True
        for mostRecentGraphPathPrefix in mostRecentGraphPathsPrefix:
            if not hook.check_for_key(key=mostRecentGraphPathPrefix + "_SUCCESS",
                                      bucket_name=vendorConfig.bucket):  # stop if graph etl is not done
                log.info(
                    f"ETL for vendor {vendorConfig.vendorName} is not ready at path {mostRecentGraphPathPrefix} in bucket {vendorConfig.bucket}"
                )
                passETLCheck = False
                break

        if passETLCheck is False:
            continue

        mostRecentGraphPaths = [
            f"s3://{vendorConfig.bucket}/{mostRecentGraphPathPrefix}" for mostRecentGraphPathPrefix in mostRecentGraphPathsPrefix
        ]

        mostRecentGraphPathString = ','.join(mostRecentGraphPaths)
        prevGraphPathString = ','.join(prevGraphPaths)
        log.info(f"find vendor: {vendorConfig.vendorName} to run")
        log.debug(f"current env: {ttd_env}")
        params[vendorConfig.vendorName] = (
            dateString, mostRecentGraphPathString, prevGraphPathString, mostRecentGraphMetricsPath, vendorConfig.groupingLevel,
            vendorConfig.useFeedbackOverlapInDeviceTypeCount, mostRecentLocationToReportMetricsBackToVendor,
            vendorConfig.useRegionalizedMetrics, vendorConfig.enableRegionalizedReceiptsSchema
        )

    return params


def get_emr_step_config(
    name: str,
    class_name: str,
    executable_location: str,
    eldorado_config_option_pairs_list: List[Tuple[str, str]] = None,
    additional_args_option_pairs_list: List[Tuple[str, str]] = None
):
    """Returns boto3-style StepConfig for RunJobFlow."""
    command_runner_args = ["spark-submit", "--deploy-mode", "cluster", "--class", class_name]
    if additional_args_option_pairs_list is not None:
        for config_pair in additional_args_option_pairs_list:
            command_runner_args.append("--" + config_pair[0])
            command_runner_args.append(config_pair[1])
    if eldorado_config_option_pairs_list is not None and len(eldorado_config_option_pairs_list) > 0:
        command_runner_args.append(
            "--driver-java-options=" + " ".join(map(lambda t: f"-D{t[0]}={t[1]}", eldorado_config_option_pairs_list))
        )
    command_runner_args.append(executable_location)
    return {"Name": name, "ActionOnFailure": "CONTINUE", "HadoopJarStep": {"Jar": "command-runner.jar", "Args": command_runner_args}}


def get_sorted_date_subfolders(s3Bucket, prefix: str, metricType='universal'):
    hook = AwsCloudStorage(conn_id=DEFAULT_AWS_CONN_ID)
    prefixesFound = hook.list_prefixes(s3Bucket, prefix, '/')
    datesFound = []
    for prefix in prefixesFound:
        folder: str
        if metricType == 'universal':
            folder = list(filter(None, prefix.split("/")))[-1]
        elif metricType == 'uniqueness':
            folder = list(filter(None, prefix.split("/")))[-1][5:]

        try:
            folderAsDate = datetime.strptime(folder, '%Y-%m-%d')
            datesFound.append(folderAsDate)
        except ValueError as e:
            log.error(f"couldn't parse '{folder}' to date ({str(e)})")
    datesFoundSorted = sorted(datesFound, reverse=True)
    return datesFoundSorted


check_work_exists = OpTask(
    op=ShortCircuitOperator(task_id='check_work_exists', python_callable=check_work_exists_callable, dag=airflow_dag, provide_context=True)
)

generate_and_distribute_metrics_emr_cluster = IdentityClusters.get_cluster(
    "generate_and_distribute_metrics", dag, 3500, ComputeType.STORAGE, use_delta=False
)

convert_work_to_emr_steps = PythonTask(task_id="convert_work_to_emr_steps", python_callable=convert_work_to_emr_steps_callable)

add_emr_steps = OpTask(
    op=TtdEmrAddStepsOperator(
        task_id='add_emr_steps',
        job_flow_id=generate_and_distribute_metrics_emr_cluster.cluster_id,
        steps=XComReturnValue.template_builder(convert_work_to_emr_steps.task_id).build(),
        region_name=DEFAULT_REGION_NAME
    )
)

# Watch the last step because TtdEmrStepSensor doesn't allow to watch multiple steps.
watch_last_emr_step = OpTask(
    op=TtdEmrStepSensor(
        task_id="watch_last_emr_step",
        job_flow_id=generate_and_distribute_metrics_emr_cluster.cluster_id,
        step_id=XComReturnValue.template_builder(add_emr_steps.task_id).with_filter("last").build(),
        cluster_task_id=EmrClusterTask.create_cluster_task_id(generate_and_distribute_metrics_emr_cluster.name),
        timeout=timedelta(hours=16),
        aws_conn_id=DEFAULT_AWS_CONN_ID,
        poke_interval=60,
        region_name=DEFAULT_REGION_NAME,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
)

check_work_exists >> generate_and_distribute_metrics_emr_cluster
generate_and_distribute_metrics_emr_cluster.add_sequential_body_task(convert_work_to_emr_steps)
generate_and_distribute_metrics_emr_cluster.add_sequential_body_task(add_emr_steps)
generate_and_distribute_metrics_emr_cluster.add_sequential_body_task(watch_last_emr_step)
# The order of '>>' calls matters here: add the tasks to the DAG last so the TtdEmr steps are included to the task group.
dag >> check_work_exists
