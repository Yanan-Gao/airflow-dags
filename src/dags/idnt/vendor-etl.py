"""Process vendors' raw data into clean outputs.

We process IA vendors: tapad, liveramp, id5's data and Deterministic vendors: throtle, truedata and liveintent's data.
Unfortunately, they all need slightly different pre-processing so this is a bit messy.

Bence tried his best to reuse as much of the code as possible and simplify the process.
We now have VendorTaskConfigs that describe which etl object we need to execute and with what
configs. These configs then can be amended for each vendor further. Note that
`data_import_bucket`, `test_folder`and `vendor_name` placeholders get overwritten for each vendor
in the configs.

Each vendor defines input and output paths with a lookback window. We find the latest input
file and check if that input file has already been processed. If so, then we skip cluster creation.
Else we check if the input is late. If it is, then we fail the task and post an alert on Slack.
Note that the etl objects will skip compute too if they detect the processing has been done.
"""
from airflow import DAG
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from datetime import datetime, timedelta
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from ttd.el_dorado.v2.emr import EmrJobTask
from dags.idnt.util.s3_utils import does_folder_have_objects, get_latest_etl_date_from_path
from dags.idnt.statics import Executables, RunTimes, Directories, VendorTypes
from dags.idnt.vendors.vendor_alerts import LateVendor
from ttd.tasks.op import OpTask
from dataclasses import dataclass
from typing import Callable, List, Dict, Optional, cast, Set
from airflow.utils.trigger_rule import TriggerRule
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.tasks.chain import ChainOfTasks
from dags.idnt.util.s3_utils import all_trigger_files_complete
import logging

TAPAD_SUB_GRAPHS = "1-apac|2-eur|3-na"
test_folder = Directories.test_dir_no_prod()
data_import_bucket = "thetradedesk-useast-data-import"
run_time = RunTimes.previous_full_day
investigation_path = f"s3://ttd-identity/data/vendors{test_folder}/investigateVendorDelivery"

# Trigger files for all valid id types for Liveramp
trigger_suffix = "global_trigger.txt"
liveramp_id_type_triggers = [
    f'AAID_{trigger_suffix}', f'ConnectedTv_{trigger_suffix}', f'Cookies_{trigger_suffix}', f'IDFA_{trigger_suffix}'
]

dag = DagHelpers.identity_dag(
    dag_id="vendor-etl",
    schedule_interval="15 */8 * * *",  # Should run every 8 hours.
    start_date=datetime(2024, 11, 3, 22, 0),
    retries=1,
    run_only_latest=True,
    dagrun_timeout=timedelta(hours=8),
    doc_md=__doc__
)


@dataclass
class VendorTaskConfig:
    """Handles ETL task description for vendors.
    """
    class_name: str
    configs: Dict[str, str]
    executable_path: str = Executables.etl_repo_executable

    def add_configs(self, extra_configs: Dict[str, str]):
        """Return a new VendorTaskConfig with extra configs added.

        Args:
            extra_configs (Dict[str, str]): Configs to add to the original configs.

        Returns:
            VendorTaskConfig: Task config with all configs merged.
        """
        configs = self.configs.copy()
        configs.update(extra_configs)

        return self.__class__(self.class_name, configs, self.executable_path)

    def get_configs(self) -> Dict[str, str]:
        """Return the full list of configs.

        Returns:
            Dict[str, str]: Configs with some universal defaults appended.
        """

        # needed for all ETL tasks
        self.configs.update({"LOCAL": "false"})

        return self.configs


@dataclass
class VendorETLConfig:
    """Handle etl processing of vendor
    """
    vendor_name: str
    vendor_type: VendorTypes
    base_input_path: str
    input_path_generator: Callable[[Optional[datetime]], str]
    output_path_generator: Callable[[Optional[datetime]], str]
    task_configs: List[VendorTaskConfig]
    cadence_days: int = 7
    buffer_days: int = 2
    cluster_size: int = 30
    input_date_format: str = "%Y-%m-%d"

    def short_circuit(self, execution_date: datetime, **kwargs) -> bool:
        """Python callable that returns false if no further processing is needed.

        Args:
            execution_date: Execution date of the DAG run.

        Returns:
            bool: False if no processing is needed, true otherwise.
        """
        hook = AwsCloudStorage(conn_id='aws_default')
        last_etl_date = get_latest_etl_date_from_path(hook, self.output_path_generator(None), execution_date)
        # Push last_etl_date to xcom for PreETL Vendor Alerts check
        kwargs["ti"].xcom_push(key=f'last_etl_date_{self.vendor_name}', value=last_etl_date.date().isoformat())

        latest_input_date = LateVendor.get_latest_input_date(
            current_date=execution_date, last_etl_date=last_etl_date, input_path_generator=self.input_path_generator
        )

        if latest_input_date is not None:
            investigation_folder = f"{investigation_path}/{self.vendor_name}/{latest_input_date.strftime(self.input_date_format)}"
            if does_folder_have_objects(investigation_folder):
                logging.info(f"Skipping ETL: {self.vendor_name} delivery is under investigation in {investigation_folder}")
                return False

            kwargs["ti"].xcom_push(key=f"latest_input_date_{self.vendor_name}", value=latest_input_date.date().isoformat())
            expected_output = self.output_path_generator(latest_input_date)
            output_exists = does_folder_have_objects(expected_output)
            if self.vendor_name == "identitylink":
                trigger_complete = all_trigger_files_complete(
                    latest_input_date, last_etl_date, self.input_path_generator, liveramp_id_type_triggers
                )
                return not output_exists and trigger_complete
            return not output_exists

        # If no input is found, check if the vendor is actually late for delivery
        # If yes, use xcom_push to store it as a LateVendor
        if LateVendor.is_vendor_late(current_date=execution_date, last_etl_date=last_etl_date, cadence_days=self.cadence_days,
                                     buffer_days=self.buffer_days):
            missed_delivery = (last_etl_date + timedelta(days=self.cadence_days)).date()

            late_vendor = LateVendor(
                name=self.vendor_name,
                vendor_type=self.vendor_type,
                input_path=self.input_path_generator(None),
                cadence_days=self.cadence_days,
                last_delivery=last_etl_date.date(),
                missed_delivery=missed_delivery,
                days_late=(execution_date.date() - missed_delivery).days
            )
            # Push the late vendor via xcom
            kwargs["ti"].xcom_push(key=f"late_vendor_{late_vendor.name}", value=late_vendor.to_dict())

            raise RuntimeError(f"No input found for {self.vendor_name} in the last {(execution_date - last_etl_date).days} days.")

        return False

    def create_task(self, task_config: VendorTaskConfig) -> EmrJobTask:
        """Creates an EMR task by filling in vendor info.

        Args:
            task_config (VendorTaskConfig): Task config to be materialized.

        Returns:
            EmrJobTask: Prepped EMR step for vendor processing.
        """

        def fill_placeholder(placeholder: str) -> str:
            return placeholder.format(
                vendor_name=self.vendor_name,
                test_folder=test_folder,
                data_import_bucket=data_import_bucket,
                run_time=run_time,
                investigation_path=investigation_path,
                base_input_path=self.base_input_path
            )

        full_configs = {key: fill_placeholder(value) for key, value in task_config.get_configs().items()}

        return IdentityClusters.task(
            class_name=task_config.class_name,
            eldorado_configs=IdentityClusters.dict_to_configs(full_configs),
            executable_path=task_config.executable_path,
            task_name_suffix=full_configs.get("TaskSuffix", "")  # to handle PreETL check for tapad subgraphs
        )

    def get_vendor_cluster_with_appended_tasks(self, main_dag: DAG):
        """Appends all necessary processing to a vendor cluster created and returns it.

        Args:
            main_dag (DAG): Main dag we're appending to.
        """

        cluster = IdentityClusters.get_cluster(f"{self.vendor_name}_cluster", main_dag, self.cluster_size * 100, ComputeType.STORAGE)

        for task_config in self.task_configs:
            cluster.add_sequential_body_task(self.create_task(task_config))

        return cluster


# ----- TASKS FOR BOTH IAV2 AND DETERMINISTIC VENDORS ----- #
pre_etl_critical_alerts_check = VendorTaskConfig(
    "jobs.identity.VendorPreETLCriticalAlerts",
    {
        "InPathWithoutDate":
        "s3://{data_import_bucket}/{input_prefix}/",
        "InvestigateDeliveryPath":
        "{investigation_path}",
        "LastETLDate":
        "{{{{ task_instance.xcom_pull(task_ids='" + "{vendor_name}_should_skip', key='last_etl_date_{vendor_name}') }}}}",
        "PercentChangeThreshold":
        "30",
        "VendorName":
        "{vendor_name}",
        "runDate":
        "{{{{ task_instance.xcom_pull(dag_id='vendor-etl', task_ids='{vendor_name}_should_skip', key='latest_input_date_{vendor_name}') }}}}"
    },
    executable_path=Executables.identity_repo_executable  # This check is in identity repo
)
# ----- TASKS FOR IAV2 VENDORS ----- #
# NOTE: you must have `{run_time}` replaced at call time (inside fill_placeholder) to render the jinja template
liveramp_submerge_task = VendorTaskConfig(
    "com.thetradedesk.etl.sxd.preprocess.IdentityLinkSubGraphMerge", {
        "INPUT_PATH": "s3://thetradedesk-fileshare/liveramp-idlink{test_folder}",
        "OUTPUT_PATH": "s3://{data_import_bucket}/{vendor_name}/merged{test_folder}"
    }
)

tapad_eu_subgraph_copy_task = VendorTaskConfig(
    "com.thetradedesk.etl.sxd.preprocess.TapadSubGraphCopy", {
        "EUR_INPUT_PATH": "s3://thetradedesk-eu-central-data-import/{vendor_name}/delivery/eu/",
        "OUTPUT_PATH": "s3://{data_import_bucket}/{vendor_name}/delivery/",
        "LOOKBACK_WINDOW": "14",
        "EUR_PREFIX": "eur",
        "NUM_PARTITIONS": "800",
    }
)

etl_task = VendorTaskConfig(
    "com.thetradedesk.etl.logformats.ETLDriver", {
        "INPUT_PATH": "s3://{data_import_bucket}/{vendor_name}",
        "OUTPUT_PATH": "s3://{data_import_bucket}/sxd-etl/universal/{vendor_name}{test_folder}",
        "METRICS_PATH": "s3://{data_import_bucket}/sxd-etl/metrics{test_folder}/",
        "GATEWAY_ADDRESS": "prom-push-gateway.adsrvr.org:80",
        "NUM_PARTITIONS": "400",
        "RETENTION_RATIO_THRESHOLD": "0.5",
        "INVESTIGATE_DELIVERY_PATH": "{investigation_path}",
        "VENDOR_NAME": "{vendor_name}"
    }
)

ia_vendor_graph_metrics_task = VendorTaskConfig(
    "jobs.identity.alliance.v2.GraphMetrics.IAVendorMetricsCrediting", {
        "jobs.identity.alliance.v2.GraphMetrics.IAVendorMetricsCrediting.vendorName":
        "{vendor_name}",
        "runDate":
        "{{{{ task_instance.xcom_pull(dag_id='vendor-etl', task_ids='{vendor_name}_should_skip', key='latest_input_date_{vendor_name}') }}}}"
    }, Executables.identity_repo_executable
)

tapad_etl_combined_task = VendorTaskConfig(
    "jobs.identity.alliance.v2.datasets.TapadPostETLGraphCombined", {
        "runDate":
        "{{{{ task_instance.xcom_pull(dag_id='vendor-etl', task_ids='{vendor_name}_should_skip', key='latest_input_date_{vendor_name}') }}}}"
    }, Executables.identity_repo_executable
)

# TODO we're re-doing this for all vendors. DAG needs to change for this to run selectively
lookuptable_gen_task = VendorTaskConfig(
    "com.thetradedesk.etl.scoring.ScoreLookupTableDriver", {
        "TAPAD_SUB_GRAPHS": TAPAD_SUB_GRAPHS,
        "NUM_PARTITIONS": "200",
        "DATA_TO_BE_SCORED_BASE_INPUT_PATH": "s3://{data_import_bucket}/sxd-etl/universal",
        "DATA_TO_BE_SCORED_BASE_OUTPUT_PATH": "s3://{data_import_bucket}/sxd-etl/universal{test_folder}",
        "DATA_TO_BE_SCORED_NAMES": "identitylink",
        "BENCHMARK_DATA_PATHS": "s3://{data_import_bucket}/sxd-etl/deterministic/uts_graph/devices/v=1",
        "BENCHMARK_DATA_NAMES": "uts",
        "FINAL_MERGED_CSV_PATH": "s3://ttd-identity/lookupTable/{run_time}",
        "TAPAD_DATA_TO_BE_SCORED_BASE_INPUT_PATH": "s3://{data_import_bucket}/sxd-etl/universal{test_folder}/tapad",
        "TAPAD_DATA_TO_BE_SCORED_OUTPUT_PATH": "s3://{data_import_bucket}/sxd-etl/universal/tapad{test_folder}/lookupTable",
        "LOOKBACK_WINDOW": "60",
        "USE_PAIR_BASED_PRECISION": "true",
        "USE_PERSON_DOWN_SAMPLE": "false",
        "USE_UTS_INSTEAD_BENCHMARKS": "true",
        "UTS_PATH": "s3://{data_import_bucket}/sxd-etl/deterministic/uts_graph/devices/v=1",
        "DATE": "{run_time}"
    }
)

score_normalization_task = VendorTaskConfig(
    "com.thetradedesk.etl.scoring.ScoreNormalizationDriver", {
        "INPUT_PATH": "s3://{data_import_bucket}/sxd-etl/universal{test_folder}/{vendor_name}",
        "NUM_PARTITIONS": "800",
        "OUTPUT_PATH": "s3://{data_import_bucket}/sxd-etl/scoreNormalized{test_folder}/{vendor_name}",
        "DETERMINSTIC_VENDOR": "averaged_scores",
        "VENDOR_NAME": "{vendor_name}"
    }
)

vertica_format_task = VendorTaskConfig(
    "com.thetradedesk.etl.logformats.UniversalToVerticaFormat", {
        "INPUT_PATH": "s3://{data_import_bucket}/sxd-etl/scoreNormalized{test_folder}/{vendor_name}",
        "OUTPUT_PATH": "s3://{data_import_bucket}/{vendor_name}/universal{test_folder}/",
        "STATS_PATH": "s3://{data_import_bucket}/sxd-etl/toVerticaFormat{test_folder}/{vendor_name}",
        "NUM_PARTITIONS": "300",
        "VENDOR_NAME": "{vendor_name}"
    }
)

# ----- IAV2 VENDORS ----- #
liveramp = VendorETLConfig(
    vendor_name="identitylink",
    vendor_type=VendorTypes.ia_type,
    base_input_path=f"s3://{data_import_bucket}/identitylink/merged{test_folder}/",
    input_path_generator=cast(
        Callable[[Optional[datetime]], str],
        lambda d=None: f"s3://thetradedesk-fileshare/liveramp-idlink{test_folder}" +
        (f"/{d.strftime('%Y/%m/%d')}/" if d is not None else "")
    ),
    output_path_generator=cast(
        Callable[[Optional[datetime]], str],
        lambda d=None: f"s3://thetradedesk-useast-data-import/identitylink/universal{test_folder}" +
        (f"/{d.strftime('%Y-%m-%d')}-00-00-00/" if d is not None else "")
    ),
    task_configs=[
        liveramp_submerge_task,
        pre_etl_critical_alerts_check.add_configs({
            "InPathWithoutDate": "{base_input_path}",
        }),
        etl_task.add_configs({
            "INPUT_PATH": "{base_input_path}",
            "DETERMINISTIC": "true",
            "DETERMINISTIC_PATH": "s3://{data_import_bucket}/sxd-etl/deterministic/",
            "RETENTION_RATIO_THRESHOLD":
            "1.0",  # Output > Input for Identitylink since we create additional rows mapping from idl (uiid) to idl (personId)
        }),
        ia_vendor_graph_metrics_task,
        lookuptable_gen_task,
        score_normalization_task.add_configs({
            "LOOKBACK_WINDOW":
            "31",
            "LOOK_UP_TABLE_INPUT_PATH":
            "s3://{data_import_bucket}/sxd-etl/universal/identitylink{test_folder}"
        }),
        vertica_format_task.add_configs({
            "LOOKBACK_WINDOW": "31",
        })
    ],
    cluster_size=80
)

subgraphs = ["na", "eur", "apac"]

# Pre-ETL tasks for each subgraph
pre_etl_tasks = [
    pre_etl_critical_alerts_check.add_configs({
        "InPathWithoutDate": f"s3://{data_import_bucket}/tapad/delivery{test_folder}/{subgraph}/",
        "TaskSuffix": subgraph
    }) for subgraph in subgraphs
]

tapad = VendorETLConfig(
    vendor_name="tapad",
    vendor_type=VendorTypes.ia_type,
    base_input_path=f"s3://{data_import_bucket}/tapad/delivery{test_folder}/",
    # TODO consider implementing checks for all subgraphs
    input_path_generator=cast(
        Callable[[Optional[datetime]], str],
        lambda d=None: f"s3://thetradedesk-useast-data-import/tapad/delivery{test_folder}/na" +
        (f"/{d.strftime('%Y-%m-%d')}/" if d is not None else "")
    ),
    output_path_generator=cast(
        Callable[[Optional[datetime]], str],
        lambda d=None: f"s3://thetradedesk-useast-data-import/tapad/universal{test_folder}/na" +
        (f"/{d.strftime('%Y-%m-%d')}-00-00-00/" if d is not None else "")
    ),
    task_configs=[
        tapad_eu_subgraph_copy_task, *pre_etl_tasks,
        etl_task.add_configs({
            "INPUT_PATH": "{base_input_path}",
            "OUTPUT_PATH": "s3://{data_import_bucket}/sxd-etl/universal/{vendor_name}{test_folder}/",
            "NUM_PARTITIONS": "800"
        }), tapad_etl_combined_task, ia_vendor_graph_metrics_task, lookuptable_gen_task,
        score_normalization_task.add_configs({
            "SUB_GRAPHS":
            "1-apac|2-eur|3-na",
            "LOOK_UP_TABLE_INPUT_PATH":
            "s3://{data_import_bucket}/sxd-etl/universal/tapad{test_folder}/lookupTable"
        }),
        vertica_format_task.add_configs({
            "SUB_GRAPHS": "1-apac|2-eur|3-na",
            "NUM_PARTITIONS": "200"
        })
    ],
    cluster_size=80
)

id5 = VendorETLConfig(
    vendor_name="id5",
    vendor_type=VendorTypes.ia_type,
    base_input_path=f"s3://{data_import_bucket}/graph/id5.io/prod-graph-delivery{test_folder}/",
    input_path_generator=cast(
        Callable[[Optional[datetime]], str],
        lambda d=None: f"s3://thetradedesk-useast-data-import/graph/id5.io/prod-graph-delivery{test_folder}" +
        (f"/{d.strftime('%Y%m%d')}/" if d is not None else "")
    ),
    output_path_generator=cast(
        Callable[[Optional[datetime]], str],
        lambda d=None: f"s3://thetradedesk-useast-data-import/sxd-etl/universal/id5-iav2{test_folder}" +
        (f"/{d.strftime('%Y-%m-%d')}" if d is not None else "")
    ),
    task_configs=[
        pre_etl_critical_alerts_check.add_configs({
            "InPathWithoutDate": "{base_input_path}",
        }),
        etl_task.add_configs({
            "NUM_PARTITIONS": "800",
            "INPUT_PATH": "{base_input_path}",
            "OUTPUT_PATH": "s3://{data_import_bucket}/sxd-etl/universal/id5-iav2{test_folder}/",
        }), ia_vendor_graph_metrics_task
    ],
    cluster_size=50,
    input_date_format="%Y%m%d"
)

emetriq = VendorETLConfig(
    vendor_name="emetriq",
    vendor_type=VendorTypes.ia_type,
    base_input_path=f"s3://{data_import_bucket}/emetriq/delivery-for-ia{test_folder}/",
    input_path_generator=cast(
        Callable[[Optional[datetime]], str],
        lambda d=None: f"s3://thetradedesk-useast-data-import/emetriq/delivery-for-ia{test_folder}" +
        (f"/{d.strftime('%Y-%m-%d')}" if d is not None else "")
    ),
    output_path_generator=cast(
        Callable[[Optional[datetime]], str],
        lambda d=None: f"s3://thetradedesk-useast-data-import/sxd-etl/universal/emetriq-iav2{test_folder}" +
        (f"/{d.strftime('%Y-%m-%d')}" if d is not None else "")
    ),
    task_configs=[
        pre_etl_critical_alerts_check.add_configs({
            "InPathWithoutDate": "{base_input_path}",
        }),
        etl_task.add_configs({
            "NUM_PARTITIONS": "800",
            "INPUT_PATH": "{base_input_path}",
            "OUTPUT_PATH": "s3://{data_import_bucket}/sxd-etl/universal/emetriq-iav2{test_folder}/",
        }), ia_vendor_graph_metrics_task
    ],
    cluster_size=50
)

# ----- TASKS FOR DETERMINISTIC VENDORS ----- #
etl_deterministic_task = VendorTaskConfig(
    "com.thetradedesk.etl.logformats.ETLDriver", {
        "INPUT_PATH": "s3://{data_import_bucket}/{input_prefix}",
        "OUTPUT_PATH": "s3://{data_import_bucket}/sxd-etl/universal/{vendor_name}{test_folder}/",
        "DETERMINISTIC_PATH": "s3://{data_import_bucket}/sxd-etl/deterministic/",
        "METRICS_PATH": "s3://ttd-identity/adbrain-stats/etl/{vendor_name}{test_folder}/",
        "NUM_PARTITIONS": "400",
        "RETENTION_RATIO_THRESHOLD": "0.5",
        "INVESTIGATE_DELIVERY_PATH": "{investigation_path}",
        "DETERMINISTIC": "true",
        "DROP_UIIDS_WITH_MULTIPLE_PERSON_ID_MAPPINGS": "true",
        "VENDOR_NAME": "{vendor_name}"
    }
)

# ----- DETERMINISTIC VENDORS ----- #
throtle = VendorETLConfig(
    vendor_name="throtle",
    vendor_type=VendorTypes.deterministic_type,
    base_input_path=f"s3://{data_import_bucket}/throtle/import{test_folder}/",
    input_path_generator=cast(
        Callable[[Optional[datetime]], str],
        lambda d=None: f"s3://thetradedesk-useast-data-import/throtle/import{test_folder}" +
        (f"/{d.strftime('%Y-%m-%d')}" if d is not None else "")
    ),
    output_path_generator=cast(
        Callable[[Optional[datetime]], str],
        lambda d=None: f"s3://thetradedesk-useast-data-import/sxd-etl/universal/throtle{test_folder}" +
        (f"/{d.strftime('%Y-%m-%d')}" if d is not None else "")
    ),
    task_configs=[
        pre_etl_critical_alerts_check.add_configs({
            "InPathWithoutDate": "{base_input_path}",
        }),
        etl_deterministic_task.add_configs({
            "INPUT_PATH": "{base_input_path}",
            "LOOKBACK_WINDOW": "9"
        })
    ]
)

truedata = VendorETLConfig(
    vendor_name="truedata",
    vendor_type=VendorTypes.deterministic_type,
    base_input_path=f"s3://{data_import_bucket}/truedata/from_td/prod/identity_graph/US{test_folder}/",
    input_path_generator=cast(
        Callable[[Optional[datetime]], str],
        lambda d=None: f"s3://thetradedesk-useast-data-import/truedata/from_td/prod/identity_graph/US{test_folder}" +
        (f"/{d.strftime('%Y-%m-%d')}" if d is not None else "")
    ),
    output_path_generator=cast(
        Callable[[Optional[datetime]], str],
        lambda d=None: f"s3://thetradedesk-useast-data-import/sxd-etl/universal/truedata{test_folder}" +
        (f"/{d.strftime('%Y-%m-%d')}" if d is not None else "")
    ),
    task_configs=[
        pre_etl_critical_alerts_check.add_configs({
            "InPathWithoutDate": "{base_input_path}",
        }),
        etl_deterministic_task.add_configs({
            "INPUT_PATH": "{base_input_path}",
            "LOOKBACK_WINDOW": "35"
        })
    ],
    cadence_days=30,
    buffer_days=7
)

liveintent = VendorETLConfig(
    vendor_name="liveintent",
    vendor_type=VendorTypes.deterministic_type,
    base_input_path=f"s3://{data_import_bucket}/LiveIntent{test_folder}/",
    input_path_generator=cast(
        Callable[[Optional[datetime]], str],
        lambda d=None: f"s3://thetradedesk-useast-data-import/LiveIntent{test_folder}" +
        (f"/{d.strftime('%Y%m%d')}" if d is not None else "")
    ),
    output_path_generator=cast(
        Callable[[Optional[datetime]], str],
        lambda d=None: f"s3://thetradedesk-useast-data-import/sxd-etl/universal/liveintent{test_folder}" +
        (f"/{d.strftime('%Y-%m-%d')}" if d is not None else "")
    ),
    task_configs=[
        pre_etl_critical_alerts_check.add_configs({
            "InPathWithoutDate": "{base_input_path}",
        }),
        etl_deterministic_task.add_configs({
            "INPUT_PATH": "{base_input_path}",
            "LOOKBACK_WINDOW": "9"
        })
    ],
    input_date_format="%Y%m%d"
)

# list of all the vendors so far
# TODO(skc): Add back throtle once their deliveries are fixed.
all_vendors: List[VendorETLConfig] = [liveramp, tapad, id5, emetriq, truedata, liveintent]


def pull_all_late_vendors_and_post(**kwargs):
    """Python function that pulls all the late vendors after the input check to a set.
       And posts the late delivery alert on slack if there is a late vendor.
    """
    late_vendors: Set[LateVendor] = set()
    all_vendor_names = list(map(lambda v: v.vendor_name, all_vendors))

    for vendor_name in all_vendor_names:
        vendor_data = kwargs["ti"].xcom_pull(task_ids=f"{vendor_name}_should_skip", key=f"late_vendor_{vendor_name}")
        if vendor_data:
            late_vendors.add(LateVendor.from_dict(vendor_data))

    if late_vendors:
        LateVendor.post_slack_alert(late_vendors, "")


check_late_delivery = OpTask(
    op=PythonOperator(
        task_id="check_late_delivery",
        python_callable=pull_all_late_vendors_and_post,
        trigger_rule=TriggerRule.ALL_DONE,
        provide_context=True
    )
)

for vendor in all_vendors:

    should_skip = OpTask(
        op=ShortCircuitOperator(
            task_id=f"{vendor.vendor_name}_should_skip",
            python_callable=vendor.short_circuit,
            ignore_downstream_trigger_rules=False,
            provide_context=True
        )
    )

    vendor_cluster = vendor.get_vendor_cluster_with_appended_tasks(dag)

    dag >> ChainOfTasks(
        task_id=f"{vendor.vendor_name}_vendor_etl", tasks=[should_skip, vendor_cluster]
    ).as_taskgroup(f"{vendor.vendor_name}_vendor_etl_task") >> check_late_delivery

final_dag: DAG = dag.airflow_dag
