"""
### Device Atlas S3 Updater DAG

This DAG controls the weekly job that downloads the device atlas json file from the device atlas API, then writes it to an accessible S3 location.
"""
from typing import Any
from airflow import DAG
from datetime import datetime
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.identity_clusters_aliyun import IdentityClustersAliyun
from dags.idnt.statics import Executables, Tags
from ttd.el_dorado.v2.base import TtdDag
from ttd.ttdenv import TtdEnvFactory

ttd_env = Tags.environment()
test_folder = "" if str(ttd_env) == "prod" else "/test"
num_cores = 16
schedule_interval = "5 7 * * MON" if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else "*/30 * * * *"

executable_path = Executables.etl_repo_executable
executable_path_aliyun = Executables.etl_repo_executable_china


def create_dag(
    dag_id: str, start_date: datetime, executable_path: str, device_atlas_S3_location: str, device_atlas_v3_S3_location: str,
    cluster_factory: Any, **kwargs
) -> TtdDag:
    # Run weekly on Monday at 7:05 AM UTC
    dag = DagHelpers.identity_dag(
        dag_id=dag_id, schedule_interval=schedule_interval, start_date=start_date, run_only_latest=True, doc_md=__doc__, **kwargs
    )

    cluster = cluster_factory.get_cluster("device_atlas_s3_updater_cluster", dag, num_cores, ComputeType.STORAGE, use_delta=False)

    eldorado_options = [("DEVICE_ATLAS_S3_LOCATION", device_atlas_S3_location),
                        ("DEVICE_ATLAS_V3_S3_LOCATION", device_atlas_v3_S3_location)]
    additional_options = [("conf", f"spark.sql.shuffle.partitions={num_cores * 2}"), ("conf", f"spark.default.parallelism={num_cores * 2}")]

    cluster.add_parallel_body_task(
        cluster_factory.task(
            class_name="com.thetradedesk.etl.misc.DeviceAtlasJsonS3Uploader",
            eldorado_configs=eldorado_options,
            extra_args=additional_options,
            executable_path=executable_path,
            timeout_hours=2
        )
    )

    dag >> cluster
    return dag


final_dag: DAG = create_dag(
    dag_id="device-atlas-s3-updater",
    start_date=datetime(2024, 10, 14),
    executable_path=executable_path,
    device_atlas_S3_location=f"s3://ttd-identity/external{test_folder}/deviceatlas/",
    device_atlas_v3_S3_location=f"s3://ttd-identity/external{test_folder}/deviceatlas_v3/",
    cluster_factory=IdentityClusters,
).airflow_dag

final_dag_china: DAG = create_dag(
    dag_id="device-atlas-updater-china",
    start_date=datetime(2025, 4, 7),
    executable_path=executable_path_aliyun,
    device_atlas_S3_location=f"oss://ttd-identity/external{test_folder}/deviceatlas/",
    device_atlas_v3_S3_location=f"oss://ttd-identity/external{test_folder}/deviceatlas_v3/",
    cluster_factory=IdentityClustersAliyun,
    tags=['CHINA'],
    slack_channel=Tags.slack_channel_alarms_testing,
).airflow_dag
