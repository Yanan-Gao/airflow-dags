"""
### Maxmind S3 Uploader DAG

This DAG controls the weekly job that writes the maxmind data to S3.
"""
from typing import Any
from airflow import DAG
from datetime import datetime
from ttd.el_dorado.v2.base import TtdDag
from dags.idnt.identity_clusters_aliyun import IdentityClustersAliyun
from dags.idnt.identity_helpers import DagHelpers
from dags.idnt.identity_clusters import IdentityClusters, ComputeType
from dags.idnt.statics import Executables, Tags
from ttd.ttdenv import TtdEnvFactory

ttd_env = Tags.environment()
test_folder = "" if str(ttd_env) == "prod" else "/test"

executable_path = Executables.etl_repo_executable
executable_path_aliyun = Executables.etl_repo_executable_china

schedule_interval = "34 17 * * THU" if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else "*/30 * * * *"


def create_dag(
    dag_id: str,
    start_date: datetime,
    executable_path: str,
    hardcoded_timezones_path: str,
    deprecated_timezones_path: str,
    maxmind_path: str,
    cluster_factory: Any,
    **kwargs,
) -> TtdDag:
    # Runs every Thursday at 5:34 PM UTC
    dag = DagHelpers.identity_dag(
        dag_id=dag_id, schedule_interval=schedule_interval, start_date=start_date, run_only_latest=True, doc_md=__doc__, **kwargs
    )

    # we need exactly one instance for this
    cluster = cluster_factory.get_cluster("maxmind_s3_uploader_cluster", dag, 32, ComputeType.STORAGE, use_delta=False, cpu_bounds=(32, 32))

    download_url = "https://download.maxmind.com/app/geoip_download?edition_id=GeoIP2"

    download_task = cluster_factory.task(
        "com.thetradedesk.etl.misc.MaxmindS3Uploader",
        eldorado_configs=[("MAXMIND_GEOIP2_CONNECTION_DOWNLOAD_LOCATION", f"{download_url}-Connection-Type-CSV&suffix=zip&license_key="),
                          ("MAXMIND_GEOIP2_ENTERPRISE_DOWNLOAD_LOCATION", f"{download_url}-Enterprise-CSV&suffix=zip&license_key="),
                          ("MAXMIND_GEOIP2_ISP_DOWNLOAD_LOCATION", f"{download_url}-ISP-CSV&suffix=zip&license_key="),
                          ("MAXMIND_GEOIP2_CITY_DOWNLOAD_LOCATION", f"{download_url}-City-CSV&suffix=zip&license_key="),
                          ("IANA_TIMEZONE_DB_DOWNLOAD_LOCATION", "https://timezonedb.com/files/timezonedb.csv.zip"),
                          ("MAXMIND_LICENSE_KEY", "BRKLHs98GL2v"), ("HARDCODED_TIMEZONES_PATH", hardcoded_timezones_path),
                          ("DEPRECATED_TIMEZONES_PATH", deprecated_timezones_path), ("MAXMIND_S3_PATH", maxmind_path)],
        executable_path=executable_path,
        timeout_hours=2
    )

    cluster.add_parallel_body_task(download_task)

    dag >> cluster
    return dag


final_dag: DAG = create_dag(
    "maxmind-s3-uploader",
    datetime(2024, 10, 10),
    executable_path,
    "s3://ttd-identity/data-governance/timezones/hardcoded.csv",
    "s3://ttd-identity/data-governance/timezones/aliased_and_deprecated_timezones.csv",
    f"s3://ttd-identity/external{test_folder}/maxmind/",
    IdentityClusters,
).airflow_dag

final_dag_china: DAG = create_dag(
    "maxmind-uploader-china",
    datetime(2025, 4, 10),
    executable_path_aliyun,
    "oss://ttd-identity/data-governance/timezones/hardcoded.csv",
    "oss://ttd-identity/data-governance/timezones/aliased_and_deprecated_timezones.csv",
    f"oss://ttd-identity/external{test_folder}/maxmind/",
    IdentityClustersAliyun,
    tags=['CHINA'],
    slack_channel=Tags.slack_channel_alarms_testing,
).airflow_dag
