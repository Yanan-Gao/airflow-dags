from typing import List, Tuple

cmkt_spark_aws_executable_path = "{{ dag_run.conf.get('aws_executable_path', 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/eldorado-cmkt-assembly.jar') }}"

emr_additional_args_option_pairs_list = [
    ("conf", "spark.driver.maxResultSize=2g"),
    ("executor-cores", "4"),
]


def get_common_eldorado_config() -> List[Tuple[str, str]]:
    """
    source: src/dags/dataproc/monitoring/spark_canary_job.py
    """
    return [
        ("datetime", "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}"),
        ("exec_id", "{{ macros.datetime.now().timestamp() | int }}"),
        ("origin", "scheduled"),
        ("logLevel", "DEBUG"),
        #   TODO: (CMKT-6628)
        # ("ttd.azure.enable", "true" if cloud_provider == CloudProviders.azure else "false")
    ]
