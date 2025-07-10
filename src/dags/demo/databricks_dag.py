from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)
from airflow.utils.dates import days_ago


def format_java_options(java_options: dict):
    return " ".join([f"-D{key}={value}" for key, value in java_options.items()])


DATABRICKS_CONN_ID = "dataproc-databricks"
DATABRICKS_POLICY_ID = "9C63D8C504005A47"
DATABRICKS_SPARK_VERSION = "12.2.x-scala2.12"
NODE_TYPE_ID = "m5d.12xlarge"
CLUSTER_TAGS = {
    "Cloud": "aws",
    "Environment": "dev",
    "Resource": "Databricks",
    "Team": "DATAPROC",
    "Source": "Airflow",
}

JAVA_OPTIONS = {
    "ttd.env": "prodTest",
    "dataTime": '{{ execution_date.strftime("%Y-%m-%dT%H:00:00") }}',
}

SPARK_CLUSTER_DEFAULTS = {
    "spark.speculation": "false",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.executor.extraJavaOptions": "-server -XX:+UseParallelGC",
    "spark.sql.files.ignoreCorruptFiles": "false",
    "spark.driver.extraJavaOptions": format_java_options(JAVA_OPTIONS),
}

DAG_DEFAULT_ARGS = {
    "owner": "DATAPROC",
    "depends_on_past": False,
}

with DAG(
        dag_id="databricks-operator-test",
        start_date=days_ago(2),
        schedule_interval=None,
        tags=["DATAPROC", "Demo"],
        default_args=DAG_DEFAULT_ARGS,
) as dag:
    run_spark_job = DatabricksSubmitRunOperator(
        databricks_conn_id=DATABRICKS_CONN_ID,
        task_id="run_spark_job",
        spark_jar_task={"main_class_name": "com.thetradedesk.spark.jobs.DummyPrintingJob"},
        libraries=[
            {
                "jar":
                "s3://ttd-build-artefacts/eldorado/mergerequests/mvn-DATAPROC-3658-airflow-x-databricks/latest/el-dorado-dataproc-assembly.jar"
            },
        ],
        new_cluster={
            "spark_version": DATABRICKS_SPARK_VERSION,
            "spark_conf": SPARK_CLUSTER_DEFAULTS
            | {
                "spark.hadoop.fs.s3a.assumed.role.arn": "arn:aws:iam::003576902480:role/ttd_cluster_compute_adhoc",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider",
                "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
                "spark.sql.adaptive.enabled": "true",
            },
            "aws_attributes": {
                "first_on_demand": "1",
                "availability": "SPOT_WITH_FALLBACK",
                "zone_id": "auto",
                "instance_profile_arn": "arn:aws:iam::503911722519:instance-profile/ttd_data_science_instance_profile",
                "spot_bid_price_percent": "100",
                "ebs_volume_count": "0",
            },
            "node_type_id": NODE_TYPE_ID,
            "custom_tags": CLUSTER_TAGS,
            "cluster_log_conf": {
                "dbfs": {
                    "destination": "dbfs:/databricks/cluster-logs",
                },
            },
            "enable_elastic_disk": False,
            "policy_id": DATABRICKS_POLICY_ID,
            "data_security_mode": "NONE",
            "runtime_engine": "PHOTON",
            "autoscale": {
                "min_workers": "3",
                "max_workers": "8",
            },
        },
    )

    run_spark_job
