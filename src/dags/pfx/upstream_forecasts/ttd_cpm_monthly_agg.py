from datetime import timedelta
from datetime import datetime

from airflow.operators.python import PythonOperator

from dags.pfx.upstream_forecasts.ctv_cpm_adjustments import (
    create_cpm_cluster,
    create_cpm_step,
    xcom_template_to_get_value,
)
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.general_purpose.m5 import M5
from ttd.ec2.emr_instance_types.memory_optimized.r5d import R5d
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import PFX
from ttd.tasks.op import OpTask

job_schedule_interval = "0 5 3 * *"  # run monthly 5am on 3rd day of each month
job_start_date = datetime(2024, 7, 1)

# 4 days prior today as the last day of monthly period, to make sure daily aggregate data is ready
delta_days_from_now = 4
# days looking back for monthly aggregation
days_span = 28

class_name = "CpmAdjustmentsMultiDayRollupJob"
dag_id = "ttd-cpm-monthly-aggregate"
gen_dates_op_id = "generate_dates_to_xcom"
start_date_key = "start_date"
end_date_key = "end_date"
emr_version = AwsEmrVersions.AWS_EMR_SPARK_3_2
num_xl_machines = 2

monthly_ttd_dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=PFX.team.alarm_channel,
    tags=["ctv", "cpm"],
    max_active_runs=2,
)

monthly_cpm_airflow_dag = monthly_ttd_dag.airflow_dag

cpm_monthly_cluster = create_cpm_cluster(
    emr_version=emr_version,
    name=class_name,
    num_xls=num_xl_machines,
    master_config=[M5.m5_2xlarge().with_fleet_weighted_capacity(1)],
    instance_config=[
        R5d.r5d_4xlarge().with_fleet_weighted_capacity(2),
        R5d.r5d_8xlarge().with_fleet_weighted_capacity(4),
    ],
)

cpm_monthly_step = create_cpm_step(
    cluster=cpm_monthly_cluster,
    class_name=class_name,
    emr_step_name="monthly_aggregate",
    timeout_timedelta=timedelta(minutes=30),
    job_params=[
        (
            "startDate",
            xcom_template_to_get_value(dag_id, gen_dates_op_id, start_date_key),
        ),
        ("endDate", xcom_template_to_get_value(dag_id, gen_dates_op_id, end_date_key)),
    ],
)

cpm_monthly_cluster.add_sequential_body_task(cpm_monthly_step)


def gen_dates(**context):
    end_date = (context['logical_date'] - timedelta(days=delta_days_from_now)).strftime("%Y%m%d")
    start_date = (context['logical_date'] - timedelta(days=delta_days_from_now + days_span - 1)).strftime("%Y%m%d")
    task_instance = context["task_instance"]
    task_instance.xcom_push(key=start_date_key, value=start_date)
    task_instance.xcom_push(key=end_date_key, value=end_date)


monthly_gen_dates_step = PythonOperator(
    task_id=gen_dates_op_id, python_callable=gen_dates, dag=monthly_cpm_airflow_dag, provide_context=True
)

monthly_gen_dates_task = OpTask(task_id='monthly_cpm_gen_dates_task_id', op=monthly_gen_dates_step)

monthly_ttd_dag >> monthly_gen_dates_task >> cpm_monthly_cluster
