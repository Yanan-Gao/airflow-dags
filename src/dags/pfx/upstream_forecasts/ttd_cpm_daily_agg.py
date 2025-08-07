from datetime import datetime, timedelta
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

job_schedule_interval = "0 5 * * *"
job_start_date = datetime(2024, 10, 18)

# calculate daily aggregate a number of days prior today, to make sure the bid feedback data is ready
delta_days_from_now = 2

class_name = "CpmAdjustmentsDailyRollupJob"
dag_id = "ttd-cpm-daily-agg"
gen_dates_op_id = "generate_dates_to_xcom"
start_date_key = "start_date"
end_date_key = "end_date"
emr_version = AwsEmrVersions.AWS_EMR_SPARK_3_2
num_xl_machines = 20

ttd_dag = TtdDag(
    dag_id=dag_id,
    start_date=job_start_date,
    end_date=None,
    schedule_interval=timedelta(days=1),
    slack_channel=PFX.team.alarm_channel,
    tags=["ctv", "cpm"],
    max_active_runs=2,
)

airflow_dag = ttd_dag.airflow_dag

cpm_daily_cluster = create_cpm_cluster(
    emr_version=emr_version,
    name=class_name,
    num_xls=num_xl_machines,
    master_config=[M5.m5_2xlarge().with_fleet_weighted_capacity(1)],
    instance_config=[
        R5d.r5d_2xlarge().with_fleet_weighted_capacity(1),
        R5d.r5d_4xlarge().with_fleet_weighted_capacity(2),
        R5d.r5d_8xlarge().with_fleet_weighted_capacity(4),
    ],
)

cpm_daily_step = create_cpm_step(
    cluster=cpm_daily_cluster,
    class_name=class_name,
    emr_step_name="daily_aggregate",
    timeout_timedelta=timedelta(minutes=60),
    job_params=[
        (
            "startDate",
            xcom_template_to_get_value(dag_id, gen_dates_op_id, start_date_key),
        ),
        ("endDate", xcom_template_to_get_value(dag_id, gen_dates_op_id, end_date_key)),
    ],
)

cpm_daily_cluster.add_sequential_body_task(cpm_daily_step)


def gen_dates(**context):
    dt = context['logical_date'] - timedelta(days=delta_days_from_now)
    start_date = dt.strftime("%Y%m%d")
    end_date = dt.strftime("%Y%m%d")
    task_instance = context["task_instance"]
    task_instance.xcom_push(key=start_date_key, value=start_date)
    task_instance.xcom_push(key=end_date_key, value=end_date)


gen_dates_step = PythonOperator(task_id=gen_dates_op_id, python_callable=gen_dates, dag=airflow_dag, provide_context=True)

gen_dates_task = OpTask(task_id='daily_cpm_gen_dates_task_id', op=gen_dates_step)

ttd_dag >> gen_dates_task >> cpm_daily_cluster
