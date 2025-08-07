from datetime import timedelta, datetime, time
from ttd.el_dorado.v2.base import TtdDag
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from dags.dprpts.metrics.update_exec_steps import execute_step

alarm_slack_channel = '#scrum-dp-rpts-alerts'
dpsr_metrics_updater = 'dpsr-metrics-updater'
daily_hour = 5

default_args = {
    'owner': 'DPRPTS',
}

dag = TtdDag(
    dag_id=dpsr_metrics_updater,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=20),
    slack_channel=alarm_slack_channel,
    schedule_interval='35 * * * *',  # Try to schedule in least busy time
    max_active_runs=1,
    run_only_latest=False,  # transforms into Airflow.DAG.catchup=True
    depends_on_past=True,
    start_date=datetime(2025, 6, 2, 10, 30, 0),  # The value for prod test
    tags=['DPRPTS', 'DPSRInfra', 'MetricsUpdater'],
)

# DAG Task Flow

is_prod = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False


def create_step_task(step_name, zero_hours=False):

    def named_step(ti):
        dag_run = ti.get_dagrun()
        dag_start = dag_run.data_interval_start
        dag_end = dag_run.data_interval_end
        if zero_hours:
            # If this is a daily node
            # NOTE: the interval start is using the END hour to be in the
            # same date as END hour before subtracting a day
            interval_start = datetime.combine(dag_end, time.min) - timedelta(days=1)
            interval_end = datetime.combine(dag_end, time.min)
        else:
            # clamp the date interval to the start of the hour
            # this will allow us to move real execution to a less busy time
            interval_start = datetime(dag_start.year, dag_start.month, dag_start.day, dag_start.hour)
            interval_end = datetime(dag_end.year, dag_end.month, dag_end.day, dag_end.hour)
        execute_step(step_name, interval_start, interval_end, is_prod)

    if zero_hours:
        # we use `none_failed` to allow previous instances of daily tasks
        # to be skipped for 23 hours out of 24
        return OpTask(op=PythonOperator(task_id=f"execute_{step_name}", python_callable=named_step, trigger_rule="none_failed"))
    else:
        return OpTask(op=PythonOperator(task_id=f"execute_{step_name}", python_callable=named_step))


def test_daily_hour(ti):
    dag_run = ti.get_dagrun()
    dag_end = dag_run.data_interval_end
    return dag_end.hour == daily_hour


skip_daily_tasks = OpTask(op=ShortCircuitOperator(task_id="skip_daily_tasks", python_callable=test_daily_hour))
dag_health_check = OpTask(op=EmptyOperator(task_id='dag_health_check'))

adag = dag.airflow_dag

exec_history = create_step_task('ExecHistory')
exec_traits = create_step_task('ExecTraits')
schedule_attrs = create_step_task('ScheduleAttrs')
exposure_feed_attrs = create_step_task('ExposureFeedAttrs')
schedule_partners = create_step_task('SchedulePartners')
exec_rsptg = create_step_task('ExecRSPTG')
exec_depclasses = create_step_task('ExecDepClasses')
update_sla_depclasses = create_step_task('UpdateSLADepClasses')
exec_resolution = create_step_task('ExecResolution')
update_exec_done_stats = create_step_task('UpdateExecStatsDone')
update_exec_cancel_stats = create_step_task('UpdateExecStatsCancel')
update_exec_fail_stats = create_step_task('UpdateExecStatsFail')
exec_stats_merge = create_step_task('MergeExecStats')
exec_errors = create_step_task('ExecErrors')
# nodes below are only doing real work once per day
spend_stats = create_step_task('SpendStats', zero_hours=True)
ptg_names = create_step_task('PTGNames', zero_hours=True)
partner_names = create_step_task('PartnerNames', zero_hours=True)
update_sla_violations_classes = create_step_task('UpdateSLAViolationsClasses', zero_hours=True)
do_constraints_check = create_step_task('ConstraintChecks', zero_hours=True)

dag >> exec_history
dag >> exec_traits
dag >> schedule_attrs
dag >> exposure_feed_attrs
dag >> schedule_partners
dag >> spend_stats
dag >> partner_names
dag >> exec_rsptg
dag >> exec_depclasses
exec_depclasses >> update_sla_depclasses
dag >> exec_resolution
dag >> ptg_names
dag >> exec_errors

# Fill expanded denormalised execution stats
exec_history >> update_exec_done_stats
exec_traits >> update_exec_done_stats
exec_depclasses >> update_exec_done_stats
schedule_attrs >> update_exec_done_stats
update_sla_depclasses >> update_exec_done_stats
update_exec_done_stats >> update_exec_cancel_stats
update_exec_cancel_stats >> update_exec_fail_stats
update_exec_fail_stats >> exec_stats_merge

# Explicit check for upstream nodes, needed to surface exception to the channel
exec_stats_merge >> dag_health_check
schedule_attrs >> dag_health_check
schedule_partners >> dag_health_check

# Now we build SLA classes for partners on top of denormalized exec stats
exec_stats_merge >> update_sla_violations_classes
schedule_attrs >> update_sla_violations_classes
schedule_partners >> update_sla_violations_classes
partner_names >> update_sla_violations_classes

# Check constarints after tables are loaded
spend_stats >> do_constraints_check
exec_rsptg >> do_constraints_check
update_sla_violations_classes >> do_constraints_check
exec_resolution >> do_constraints_check
exec_stats_merge >> do_constraints_check
ptg_names >> do_constraints_check
exec_errors >> do_constraints_check

# Skip daily tasks for the most of the day
dag >> skip_daily_tasks
skip_daily_tasks >> spend_stats
skip_daily_tasks >> ptg_names
skip_daily_tasks >> partner_names
skip_daily_tasks >> update_sla_violations_classes
# NOTE: do_constraint_check will also be performed once per day
# since it depends on skipable tasks and skip task is configured
# to skip all downstream nodes (look up ignore_downstream_trigger_rules
