from datetime import timedelta, datetime
# from airflow import DAG

from ttd.slack.slack_groups import dprpts
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry

alert_channel = "#dev-agiles-alerts"
start_date = datetime.now() - timedelta(hours=3)

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AgileLateDataConsistencyCheckTask",
        scrum_team=dprpts,
        start_date=start_date,
        job_schedule_interval="0 * * * *",
        alert_channel=alert_channel,
        resources=TaskServicePodResources.small(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AgileConsistencyCheckTask",
        task_config_name="DailyAdvertiserCostInUSDCheckConfig",
        task_name_suffix="daily_advertiser_cost",
        scrum_team=dprpts,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        alert_channel=alert_channel,
        resources=TaskServicePodResources.small(),
        telnet_commands=[
            "try changeField AdvertiserCostInUSDComparisonRow.DiscrepancyAllowedThresholdForLateData 0.05",
            "try changeField AdvertiserCostInUSDComparisonRow.WalmartLateDataComparisonGracePeriodInDays 4",
        ],
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AgileConsistencyCheckTask",
        task_config_name="MonthlyAdvertiserCostInUSDCheckConfig",
        task_name_suffix="monthly_advertiser_cost",
        scrum_team=dprpts,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        alert_channel=alert_channel,
        resources=TaskServicePodResources.small(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AgileConsistencyCheckTask",
        task_config_name="AgileAdGroupPotentialSpendDailyCheckConfig",
        task_name_suffix="potential_spend",
        scrum_team=dprpts,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        alert_channel=alert_channel,
        resources=TaskServicePodResources.small(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="VerticaConsistencyCheckTaskOfficialVerticaDriver",
        task_config_name="VerticaAdGroupPotentialSpendDailyCheckConfig",
        task_name_suffix="potential_spend",
        scrum_team=dprpts,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        alert_channel=alert_channel,
        resources=TaskServicePodResources.small(),
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="VerticaConsistencyCheckTaskOfficialVerticaDriver",
        task_config_name="RtbAgilesHourlyCheckConfig",
        task_name_suffix="rtb_agiles_hourly",
        scrum_team=dprpts,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/15 * * * *",
        alert_channel=alert_channel,
        resources=TaskServicePodResources.small(),
    )
)
