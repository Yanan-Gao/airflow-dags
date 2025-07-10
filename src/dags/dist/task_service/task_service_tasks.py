from datetime import datetime, timedelta
# Required comment to add them to the list of dags
# from airflow import DAG

from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.slack.slack_groups import dist
from ttd.task_service.k8s_pod_resources import TaskServicePodResources

job_slack_channel = '#taskforce-budget-alarms-high-pri'
retries = 3
retry_delay = timedelta(minutes=2)

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="BudgetSpendPotentialCurveTask",
        task_config_name="BudgetSpendPotentialCurveConfig",
        scrum_team=dist,
        job_schedule_interval="0 2 * * *",  # Runs daily at 2:00 UTC time.
        start_date=datetime(2023, 9, 6, 10, 10),
        resources=TaskServicePodResources.custom(
            request_cpu="8", request_memory="64Gi", request_ephemeral_storage="15Gi", limit_memory="128Gi", limit_ephemeral_storage="30Gi"
        ),
        retries=retries,
        retry_delay=retry_delay,
        run_only_latest=True,
        alert_channel=job_slack_channel,
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="BudgetVarianceTask",
        task_config_name="BudgetVarianceTaskConfigAws",
        scrum_team=dist,
        task_name_suffix="VerticaAWS",
        start_date=datetime(2023, 9, 20, 21, 0),
        job_schedule_interval=timedelta(minutes=30),
        resources=TaskServicePodResources.custom(
            request_cpu="4", request_memory="16Gi", request_ephemeral_storage="15Gi", limit_memory="32Gi", limit_ephemeral_storage="30Gi"
        ),
        retries=retries,
        retry_delay=retry_delay,
        run_only_latest=True,
        alert_channel=job_slack_channel,
        telnet_commands=[
            "change BudgetVarianceQueryBuilder.EnableNewVarianceQueryBuilder true",
            "change BudgetVarianceTask.CheckChinaImportTask true",
            "change BudgetVarianceTask.UseMultiDomainVarianceBuilder true",
            "invokeMethod BudgetVarianceTask.AddPartnerToChinaPartnerVarianceOptOutList p7k8847",
            "change BudgetVarianceTask.EnableLockedSpendDateOnReportingDate true",
            "invokeMethod BudgetVarianceTask.AddPartnerToChinaPartnerVarianceOptOutList 3ayihi6",
        ],
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="BudgetVarianceTask",
        task_config_name="BudgetVarianceTaskConfigAzure",
        scrum_team=dist,
        task_name_suffix="VerticaAzure",
        start_date=datetime(2023, 9, 20, 21, 0),
        job_schedule_interval=timedelta(minutes=30),
        resources=TaskServicePodResources.custom(
            request_cpu="4", request_memory="16Gi", request_ephemeral_storage="15Gi", limit_memory="32Gi", limit_ephemeral_storage="30Gi"
        ),
        retries=retries,
        retry_delay=retry_delay,
        run_only_latest=True,
        alert_channel=job_slack_channel,
        telnet_commands=[
            "change BudgetVarianceQueryBuilder.EnableNewVarianceQueryBuilder true",
            "change BudgetVarianceTask.UseMultiDomainVarianceBuilder true",
            "invokeMethod BudgetVarianceTask.AddPartnerToChinaPartnerVarianceOptOutList p7k8847",
            "change BudgetVarianceTask.EnableLockedSpendDateOnReportingDate true",
            "invokeMethod BudgetVarianceTask.AddPartnerToChinaPartnerVarianceOptOutList 3ayihi6",
        ],
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="CurrencyExchangeRateImportTask",
        scrum_team=dist,
        task_config_name="CurrencyExchangeRateImportTaskConfig",
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="*/10 * * * *",
        resources=TaskServicePodResources.small(),
        alert_channel='#tf-rates-and-fees-alerts'
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="WinrateExclusionTask",
        task_config_name="WinrateExclusionTaskConfig",
        scrum_team=dist,
        job_schedule_interval=timedelta(minutes=20),
        start_date=datetime.now() - timedelta(hours=3),
        resources=TaskServicePodResources.medium(),
        alert_channel='#scrum-dist-alarms',
        enable_slack_alert=False,  # Bidding.WinrateExclusionTask alarm is used instead
        retries=1,
        retry_delay=timedelta(minutes=2),
        telnet_commands=[
            "changeField ExcludeBiddingHandler.GetDimensionsQueryTimeoutInSeconds 5400",  # 90 minute timeout for long running query that happens once every 6 hours
            "invoke WinrateExclusionTask.DisabledAdGroupIds.Add vwcrze9",  # SWAT-17912 - LiveEvents that start with unwinnable inventory
            "invoke WinrateExclusionTask.DisabledAdGroupIds.Add wn6mdb0",  # SWAT-17912 - LiveEvents that start with unwinnable inventory
            "invoke WinrateExclusionTask.DisabledAdGroupIds.Add zydhyr3",  # SWAT-17912 - LiveEvents that start with unwinnable inventory
            "invoke WinrateExclusionTask.DisabledAdGroupIds.Add fw9nctq",  # SWAT-17912 - LiveEvents that start with unwinnable inventory
            "invoke WinrateExclusionTask.DisabledAdGroupIds.Add qxle75u",  # SWAT-17912 - LiveEvents that start with unwinnable inventory
            "invoke WinrateExclusionTask.DisabledAdGroupIds.Add am5aqxb",  # SWAT-17912 - LiveEvents that start with unwinnable inventory
            "invoke WinrateExclusionTask.DisabledAdGroupIds.Add pil4akx",  # SWAT-17912 - LiveEvents that start with unwinnable inventory
            "invoke WinrateExclusionTask.DisabledAdGroupIds.Add 5xkceud",  # SWAT-17912 - LiveEvents that start with unwinnable inventory
            "invoke WinrateExclusionTask.DisabledAdGroupIds.Add lhm96om",  # SWAT-17912 - LiveEvents that start with unwinnable inventory
            "invoke WinrateExclusionTask.DisabledAdGroupIds.Add 95x23c3",  # SWAT-17912 - LiveEvents that start with unwinnable inventory
            "invoke WinrateExclusionTask.DisabledAdGroupIds.Add ekzas2n",  # SWAT-17912 - LiveEvents that start with unwinnable inventory
            "invoke WinrateExclusionTask.DisabledAdGroupIds.Add g0l70eh",  # SWAT-17912 - LiveEvents that start with unwinnable inventory
            "invoke WinrateExclusionTask.DisabledAdGroupIds.Add rc2npr2",  # SWAT-17912 - LiveEvents that start with unwinnable inventory
            "invoke WinrateExclusionTask.DisabledAdGroupIds.Add 6e60j80",  # SWAT-17912 - LiveEvents that start with unwinnable inventory
            "invoke WinrateExclusionTask.DisabledAdGroupIds.Add hmk53xk",  # SWAT-17912 - LiveEvents that start with unwinnable inventory
        ],
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="PublisherExpirationEstimationsUpdateTask",
        task_config_name="PublisherExpirationEstimationsUpdateTaskConfig",
        scrum_team=dist,
        job_schedule_interval=timedelta(hours=48),
        start_date=datetime.now() - timedelta(hours=49),
        resources=TaskServicePodResources.medium(),
        alert_channel='#taskforce-budget-alarms-high-pri',
        run_only_latest=True,
        retries=retries,
        retry_delay=retry_delay,
        configuration_overrides={"PublisherExpirationEstimationsUpdateTask.TtdSchema": "ttd"}
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="BudgetSplitExperimentChangeDetectingTask",
        task_config_name="BudgetSplitExperimentChangeDetectingTaskConfig",
        scrum_team=dist,
        job_schedule_interval=timedelta(minutes=5),
        start_date=datetime.now() - timedelta(hours=1),
        resources=TaskServicePodResources.medium(),
        alert_channel='#taskforce-budget-split-alarms',
        run_only_latest=True,
        retries=retries,
        retry_delay=retry_delay
    )
)
