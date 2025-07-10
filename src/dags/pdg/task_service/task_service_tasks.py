from datetime import datetime, timedelta
from collections import namedtuple
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import pdg

registry = TaskServiceDagRegistry(globals())

Config = namedtuple("Config", "task_data, suffix")
referrer_url_configuration = [
    Config("""{"logType":"ConversionTracker", "extractionType":"Collected"}""", "ConversionTracker.Collected"),
    Config("""{"logType":"ConversionTracker", "extractionType":"Cleansed"}""", "ConversionTracker.Cleansed"),
    Config("""{"logType":"ConversionTracker", "extractionType":"VerticaLoad"}""", "ConversionTracker.VerticaLoad"),
    Config("""{"logType":"EventTracker", "extractionType":"Collected"}""", "EventTracker.Collected"),
    Config("""{"logType":"EventTracker", "extractionType":"Cleansed"}""", "EventTracker.Cleansed"),
    Config("""{"logType":"EventTracker", "extractionType":"VerticaLoad"}""", "EventTracker.VerticaLoad"),
    Config("""{"logType":"AttributedEvent", "extractionType":"Collected"}""", "AttributedEvent.Collected"),
    Config("""{"logType":"AttributedEvent", "extractionType":"VerticaLoad"}""", "AttributedEvent.VerticaLoad"),
    Config("""{"logType":"BidFeedback", "extractionType":"Collected"}""", "BidFeedback.Collected"),
    Config("""{"logType":"BidFeedback", "extractionType":"Cleansed"}""", "BidFeedback.Cleansed"),
    Config("""{"logType":"BidFeedback", "extractionType":"VerticaLoad"}""", "BidFeedback.VerticaLoad"),
    Config("""{"logType":"ImpressionTracker", "extractionType":"Collected"}""", "ImpressionTracker.Collected"),
    Config("""{"logType":"ImpressionTracker", "extractionType":"Cleansed"}""", "ImpressionTracker.Cleansed"),
    Config("""{"logType":"ImpressionTracker", "extractionType":"VerticaLoad"}""", "ImpressionTracker.VerticaLoad"),
    Config("""{"logType":"ClickTracker", "extractionType":"Collected"}""", "ClickTracker.Collected"),
    Config("""{"logType":"ClickTracker", "extractionType":"Cleansed"}""", "ClickTracker.Cleansed"),
    Config("""{"logType":"ClickTracker", "extractionType":"VerticaLoad"}""", "ClickTracker.VerticaLoad"),
    Config("""{"logType":"VideoEvent", "extractionType":"Collected"}""", "VideoEvent.Collected"),
    Config("""{"logType":"VideoEvent", "extractionType":"Cleansed"}""", "VideoEvent.Cleansed"),
    Config("""{"logType":"VideoEvent", "extractionType":"VerticaLoad"}""", "VideoEvent.VerticaLoad")
]

for config in referrer_url_configuration:
    registry.register_dag(
        TaskServiceDagFactory(
            task_name="ReferrerUrlPIIScrubTask",
            task_config_name="ReferrerUrlPIIScrubTaskConfig",
            task_name_suffix=config.suffix,
            scrum_team=pdg,
            start_date=datetime(2023, 11, 13),
            job_schedule_interval="0 0 * * *",
            resources=TaskServicePodResources.medium(),
            configuration_overrides={"ReferrerUrlPIIScrubTask.Enabled": "true"},
            task_data=config.task_data
        )
    )

registry.register_dag(
    TaskServiceDagFactory(
        task_name="TrustArcImportTask",
        task_config_name="TrustArcConfig",
        scrum_team=pdg,
        start_date=datetime(2023, 7, 31),
        job_schedule_interval="0 0 * * *",
        resources=TaskServicePodResources.medium(),
        telnet_commands=["try changeField TrustArcImportTask.EnableImport true"],
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="IABGlobalVendorListUpdaterTask",
        task_config_name="IABGlobalVendorListUpdaterTaskConfig",
        scrum_team=pdg,
        start_date=datetime(2024, 6, 24),
        job_schedule_interval=timedelta(days=1),
        resources=TaskServicePodResources.small(),
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="MicroTargetingStateEvaluationTask",
        task_config_name="MicroTargetingStateEvaluationTaskConfig",
        scrum_team=pdg,
        start_date=datetime(2023, 12, 12),
        job_schedule_interval="0 13 * * *",  # 1 hour before MicroTargetingNotificationTask is scheduled
        resources=TaskServicePodResources.medium(),
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="MicroTargetingNotificationTask",
        task_config_name="MicroTargetingNotificationTaskConfig",
        scrum_team=pdg,
        start_date=datetime(2023, 12, 12),
        job_schedule_interval="0 14 * * *",
        resources=TaskServicePodResources.medium(),
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="PadfaPopulationTask",
        task_config_name="PadfaPopulationTaskConfig",
        scrum_team=pdg,
        start_date=datetime(2024, 6, 26),
        # execute every day at 11:00 PM
        job_schedule_interval="0 23 * * *",
        resources=TaskServicePodResources.small(),
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="PersistAdPolicyViolatingAdGroupsTask",
        task_config_name="PersistAdPolicyViolatingAdGroupsTaskConfig",
        scrum_team=pdg,
        start_date=datetime(2024, 6, 19),
        job_schedule_interval="29 1 * * *",  # Every day at 1:29 am
        resources=TaskServicePodResources.small(),
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="UpdateHecApplicableAdvertisersTask",
        task_config_name="UpdateHecApplicableAdvertisersTaskConfig",
        scrum_team=pdg,
        start_date=datetime(2024, 11, 19),
        job_schedule_interval="55 9 * * *",  # Run daily at 9:55 UTC, right before the HEC pipeline at 10 UTC
        resources=TaskServicePodResources.small(),
    )
)
