from datetime import datetime, timedelta
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import cmkt

_audienceDataImportTaskExecutionTimeout = timedelta(hours=3)

# This is equivalent to TaskServicePodResources.large(), but with increased ephemeral storage limit
_audienceDataImportTaskResources = TaskServicePodResources.custom(
    request_cpu="2",
    request_memory="4Gi",
    limit_memory="8Gi",
    limit_ephemeral_storage="5Gi",
)

_thirdPartyDataImportScheduleTaskResources = TaskServicePodResources.custom(
    request_cpu="1",
    request_memory="16Gi",
    limit_memory="16Gi",
    limit_ephemeral_storage="1Gi",
)

_landingPageRolloutResources = TaskServicePodResources.custom(
    request_cpu="1",
    request_memory="1Gi",
    limit_memory="16Gi",
    limit_ephemeral_storage="1Gi",
)

_thirdPartyDataImportScheduleTaskExecutionTimeout = timedelta(hours=3)

_thirdPartyDataImportScheduleTaskConfigurationOverrides = {
    "WriteMetricsToDatabase": "false",
}

_thirdPartyDataImportScheduleTaskReducedParallelismConfigurationOverrides = {
    **_thirdPartyDataImportScheduleTaskConfigurationOverrides,
    **{
        "ThirdPartyDataImportScheduleTask.MaxConcurrentWorkItemRequests": "2",
    },
}

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AudienceDataImportTask",
        task_config_name="AudienceDataImportTaskConfig",
        scrum_team=cmkt,
        start_date=datetime(2023, 8, 29),
        job_schedule_interval="0 0/12 * * *",
        resources=_audienceDataImportTaskResources,
        task_name_suffix="TaskGroupId1",
        task_data="""{"TaskGroupId": 1}""",
        task_execution_timeout=_audienceDataImportTaskExecutionTimeout,
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AudienceDataImportTask",
        task_config_name="AudienceDataImportTaskConfig",
        scrum_team=cmkt,
        start_date=datetime(2023, 8, 15),
        job_schedule_interval="0 1/12 * * *",
        resources=_audienceDataImportTaskResources,
        task_name_suffix="TaskGroupId2",
        task_data="""{"TaskGroupId": 2}""",
        task_execution_timeout=_audienceDataImportTaskExecutionTimeout,
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AudienceDataImportTask",
        task_config_name="AudienceDataImportTaskConfig",
        scrum_team=cmkt,
        start_date=datetime(2023, 8, 15),
        job_schedule_interval="0 2/12 * * *",
        resources=_audienceDataImportTaskResources,
        task_name_suffix="TaskGroupId3",
        task_data="""{"TaskGroupId": 3}""",
        task_execution_timeout=_audienceDataImportTaskExecutionTimeout,
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AudienceDataImportTask",
        task_config_name="AudienceDataImportTaskConfig",
        scrum_team=cmkt,
        start_date=datetime(2023, 8, 15),
        job_schedule_interval="0 3/12 * * *",
        resources=_audienceDataImportTaskResources,
        task_name_suffix="TaskGroupId4",
        task_data="""{"TaskGroupId": 4}""",
        task_execution_timeout=_audienceDataImportTaskExecutionTimeout,
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AudienceDataImportTask",
        task_config_name="AudienceDataImportTaskConfig",
        scrum_team=cmkt,
        start_date=datetime(2023, 8, 15),
        job_schedule_interval="0 4/12 * * *",
        resources=_audienceDataImportTaskResources,
        task_name_suffix="TaskGroupId5",
        task_data="""{"TaskGroupId": 5}""",
        task_execution_timeout=_audienceDataImportTaskExecutionTimeout,
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AudienceDataImportTask",
        task_config_name="AudienceDataImportTaskConfig",
        scrum_team=cmkt,
        start_date=datetime(2023, 8, 15),
        job_schedule_interval="0 5/12 * * *",
        resources=_audienceDataImportTaskResources,
        task_name_suffix="TaskGroupId6",
        task_data="""{"TaskGroupId": 6}""",
        task_execution_timeout=_audienceDataImportTaskExecutionTimeout,
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AudienceDataImportTask",
        task_config_name="AudienceDataImportTaskConfig",
        scrum_team=cmkt,
        start_date=datetime(2023, 8, 15),
        job_schedule_interval="0 6/12 * * *",
        resources=_audienceDataImportTaskResources,
        task_name_suffix="TaskGroupId7",
        task_data="""{"TaskGroupId": 7}""",
        task_execution_timeout=_audienceDataImportTaskExecutionTimeout,
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ThirdPartyDataImportScheduleTask",
        task_config_name="ThirdPartyDataImportScheduleTaskConfig",
        scrum_team=cmkt,
        start_date=datetime(2023, 8, 29),
        job_schedule_interval="0 7/12 * * *",
        resources=_thirdPartyDataImportScheduleTaskResources,
        task_name_suffix="TaskGroupId1",
        task_data="""{"TaskGroupId": 1}""",
        task_execution_timeout=_thirdPartyDataImportScheduleTaskExecutionTimeout,
        configuration_overrides=_thirdPartyDataImportScheduleTaskConfigurationOverrides,
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ThirdPartyDataImportScheduleTask",
        task_config_name="ThirdPartyDataImportScheduleTaskConfig",
        scrum_team=cmkt,
        start_date=datetime(2023, 8, 15),
        job_schedule_interval="0 8/12 * * *",
        resources=_thirdPartyDataImportScheduleTaskResources,
        task_name_suffix="TaskGroupId2",
        task_data="""{"TaskGroupId": 2}""",
        task_execution_timeout=_thirdPartyDataImportScheduleTaskExecutionTimeout,
        configuration_overrides=_thirdPartyDataImportScheduleTaskConfigurationOverrides,
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ThirdPartyDataImportScheduleTask",
        task_config_name="ThirdPartyDataImportScheduleTaskConfig",
        scrum_team=cmkt,
        start_date=datetime(2023, 8, 15),
        job_schedule_interval="0 9/12 * * *",
        resources=_thirdPartyDataImportScheduleTaskResources,
        task_name_suffix="TaskGroupId3",
        task_data="""{"TaskGroupId": 3}""",
        task_execution_timeout=_thirdPartyDataImportScheduleTaskExecutionTimeout,
        configuration_overrides=_thirdPartyDataImportScheduleTaskReducedParallelismConfigurationOverrides,
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ThirdPartyDataImportScheduleTask",
        task_config_name="ThirdPartyDataImportScheduleTaskConfig",
        scrum_team=cmkt,
        start_date=datetime(2023, 8, 15),
        job_schedule_interval="0 10/12 * * *",
        resources=_thirdPartyDataImportScheduleTaskResources,
        task_name_suffix="TaskGroupId4",
        task_data="""{"TaskGroupId": 4}""",
        task_execution_timeout=_thirdPartyDataImportScheduleTaskExecutionTimeout,
        configuration_overrides=_thirdPartyDataImportScheduleTaskReducedParallelismConfigurationOverrides,
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ThirdPartyDataImportScheduleTask",
        task_config_name="ThirdPartyDataImportScheduleTaskConfig",
        scrum_team=cmkt,
        start_date=datetime(2023, 8, 15),
        job_schedule_interval="0 11/12 * * *",
        resources=_thirdPartyDataImportScheduleTaskResources,
        task_name_suffix="TaskGroupId5",
        task_data="""{"TaskGroupId": 5}""",
        task_execution_timeout=_thirdPartyDataImportScheduleTaskExecutionTimeout,
        configuration_overrides=_thirdPartyDataImportScheduleTaskReducedParallelismConfigurationOverrides,
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ThirdPartyDataImportScheduleTask",
        task_config_name="ThirdPartyDataImportScheduleTaskConfig",
        scrum_team=cmkt,
        start_date=datetime(2023, 8, 15),
        job_schedule_interval="0 0/12 * * *",
        resources=_thirdPartyDataImportScheduleTaskResources,
        task_name_suffix="TaskGroupId6",
        task_data="""{"TaskGroupId": 6}""",
        task_execution_timeout=_thirdPartyDataImportScheduleTaskExecutionTimeout,
        configuration_overrides=_thirdPartyDataImportScheduleTaskReducedParallelismConfigurationOverrides,
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="ThirdPartyDataImportScheduleTask",
        task_config_name="ThirdPartyDataImportScheduleTaskConfig",
        scrum_team=cmkt,
        start_date=datetime(2023, 8, 15),
        job_schedule_interval="0 1/12 * * *",
        resources=_thirdPartyDataImportScheduleTaskResources,
        task_name_suffix="TaskGroupId7",
        task_data="""{"TaskGroupId": 7}""",
        task_execution_timeout=_thirdPartyDataImportScheduleTaskExecutionTimeout,
        configuration_overrides=_thirdPartyDataImportScheduleTaskConfigurationOverrides,
        enable_slack_alert=False,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="TheMediaTrustLandingPageDetectionPolicyRolloutTask",
        task_config_name="TheMediaTrustLandingPageDetectionPolicyRolloutConfig",
        scrum_team=cmkt,
        start_date=datetime(2025, 1, 22),
        job_schedule_interval="30 */12 * * *",
        alert_channel="#scrum-cmkt-alarms",
        resources=_landingPageRolloutResources,
        telnet_commands=[
            "try changeField TheMediaTrustLandingPageDetectionPolicyRolloutTask.ExecutionLimitNewAdvertisersToBeRolledOutWithLandingPage 17000"
        ]
    )
)
