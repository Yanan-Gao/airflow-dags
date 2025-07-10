# from airflow import DAG
from datetime import datetime, timedelta
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import sav


def prefix_config_keys(config, prefix):
    return {f"{prefix}.{key}": value for key, value in config.items()}


registry = TaskServiceDagRegistry(globals())

congressional_geofence_update_task_config_overrides = {
    "UpdateAll": "true",
}

registry.register_dag(
    TaskServiceDagFactory(
        task_name="GeoFenceUpdateTask",
        task_name_suffix="congressional-district",
        task_config_name="CongressionalDistrictGeoFenceUpdateTaskConfig",
        scrum_team=sav,
        start_date=datetime(2024, 7, 2),
        task_execution_timeout=timedelta(hours=10),
        job_schedule_interval="0 0 1 */3 *",  # Run every three months
        resources=TaskServicePodResources
        .custom(request_cpu="8", request_memory="32Gi", limit_memory="64Gi", limit_ephemeral_storage="1Gi"),
        configuration_overrides=
        prefix_config_keys(congressional_geofence_update_task_config_overrides, "CongressionalDistrictGeoFenceUpdateTask")
    )
)

legislative_geofence_update_task_config_overrides = {
    "UpdateAll": "false",
    "StatesToUpdate": "{{ dag_run.conf.get('StatesToUpdate') }}",
    "DistrictsToUpdate": "{{ dag_run.conf.get('DistrictsToUpdate') }}",
    "ForceUpdateSegment": "{{ dag_run.conf.get('ForceUpdateSegment') }}",
    "UsePolygon": "{{ dag_run.conf.get('UsePolygon') }}",
    "AllowSegmentCountOrNameChange": "{{ dag_run.conf.get('AllowSegmentCountOrNameChange') }}",
    "ExpandCirclesToBoundary": "{{ dag_run.conf.get('ExpandCirclesToBoundary') }}",
}

registry.register_dag(
    TaskServiceDagFactory(
        task_name="GeoFenceUpdateTask",
        task_name_suffix="state-legislative-district-upper",
        task_config_name="StateLegislativeDistrictUpperGeoFenceUpdateTaskConfig",
        scrum_team=sav,
        start_date=datetime(2024, 7, 2),
        task_execution_timeout=timedelta(hours=10),
        job_schedule_interval="0 0 10 */3 *",  # Run every three months
        resources=TaskServicePodResources
        .custom(request_cpu="8", request_memory="32Gi", limit_memory="64Gi", limit_ephemeral_storage="1Gi"),
        configuration_overrides=
        prefix_config_keys(legislative_geofence_update_task_config_overrides, "StateLegislativeDistrictUpperGeoFenceUpdateTask")
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="GeoFenceUpdateTask",
        task_name_suffix="state-legislative-district-lower",
        task_config_name="StateLegislativeDistrictLowerGeoFenceUpdateTaskConfig",
        scrum_team=sav,
        start_date=datetime(2024, 7, 2),
        task_execution_timeout=timedelta(hours=10),
        job_schedule_interval="0 0 19 */3 *",  # Run every three months
        resources=TaskServicePodResources
        .custom(request_cpu="8", request_memory="32Gi", limit_memory="64Gi", limit_ephemeral_storage="1Gi"),
        configuration_overrides=
        prefix_config_keys(legislative_geofence_update_task_config_overrides, "StateLegislativeDistrictLowerGeoFenceUpdateTask")
    )
)
