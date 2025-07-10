import json
from datetime import datetime
# from airflow import DAG

from ttd.slack.slack_groups import dataproc, AIFUN
from ttd.task_service.k8s_connection_helper import alicloud, azure
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.task_service_dag import TaskServiceDagRegistry, TaskServiceDagFactory
from ttd.ttdenv import TtdEnvFactory

schedule_interval_non_prod = None
max_active_runs = 1 if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else 5
task_data = json.dumps({
    "origin": "{{ dag_run.conf['origin']|default('scheduled') }}",
    "exec_id": "{{ dag_run.conf['exec_id']|default(macros.datetime.now().timestamp() | int) }}"
})

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name='CanaryTask',
        task_config_name='AwsCanaryTaskConfig',
        task_name_suffix='aws',
        scrum_team=dataproc,
        start_date=datetime(2024, 10, 1),
        job_schedule_interval='1/10 * * * *' if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else schedule_interval_non_prod,
        max_active_runs=max_active_runs,
        dag_tags=['Canary', 'Regression', 'Monitoring'],
        resources=TaskServicePodResources.small(),
        teams_allowed_to_access=[AIFUN.team.jira_team],
        task_data=task_data,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name='CanaryTask',
        task_config_name='AzureCanaryTaskConfig',
        task_name_suffix='azure',
        scrum_team=dataproc,
        start_date=datetime(2024, 10, 1),
        job_schedule_interval='2/10 * * * *' if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else schedule_interval_non_prod,
        max_active_runs=max_active_runs,
        dag_tags=['Canary', 'Regression', 'Monitoring'],
        resources=TaskServicePodResources.small(),
        cloud_provider=azure,
        teams_allowed_to_access=[AIFUN.team.jira_team],
        task_data=task_data,
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name='CanaryTask',
        task_config_name='AliCloudCanaryTaskConfig',
        task_name_suffix='alicloud',
        scrum_team=dataproc,
        start_date=datetime(2024, 10, 1),
        job_schedule_interval='3/10 * * * *' if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else schedule_interval_non_prod,
        max_active_runs=max_active_runs,
        dag_tags=['Canary', 'Regression', 'Monitoring'],
        resources=TaskServicePodResources.small(),
        cloud_provider=alicloud,
        teams_allowed_to_access=[AIFUN.team.jira_team],
        task_data=task_data,
    )
)
