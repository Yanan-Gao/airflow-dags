from datetime import datetime, timedelta
from ttd.slack.slack_groups import dprpts
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.task_service.k8s_connection_helper import aws

scrum_team = dprpts
alert_channel = "#dev-agiles-alerts"
start_date = datetime.now() - timedelta(hours=1)

default_dag_args = {
    "slack_channel": alert_channel,
    "tags": ["TaskService", scrum_team.jira_team, "PipeBuilder"],
    "start_date": start_date,
    "depends_on_past": False,
    "run_only_latest": False,
    "default_args": {
        "start_date": start_date,
        "email_on_failure": False,
        "email_on_retry": False,
        "owner": scrum_team.jira_team
    },
}

default_task_args = {
    "scrum_team": scrum_team,
    "k8s_sovereign_connection_helper": aws,
    "resources": TaskServicePodResources().small(),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # set branch_name here to have it for all Pipe Builder driven DAGs
}
