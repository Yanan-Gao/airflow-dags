from abc import ABC
from airflow.providers.cncf.kubernetes.secret import Secret
from datetime import datetime

DAG_PREFIX = "team-reports"
JOB_PREFIX = "report-runner"


class Cadence(ABC):

    def __init__(self, cadence: str, schedule: str, start_date: datetime):
        self.cadence_name = cadence
        self.schedule = schedule
        self.start_date = start_date


class SlackReportConfig:

    def __init__(self, cadence, secrets):
        self.dag_name = f"{DAG_PREFIX}-{cadence.cadence_name}"
        self.job_name = f"{JOB_PREFIX}-{cadence.cadence_name}"
        self.cadence = cadence
        self.secrets = secrets
        self.schedule = cadence.schedule
        self.start_date = cadence.start_date


DEV_JOB_IMAGE = ""  # modify this if developing in dev
common_secrets_across_all_cadences = [
    Secret(
        deploy_type="env",
        deploy_target="SLACK_API_TOKEN",
        secret="db-connection-creds",
        key="SLACK_API_TOKEN",
    ),
    Secret(
        deploy_type="env",
        deploy_target="POSTGRES_DB",
        secret="db-connection-creds",
        key="POSTGRES_DB",
    ),
    Secret(
        deploy_type="env",
        deploy_target="POSTGRES_USER",
        secret="db-connection-creds",
        key="POSTGRES_USER",
    ),
    Secret(
        deploy_type="env",
        deploy_target="POSTGRES_PASSWORD",
        secret="db-connection-creds",
        key="POSTGRES_PASSWORD",
    ),
    Secret(
        deploy_type="env",
        deploy_target="POSTGRES_HOST",
        secret="db-connection-creds",
        key="POSTGRES_HOST",
    )
]


# set up new cadences here, and add to bottom of slack-report.py
class WeeklyCadence(Cadence):

    def __init__(self):
        super().__init__(cadence='weekly', schedule='0 16 * * 1', start_date=datetime(2024, 8, 12))


class BiweeklyCadence(Cadence):

    def __init__(self):
        super().__init__(cadence='biweekly', schedule='0 16 1,15 * *', start_date=datetime(2024, 8, 12))


class MonthlyCadence(Cadence):

    def __init__(self):
        super().__init__(cadence='monthly', schedule='0 16 1 * *', start_date=datetime(2024, 8, 1))


class DailyCadence(Cadence):

    def __init__(self):
        super().__init__(cadence='daily', schedule='0 16 * * MON-FRI', start_date=datetime(2024, 8, 1))


WEEKLY_CONFIG = SlackReportConfig(
    cadence=WeeklyCadence(),
    secrets=common_secrets_across_all_cadences + [
        Secret(
            deploy_type="env",
            deploy_target="CATALOG_USER",
            secret="db-connection-creds",
            key="CATALOG_USER",
        ),
        Secret(
            deploy_type="env",
            deploy_target="CATALOG_DB",
            secret="db-connection-creds",
            key="CATALOG_DB",
        ),
        Secret(
            deploy_type="env",
            deploy_target="CATALOG_HOST",
            secret="db-connection-creds",
            key="CATALOG_HOST",
        ),
        Secret(
            deploy_type="env",
            deploy_target="CATALOG_PASSWORD",
            secret="db-connection-creds",
            key="CATALOG_PASSWORD",
        ),
    ],
)
DAILY_CONFIG = SlackReportConfig(
    cadence=DailyCadence(),
    secrets=common_secrets_across_all_cadences + [
        Secret(
            deploy_type="env",
            deploy_target="CATALOG_USER",
            secret="db-connection-creds",
            key="CATALOG_USER",
        ),
        Secret(
            deploy_type="env",
            deploy_target="CATALOG_DB",
            secret="db-connection-creds",
            key="CATALOG_DB",
        ),
        Secret(
            deploy_type="env",
            deploy_target="CATALOG_HOST",
            secret="db-connection-creds",
            key="CATALOG_HOST",
        ),
        Secret(
            deploy_type="env",
            deploy_target="CATALOG_PASSWORD",
            secret="db-connection-creds",
            key="CATALOG_PASSWORD",
        ),
    ],
)
BIWEEKLY_CONFIG = SlackReportConfig(cadence=BiweeklyCadence(), secrets=common_secrets_across_all_cadences)
MONTHLY_CONFIG = SlackReportConfig(cadence=MonthlyCadence(), secrets=common_secrets_across_all_cadences)
