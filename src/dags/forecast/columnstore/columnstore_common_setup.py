from ttd.slack.slack_groups import FORECAST


class ColumnStoreEmrSetup:

    def __init__(self):
        self.log_uri = None
        self.job_jar = "s3://ttd-build-artefacts/etl-based-forecasts/prod/latest/jars/etl-forecast-jobs.jar"
        self.emr_release_label = "emr-6.7.0"
        self.cluster_tags = {
            "Team": FORECAST.team.jira_team,
            "SubTeam": "forecast_column_store",
        }


class ColumnStoreDAGSetup:

    def __init__(self):
        self.slack_channel = FORECAST.team.alarm_channel
        self.tags = ["uf_column_store", FORECAST.team.jira_team]
        self.default_args = {"owner": "FORECAST"}
        self.dag_tsg = "https://atlassian.thetradedesk.com/confluence/display/EN/Column+store+data+generation+DAGs"
