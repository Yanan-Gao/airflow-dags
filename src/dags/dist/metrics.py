from ttd.ttdprometheus import get_or_register_gauge, push_all

metrics_job = "airflow_monitoring"

# region Metrics


def get_dag_error_gauge(slack_channel, slack_tags=""):
    return get_or_register_gauge(
        metrics_job, "airflow_dag_execution_error", "Airflow DAG error on attempted run", ["slack_channel", "slack_tags"]
    ).labels(
        slack_channel=slack_channel, slack_tags=slack_tags
    )


def get_dag_last_success_time():
    return get_or_register_gauge(metrics_job, "airflow_dag_execution_success_time", "Airflow DAG succeeded on run", [])


# endregion

# region Metric calls


def dag_metric_success_callback(dag_name, slack_channel, slack_tags=""):

    def post_callback(context):
        get_dag_error_gauge(slack_channel, slack_tags).set(0)
        push_metrics(dag_name, 'status')

        get_dag_last_success_time().set_to_current_time()
        push_metrics(dag_name, 'success')

    return post_callback


def push_metrics(dag_name: str, metric_group: str = None):
    group_key = {"dag": dag_name}
    if metric_group is not None:
        group_key["metric_group"] = metric_group

    push_all(metrics_job, group_key)


# endregion
