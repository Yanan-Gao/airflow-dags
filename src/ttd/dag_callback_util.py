from typing import Optional, Callable

from ttd.ttdslack import dag_post_to_slack_callback
"""
Provides default DAG callback functionality that will automatically push metrics upon failure or success.
Failures will also post slack messages, if enabled.

You can also specify your own callback that will be invoked along with the defaults.
"""


def default_on_success_callback(on_success_callback: Optional[Callable], ):

    def post_callback(context):
        if on_success_callback is not None:
            on_success_callback(context)

    return post_callback


def default_on_error_callback(
    dag_id: str,
    slack_channel: Optional[str],
    slack_tags: Optional[str],
    dag_tsg,
    enable_slack_alert: bool,
    slack_alert_only_for_prod: bool,
    on_failure_callback: Optional[Callable],
):

    def post_callback(context):
        # slack callback also emits error metrics
        dag_post_to_slack_callback(
            dag_id,
            "DAG",
            slack_channel,
            "DAG has failed",
            dag_tsg,
            slack_tags,
            enable_slack_alert,
            slack_alert_only_for_prod,
        )(context)
        if on_failure_callback is not None:
            on_failure_callback(context)

    return post_callback
