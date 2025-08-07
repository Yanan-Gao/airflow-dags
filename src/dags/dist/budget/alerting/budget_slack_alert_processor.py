import io
import json

import boto3
import logging
from jinja2 import Template
from slack.errors import SlackApiError
from slack_sdk import WebClient
from typing import List, Tuple, Optional, Dict, Any, TYPE_CHECKING
from slack_sdk.models.blocks.blocks import Block
from slack_sdk.models.blocks.block_elements import MarkdownTextObject
from slack_sdk.models.blocks.blocks import (SectionBlock, ContextBlock)
from datetime import timedelta, datetime

from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.ttdslack import get_slack_client

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


class PySparkSqlTask(EmrJobTask):
    EXECUTABLE_PATH = ''  # Override parent's default path

    def __init__(
        self,
        name: str,
        sql_query: str,
        output_path: Optional[str] = None,
        additional_args_option_pairs_list: List[Tuple[str, str]] = [],
        timeout_timedelta: timedelta = timedelta(hours=1),
    ):
        configs = []
        for key, value in additional_args_option_pairs_list:
            configs.extend([f"--{key}", value])

        if output_path:
            spark_args = [
                "spark-sql", "-e", f"""
                INSERT OVERWRITE DIRECTORY '{output_path}'
                USING parquet
                {sql_query}
                """, "-v"
            ] + configs
        else:
            spark_args = ["spark-sql", "-e", f"{sql_query}", "-v"] + configs

        super().__init__(
            name=name,
            class_name='',
            job_jar="command-runner.jar",
            command_line_arguments=spark_args,
            additional_args_option_pairs_list=additional_args_option_pairs_list,
            is_pyspark=False,
            timeout_timedelta=timeout_timedelta,
            retries=3,
            executable_path="",
            action_on_failure='CONTINUE'
        )


class BaseAlert(object):

    def __init__(self, name: str, sql: str, channel: str, metric_name: str, threshold: str):
        self.sql_template = sql
        self.name = name
        self.channel = channel
        self.threshold = threshold
        self.metric_name = metric_name

    def main_blocks(self) -> List[Block]:
        """Define main message blocks"""
        raise NotImplementedError

    def thread_messages(self) -> List[List[Block]]:
        """Define thread messages blocks"""
        raise NotImplementedError

    def get_template_vars(self, df: "pd.DataFrame", run_date: datetime, s3_bucket: str, s3_prefix: str, sql_rendered: str) -> dict:
        max_query_length = 1000
        truncated_query = (sql_rendered[:max_query_length] + '...') if len(sql_rendered) > max_query_length else sql_rendered
        return {
            "run_date": run_date,
            "query": truncated_query,
            "metric_name": self.metric_name.replace('_', ' '),
            "name": self.name.replace('_', ' '),
            "run_date_str": run_date.strftime('%Y-%m-%d'),
            "threshold": self.threshold
        }

    def create_emr_task(self, s3_bucket: str, s3_base_path: str, queries_template_arg: Dict,
                        spark_options_list: List[Tuple[str, str]]) -> Tuple[EmrJobTask, str]:
        output_path = f's3://{s3_bucket}/{self.get_output_prefix(s3_base_path)}/'

        sql_rendered = Template(self.sql_template).render(queries_template_arg)
        logger.info(f"create pyspark sql for {self.name} and full SQL query: {sql_rendered} to output to {output_path}")
        return (
            PySparkSqlTask(
                name=self.name,
                output_path=output_path,
                sql_query=sql_rendered,
                additional_args_option_pairs_list=spark_options_list,
                timeout_timedelta=timedelta(hours=1)
            ), sql_rendered
        )

    def get_output_prefix(self, s3_base_path: str):
        output_path = f"{s3_base_path}/{self.name}/query_output/"
        return output_path


class GlobalAlert(BaseAlert):

    def main_blocks(self) -> List[Block]:
        return [
            SectionBlock(
                text=MarkdownTextObject(
                    text="🚨 *{{ metric_name.title() }}* is {{ metric_value }} which exceeds threshold of {{ threshold }}"
                )
            ),
            ContextBlock(elements=[MarkdownTextObject(text="Run date: {{ run_date_str }}")])
        ]

    def thread_messages(self) -> List[List[Block]]:
        return [[
            SectionBlock(text=MarkdownTextObject(text="Sql query (execute it on databricks using `WITH (credential medivac)`:")),
            SectionBlock(text=MarkdownTextObject(text="```\n{{ query }}\n```")),
        ]]

    def get_template_vars(self, df: "pd.DataFrame", run_date: datetime, s3_bucket: str, s3_prefix: str, sql_rendered: str) -> dict:
        metric_value = df[self.metric_name].iloc[0]
        # Format values for display
        metric_value = f"{metric_value:.2f}" if isinstance(metric_value, float) else str(metric_value)

        return super().get_template_vars(
            df, run_date, s3_bucket=s3_bucket, s3_prefix=s3_prefix, sql_rendered=sql_rendered
        ) | {
            "metric_value": metric_value
        }


class EntityAlert(BaseAlert):

    def __init__(self, name: str, sql: str, channel: str, entity_name: str, metric_name: str, threshold: str, preview_limit: int = 3):
        super().__init__(name=name, sql=sql, channel=channel, metric_name=metric_name, threshold=threshold)
        self.entity_name = entity_name
        self.preview_limit = preview_limit

    def main_blocks(self) -> List[Block]:
        return [
            SectionBlock(
                text=MarkdownTextObject(
                    text="🚨 Found *{{ row_count }}* {{entity_name}}s with {{ metric_name.title() }} over {{threshold}}"
                )
            ),
            ContextBlock(elements=[MarkdownTextObject(text="Run Date: {{ run_date_str }} | See thread for details")])
        ]

    def thread_messages(self) -> List[List[Block]]:
        # Return a list of messages, where each message is a list of blocks
        return [
            # First message: SQL query
            [
                SectionBlock(text=MarkdownTextObject(text="Sql query (execute it on databricks using `WITH (credential medivac)`:")),
                SectionBlock(text=MarkdownTextObject(text="```\n{{ query }}\n```"))
            ],
            # Second message: Preview data
            [
                SectionBlock(
                    text=MarkdownTextObject(text="Preview {{preview_limit}} {{entity_name}}s out of {{row_count}} total {{entity_name}}s:")
                ),
                SectionBlock(text=MarkdownTextObject(text="```\n{{ table_preview }}\n```"))
            ],
            # Third message: Download link
            [SectionBlock(text=MarkdownTextObject(text="*<{{ s3_url }}|Download full CSV>*"))]
        ]

    def get_template_vars(self, df: "pd.DataFrame", run_date: datetime, s3_bucket: str, s3_prefix: str, sql_rendered: str) -> dict:
        s3_url = self.upload_to_s3(df, s3_bucket, s3_prefix)

        return super().get_template_vars(
            df, run_date, s3_bucket=s3_bucket, s3_prefix=s3_prefix, sql_rendered=sql_rendered
        ) | {
            "row_count": len(df),
            "table_preview": self._format_preview_data(df.head(self.preview_limit)),
            "entity_name": self.entity_name,
            "s3_url": s3_url,
            "preview_limit": min(len(df), self.preview_limit)
        }

    def upload_to_s3(self, df: "pd.DataFrame", s3_bucket: str, s3_prefix) -> str:
        key = f'{s3_prefix}/{self.name}/result.csv'

        s3_client = boto3.client('s3')
        csv_buffer = io.StringIO()  # Create StringIO object
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=s3_bucket, Key=key, Body=csv_buffer.getvalue())

        return f"https://{s3_bucket}.s3.us-east-1.amazonaws.com/{key}"

    def _format_preview_data(self, df: "pd.DataFrame") -> str:
        # Round numeric columns for cleaner display
        df_display = df.copy()
        numeric_cols = df_display.select_dtypes(include=['float64', 'float32']).columns
        df_display[numeric_cols] = df_display[numeric_cols].round(2)

        # Convert each row to a vertical format
        lines = []
        for idx, row in df_display.iterrows():
            lines.append(f"--- {idx + 1} ---")
            # Use pandas Series string representation which gives vertical format
            lines.append(row.to_string())
            lines.append("")

        return "\n".join(lines)


class SlackAlertProcessor:

    def __init__(self, s3_bucket: str, s3_base_path: str, run_date: str):
        self.s3_client = boto3.client('s3')
        self.client = WebClient(token=get_slack_client().token)
        self.s3_bucket = s3_bucket
        self.s3_base_path = s3_base_path.rstrip('/')
        self.run_date = run_date
        self.logger = logging.getLogger(__name__)

    def _render_blocks(self, blocks: List[Block], template_vars: dict) -> List[dict]:
        """
        Render templates in JSON string directly
        """

        def render_value(value: Any, path: str = "") -> Any:
            if isinstance(value, str):
                try:
                    return Template(value).render(**template_vars)
                except Exception as e:
                    logger.warning(f"Failed to render template at {path}: {str(e)}")
                    return value
            elif isinstance(value, dict):
                if path:
                    logger.debug(f"Processing dict at {path}")
                return {k: render_value(v, f"{path}.{k}" if path else k) for k, v in value.items()}
            elif isinstance(value, list):
                if path:
                    logger.debug(f"Processing list at {path}")
                return [render_value(v, f"{path}[{i}]") for i, v in enumerate(value)]
            else:
                logger.debug(f"Skipping non-string value at {path}: {type(value).__name__}")
                return value

        res = []
        for block in blocks:
            block_dict = block.to_dict()
            rendered_dict = render_value(block_dict)
            res.append(rendered_dict)

        return res

    def process_alert_output(self, df: "pd.DataFrame", alert: BaseAlert, sql_rendered: str, run_date: datetime):
        """Process budget alert output with logging"""
        logger.info(f"Processing budget alert: {alert.name}", extra={"alert": alert.name, "rows": len(df)})

        if df.empty:
            logger.warning(f"Empty budget data for alert: {alert.name}")
            return

        try:
            template_vars = alert.get_template_vars(
                df, run_date=run_date, s3_bucket=self.s3_bucket, s3_prefix=self.s3_base_path, sql_rendered=sql_rendered
            )
            main_blocks = self._render_blocks(alert.main_blocks(), template_vars)
            thread_messages = [self._render_blocks(blocks, template_vars) for blocks in alert.thread_messages()]

            self._send_slack_message(alert.channel, main_blocks, thread_messages)

            logger.info(f"Processed budget alert: {alert.name}")

        except Exception as e:
            logger.error(f"Budget alert processing failed: {alert.name}", extra={"alert": alert.name, "error": str(e)}, exc_info=True)
            raise

    def _send_slack_message(self, channel: str, blocks: List[dict], thread_messages: List[List[dict]]):
        """Send budget alert to Slack"""
        logger.info(f"Sending budget alert to: {channel}, blocks: {json.dumps(blocks)}, thread_blocks: {json.dumps(thread_messages)}")

        try:
            main_response = self.client.chat_postMessage(channel=channel, blocks=blocks)

            if not main_response['ok']:
                logger.error("Failed to send budget alert", extra={"response": main_response})
                raise SlackApiError("Failed to send budget alert", main_response)

            thread_ts = main_response['ts']

            for thread_blocks in thread_messages:
                self.client.chat_postMessage(channel=channel, thread_ts=thread_ts, blocks=thread_blocks)

        except SlackApiError as e:
            logger.error("Slack API error", extra={"error": str(e)}, exc_info=True)
            raise
        except Exception as e:
            logger.error("Failed to send budget alert", extra={"error": str(e)}, exc_info=True)
            raise
