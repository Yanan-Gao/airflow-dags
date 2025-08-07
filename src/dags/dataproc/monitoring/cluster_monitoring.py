from abc import abstractmethod
from collections import defaultdict
from dataclasses import dataclass

import boto3
import logging
import re
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta, UTC

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from botocore.client import BaseClient
from slack import WebClient
from slack.errors import SlackApiError
from typing import Dict, List, Optional, Union, Any, Callable, Type

from ttd.constants import ClusterDurations
from ttd.eng_org_structure_utils import is_valid_team, is_valid_user, is_user_in_team, get_slack_user_id, get_alarm_channel_id
from ttd.alicloud.ali_hook import AliEMRHook
from ttd.cloud_provider import CloudProviders, CloudProvider
from ttd.eldorado import tag_utils
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import dataproc
from ttd.tasks.op import OpTask
from dateutil.tz import tzlocal

from ttd.ttdenv import TtdEnvFactory

from ttd.monads.maybe import Maybe, Just, Nothing

job_schedule_interval = "*/10 * * * *"
lookback_window = timedelta(minutes=10)
job_start_date = datetime(2022, 5, 19, 0, 0)

default_channel_id = "C01EX28376U"  # dev-spark-alarms
dataproc_blackhole_channel_id = "C05J5691U3D"

clusters_to_ignore = [
    "DS-shared-zeppelin",
    "cth-m5.xlarge-blank",
    "RtiAgilesSqlImportCluster_1",
    "RtiAgilesSqlImportCluster_2",
    "RtiAgilesSqlImportCluster_3",
    "RtiAgilesSqlImportCluster_4",
    "RtiAgilesSqlImportCluster_5",
    "RtiAgilesSqlImportCluster_6",
    "RtiAgilesSqlImportCluster_7",
    "RtiAgilesSqlImportCluster_8",
    "RtiAgilesSqlImportCluster_9",
    "RtiAgilesSqlImportCluster_10",
    "RtiAgilesSqlImportCluster_11",
    "RtiAgilesSqlImportCluster_12",
]

required_tags = ["Environment", "Job", "Resource", "Source", "Team"]
duration_tag = 'Duration'


@dataclass
class AlertThreadConfiguration:
    """
    :param thread_header: The message that gets posted at the top of the thread
    :param get_thread_identifier: Returns the string used to generate an ID that gets sent in the message. This is then
    used to find the thread for subsequent messages. The default value therefore results in a thread for each cluster.
    This can be overridden if we want some clusters to share a Slack thread.
    :param display_cluster_details: Whether we want the message at the top of the thread to print out details of the
    cluster.
    """

    @staticmethod
    def default_thread_identifier(cluster: "ClusterDetails") -> str:
        return cluster.cluster_id

    thread_header: str
    get_thread_identifier: Callable[["ClusterDetails"], str] = default_thread_identifier
    display_cluster_details: bool = True


@dataclass
class AlarmReason:
    slack_message: str
    alert_name: str
    metric_label_name: str
    metric_label_value: str
    thread_configuration: Optional[AlertThreadConfiguration] = None

    @classmethod
    @abstractmethod
    def check(cls, cluster: "ClusterDetails") -> Optional["AlarmReason"]:
        raise NotImplementedError


class MissingTags(AlarmReason):

    def __init__(self, tags: list[str]):
        super().__init__(
            slack_message=f"""The following tags should have been present: {', '.join(f'`{tag}`' for tag in tags)}""",
            alert_name="Missing tags",
            metric_label_name="missing_tags",
            metric_label_value=str(tags)
        )

    @classmethod
    def check(cls, cluster: "ClusterDetails") -> Optional["AlarmReason"]:
        missing_tags = [tag for tag in required_tags if tag not in cluster.tags]

        if cluster.creator_details.require_creator_tag and cluster.creator_details.creator_tag.is_nothing():
            missing_tags.append("Creator")

        if len(missing_tags) > 0:
            return cls(missing_tags)
        return None


class InvalidCreator(AlarmReason):

    def __init__(self, creator: str):
        super().__init__(
            slack_message=f"""The creator of this cluster wasn't present in eng-org structure: `{creator}`""",
            alert_name="Creator invalid",
            metric_label_name="invalid_creator",
            metric_label_value=creator
        )

    @classmethod
    def check(cls, cluster: "ClusterDetails") -> Optional["AlarmReason"]:
        creator_details = cluster.creator_details
        if not creator_details.require_creator_tag:
            return None

        return creator_details.creator_tag.flat_map(lambda tag: Just(cls(tag)) if not is_valid_user(tag) else Nothing()).as_optional()


class DurationExceeded(AlarmReason):

    def __init__(self, duration: float, threshold: float):
        super().__init__(
            slack_message=f"""
The duration of the cluster exceeded the threshold!\n
Duration:  `{SlackThreadPoster.human_readable_from_seconds(duration)}`
Threshold:  `{SlackThreadPoster.human_readable_from_seconds(threshold)}`
""",
            alert_name="Duration Exceeded",
            metric_label_name="duration_exceeded",
            metric_label_value=str(duration - threshold)
        )

    @classmethod
    def get_max_permitted_duration(cls, tags: Dict[str, str]) -> float:
        if extension := tags.get(duration_tag):
            return ClusterDurations.DURATION_VALUES.get(extension, ClusterDurations.DEFAULT_MAX_DURATION)
        else:
            for key in ClusterDurations.LEGACY_DURATION_TAGS.keys():
                if key in tags:
                    return ClusterDurations.LEGACY_DURATION_TAGS[key]
            return ClusterDurations.DEFAULT_MAX_DURATION

    @classmethod
    def check(cls, cluster: "ClusterDetails") -> Optional["AlarmReason"]:
        max_permitted_duration = DurationExceeded.get_max_permitted_duration(cluster.tags)

        if cluster.duration >= max_permitted_duration and not cluster.permanent:
            return cls(cluster.duration, max_permitted_duration)
        return None


class DuplicateClusterDetected(AlarmReason):

    def __init__(self, cluster: "ClusterDetails", duplicates: List["ClusterDetails"]):
        duplicate_links = ', '.join(f"*<{c.link}|{c.name}>*" for c in duplicates)
        message = f"Duplicate clusters: *<{cluster.link}|{cluster.name}>*, {duplicate_links} {cluster.link_note}"

        header = f":rotating_light: Duplicate clusters were detected! {'They have been deleted! ' if delete_clusters() else ''}:thread:"

        super().__init__(
            slack_message=message,
            alert_name="Duplicate clusters detected",
            metric_label_name="duplicate",
            metric_label_value=str(c.cluster_id for c in duplicates),
            thread_configuration=AlertThreadConfiguration(
                thread_header=header,
                get_thread_identifier=lambda c: c.duplicate_identifier,
                display_cluster_details=False,
            ),
        )

    @classmethod
    def check(cls, cluster: "ClusterDetails") -> Optional["AlarmReason"]:
        raise NotImplementedError("DuplicateCluster is a special case")


class InvalidTeam(AlarmReason):

    def __init__(self, team: str):
        super().__init__(
            slack_message=f"""The team associated with this cluster was invalid: {team}""",
            alert_name="Team invalid",
            metric_label_name="invalid_team",
            metric_label_value=team
        )

    @classmethod
    def check(cls, cluster: "ClusterDetails") -> Optional["AlarmReason"]:
        if cluster.team is not None and not is_valid_team(cluster.team):
            return cls(cluster.team)
        return None


class NewPermanentCluster(AlarmReason):

    def __init__(self):
        super().__init__(
            slack_message="""Heads up! This is a new permanent cluster""",
            alert_name="New permanent cluster",
            metric_label_name="new_permanent_cluster",
            metric_label_value='1'
        )

    @classmethod
    def check(cls, cluster: "ClusterDetails") -> Optional["AlarmReason"]:
        if cluster.permanent:
            return cls()
        return None


class UserNotInTeam(AlarmReason):

    def __init__(self, user: str, team: str, link: str, cluster_id: str, link_note: str) -> None:
        super().__init__(
            slack_message=f"""This cluster (*<{link}|{cluster_id}>*) has been created! {link_note}""",
            alert_name="User not in team",
            metric_label_name="user_not_in_team",
            metric_label_value=user,
            thread_configuration=AlertThreadConfiguration(
                thread_header=f":announcement: Heads up! A user is running clusters owned by the {team} team!",
                get_thread_identifier=lambda c: user + team,
                display_cluster_details=False,
            ),
        )

    @classmethod
    def check(cls, cluster: "ClusterDetails") -> Optional["AlarmReason"]:
        creator_tag = cluster.creator_details.creator_tag

        if creator_tag.is_nothing() or cluster.team is None:
            return None

        creator = creator_tag.get()
        if not is_user_in_team(creator, cluster.team):
            return cls(creator, cluster.team, cluster.link, cluster.cluster_id, cluster.link_note)
        return None


class CreatorDetails:

    def __init__(self, tags: Dict[str, str]):
        self.require_creator_tag = self._should_require_creator_tag(tags)
        self.creator_tag = Just(tags["Creator"]) if "Creator" in tags else Nothing()
        self.slack_user_id = self.creator_tag.flat_map(get_slack_user_id)

    def format_creator_for_slack(self) -> Maybe[str]:
        return self.slack_user_id.map(lambda user_id: f"<@{user_id}>").or_else(lambda: self.creator_tag.map(lambda tag: f"`{tag}`"))

    def _should_require_creator_tag(self, tags: dict[str, str]) -> bool:
        return tags.get("Source") in {None, "adhoc"} or tags.get("Environment") in {"prodTest"}


class ClusterDetails:
    deletion_alarms: list[Type[AlarmReason]] = [MissingTags, DurationExceeded, InvalidTeam, InvalidCreator]
    warning_alarms: list[Type[AlarmReason]] = [NewPermanentCluster, UserNotInTeam]

    def __init__(
        self,
        cluster_id: str,
        name: str,
        creation_date_time: datetime,
        cloud_provider: CloudProvider,
        region_name: str,
        tags: Optional[Dict[str, str]] = None,
        normalized_instance_hours: Optional[int] = None,
        termination_protected: bool = False,
        account: str = "prod",
    ):
        self.cluster_id = cluster_id
        self.name = name
        self.creation_date_time = creation_date_time
        self.cloud_provider = cloud_provider
        self.region_name = region_name
        self.tags = tags or {}
        self.normalized_instance_hours = normalized_instance_hours
        self.termination_protected = termination_protected
        self.account = account

        self.creator_details = CreatorDetails(self.tags)
        self.link: str = self._generate_cluster_link()
        self.link_note = self._add_dev_console_note()
        self.duplicate_identifier = self._create_identifier()
        self.permanent = self._is_permanent()
        self.duration = (datetime.now(tz=tzlocal()) - self.creation_date_time).total_seconds()
        self.team = self.tags.get("Team")

        self.deletion_reasons = [reason for cls in self.deletion_alarms if (reason := cls.check(self)) is not None]
        if self.duration <= lookback_window.total_seconds():
            self.warning_reasons = [reason for cls in self.warning_alarms if (reason := cls.check(self)) is not None]
        else:
            self.warning_reasons = []

    def _generate_cluster_link(self) -> str:
        match self.cloud_provider:
            case CloudProviders.ali:
                return f"https://emr-next.console.aliyun.com/#/region/{self.region_name}/resource/all/ecs/detail/{self.cluster_id}/overview"
            case CloudProviders.aws:
                return f"https://console.aws.amazon.com/emr/home?region={self.region_name}#/clusterDetails/{self.cluster_id}"
            case _:
                raise NotImplementedError(f"Unexpected cloud provider: {self.cloud_provider}")

    def _add_dev_console_note(self) -> str:
        if self.account == "dev":
            return "\nNote: You'll need to be logged into the dev AWS console to access this link.\n"
        return ""

    def _is_permanent(self) -> bool:
        legacy_is_permanent = self.name in clusters_to_ignore or "DataProductRole" in self.tags
        return self.tags.get(duration_tag) == "permanent" or "permanent" in self.tags or legacy_is_permanent

    def _create_identifier(self) -> str:
        # Create string that should identify duplicate active clusters
        if "ClusterName" not in self.tags or "Environment" not in self.tags:
            return "unidentified"

        # Should strip the try part of the clustername as multiple try's that are active at the same time should be
        # flagged
        cluster_name_stripped = re.sub(r'try[0-9]+', '', self.tags['ClusterName'])
        return f"{cluster_name_stripped}-{self.tags['Environment']}".lower() + self.creator_details.creator_tag.or_else(lambda: Just("")
                                                                                                                        ).get()

    def __repr__(self):
        return (
            f"Id={self.cluster_id}, "
            f"Name={self.name}, "
            f"CreationDateTime={self.creation_date_time}, "
            f"Tags={self.tags}, "
            f"cluster_creator={self.creator_details.creator_tag}, "
            f"deletion_reasons={self.deletion_reasons}, "
            f"warning_reasons={self.warning_reasons}"
        )


class TtdSlackClient:

    def __init__(self, api_token):
        self._client = WebClient(token=api_token)

    def post_message(self, channel: str, text: str, attachments: List[Dict[str, Any]], thread_ts: str = None) -> str:
        try:
            response = self._client.chat_postMessage(
                channel=channel,
                text=text,
                attachments=attachments,
                thread_ts=thread_ts,
                link_names="true",
            )
            return response["message"]["ts"]  # type: ignore
        except SlackApiError as e:
            logging.error(
                f"Failed to post message in channel={channel}",
                exc_info=e,
            )
            raise e

    def join_channel(self, channel: str) -> None:
        try:
            self._client.conversations_join(channel=channel)
        except SlackApiError as e:
            logging.error(f"Failed to join channel {channel}", exc_info=e)
            raise e

    def invite_slack_creator_id(self, creator_details: CreatorDetails, channel: str) -> None:
        if creator_details.slack_user_id.is_nothing():
            logging.warning(f"No Slack ID for {creator_details.creator_tag}")
            return

        if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
            try:
                self._client.conversations_invite(channel=channel, users=creator_details.slack_user_id.get())
            except SlackApiError as e:
                if e.response.get("error", "") == "already_in_channel":
                    logging.info(f"User {creator_details.creator_tag} already in channel {channel}")
                else:
                    logging.warning(f"Failed to invite {creator_details.creator_tag} to channel {channel}", exc_info=e)

    @staticmethod
    def slack_attachment(
        text: str = None,
        color: str = "#1d9ad6",
        fields: dict[str, str] = None,
        short_fields: bool = True,
        title: str = None,
        title_link: str = None,
        footer: str = None,
    ) -> Dict:
        return {
            "color": color,
            "text": text,
            "title": title,
            "title_link": title_link,
            "mrkdwn_in": ["text", "fields"],
            "footer": footer,
            "fields": [{
                "title": key,
                "value": value,
                "short": short_fields
            } for key, value in fields.items()] if fields else None,
        }

    def find_latest_thread_ts(self, lookback: timedelta, channel: str, message_id: str) -> str | None:

        def footer_contains_message_id(attachment: Dict, message_id: str) -> bool:
            return message_id in attachment.get("footer", "")

        lookback_timestamp = int((datetime.now(UTC) - lookback).timestamp())
        history = self._client.conversations_history(channel=channel, oldest=lookback_timestamp, limit=999)
        messages = history.get("messages", [])  # type: ignore
        ts_values = [
            message['ts'] for message in messages
            if any(footer_contains_message_id(attachment, message_id) for attachment in message.get("attachments", []))
        ]
        return max(ts_values, default=None)


class SlackThreadPoster:

    def __init__(self, client: TtdSlackClient, cluster: ClusterDetails, is_deletion_message: bool, account: str):
        self._client = client
        self._cluster = cluster
        self._is_deletion_message = is_deletion_message
        self._account = account
        self._alarm_reasons = self._cluster.deletion_reasons if is_deletion_message else self._cluster.warning_reasons

        self._channel = self._get_channel_id()
        self._client.join_channel(self._channel)
        self._client.invite_slack_creator_id(self._cluster.creator_details, self._channel)
        self._thread_configuration = self._get_thread_configuration()
        self._message_id = self._generate_message_id()

    def post(self) -> None:
        thread_ts = self.get_or_create_thread()
        self._post_cluster_thread_message(thread_ts)

    def _get_channel_id(self) -> str:
        if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
            return get_alarm_channel_id(self._cluster.team) or default_channel_id
        return dataproc_blackhole_channel_id

    def get_or_create_thread(self) -> str:
        lookback = timedelta(days=3) - timedelta(minutes=10)
        thread_ts = self._client.find_latest_thread_ts(lookback=lookback, channel=self._channel, message_id=self._message_id)

        if thread_ts is not None:
            return thread_ts

        return self._post_daily_thread_message()

    def _post_daily_thread_message(self) -> str:
        logging.info(f"Creating new daily slack thread for cluster {self._cluster.cluster_id} in channel {self._channel}.")

        fields = {"*More info on alarms*": "<https://atlassian.thetradedesk.com/confluence/x/-4s6C|Info and TSG>"}

        if self._thread_configuration.display_cluster_details:
            fields["*Cluster ID*"] = f"`{self._cluster.cluster_id}`"
            fields["*Cluster created*"] = f"`{self._cluster.creation_date_time.strftime('%Y-%m-%d %H:%M:%S')}`"

        self._cluster.creator_details.format_creator_for_slack().map(lambda creator: fields.update({"*Creator*": creator}))

        attachment = self._client.slack_attachment(
            fields=fields, title=self._cluster.name, title_link=self._cluster.link, footer=f'Message ID: _{self._message_id}_'
        )

        return self._client.post_message(channel=self._channel, text=self._thread_configuration.thread_header, attachments=[attachment])

    def _post_cluster_thread_message(self, thread_ts: str) -> None:
        logging.info(f"Replying to daily slack thread for cluster {self._cluster.cluster_id} in channel {self._channel}.")

        alarm_summaries = {d.alert_name: d.slack_message for d in self._alarm_reasons}
        tags = self._format_tags_for_posting(self._cluster.tags)

        plural = "s" if len(self._alarm_reasons) > 1 else ""
        text = f"*Current duration:* `{self.human_readable_from_seconds(self._cluster.duration)}`\n*Compute Account:* {self._account}\n\n"
        if self._cluster.termination_protected and self._is_deletion_message:
            text += ":warning: *This cluster is termination-protected.* The protection will be removed to proceed with deletion.\n\n"
        text += f"*Reason{plural} for this alarm:*"

        self._client.post_message(
            channel=self._channel,
            text=text,
            attachments=[
                self._client.slack_attachment(fields=alarm_summaries, short_fields=False, color="#e74c3c"),
                self._client.slack_attachment(tags, "#ffc80a")
            ],
            thread_ts=thread_ts
        )

    @staticmethod
    def _format_tags_for_posting(tags: dict[str, str]) -> str:
        if len(tags) == 0:
            return "*This cluster has no tags!*"

        sorted_keys = sorted(tags.keys())
        max_key_length = max(len(key) for key in sorted_keys)
        formatted_tags = "\n".join([f"{key.ljust(max_key_length)} : {tags[key]}" for key in sorted_keys])

        return f"""Tags on this cluster:\n```{formatted_tags}```"""

    @staticmethod
    def human_readable_from_seconds(seconds: Optional[float]) -> str:
        if seconds is None:
            return "N/A"

        td = timedelta(seconds=int(seconds))
        return str(td)

    def _generate_message_id(self) -> str:
        from hashlib import sha256

        identifier = self._thread_configuration.get_thread_identifier(self._cluster)
        return sha256(identifier.encode()).hexdigest()[:12]

    def _get_thread_configuration(self) -> AlertThreadConfiguration:
        if len(self._alarm_reasons) == 1 and self._alarm_reasons[0].thread_configuration is not None:
            return self._alarm_reasons[0].thread_configuration

        if self._is_deletion_message:
            qualifier = 'Issues were' if len(self._alarm_reasons) > 1 else 'An issue was'
            return AlertThreadConfiguration(
                f":rotating_light: {qualifier} identified with the following cluster. {'It has been deleted! ' if delete_clusters() else ''}:thread:"
            )
        else:
            return AlertThreadConfiguration(":announcement: Heads up! A cluster has been created to be aware of :thread:")


def delete_clusters() -> bool:
    is_prod = TtdEnvFactory.get_from_system() == TtdEnvFactory.prod
    feature_switch = Variable.get("DELETE_VIOLATING_CLUSTERS", deserialize_json=True, default_var=False)

    return is_prod and feature_switch


def push_to_metrics_db(
    clusters_for_deletion: list[ClusterDetails],
    clusters_for_warnings: list[ClusterDetails],
    cloud_provider: CloudProvider,
):

    def push_cluster_metrics(clusters, metric_name, reason_attr):
        for cluster in clusters:
            labels = {
                'team': cluster.team,
                'cluster_name': cluster.name if cluster.name is not None else "UNDEFINED",
                'cluster_id': cluster.cluster_id,
                'cloud_provider': cloud_provider.__str__(),
            }

            for reason in getattr(cluster, reason_attr):
                labels[reason.metric_label_name] = reason.metric_label_value

            metric_pusher.push(
                name=metric_name,
                value=cluster.duration or 1,
                labels=labels,
                timestamp=timestamp,
            )

    from ttd.metrics.metric_pusher import MetricPusher
    metric_pusher = MetricPusher()
    timestamp = datetime.now()

    push_cluster_metrics(clusters_for_deletion, "cluster_to_delete", "deletion_reasons")
    push_cluster_metrics(clusters_for_warnings, "cluster_warning", "warning_reasons")


def cluster_groupby(cluster_list: List[ClusterDetails], key: Callable) -> Dict[str, List[ClusterDetails]]:
    out = defaultdict(list)
    for cluster in cluster_list:
        out[key(cluster)].append(cluster)
    return out


def clusters_to_delete(cluster_details: List[ClusterDetails]) -> List[ClusterDetails]:
    return [c_d for c_d in cluster_details if len(c_d.deletion_reasons) > 0]


def clusters_to_warn_about(cluster_details: List[ClusterDetails]) -> List[ClusterDetails]:
    return [c_d for c_d in cluster_details if len(c_d.warning_reasons) > 0 and len(c_d.deletion_reasons) == 0]


def add_duplicate_clusters(clusters: List[ClusterDetails]) -> None:
    identifier_lambda = lambda c: c.duplicate_identifier
    by_identifier = cluster_groupby(clusters, identifier_lambda)
    for identifier, cluster_group in by_identifier.items():
        if identifier != "unidentified" and len(cluster_group) > 1:
            for i in range(len(cluster_group)):
                cluster_group[
                    i].deletion_reasons.append(DuplicateClusterDetected(cluster_group[i], cluster_group[:i] + cluster_group[i + 1:]))


def send_slack_alarms(clusters_to_delete: list[ClusterDetails], clusters_to_warn: list[ClusterDetails], account: str) -> None:
    api_token = BaseHook.get_connection("slack").password
    slack_client = TtdSlackClient(api_token=api_token)
    for cluster in clusters_to_delete:
        deletion_thread_poster = SlackThreadPoster(slack_client, cluster, True, account)
        deletion_thread_poster.post()

    for cluster in clusters_to_warn:
        warning_thread_poster = SlackThreadPoster(slack_client, cluster, False, account)
        warning_thread_poster.post()


def assume_role_for_dev(region_name: str) -> BaseClient:
    aws_hook = AwsBaseHook(aws_conn_id="aws_dev", client_type="emr", region_name=region_name)
    emr_client = aws_hook.get_conn()

    logging.info(f"Successfully assumed role and created EMR client for region: {region_name}")

    return emr_client


def get_client(cloud_provider: CloudProvider, region_name: str, account: str) -> Union[BaseClient, AliEMRHook]:
    match cloud_provider:
        case CloudProviders.ali:
            return AliEMRHook()
        case CloudProviders.aws:
            if account == "dev":
                return assume_role_for_dev(region_name)
            return boto3.client("emr", region_name=region_name)
        case _:
            raise NotImplementedError(f"Unsupported cloud provider: {cloud_provider}")


def get_active_cluster_details(client: Union[BaseClient, AliEMRHook], region_name: str, account: str) -> List[ClusterDetails]:
    active_cluster_details = {}
    match client:
        case BaseClient():
            paginator = client.get_paginator("list_clusters")
            pages = paginator.paginate(ClusterStates=[
                "STARTING",
                "WAITING",
                "BOOTSTRAPPING",
                "RUNNING",
            ])
            for cluster in list(cluster for page in pages for cluster in page["Clusters"]):
                cluster_id = cluster["Id"]
                details = client.describe_cluster(ClusterId=cluster_id)["Cluster"]
                cluster_details = ClusterDetails(
                    cluster_id=cluster_id,
                    name=details["Name"],
                    normalized_instance_hours=details["NormalizedInstanceHours"],
                    creation_date_time=details["Status"]["Timeline"]["CreationDateTime"],
                    tags=tag_utils.get_raw_cluster_tags(details.get("Tags", [])),
                    cloud_provider=CloudProviders.aws,
                    region_name=region_name,
                    termination_protected=details["TerminationProtected"],
                    account=account,
                )
                active_cluster_details[cluster_id] = cluster_details
        case AliEMRHook():
            active_clusters = client.get_clusters(region_id=region_name)
            for cluster in active_clusters:
                timestamp_s = cluster.create_time / 1000
                creation_datetime = datetime.fromtimestamp(timestamp_s, tz=tzlocal())
                cluster_details = ClusterDetails(
                    cluster_id=cluster.id,
                    name=cluster.name,
                    creation_date_time=creation_datetime,
                    tags=tag_utils.get_raw_cluster_tags([tag.to_map() for tag in cluster.tags.tag], cloud_provider=CloudProviders.ali),
                    cloud_provider=CloudProviders.ali,
                    region_name=region_name,
                )
                if cluster_details.duration > 60:
                    active_cluster_details[cluster.id] = cluster_details
        case _:
            raise NotImplementedError(f"Unsupported client type: {type(client)}")

    return list(active_cluster_details.values())


def log_cluster_info(clusters_for_deletion: list[ClusterDetails], clusters_for_warnings: list[ClusterDetails]) -> None:
    logging.info("Clusters to be deleted:")
    for cluster in clusters_for_deletion:
        logging.info(cluster)
    logging.info("Clusters with warnings:")
    for cluster in clusters_for_warnings:
        logging.info(cluster)


def cleanup(client: Union[BaseClient, AliEMRHook], clusters_for_deletion: list[ClusterDetails]) -> None:
    if not delete_clusters():
        cluster_names = ", ".join(cluster.name for cluster in clusters_for_deletion)
        logging.info(f"Skipping cleanup. Would have deleted: {cluster_names}")
        return

    for cluster in clusters_for_deletion:
        logging.info(f"Deleting cluster: {cluster.name}")

        match client:
            case BaseClient():
                if cluster.termination_protected:
                    client.set_termination_protection(JobFlowIds=[cluster.cluster_id], TerminationProtected=False)
                    logging.info(f"Deletion protection removed for cluster: {cluster.name}")

                client.terminate_job_flows(JobFlowIds=[cluster.cluster_id])
            case AliEMRHook():
                client.delete_cluster(cluster.region_name, cluster.cluster_id)
            case _:
                raise NotImplementedError(f"Unsupported client: {client}")


def check_clusters(cloud_provider: CloudProvider, region_name: str, account: str) -> None:
    client = get_client(cloud_provider=cloud_provider, region_name=region_name, account=account)
    active_cluster_details = get_active_cluster_details(client=client, region_name=region_name, account=account)
    add_duplicate_clusters(active_cluster_details)

    clusters_for_deletion = clusters_to_delete(active_cluster_details)
    clusters_for_warnings = clusters_to_warn_about(active_cluster_details)

    log_cluster_info(clusters_for_deletion, clusters_for_warnings)
    send_slack_alarms(clusters_for_deletion, clusters_for_warnings, account)
    cleanup(client=client, clusters_for_deletion=clusters_for_deletion)

    push_to_metrics_db(clusters_for_deletion, clusters_for_warnings, cloud_provider)


def create_cluster_check_task(cloud_provider: CloudProvider, account: str, region: str) -> OpTask:
    return OpTask(
        op=PythonOperator(
            task_id=f"check_{account}_account_clusters_{region}",
            python_callable=check_clusters,
            op_kwargs={
                "cloud_provider": cloud_provider,
                "region_name": region,
                "account": account,
            },
            retries=6,
            retry_delay=timedelta(minutes=10),
        )
    )


def create_dag(job_name: str, cloud_provider: CloudProvider, accounts: list[str], regions: list[str]) -> TtdDag:
    ttd_dag = TtdDag(
        dag_id=job_name,
        start_date=job_start_date,
        schedule_interval=job_schedule_interval,
        run_only_latest=True,
        tags=["Monitoring", dataproc.jira_team],
        default_args={"owner": dataproc.jira_team},
        enable_slack_alert=False,
    )

    for account in accounts:
        for region in regions:
            task = create_cluster_check_task(cloud_provider, account, region)
            ttd_dag >> task

    return ttd_dag


dag_info = [
    ("emr-cluster-monitoring", CloudProviders.aws, ["prod", "dev"], ["us-east-1"]),
    ("alicloud-emr-cluster-monitoring", CloudProviders.ali, ["prod"], ["cn-shanghai"]),
]

for (dag_id, cloud, accounts, regions) in dag_info:
    dag = create_dag(dag_id, cloud, accounts, regions)
    globals()[dag.airflow_dag.dag_id] = dag.airflow_dag
