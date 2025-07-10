from typing import Dict, Any
from ttd.eldorado.aws.cluster_configs.base import EmrConf
from enum import Enum
from ttd.semver import SemverVersion
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions


class Log4jLogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


class Log4jConf(EmrConf):

    def __init__(self, http_transport_log_level: Log4jLogLevel = Log4jLogLevel.INFO):
        self.transport_log_level = http_transport_log_level


MIN_EMR_VERSION_FOR_LOG4J2 = SemverVersion(6, 8, 0)


class Log4j1AsyncEventQueueConf(Log4jConf):

    def supported_on(self, emr_release_label: str):
        return AwsEmrVersions.parse_version(emr_release_label) < MIN_EMR_VERSION_FOR_LOG4J2

    def to_dict(self) -> Dict[str, Any]:
        return {
            'Classification': 'spark-log4j',
            'Properties': {
                "log4j.logger.io.openlineage.client.transports.HttpTransport": self.transport_log_level.value,
                "log4j.logger.org.apache.spark.scheduler.AsyncEventQueue": "WARN"
            }
        }


class Log4j2AsyncEventQueueConf(Log4jConf):

    def supported_on(self, emr_release_label: str):
        return AwsEmrVersions.parse_version(emr_release_label) >= MIN_EMR_VERSION_FOR_LOG4J2

    def to_dict(self) -> Dict[str, Any]:
        return {
            'Classification': 'spark-log4j2',
            'Properties': {
                "logger.httpTransport.level": self.transport_log_level.value,
                "logger.httpTransport.name": "io.openlineage.client.transports.HttpTransport",
                "logger.asyncEventQueue.level": "WARN",
                "logger.asyncEventQueue.name": "log4j.logger.org.apache.spark.scheduler.AsyncEventQueue",
            }
        }
