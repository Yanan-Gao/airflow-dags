from collections import defaultdict, Counter
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Sequence, Dict, Tuple, Optional, Callable

import boto3
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.providers.amazon.aws.links.emr import EmrClusterLink, get_log_uri
from airflow.utils.context import Context
from airflow.utils.helpers import prune_dict
from botocore.exceptions import WaiterError, ClientError

from ttd.decorators import return_on_key_error
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.metrics.cluster_lifecycle import ClusterLifecycleMetricPusher
from ttd.mixins.retry_mixin import RetryMixin, RetryLimitException
from ttd.aws.emr.cluster_logs_link import EmrLogsLink

COUNTS_BY_TYPE = Dict[str, Dict[str, int]]


@dataclass
class ErrorDetails:
    code: Optional[str] = None
    message: Optional[str] = None


@dataclass
class ResourceDetails:
    cores: Optional[int] = None
    memory: Optional[int] = None
    disk: Optional[int] = None
    instance_counts: Optional[Dict[str, int]] = None


class EmrMonitorClusterStartup(BaseOperator, RetryMixin):
    ui_color: str = "#EC9BAD"
    ui_fgcolor: str = "#fff"

    template_fields: Sequence[str] = ("job_flow_id", )

    operator_extra_links = (
        EmrClusterLink(),
        EmrLogsLink(),
    )

    def __init__(
        self,
        task_id: str,
        cluster_task_name: str,
        job_flow_id: str,
        aws_conn_id: str,
        region_name: str,
        master_fleet_instance_type_configs: EmrFleetInstanceTypes,
        core_fleet_instance_type_configs: EmrFleetInstanceTypes,
        **kwargs,
    ):
        self.job_flow_id = job_flow_id
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.cluster_task_name = cluster_task_name
        self.master_fleet_types = master_fleet_instance_type_configs
        self.core_fleet_types = core_fleet_instance_type_configs

        super().__init__(task_id=task_id, **kwargs)
        RetryMixin.__init__(self, max_retries=2, retry_interval=120, exponential_retry=True)

    @cached_property
    def _emr_hook(self) -> EmrHook:
        """Create and return an EmrHook."""
        return EmrHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

    def _call_api_with_retry(self, api_callable: Callable[[], Dict]) -> Dict:
        maybe_response = self.with_retry(
            api_callable,
            lambda ex: isinstance(ex, ClientError) and self._should_keep_waiting(ex.response),
        )

        try:
            response = maybe_response.get()
            return response
        except Exception as e:
            self.log.error(e, exc_info=True)
            return {}

    @return_on_key_error()
    def _get_cluster_status(self) -> Optional[Dict]:
        client = self._emr_hook.get_client_type()
        cluster_details = self._call_api_with_retry(lambda: client.describe_cluster(ClusterId=self.job_flow_id))

        return cluster_details['Cluster']['Status']

    def _should_keep_waiting(self, response: Dict) -> bool:
        error_code = response.get("Error", {}).get('Code') if response else None
        if error_code == 'ThrottlingException' or error_code == 'RequestLimitExceeded':
            self.log.info("Throttled by AWS. Keep waiting.")
            return True

        return False

    def _extract_error_details(self, status: Dict) -> ErrorDetails:
        state_change_code = status['StateChangeReason'].get('Code', 'No code provided')
        state_change_message = status['StateChangeReason'].get('Message', 'No message provided')

        return ErrorDetails(code=state_change_code, message=state_change_message)

    def _get_cluster_creation_failure_details(self) -> Tuple[bool, ErrorDetails]:
        acceptable_termination_codes = ['ALL_STEPS_COMPLETED', 'USER_REQUEST', 'STEP_FAILURE']

        error_details = ErrorDetails()

        status = self._get_cluster_status()

        if status is None:
            self.log.info("Rate limited describing cluster. Let's assume it was successful")
            return True, error_details

        match status.get('State'):
            case 'TERMINATED':
                self.log.warning('Terminated without any errors.')
                was_creation_successful = True

            case 'TERMINATING':
                state_change_code = status.get('StateChangeReason', {}).get('Code')
                if state_change_code not in acceptable_termination_codes:
                    self.log.info('Terminating due to an unsuccessful startup')
                    error_details = self._extract_error_details(status)
                    was_creation_successful = False

                else:
                    self.log.info('Terminating but seems to have been created successfully')
                    was_creation_successful = True

            case 'TERMINATED_WITH_ERRORS':
                error_details = self._extract_error_details(status)
                was_creation_successful = False

            case _:
                self.log.warning("Terminated but issue with startup. Outputting status and bailing!")
                self.log.info(status)
                was_creation_successful = False

        return was_creation_successful, error_details

    def _get_instance_counts(self, emr_client: boto3.client) -> Tuple[COUNTS_BY_TYPE, dict[str, int]]:
        instance_counts_by_fleet: COUNTS_BY_TYPE = defaultdict(lambda: defaultdict(int))
        instance_counts: dict[str, int] = defaultdict(int)

        paginator = emr_client.get_paginator('list_instances')
        for page in paginator.paginate(ClusterId=self.job_flow_id):
            for instance in page['Instances']:
                instance_type = instance['InstanceType']
                instance_fleet = instance['InstanceFleetId']
                instance_counts_by_fleet[instance_fleet][instance_type] += 1
                instance_counts[instance_type] += 1
        return instance_counts_by_fleet, instance_counts

    @return_on_key_error()
    def _get_ebs_storage(self, instance_counts_by_fleet: COUNTS_BY_TYPE, emr_client: boto3.client) -> Optional[int]:
        total_disk = 0

        instance_fleets = self._call_api_with_retry(lambda: emr_client.list_instance_fleets(ClusterId=self.job_flow_id))

        for fleet in instance_fleets['InstanceFleets']:
            instance_counts = instance_counts_by_fleet.get(fleet['Id'], {})
            for spec in fleet['InstanceTypeSpecifications']:
                if (instance_type := spec['InstanceType']) in instance_counts:
                    for ebs_config in spec.get('EbsBlockDevices', []):
                        total_disk += instance_counts[instance_type] * ebs_config['VolumeSpecification']['SizeInGB']
        return int(total_disk)

    @return_on_key_error((None, None))
    def _get_cores_and_memory(self, instance_counts_by_fleet: COUNTS_BY_TYPE,
                              ec2_client: boto3.client) -> Tuple[Optional[int], Optional[int]]:
        total_cores = 0
        total_memory = 0

        instance_counts = dict(sum((Counter(inner) for inner in instance_counts_by_fleet.values()), Counter()))
        instance_types = self._call_api_with_retry(lambda: ec2_client.describe_instance_types(InstanceTypes=list(instance_counts.keys())))

        for instance_info in instance_types['InstanceTypes']:
            instance_type = instance_info['InstanceType']
            memory = instance_info['MemoryInfo']['SizeInMiB'] / 1024
            cores = instance_info['VCpuInfo']['DefaultVCpus']

            total_memory += memory * instance_counts[instance_type]
            total_cores += cores * instance_counts[instance_type]
        return int(total_cores), int(total_memory)

    def _get_allocated_resources(self) -> ResourceDetails:
        total_cores = None
        total_memory = None
        total_disk = None
        instance_counts = None

        try:
            from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
            emr_client = self._emr_hook.get_client_type()
            ec2_client = EC2Hook(aws_conn_id="aws_default", region_name="us-east-1", api_type="client_type").get_client_type()

            instance_counts_by_fleet, instance_counts = self._get_instance_counts(emr_client)

            total_disk = self._get_ebs_storage(instance_counts_by_fleet, emr_client)

            total_cores, total_memory = self._get_cores_and_memory(instance_counts_by_fleet, ec2_client)
        except Exception as e:
            self.log.error("There was an issue with the retrieval of the allocated resources")
            self.log.error(e, exc_info=True)

        return ResourceDetails(cores=total_cores, memory=total_memory, disk=total_disk, instance_counts=instance_counts)

    def execute(self, context: Context) -> Any:
        cluster_metric_pusher = ClusterLifecycleMetricPusher()

        try:
            EmrClusterLink.persist(
                context=context,
                operator=self,
                region_name=self.region_name,
                aws_partition=self._emr_hook.conn_partition,
                job_flow_id=self.job_flow_id,
            )
            EmrLogsLink.persist(
                context=context,
                operator=self,
                region_name=self.region_name,
                aws_partition=self._emr_hook.conn_partition,
                job_flow_id=self.job_flow_id,
                log_uri=get_log_uri(emr_client=self._emr_hook.get_conn(), job_flow_id=self.job_flow_id),
            )
        except Exception as e:
            self.log.error("There was an error creating the logs link")
            self.log.error(e, exc_info=True)

        try:
            maybe_waiter = self.with_retry(
                lambda: self._emr_hook.get_waiter("cluster_running").wait(
                    ClusterId=self.job_flow_id,
                    WaiterConfig=prune_dict({
                        "Delay": 60,
                        "MaxAttempts": 120
                    }),
                ),
                lambda ex: isinstance(ex, WaiterError) and self._should_keep_waiting(ex.last_response),
            )
            maybe_waiter.get()
        except WaiterError as e:
            self.log.error("Cluster doesn't seem to be in a successful state!")
            self.log.error(e, exc_info=True)

            creation_successful, error_details = self._get_cluster_creation_failure_details()
            cluster_metric_pusher.cluster_startup_concluded(
                cluster_id=self.job_flow_id,
                context=context,
                success=creation_successful,
                error_code=error_details.code,
                error_message=error_details.message
            )

        except RetryLimitException as e:
            self.log.error("We got rate limited too many times. Assuming creation was successful")
            self.log.error(e, exc_info=True)

            cluster_metric_pusher.cluster_startup_concluded(cluster_id=self.job_flow_id, context=context, success=True)
        else:
            self.log.info("JobFlow created successfully")
            resources = self._get_allocated_resources()
            cluster_metric_pusher.cluster_startup_concluded(
                cluster_id=self.job_flow_id,
                context=context,
                success=True,
                total_cores=resources.cores,
                total_memory=resources.memory,
                total_disk=resources.disk,
                instance_counts=resources.instance_counts
            )
