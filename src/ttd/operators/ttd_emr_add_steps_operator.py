from typing import Optional, FrozenSet, List, Union

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.emr import (
    EmrHook,
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
)
from airflow.providers.amazon.aws.links.emr import (
    EmrClusterLink,
    get_log_uri,
)
from ttd.aws.emr.cluster_logs_link import EmrLogsLink
from airflow.models import DAG, TaskInstance, XCom

from ttd.mixins.retry_mixin import RetryMixin
from ttd.monads.maybe import Maybe, Nothing, Just
from ttd.metrics.opentelemetry.ttdopentelemetry import get_or_register_counter, push_all

import botocore


class TtdEmrAddStepsOperator(EmrAddStepsOperator, RetryMixin):
    """
    An operator that adds steps to an existing EMR job_flow.

    :param job_flow_id: id of the JobFlow to add steps to. (templated)
    :type job_flow_id: Optional[str]
    :param job_flow_name: name of the JobFlow to add steps to. Use as an alternative to passing
        job_flow_id. will search for id of JobFlow with matching name in one of the states in
        param cluster_states. Exactly one cluster like this should exist or will fail. (templated)
    :type job_flow_name: Optional[str]
    :param cluster_states: Acceptable cluster states when searching for JobFlow id by job_flow_name.
        (templated)
    :type cluster_states: list
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    :param steps: boto3 style steps or reference to a steps file (must be '.json') to
        be added to the jobflow. (templated)
    :type steps: list|str
    :param do_xcom_push: if True, job_flow_id is pushed to XCom with key job_flow_id.
    :type do_xcom_push: bool
    """

    # The _serialized_fields are lazily loaded when get_serialized_fields() method is called
    __serialized_fields: Optional[FrozenSet[str]] = None

    def __init__(
        self,
        *args,
        job_flow_id: Optional[str] = None,
        job_flow_name: Optional[str] = None,
        cluster_states: Optional[List[str]] = None,
        aws_conn_id: str = "aws_default",
        region_name: Optional[str] = None,
        steps: Optional[Union[List[dict], str]] = None,
        max_retries: int = 6,
        exponential_retry: bool = True,
        retry_interval: int = 2,
        **kwargs,
    ):
        super(TtdEmrAddStepsOperator, self).__init__(
            *args,
            job_flow_id=job_flow_id,
            job_flow_name=job_flow_name,
            cluster_states=cluster_states,
            aws_conn_id=aws_conn_id,
            steps=steps,
            **kwargs,
        )
        RetryMixin.__init__(
            self,
            max_retries=max_retries,
            retry_interval=retry_interval,
            exponential_retry=exponential_retry,
        )
        self.region_name = region_name

    def execute(self, context):
        if isinstance(self.steps, str):  # type: ignore
            self.steps = eval(self.steps)  # type: ignore
        try:
            self._push_emr_metrics(context)
        except Exception as e:
            self.log.error("Pushing emr metrics failed", exc_info=e)

        # If no region_name was passed in (i.e. we're running in us-east-1), the default functionality is fine.
        # Otherwise, we need to use our custom logic
        if self.region_name is None:
            execute_op = lambda: super(TtdEmrAddStepsOperator, self).execute(context)
        else:
            execute_op = lambda: self.execute_with_region(context)

        return self.with_retry(execute_op, lambda ex: isinstance(ex, botocore.exceptions.ClientError)).get()

    def execute_with_region(self, context):
        """
        This is a blatant copy-paste of the base class execute(self, context) function, but with region_name added
        to the EmrHook. Why? Because the API is inconsistent and adds unnecessary complexity. The CreateJobFlow operator
        takes in region_name as an argument and works just fine for starting a cluster in the specified AWS region.
        But none of the other EMR operators or sensors take in that argument, despite the fact they all COULD because
        they all still just wrap around EmrHook. Instead, they want you to do this weird thing with defining alternate
        aws connection IDs, but none of the docs actually explain how to do this clearly. So this is an attempt to
        address this weird gap in the API for what should be very basic functionality
        """
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

        emr = emr_hook.get_conn()

        job_flow_id = self.job_flow_id or emr_hook.get_cluster_id_by_name(self.job_flow_name, self.cluster_states)  # type: ignore
        if not job_flow_id:
            raise AirflowException("No cluster found for name: " + self.job_flow_name)  # type: ignore

        if self.do_xcom_push:
            context["ti"].xcom_push(key="job_flow_id", value=job_flow_id)

        EmrClusterLink.persist(
            context=context,
            operator=self,
            region_name=self.region_name,
            aws_partition=emr_hook.conn_partition,
            job_flow_id=job_flow_id,
        )
        EmrLogsLink.persist(
            context=context,
            operator=self,
            region_name=self.region_name,
            aws_partition=emr_hook.conn_partition,
            job_flow_id=self.job_flow_id,
            log_uri=get_log_uri(emr_client=emr_hook.conn, job_flow_id=job_flow_id),
        )

        self.log.info("Adding steps to %s", job_flow_id)
        response = emr.add_job_flow_steps(JobFlowId=job_flow_id, Steps=self.steps)

        if not response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            raise AirflowException("Adding steps failed: %s" % response)
        else:
            self.log.info("Steps %s added to JobFlow", response["StepIds"])
            return response["StepIds"]

    def _push_emr_metrics(self, context):
        emr_version = self.get_emr_version(context, self.job_flow_id)
        dag: DAG = context["dag"]
        dag_id = dag.dag_id
        task_id = context["task"].task_id
        airflow_emr_step_job = "airflow_emr_step_job"
        steps = self.steps

        if steps is None or len(steps) == 0:
            return

        counter = get_or_register_counter(
            airflow_emr_step_job,
            "airflow_emr_step",
            "Airflow emr steps with version label",
        )

        for step in steps:
            jar_name = self.get_jar_from_step(step)  # type: ignore
            if isinstance(jar_name, Nothing):
                self.log.debug("No jar found in step definition under HadoopJarStep.Args")
                self.log.debug(str(step))
                jar_name = ""

            class_name = self.get_class_from_step(step)  # type: ignore
            if isinstance(class_name, Nothing):
                self.log.debug("No '--class' arg found in step definition under HadoopJarStep.Args")
                self.log.debug(str(step))
                class_name = ""
            counter.labels({
                "jar_name": str(jar_name),
                "class_name": str(class_name),
                "emr_version": str(emr_version),
                "owner": dag.owner,
                "dag_id": dag_id,
                "task_id": task_id,
            }).inc()
        push_all(airflow_emr_step_job)

    @staticmethod
    def get_emr_version(context, job_flow_id) -> str:
        if job_flow_id is None:
            raise AirflowException("Unable to locate cluster when job_flow_id is None")

        task_instance: TaskInstance = context["task_instance"]
        dag: DAG = context["dag"]

        # Get all XComs for this run of the dag, and find the create jobflow task that matches the job_flow_id
        xcoms: List[XCom] = XCom.get_many(dag_ids=task_instance.dag_id, run_id=task_instance.run_id)
        create_task = None
        for xcom in xcoms:
            if xcom.value == job_flow_id and isinstance(dag.get_task(xcom.task_id), EmrCreateJobFlowOperator):
                create_task = dag.get_task(xcom.task_id)
                break

        if isinstance(create_task, EmrCreateJobFlowOperator):
            return create_task.job_flow_overrides.get("ReleaseLabel", "")  # type: ignore
        else:
            raise AirflowException(f"Unable to locate cluster for job_flow_id={job_flow_id}")

    @staticmethod
    def get_class_from_step(step: dict) -> Maybe[str]:
        if isinstance(step, dict):
            class_found = False
            for arg in step.get("HadoopJarStep", {}).get("Args", []):
                if class_found:
                    return Just(arg)
                if arg == "--class":
                    class_found = True
        return Nothing()

    @staticmethod
    def get_jar_from_step(step: dict) -> Maybe[str]:
        if isinstance(step, dict):
            args = step.get("HadoopJarStep", {}).get("Args", [])

            if len(args) and args[-1].lower().endswith(".jar"):
                return Just(args[-1])
            elif len(args) > 1 and args[-2].lower().endswith(".jar"):
                return Just(args[-2])
        return Nothing()

    @classmethod
    def get_serialized_fields(cls):
        if not cls.__serialized_fields:
            cls.__serialized_fields = frozenset(super().get_serialized_fields() | RetryMixin.serialized_fields)
        return cls.__serialized_fields
