from abc import ABCMeta

from ttd.eldorado.base import TtdDag
from ttd.tasks.base import BaseTask
from typing import Union, Dict, List
from ttd.dag_owner_utils import infer_team_from_call_stack


class TaggedClusterMixin(BaseTask, metaclass=ABCMeta):

    def __init__(
        self,
        create_cluster_op_tags: Union[Dict[str, str], List[Dict[str, str]]],
        max_tag_length: int = 128,
        include_cluster_name_tag: bool = True,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.max_tag_length = max_tag_length
        self.include_cluster_name_tag = include_cluster_name_tag
        self.create_cluster_op_tags = create_cluster_op_tags

    @staticmethod
    def get_cluster_name(
        dag_name: str,
        task_name: str,
        max_tag_length: int,
        timestamp_macro: str = '{{ logical_date.strftime("%Y%m%d%H%M%S") }}',
    ) -> str:
        try_number = "{{ task_instance.try_number }}"
        formatted_dag_name = dag_name.lower().replace("-", "")
        formatted_task_type = task_name.lower().replace("-", "")
        cluster_name = f"job-{formatted_dag_name}-run-{timestamp_macro}try{try_number}-{formatted_task_type}"

        final_cluster_name_length = len(cluster_name) + (14 - len(timestamp_macro)) + (2 - len(try_number))
        if final_cluster_name_length > max_tag_length:
            raise ValueError('DAG/task names are too long to be used as a cluster tag.')
        return cluster_name

    @staticmethod
    def get_job_name(ttd_dag: TtdDag) -> str:
        job_name = ttd_dag.airflow_dag.dag_id.rpartition(".")[0]
        return ttd_dag.airflow_dag.dag_id if job_name is None or len(job_name) == 0 else job_name

    def generate_and_set_tags(self, ttd_dag: TtdDag) -> None:
        job_name: str = self.get_job_name(ttd_dag)
        cluster_name = self.get_cluster_name(job_name, self.task_id, self.max_tag_length)
        team_name = infer_team_from_call_stack()

        if isinstance(self.create_cluster_op_tags, dict):
            if "ClusterName" not in self.create_cluster_op_tags and self.include_cluster_name_tag:
                self.create_cluster_op_tags["ClusterName"] = cluster_name
            self.create_cluster_op_tags["Job"] = job_name
            self.create_cluster_op_tags["Service"] = job_name
            if team_name is not None:
                self.create_cluster_op_tags["Team"] = team_name.upper()

        elif isinstance(self.create_cluster_op_tags, list):
            existing_cluster_name = next(
                (tag for tag in self.create_cluster_op_tags if tag["Key"] == "ClusterName"),
                None,
            )
            if existing_cluster_name is None and self.include_cluster_name_tag:
                self.create_cluster_op_tags.append({"Key": "ClusterName", "Value": cluster_name})
            existing_job_name = next(
                (tag for tag in self.create_cluster_op_tags if tag["Key"] == "Job"),
                None,
            )
            if existing_job_name is None:
                self.create_cluster_op_tags.append({"Key": "Job", "Value": job_name})
            else:
                existing_job_name["Value"] = job_name

            existing_service_name = next(
                (tag for tag in self.create_cluster_op_tags if tag["Key"] == "Service"),
                None,
            )
            if existing_service_name is None:
                self.create_cluster_op_tags.append({"Key": "Service", "Value": job_name})
            else:
                existing_service_name["Value"] = job_name

            if team_name is not None:
                existing_team_name = next(
                    (tag for tag in self.create_cluster_op_tags if tag["Key"] == "Team"),
                    None,
                )
                if existing_team_name is None:
                    self.create_cluster_op_tags.append({"Key": "Team", "Value": team_name.upper()})
                else:
                    existing_team_name["Value"] = team_name.upper()
