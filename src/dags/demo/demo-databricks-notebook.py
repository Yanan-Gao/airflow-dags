from __future__ import annotations

from airflow.models import Param

from ttd.ec2.emr_instance_types.general_purpose.m6g import M6g
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.databricks.ebs_config import DatabricksEbsConfiguration
from ttd.eldorado.databricks.tasks.notebook_databricks_task import NotebookDatabricksTask
from ttd.eldorado.databricks.workflow import DatabricksWorkflow
from ttd.slack.slack_groups import dataproc

name = "demo-databricks-notebook"

cluster_tags = {"Team": dataproc.jira_team}
notebook_path = '/Shared/path/to/notebook'

demo_dag = TtdDag(
    dag_id=name,
    tags=["Demo"],
    params={
        "demo_int_param": 1,
        "demo_int_param_with_validation": Param(5, type="integer", minimum=0, maximum=100),
        "demo_str_param": "demo",
        "demo_str_param_with_validation": Param("demo", type="string", maxLength=100),
    }
)

cluster = DatabricksWorkflow(
    job_name=name,
    cluster_name=f"{name}_cluster",
    worker_node_type=M6g.m6g_xlarge().instance_name,
    worker_node_count=1,
    cluster_tags=cluster_tags,
    tasks=[
        NotebookDatabricksTask(
            job_name=name,
            notebook_path=notebook_path,
            notebook_params={
                "demo_int_param": "{{ params.demo_int_param }}",
                "demo_int_param_with_validation": "{{ params.demo_int_param_with_validation }}",
                "demo_str_param": "{{ params.demo_str_param }}",
                "demo_str_param_with_validation": "{{ params.demo_str_param_with_validation }}",
                "demo_datetime": "{{ data_interval_start.strftime(\"%Y-%m-%dT%H:00:00\") }}",
            },
        )
    ],
    ebs_config=DatabricksEbsConfiguration(ebs_volume_count=1, ebs_volume_size_gb=32),
)

demo_dag >> cluster
adag = demo_dag.airflow_dag
