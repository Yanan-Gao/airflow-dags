from airflow.operators.python import PythonOperator

from ttd.eldorado.base import TtdDag
from datetime import datetime

from ttd.tasks.op import OpTask

job_schedule_interval = "0 * * * *"
job_start_date = datetime(2024, 8, 28)

dag = TtdDag(
    dag_id="demo-hello-world",
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    tags=["DATAPROC", "Demo"],
)

adag = dag.airflow_dag


def step_1():
    print("Hello World from Step 1!")


def step_2(val_from_template: str = "none"):
    print("Hello World from Step 2!")
    print(f"val_from_template={val_from_template}")


def step_parallel():
    print("Hello World from the Parallel Step!")


task_1 = OpTask(op=PythonOperator(task_id="step_1", python_callable=step_1))

task_2 = OpTask(
    op=PythonOperator(
        task_id="step_2",
        python_callable=step_2,
        op_kwargs={
            "val_from_template":
            '{{macros.ttd_extras.resolve_consul_url("aerospike-use-ramv.aerospike.service.useast.consul", port=3000, limit=1)}}'
        }
    )
)

task_parallel = OpTask(op=PythonOperator(task_id="step_parallel", python_callable=step_parallel))

dag >> task_1 >> task_2
dag >> task_parallel
