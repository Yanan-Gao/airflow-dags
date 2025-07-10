from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.xcom_arg import PlainXComArg
from airflow.operators.empty import EmptyOperator


@task
def print_value(v: str):
    print(v)
    return v


p: PlainXComArg = print_value.expand(v=["qer"])  # type: ignore

with DAG(
        dag_id="demo-dynamic-task",
        start_date=datetime(2024, 3, 4),
        schedule=None,
) as dag:

    @task
    def make_list() -> list[int]:
        return [1, 2, 3, 4, 5]

    @task
    def show(x: int):
        print(f"Hello {x}")

    @task
    def some_task():
        print("Some task no action")

    # p.operator.dag = dag
    show_task = show.expand(x=make_list())

    c = EmptyOperator(task_id="c")

    show_task >> c >> p

    # print(f"make_list: {type(make_list)} {type(make_list())} {repr(make_list)} {repr(make_list())}")
