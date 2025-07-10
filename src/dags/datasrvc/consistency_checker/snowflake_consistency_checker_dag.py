from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator

from ttd.ttdslack import dag_post_to_slack_callback

from dags.datasrvc.consistency_checker.constants import snowflake_conn_id, lwdb_conn_id
from dags.datasrvc.consistency_checker.query_executor import SnowflakeQueryExecutor, MsSQLQueryExecutor, VerticaQueryExecutor
from dags.datasrvc.consistency_checker.validator import default_simple_row_validate
from dags.datasrvc.consistency_checker.util import read_query_file
from dags.datasrvc.materialized_gating.materialized_gating_sensor import MaterializedGatingSensor


class BaseConsistencyCheckerDag:
    """
    This object is the base consistency checker to validate the data consistency between different data sources.

    User can provide the queries and validation method to customize the checker behaviour.
    """

    def __init__(
        self,
        dag_id,
        owner,
        schedule_interval,
        dependency_gates,
        query_configs,
        query_executors,
        validator=default_simple_row_validate,
        slack_channel=None,
        tsg=None
    ):
        dag = DAG(
            dag_id,
            default_args={
                'owner': owner,
                'retries': 5,
                'retry_delay': timedelta(minutes=5),
                'retry_exponential_backoff': True,
                'max_retry_delay': timedelta(minutes=30)
            },
            start_date=datetime(2024, 1, 1),
            catchup=False,
            description=f'Consistency checker for {dag_id}',
            schedule_interval=schedule_interval,
            tags=['ConsistencyChecker', owner],
            on_failure_callback=None if not slack_channel else
            dag_post_to_slack_callback(dag_name=dag_id, step_name='parent dagrun', slack_channel=slack_channel, tsg=tsg),
        )

        self.airflow_dag = dag

        # materialized gating sensor
        upstream_data_sensor = MaterializedGatingSensor(
            task_id='data_ready',
            dag=dag,
            poke_interval=5 * 60,  # poke every 5 minute
            timeout=24 * 60 * 60,  # timeout 24 hours
            mode="reschedule",  # don't block a worker thread
            retries=3
        )

        for gatingType in dependency_gates:
            upstream_data_sensor.add_dependency(GatingTypeId=gatingType)

        # query executor tasks
        def run_query(query_config, query_executor, **context):
            startDate = context['data_interval_start']
            endDate = context['data_interval_end']
            print(f'Running query for data interval [{startDate}, {endDate})')

            query = read_query_file(query_config, owner)
            consistency_query = query.format(startDate=startDate, endDate=endDate)

            query_msg = f"""Consistency query is:
            {consistency_query}
            """
            print(query_msg)

            res = query_executor.run(query)
            context['ti'].xcom_push(key='query_result', value=res)

        query_executor_tasks = [
            PythonOperator(
                task_id=f'run_query_{query_executor.name}',
                dag=dag,
                python_callable=run_query,
                provide_context=True,
                op_args=[query_config, query_executor]
            ) for (query_config, query_executor) in zip(query_configs, query_executors)
        ]

        # validator task
        def validate_query_result(**context):
            startDate = context['data_interval_start']
            endDate = context['data_interval_end']
            print(f'Running validation for data interval [{startDate}, {endDate})')

            query_result = {}
            for query_executor in query_executors:
                query_result[query_executor.name] = context['ti'].xcom_pull(task_ids=f'run_query_{query_executor.name}', key='query_result')

            err_msg = validator(query_result=query_result)
            if err_msg != '':
                print("Consistency check is Inconsistent!")
                raise AirflowFailException(err_msg)  # AirflowFailException causes task to fail without retrying
            else:
                print("Consistency check is OK!")

        data_validator_task = PythonOperator(
            task_id='validate_query_result', dag=dag, python_callable=validate_query_result, provide_context=True
        )

        # set dependencies
        upstream_data_sensor >> query_executor_tasks >> data_validator_task


class SnowflakeLWDBConsistencyCheckerDag(BaseConsistencyCheckerDag):
    """
    This object is the consistency checker to validate the data consistency between snowflake and LWDB.

    User can provide the queries and validation method to customize the checker behaviour.
    """

    def __init__(
        self,
        dag_id,
        owner,
        schedule_interval,
        dependency_gates,
        snowflake_query_config,
        snowflake_warehouse,
        snowflake_database,
        snowflake_schema,
        lwdb_query_config,
        validator=default_simple_row_validate,
        slack_channel=None,
        tsg=None
    ):

        snowflake_query_executor = SnowflakeQueryExecutor(
            conn_id=snowflake_conn_id, warehouse=snowflake_warehouse, database=snowflake_database, schema=snowflake_schema
        )

        lwdb_query_executor = MsSQLQueryExecutor(conn_id=lwdb_conn_id, name='lwdb')

        super().__init__(
            dag_id,
            owner,
            schedule_interval,
            dependency_gates=dependency_gates,
            query_configs=[snowflake_query_config, lwdb_query_config],
            query_executors=[snowflake_query_executor, lwdb_query_executor],
            validator=validator,
            slack_channel=slack_channel,
            tsg=tsg
        )


class SnowflakeVerticaConsistencyCheckerDag(BaseConsistencyCheckerDag):
    """
    This object is the consistency checker to validate the data consistency between snowflake and vertica.

    User can provide the queries and validation method to customize the checker behaviour.
    """

    def __init__(
        self,
        dag_id,
        owner,
        schedule_interval,
        dependency_gates,
        snowflake_query_config,
        snowflake_warehouse,
        snowflake_database,
        snowflake_schema,
        vertica_conn_id,
        vertica_query_config,
        validator=default_simple_row_validate,
        slack_channel=None,
        tsg=None
    ):

        snowflake_query_executor = SnowflakeQueryExecutor(
            conn_id=snowflake_conn_id, warehouse=snowflake_warehouse, database=snowflake_database, schema=snowflake_schema
        )

        vertica_query_executor = VerticaQueryExecutor(conn_id=vertica_conn_id, name='vertica')

        super().__init__(
            dag_id,
            owner,
            schedule_interval,
            dependency_gates=dependency_gates,
            query_configs=[snowflake_query_config, vertica_query_config],
            query_executors=[snowflake_query_executor, vertica_query_executor],
            validator=validator,
            slack_channel=slack_channel,
            tsg=tsg
        )
