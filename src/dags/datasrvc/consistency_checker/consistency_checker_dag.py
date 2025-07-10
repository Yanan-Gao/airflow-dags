from datetime import datetime, timedelta
from typing import List
import time
import vertica_python

from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

from dags.datasrvc.materialized_gating.materialized_gating_sensor import MaterializedGatingSensor
from dags.datasrvc.consistency_checker.constants import lwdb_grain_hourly, lwdb_grain_daily
from dags.datasrvc.consistency_checker.vertica_cluster import VERTICA_CLUSTERS_TO_CHECK, US_AWS_VERTICA_CLUSTERS, ALL_EXPORTABLE_DATA_DOMAINS
from dags.datasrvc.consistency_checker.validator import default_validate, default_cross_cluster_validate
from dags.datasrvc.consistency_checker.util import read_query_file

from ttd.ttdslack import dag_post_to_slack_callback


class ConsistencyCheckerDag():
    """
    This object is the base consistency checker to validate the data consistency in vertica clusters.

    User can provide the vertica query and validation method to customize the checker behaviour.
    """

    def __init__(
        self,
        dag_id,
        owner,
        schedule_interval,
        dependency_gates,
        query_config,
        clusters=list(VERTICA_CLUSTERS_TO_CHECK.keys()),
        sources=[],
        validator=default_validate,
        on_consistency_success=[],
        on_consistency_failure=[],
        cross_cluster_enabled=False,
        cross_cluster_dependency_gates=[],
        cross_cluster_sources=[],
        cross_cluster_exported_data_domains=ALL_EXPORTABLE_DATA_DOMAINS,
        cross_cluster_validator=default_cross_cluster_validate,
        on_cross_cluster_consistency_success=[],
        on_cross_cluster_consistency_failure=[],
        slack_channel=None,
        tsg=None,
        sensor_poke_interval=5 * 60,  # poke every 5 minute
        sensor_poke_timeout=24 * 60 * 60,  # timeout 24 hours
        start_date=datetime.now() - timedelta(days=1),
        retry_on_inconsistent_result=False
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
            start_date=start_date,
            catchup=True,  # this parameter needs to set to True: https://thetradedesk.atlassian.net/browse/DATASRVC-4321
            description=f'Consistency checker for {dag_id}',
            schedule_interval=schedule_interval,
            tags=['ConsistencyChecker', owner],
            on_failure_callback=None if not slack_channel else
            dag_post_to_slack_callback(dag_name=dag_id, step_name='parent dagrun', slack_channel=slack_channel, tsg=tsg),
        )

        self.airflow_dag = dag

        def run_single_cluster_query(cluster_info, **context):
            startDate = context['data_interval_start']
            endDate = context['data_interval_end']
            print(f'Running query on cluster {cluster_info.name} for data interval [{startDate}, {endDate})')

            query = read_query_file(query_config, owner)
            consistency_query = query.format(dataDomainName=cluster_info.data_domain, startDate=startDate, endDate=endDate)
            print('Consistency_query is: ', consistency_query)

            conn = BaseHook.get_connection(cluster_info.conn_id)
            conn_info = {'host': conn.host, 'port': conn.port, 'user': conn.login, 'password': conn.password, 'database': conn.schema}

            with vertica_python.connect(**conn_info) as conn:
                print('Querying vertica')
                cur = conn.cursor('dict')
                cur.execute(consistency_query)
                res = cur.fetchall()
                print('Query result:', res)
                context['ti'].xcom_push(key='query_result', value=res)
                return res

        def run_single_cluster_check(cluster_info, **context):
            startDate = context['data_interval_start']
            endDate = context['data_interval_end']
            print(f'Running validation on cluster {cluster_info.name} for data interval [{startDate}, {endDate})')

            query_result = context['ti'].xcom_pull(task_ids=f'{cluster_info.name}_query', key='query_result')

            # run validation
            err_msg = validator(cluster_info=cluster_info, sources=sources, query_result=query_result)

            if err_msg != '' and retry_on_inconsistent_result:
                for _ in range(3):
                    print(err_msg)
                    print(
                        "Inconsistent result is detected, but retry_on_inconsistent_result flag is on, so will retry the query after 5 mins"
                    )
                    time.sleep(5 * 60)  # sleep 5 mins
                    query_result = run_single_cluster_query(cluster_info, **context)
                    err_msg = validator(cluster_info=cluster_info, sources=sources, query_result=query_result)
                    if err_msg == '':
                        break

            if err_msg != '':
                print("Consistency check is Inconsistent!")
                if on_consistency_failure:
                    try:
                        print("Running post check action on failure")
                        for f in on_consistency_failure:
                            f(dag_id, startDate, cluster_info)
                    except Exception as e:
                        print("Post check action failed:", e)

                raise AirflowFailException(err_msg)  # AirflowFailException causes task to fail without retrying
            else:
                print("Consistency check is OK!")
                if on_consistency_success:
                    try:
                        print("Running post check action on success")
                        for f in on_consistency_success:
                            f(dag_id, startDate, cluster_info)
                    except Exception as e:
                        raise AirflowException("Post check action failed") from e

        materialized_gating_grain = lwdb_grain_hourly
        if schedule_interval == '0 0 * * *' or schedule_interval == '@daily':
            materialized_gating_grain = lwdb_grain_daily

        single_cluster_checks: List = []
        for cluster_name in clusters:
            cluster_info = VERTICA_CLUSTERS_TO_CHECK[cluster_name]
            upstream_data_ready = MaterializedGatingSensor(
                task_id=f'{cluster_info.name}_data_ready',
                dag=dag,
                poke_interval=sensor_poke_interval,
                timeout=sensor_poke_timeout,
                mode="reschedule",  # don't block a worker thread
                retries=3
            )

            for gatingType in dependency_gates:
                upstream_data_ready.add_dependency(
                    GatingTypeId=gatingType, GrainId=materialized_gating_grain, TaskVariantId=cluster_info.task_variant
                )

            single_cluster_query = PythonOperator(
                task_id=f'{cluster_info.name}_query',
                dag=dag,
                python_callable=run_single_cluster_query,
                provide_context=True,
                op_args=[cluster_info]
            )

            single_cluster_check = PythonOperator(
                task_id=f'{cluster_info.name}_check',
                dag=dag,
                python_callable=run_single_cluster_check,
                provide_context=True,
                op_args=[cluster_info]
            )

            upstream_data_ready >> single_cluster_query >> single_cluster_check
            single_cluster_checks.append(single_cluster_check)

        if not cross_cluster_enabled:
            return

        cross_cluster_upstream_data_ready = MaterializedGatingSensor(
            task_id='cross_cluster_data_ready',
            dag=dag,
            poke_interval=sensor_poke_interval,
            timeout=sensor_poke_timeout,
            mode="reschedule",  # don't block a worker thread
            retries=3
        )

        for gatingType in cross_cluster_dependency_gates:
            for aws_cluster in US_AWS_VERTICA_CLUSTERS:
                cross_cluster_upstream_data_ready.add_dependency(
                    GatingTypeId=gatingType,
                    GrainId=materialized_gating_grain,
                    TaskVariantId=VERTICA_CLUSTERS_TO_CHECK[aws_cluster].task_variant
                )

        def refresh_cross_cluster_date_query(**context):
            startDate = context['data_interval_start']
            endDate = context['data_interval_end']
            print(f'Refreshing cross cluster data for data interval [{startDate}, {endDate})')

            if len(cross_cluster_dependency_gates) > 0:
                query = read_query_file(query_config, owner)

                for aws_cluster in US_AWS_VERTICA_CLUSTERS:
                    print(f'Refreshing dataset for cluster {aws_cluster}')
                    cluster_info = VERTICA_CLUSTERS_TO_CHECK[aws_cluster]

                    consistency_query = query.format(dataDomainName=cluster_info.data_domain, startDate=startDate, endDate=endDate)
                    print('Consistency_query is: ', consistency_query)

                    conn = BaseHook.get_connection(cluster_info.conn_id)
                    conn_info = {
                        'host': conn.host,
                        'port': conn.port,
                        'user': conn.login,
                        'password': conn.password,
                        'database': conn.schema
                    }

                    with vertica_python.connect(**conn_info) as conn:
                        print('Querying vertica')
                        cur = conn.cursor('dict')
                        cur.execute(consistency_query)
                        res = cur.fetchall()
                        print('Query result:', res)
                        context['ti'].xcom_push(key=f'{aws_cluster}_query_result', value=res)

        def run_cross_cluster_check(**context):
            startDate = context['data_interval_start']
            endDate = context['data_interval_end']
            print(f'Running cross cluster check for data interval [{startDate}, {endDate})')

            cross_cluster_data = {}
            for cluster_name in clusters:
                if cluster_name in US_AWS_VERTICA_CLUSTERS and len(cross_cluster_dependency_gates) > 0:
                    cross_cluster_data[cluster_name] = context['ti'].xcom_pull(
                        task_ids='cross_cluster_query', key=f'{cluster_name}_query_result'
                    )
                else:
                    cross_cluster_data[cluster_name] = context['ti'].xcom_pull(task_ids=f'{cluster_name}_query', key='query_result')

            err_msg = cross_cluster_validator(
                cross_cluster_sources=cross_cluster_sources,
                cross_cluster_exported_data_domains=cross_cluster_exported_data_domains,
                cross_cluster_data=cross_cluster_data
            )
            if err_msg != '':
                print("Consistency check is Inconsistent!")
                if on_cross_cluster_consistency_failure:
                    try:
                        print("Running post check action on failure")
                        for f in on_cross_cluster_consistency_failure:
                            f(dag_id, startDate)
                    except Exception as e:
                        print("Post check action failed:", e)

                raise AirflowFailException(err_msg)  # AirflowFailException causes task to fail without retrying
            else:
                print("Consistency check is OK!")
                if on_cross_cluster_consistency_success:
                    try:
                        print("Running post check action on success")
                        for f in on_cross_cluster_consistency_success:
                            f(dag_id, startDate)
                    except Exception as e:
                        raise AirflowException("Post check action failed") from e

        cross_cluster_query = PythonOperator(
            task_id='cross_cluster_query', dag=dag, python_callable=refresh_cross_cluster_date_query, provide_context=True
        )

        cross_cluster_check = PythonOperator(
            task_id='cross_cluster_check', dag=dag, python_callable=run_cross_cluster_check, provide_context=True
        )

        single_cluster_checks >> cross_cluster_upstream_data_ready >> cross_cluster_query >> cross_cluster_check
