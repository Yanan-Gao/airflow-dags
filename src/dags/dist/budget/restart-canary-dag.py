from datetime import datetime, timedelta
import random
from airflow.sensors.python import PythonSensor

import boto3
from airflow.operators.python import PythonOperator
from opsapi.api.deployment_api import DeploymentApi
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator

from ttd.el_dorado.v2.base import TtdDag
from opsapi.api.node_api import NodeApi
from ttd.ops_api_hook import OpsApiHook
from ttd.tasks.op import OpTask
import logging

logger = logging.getLogger(__name__)

# DAG name
dag_name = "budget-restart-canary"

###############################################################################
# DAG Definition
###############################################################################

restart_canary_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2025, 1, 20),
    schedule_interval="0 14 * * *",
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/DoIOIQ',
    retries=3,
    max_active_runs=1,
    retry_delay=timedelta(minutes=5),
    slack_channel="#taskforce-budget-alarms-low-pri",
    slack_alert_only_for_prod=True,
    tags=["DIST"],
)

dag = restart_canary_dag.airflow_dag

###############################################################################
# Task Functions
###############################################################################


def check_for_budget_sherpa_deployment(active_deployments, role_name):
    """
    Checks if any active deployments match the given role name
    and were created by sherpa.
    """
    deployments = getattr(active_deployments, 'deployments', active_deployments)
    matching = [
        d for d in deployments
        if getattr(d, 'roleName', '').lower() == role_name.lower() and 'sherpa' in getattr(d, 'createdBy', '').lower()
    ]
    return bool(matching)


def sherpa_not_deploying(**kwargs):
    ops_api_hook = OpsApiHook(conn_id=kwargs.get('conn_id'), api_client_type=DeploymentApi)
    role_name = "BudgetServerLinux"
    response = ops_api_hook.get_active_deployments()
    is_deploying = check_for_budget_sherpa_deployment(response, role_name)

    if is_deploying:
        logger.info("Sherpa deployment active — will check again later.")
        return False
    else:
        logger.info("No active Sherpa deployment — proceeding.")
        return True


def replace_instance(**kwargs):
    ops_api_hook_node = OpsApiHook(conn_id=kwargs.get('conn_id'), api_client_type=NodeApi)
    name_fragment = "use-budlcan"
    role_name = "BudgetServerLinux"
    cluster = 7

    try:
        response = ops_api_hook_node.client.node_get(name_fragment=name_fragment, role_name=role_name, cluster=cluster)
        nodes = response if response else []
        if not nodes:
            raise ValueError("No nodes found with the specified filters.")

        instance_id = random.choice([node["instance_id"] for node in nodes])
    except Exception as e:
        raise RuntimeError(f"Failed to fetch node details: {str(e)}")

    try:
        ec2 = boto3.client('ec2')
        ec2.terminate_instances(InstanceIds=[instance_id])
        logger.info(f"Terminated budget canary instance: {instance_id}")
    except Exception as e:
        raise RuntimeError(f"Failed to reboot instance: {str(e)}")


###############################################################################
# Tasks
###############################################################################

wait_for_no_sherpa_deployment = OpTask(
    op=PythonSensor(
        task_id="wait_for_no_sherpa_deployment",
        python_callable=sherpa_not_deploying,
        op_kwargs={'conn_id': 'ttdopsapi'},
        poke_interval=1800,  # check every 30 minutes
        timeout=10800,  # max wait time: 3 hours
        mode="reschedule",  # frees up worker slot
        dag=dag,
    )
)

reboot_task = OpTask(
    op=PythonOperator(
        task_id='reboot_instance',
        python_callable=replace_instance,
        op_kwargs={'conn_id': 'ttdopsapi'},
        dag=dag,
    )
)

###############################################################################
# Final DAG Status Check
###############################################################################

final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

###############################################################################
# DAG Dependencies
##############################################################################
restart_canary_dag >> wait_for_no_sherpa_deployment >> reboot_task >> final_dag_status_step
