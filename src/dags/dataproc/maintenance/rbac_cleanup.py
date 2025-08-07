import logging
from datetime import datetime, timedelta
from typing import List

from airflow.operators.python import PythonOperator
from airflow import settings

from ttd.slack.slack_groups import dataproc
from ttd.eldorado.base import TtdDag
from ttd.tasks.op import OpTask

from sqlalchemy import or_, not_
from airflow.providers.fab.auth_manager.models import Role, Resource, Permission, assoc_user_role

dag_name = "airflow-rbac-cleanup"
dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2024, 5, 1),
    schedule_interval="0 2 * * 6",
    retries=1,
    retry_delay=timedelta(minutes=5),
    run_only_latest=True,
    slack_channel=dataproc.alarm_channel,
    slack_alert_only_for_prod=False,
    tags=["DATAPROC", "Maintenance"],
    default_args={"owner": "DATAPROC"},
)
adag = dag.airflow_dag


def get_orphaned_roles_list() -> List[str]:
    roles_list = []
    with settings.Session() as session:
        query = (
            session.query(Role.name)
            .outerjoin(assoc_user_role, Role.id == assoc_user_role.c.role_id)
            .outerjoin(Role.permissions)
            .outerjoin(Permission.resource)
            .filter(Role.name.like("SCRUM-%"))
            .filter(assoc_user_role.c.user_id.is_(None))
            .filter(or_(Resource.id.is_(None), not_(Resource.name.like("DAG:%"))))
            .distinct()
        )  # yapf: disable

        roles_list = session.execute(query).scalars().all()

    logging.info(f"Found {len(roles_list)} orphaned roles" + (f": {roles_list}" if len(roles_list) > 0 else ""))
    return roles_list


def delete_orphaned_roles() -> None:
    delete_query = """
    DELETE FROM ab_role r WHERE r.name = ANY(:names)
    """

    roles_list = get_orphaned_roles_list()
    with settings.Session() as session:
        if len(roles_list) > 0:
            session.execute(delete_query, {"names": roles_list})
            session.commit()


clean_orphaned_roles = OpTask(op=PythonOperator(
    task_id="cleanup-orphaned-roles",
    python_callable=delete_orphaned_roles,
    dag=adag,
))
dag >> clean_orphaned_roles
