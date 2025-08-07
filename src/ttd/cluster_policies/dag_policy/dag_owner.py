import logging

from airflow.models import DAG
from sqlalchemy.orm import Session
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.exceptions import AirflowClusterPolicyViolation
import os
from typing import Set, List
from cachetools.func import ttl_cache

from ttd.rbac.util import format_airflow_role
from ttd.dag_owner_utils import get_team_from_filelocation
from ttd.eng_org_structure_utils import get_scrum_teams

FALLBACK_TEAM_LIST = {
    "ADPB",
    "AIFUN",
    "AILAB",
    "APEX",
    "AUDAUTO",
    "AUX",
    "BAM",
    "BEX",
    "BI",
    "BID",
    "BINFRA",
    "BOOT",
    "CAMPEX",
    "CHGROW",
    "CHINA",
    "CHNL",
    "CLOUD",
    "CMKT",
    "CMO",
    "CRE",
    "CSOL",
    "CSX",
    "CTX",
    "CTXMP",
    "CX",
    "DATAOPS",
    "DATAPROC",
    "DATASRVC",
    "DATEX",
    "DATMKT",
    "DATPERF",
    "DATPRD",
    "DBAPI",
    "DC",
    "DCENG",
    "DESKUI",
    "DEVACC",
    "DIST",
    "DME",
    "DMG",
    "DMX",
    "DPRPTS",
    "DW",
    "DX",
    "EDGE",
    "FINENG",
    "FORECAST",
    "FTRSRV",
    "FWMKT",
    "GLOBAL",
    "HPC",
    "IDNT",
    "IDQS",
    "INFSRV",
    "INVMKT",
    "INVSEL",
    "JAN",
    "KPOP",
    "LDEV",
    "LINGO",
    "LUX",
    "MASS",
    "MEASURE",
    "MKTS",
    "MQE",
    "NET",
    "NOSQL",
    "OCT",
    "OMNIUX",
    "OPATH",
    "OPENPASS",
    "PARTPORTAL",
    "PBMKT",
    "PDG",
    "PFX",
    "PLATSEC",
    "PRODSEC",
    "PSR",
    "PUMA",
    "QI",
    "RELEASE",
    "SA",
    "SAV",
    "SCORE",
    "SE",
    "SECPOST",
    "SINC",
    "SMB",
    "SPIDER",
    "SRVEX",
    "SRVFAB",
    "SRVFUN",
    "SSPS",
    "ST",
    "SWAT",
    "SWITCHMATE",
    "TAG",
    "TREX",
    "TRGT",
    "TTDCP",
    "TV",
    "UID2",
    "USERTRAIL",
    "VSINC",
}


def get_team_list() -> Set[str]:
    try:
        teams = get_scrum_teams()
        return set(teams.keys())
    except Exception as e:
        logging.error(f"Failed to get teams list from eng-org-structure, falling back to FALLBACK_TEAM_LIST. Error: {e}")
        return FALLBACK_TEAM_LIST


def warm_team_list_cache() -> None:
    try:
        logging.info("Warming up team list cache...")
        get_team_list()
        logging.info("Team list cache warmed")
    except Exception as e:
        logging.warning(f"Team list cache warm-up failed: {e}")


def team_exists(team_name: str) -> bool:
    upper_cased_team = team_name.upper()
    return upper_cased_team in get_team_list()


DEMO_FOLDER = "demo"
NO_TEAM = "None"


# Assuming that we have a structure like: dags/<team-name>/etc
def fetch_dag_owner_from_filestructure(dag: DAG) -> str:
    # Relative file location. We're going to see if this then starts with dags/
    file_location = dag.fileloc

    try:
        team = get_team_from_filelocation(file_location)
    except ValueError as e:
        raise AirflowClusterPolicyViolation(e)

    return NO_TEAM if team is None or team == DEMO_FOLDER or team == "" else team.upper()


@provide_session
@ttl_cache(maxsize=1, ttl=600)
def get_role_list(
    session: Session = NEW_SESSION  # type: ignore
) -> List[str]:
    sql = "select name from ab_role"
    roles = session.execute(sql).scalars().all()
    return roles


def check_role_exists(role_name: str) -> bool:
    roles = get_role_list()
    return role_name in roles


# Update each of the owners to be the new thing
def update_dag_owner(dag: DAG, owner: str):

    for task in dag.tasks:
        try:
            task.owner = owner  # type: ignore
        except AttributeError:
            logging.warning(f"DAG {dag.dag_id} cannot have it's owner written. AttributeOwner in task: {task.task_id}!")


def validate_team(dag: DAG, team: str):
    if not team_exists(team):
        raise AirflowClusterPolicyViolation(
            f"DAG {dag.dag_id} is either labeled with the team: {team} or is placed inside of the directory: {team}. This team does not exist in the eng-org structure"
        )


def update_access_control(dag: DAG, team: str) -> None:
    if os.getenv('GITLAB_CI') == 'true':
        return

    access_control_extra = {}

    role_name = format_airflow_role(team)
    if check_role_exists(role_name):
        access_control_extra[role_name] = {"can_read", "can_edit", "can_delete"}

    if dag.access_control is not None:
        access_control_extra.update(dag.access_control)
    dag.access_control = access_control_extra


def assign_dag_owner(dag: DAG) -> None:
    team = fetch_dag_owner_from_filestructure(dag)

    update_dag_owner(dag, team)
    if team != NO_TEAM:
        validate_team(dag, team)
        update_access_control(dag, team)
