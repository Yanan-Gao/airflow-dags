""" Entities used for working with Airflow RBAC. """
import re

# def get_admin_role():
#     from airflow.configuration import WEBSERVER_CONFIG
#     from flask import Config
#
#     conf = Config("")
#     conf.from_pyfile(WEBSERVER_CONFIG)
#     admin_role = conf["AUTH_ROLE_ADMIN"]
#     return admin_role

EXTERNAL_ROLE_NAME_REGEXP = "^prod-airflow-(?P<team>[a-z0-9]+)-edit$|^OnCall$"


def map_airflow_role(group_name: str) -> str:
    role_mapping = {
        'OCT': 'OCT-OperationalAccess',
        'SWAT': 'SWAT-OperationalAccess',
        'DATAPROC': 'Admin',
        'AIFUN': 'Admin',
    }

    role_pattern = re.compile(EXTERNAL_ROLE_NAME_REGEXP, re.IGNORECASE)
    match = role_pattern.fullmatch(group_name)
    if not match:
        return ""

    team_name = match.group('team')
    if team_name is None:
        return group_name
    team_name_upper = team_name.upper()
    return role_mapping.get(team_name_upper, format_airflow_role(team_name_upper))


def format_airflow_role(team_name: str) -> str:
    return f"SCRUM-{team_name.upper()}-EDIT"
