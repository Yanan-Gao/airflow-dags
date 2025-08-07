import logging
import re
from base64 import b64decode
from functools import cached_property
from typing import Any, Union, List, Optional

import jwt
import requests
from airflow.providers.fab.auth_manager.models import Role
from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
from airflow.security import permissions
from airflow.security.permissions import (
    RESOURCE_DAG_RUN,
    RESOURCE_TASK_INSTANCE,
    ACTION_CAN_CREATE,
    ACTION_CAN_EDIT,
)
from cryptography.hazmat.primitives import serialization

from ttd.rbac.util import map_airflow_role, format_airflow_role

log = logging.getLogger(__name__)


class KeycloakSecurityManager(FabAirflowSecurityManagerOverride):
    READ_AUDIT_LOG = (permissions.ACTION_CAN_READ, permissions.RESOURCE_AUDIT_LOG)
    ACCESS_MENU_AUDIT_LOG = (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_AUDIT_LOG)

    @cached_property
    def public_key(self):
        oidc_issuer = self.appbuilder.get_app.config["OIDC_ISSUER"]

        key_der_base64 = requests.get(oidc_issuer).json()["public_key"]
        key_der = b64decode(key_der_base64.encode())
        return serialization.load_der_public_key(key_der)

    @staticmethod
    def map_roles(groups):
        return [mapped_role for group in groups if (mapped_role := map_airflow_role(group)) != ""]

    def sync_roles(self) -> None:
        viewer_permissions = super().VIEWER_PERMISSIONS
        if self.READ_AUDIT_LOG not in viewer_permissions:
            viewer_permissions.append(self.READ_AUDIT_LOG)
        if self.ACCESS_MENU_AUDIT_LOG not in viewer_permissions:
            viewer_permissions.append(self.ACCESS_MENU_AUDIT_LOG)
        super().sync_roles()

    def get_oauth_user_info(self, provider: str, response: Any) -> dict[str, Union[str, list[str]]]:
        # Creates the user info payload from Keycloak.
        token = response["access_token"]
        me = jwt.decode(token, self.public_key, algorithms=["HS256", "RS256"], audience="account")
        groups = me["realm_access"]["roles"]

        user_teams = self._get_team_names(user_groups=groups)
        for user_team in user_teams:
            self._create_scrum_role_if_needed(team_name=user_team)

        airflow_roles = [
            self.auth_user_registration_role,
        ]
        mapped_roles = self.map_roles(groups)
        airflow_roles.extend(mapped_roles)

        userinfo = {
            "username": me.get("preferred_username"),
            "first_name": me.get("given_name", ""),
            "last_name": me.get("family_name", ""),
            "email": me.get("email", ""),
            "role_keys": airflow_roles,
        }
        log.info("User info: {0}".format(userinfo))
        return userinfo

    def _oauth_calculate_user_roles(self, userinfo) -> list[Role]:  # type: ignore
        user_role_objects = set()

        user_role_keys = userinfo.get("role_keys", [])
        for role_key in user_role_keys:
            fab_role = self.find_role(role_key)
            if fab_role is not None:
                user_role_objects.add(fab_role)
            else:
                log.warning("Can't find role: %s", role_key)

        return list(user_role_objects)

    @staticmethod
    def _get_team_names(user_groups: List[str]) -> List[str]:
        team_group_regex = re.compile("^SCRUM-([A-Z0-9]+)$")
        filtered_groups = [group for x in user_groups if (group := team_group_regex.search(x))]

        return [x.group(1) for x in filtered_groups]

    def _create_scrum_role_if_needed(self, team_name: Optional[str]) -> None:
        if team_name is None:
            return

        scrum_role_name = format_airflow_role(team_name)
        fab_role = self.find_role(scrum_role_name)
        if fab_role is None:
            self._create_fab_role(scrum_role_name)

    def _create_fab_role(self, role_name: str) -> Role:
        fab_role = self.appbuilder.sm.add_role(role_name)
        self._add_default_scrum_permissions(fab_role)

        return fab_role

    def _add_default_scrum_permissions(self, role: Role) -> None:
        permissions = [
            (ACTION_CAN_CREATE, RESOURCE_DAG_RUN),
            (ACTION_CAN_EDIT, RESOURCE_DAG_RUN),
            (ACTION_CAN_CREATE, RESOURCE_TASK_INSTANCE),
            (ACTION_CAN_EDIT, RESOURCE_TASK_INSTANCE),
        ]
        for action, resource in permissions:
            sm_permission = self.appbuilder.sm.get_permission(action_name=action, resource_name=resource)
            self.appbuilder.sm.add_permission_to_role(role=role, permission=sm_permission)
