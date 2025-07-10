from typing import Any

from airflow.api_connexion.exceptions import BadRequest, NotFound
from airflow.api_connexion.schemas.user_schema import user_schema
from airflow.api_connexion.security import requires_access_custom_view
from airflow.api_connexion.types import APIResponse
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.security import permissions
from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import csrf

from flask_appbuilder import expose, BaseView
from flask import request
from marshmallow import ValidationError

from ttd_airflow_extended_api.models import userRole


class TtdExtendedApi(BaseView):
    route_base = "/api/ttd/v1"

    @csrf.exempt
    @expose("/user_role", methods=("POST", ))
    @requires_access_custom_view(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_USER)  # type: ignore
    def add_user_role(self) -> APIResponse:
        body = request.get_json()
        return self.manage_user_roles(body, append=True)

    @csrf.exempt
    @expose("/user_role", methods=("DELETE", ))
    @requires_access_custom_view(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_USER)  # type: ignore
    def remove_user_role(self) -> APIResponse:
        body = request.get_json()
        return self.manage_user_roles(body, append=False)

    @staticmethod
    def manage_user_roles(body: Any, append: bool = True) -> dict:
        if body is None:
            raise BadRequest(detail="The request body content is not a json")

        try:
            username, airflow_role = userRole.load(body)
        except ValidationError as err:
            raise BadRequest(detail=str(err.messages))

        security_manager = get_airflow_app().appbuilder.sm
        user = security_manager.find_user(username=username)
        if user is None:
            raise NotFound(
                title="User not found",
                detail=f"The User with username `{username}` was not found",
            )

        role = security_manager.find_role(airflow_role)
        if role is None:
            return user_schema.dump(user)

        if append:
            if role not in user.roles:
                user.roles.append(role)
        else:
            user.roles = [r for r in user.roles if r != role]
        security_manager.update_user(user)

        return user_schema.dump(user)


class ExtendedAPIPlugin(AirflowPlugin):
    name = "ttd_airflow_extended_api_plugin"
    appbuilder_views = [{"view": TtdExtendedApi()}]
