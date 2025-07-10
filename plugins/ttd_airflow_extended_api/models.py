from typing import Tuple

from marshmallow import Schema, fields, post_load, validate
from ttd.rbac.util import EXTERNAL_ROLE_NAME_REGEXP, map_airflow_role


class UserRole(Schema):
    user = fields.String(required=True)
    role = fields.String(
        required=True,
        validate=validate.Regexp(
            regex=EXTERNAL_ROLE_NAME_REGEXP,
            error="Role value should either be AIRFLOW-<team>-EDIT or OnCall",
        ),
    )

    @post_load
    def get_airflow_role(self, data: dict, **kwargs) -> Tuple[str, str]:
        airflow_role = map_airflow_role(data["role"])

        return data["user"], airflow_role


userRole = UserRole()
