"""Plugins example"""

from __future__ import annotations

import json

from airflow.auth.managers.models.resource_details import AccessView
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.www.auth import has_access_view
from flask import Blueprint, request
from flask_appbuilder import BaseView
from flask_appbuilder import expose
from flask_caching import Cache

from ttd.aws.emr.cluster_utils import get_cluster_desc, get_converted_steps, get_aws_emr_versions

app = get_airflow_app()

# Configure cache
cache_config = {
    "CACHE_TYPE": "SimpleCache",  # In-memory cache in the Flask process
    "CACHE_DEFAULT_TIMEOUT": 300  # Default timeout in seconds
}
cache = Cache(app, config=cache_config)


class AwsEmrClusterClone(BaseView):
    """Creating a Flask-AppBuilder View"""

    default_view = "index"

    @expose("/")
    @has_access_view(AccessView.PLUGINS)
    def index(self):
        app = get_airflow_app().appbuilder
        return self.render_template("aws_cluster_cloning/home.html")

    @expose("/cluster", methods=["GET"])
    @has_access_view()
    def find(self):

        cluster_id = request.args.get("cluster_id", default="")

        if cluster_id == "" or cluster_id is None:
            return self.render_template("aws_cluster_cloning/cluster.html")

        cluster_desc = get_cluster_desc(cluster_id)

        cluster_desc_text = json.dumps(cluster_desc, default=str, indent=2)

        steps = get_converted_steps(cluster_id)

        if cache.has("emr_versions"):
            emr_versions = cache.get("emr_versions")
        else:
            print("Not in cache!")
            emr_versions = get_aws_emr_versions()
            cache.set("emr_versions", emr_versions, timeout=0)

        return self.render_template(
            "aws_cluster_cloning/cluster.html",
            cluster_id=cluster_id,
            cluster_desc=cluster_desc_text,
            steps=steps,
            emr_versions=emr_versions,
            emr_release_label=cluster_desc["ReleaseLabel"],
        )

    @expose("/clone", methods=["POST"])
    @has_access_view()
    def clone(self):
        from ttd.aws.emr.cluster_utils import copy_cluster
        app = get_airflow_app().appbuilder

        cluster_id = request.form.get("cluster_id", "").strip()
        if cluster_id == "":
            return "Cluster Id can't be empty!"
        include_steps = request.form.get("include_steps", type=bool, default=False)

        steps = request.form.getlist("steps") if include_steps else []

        from flask_login import current_user
        import typing
        if typing.TYPE_CHECKING:
            from airflow.providers.fab.auth_manager.models import User
            current_user: User  # type: ignore[no-redef]

        result = copy_cluster(cluster_id, steps, current_user.username)

        new_cluster_id = result["JobFlowId"]

        return self.render_template(
            "aws_cluster_cloning/cloned_cluster.html", new_cluster_id=new_cluster_id, result=json.dumps(result, default=str, indent=2)
        )


# Creating a flask blueprint
cluster_clone_bp = Blueprint(
    "AWS EMR Cluster Cloning",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/aws_cluster_cloning",
)
