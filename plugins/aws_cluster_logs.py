"""Plugins example"""

from __future__ import annotations
from airflow.auth.managers.models.resource_details import AccessView
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.www.auth import has_access_view
from flask import Blueprint, request, send_file, Response
from flask_appbuilder import BaseView
from flask_appbuilder import expose
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import gzip
import io
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REGIONS = [
    "us-east-1", "us-east-2", "us-west-1", "us-west-2", "ap-east-1", "ap-south-1", "ap-northeast-3", "ap-northeast-2", "ap-northeast-1",
    "ap-southeast-1", "ap-southeast-2", "ca-central-1", "eu-central-1", "eu-west-1", "eu-west-2", "eu-west-3", "eu-north-1", "sa-east-1"
]


class AwsEmrClusterLogs(BaseView):
    """Creating a Flask-AppBuilder View"""

    default_view = "index"

    @expose("/")
    @has_access_view(AccessView.PLUGINS)
    def index(self):
        app = get_airflow_app().appbuilder
        return self.render_template("emr_cluster_logs/home.html")

    @expose("/log_search")
    @has_access_view(AccessView.PLUGINS)
    def log_search(self):
        app = get_airflow_app().appbuilder
        return self.render_template("emr_cluster_logs/cluster_logs.html", **request.args, regions=REGIONS)

    @expose("/logs")
    def logs(self):
        from ttd.aws.emr.cluster_utils import fetch_cluster_logs
        cluster_id = request.args.get("cluster_id", "").strip()
        region = request.args.get("chosen_region", "").strip()

        # default region to us east 1
        if region == "":
            region = "us-east-1"
        cluster_logs = None
        if cluster_id != "":
            try:
                cluster_logs = fetch_cluster_logs(cluster_id, region)
            except Exception as e:
                return self.render_template(
                    "emr_cluster_logs/cluster_logs.html",
                    cluster_id=cluster_id,
                    chosen_region=region,
                    exception_message=str(e),
                    regions=REGIONS
                )
        return self.render_template(
            "emr_cluster_logs/cluster_logs.html", cluster_id=cluster_id, chosen_region=region, cluster_logs=cluster_logs, regions=REGIONS
        )

    def _get_s3_file_data(self):
        file_key = request.args.get("file_name", None)
        bucket = request.args.get("bucket", None)

        if not bucket or not file_key:
            raise ValueError(f"Missing bucket or file key in request. Bucket: {bucket}, File Key: {file_key}")

        s3_hook = S3Hook(aws_conn_id='aws_default')

        try:
            # get the S3 object body as bytes using the hook
            s3_object = s3_hook.get_key(key=file_key, bucket_name=bucket)
            file_data = s3_object.get()['Body'].read()
            return file_data

        except Exception as e:
            logger.info(f"Exception {e} occurred while fetching file data from s3")
            raise e

    @expose("/download")
    def download(self):
        file_key = request.args.get("file_name", None)
        file_data = self._get_s3_file_data()

        try:
            # wrap in an in-memory buffer
            buffer = io.BytesIO(file_data)
            buffer.seek(0)

            return send_file(buffer, as_attachment=True, download_name=file_key)

        except Exception as e:
            logger.info(f"Exception {e} occurred while sending s3 log file as attachment.")
            raise e

    @expose("/display_text")
    def display_text(self):
        file_data = self._get_s3_file_data()
        try:
            buffer = io.BytesIO(file_data)
            with gzip.GzipFile(fileobj=buffer, mode='rb') as gz:
                decompressed_data = gz.read()

            text = decompressed_data.decode('utf-8')

            return Response(f"<pre>{text}</pre>", mimetype="text/html")

        except Exception as e:
            logger.info(f"Exception {e} occurred while displaying s3 file in browser.")
            raise e


# Creating a flask blueprint
cluster_logs_bp = Blueprint(
    "AWS EMR Cluster Logs",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/emr_cluster_logs",
)
