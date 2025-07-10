from typing import TYPE_CHECKING, Sequence

from databricks.sdk import WorkspaceClient
from airflow.providers.databricks.operators.databricks import XCOM_RUN_PAGE_URL_KEY, XCOM_RUN_ID_KEY, DatabricksCreateJobsOperator, DatabricksJobRunLink

from ttd.ttdenv import TtdEnv, TtdEnvFactory

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DatabricksCreateAndRunJobOperator(DatabricksCreateJobsOperator):
    """
    Custom implementation meant to combine the functionality of DatabricksCreateJobsOperator
    and DatabricksRunNowOperator. Having them as separate operators allows for a race condition
    if multiple dag runs are going at once. One dag run could override the job configuration
    set up by another dag run before it gets to run.

    But by combining them into one operator, we can use the max_active_tis_per_dag parameter
    to ensure exclusive execution per dag run, thereby eliminating the race condition
    """
    operator_extra_links = (DatabricksJobRunLink(), )

    template_fields: Sequence[str] = ("json", "databricks_conn_id", "run_job_json")

    def __init__(self, run_job_json: dict, use_unity_catalog: bool, team: str, env: TtdEnv, **kwargs):
        self.use_unity_catalog = use_unity_catalog
        self.team = team
        self.env = env
        super().__init__(max_active_tis_per_dag=1, **kwargs)
        self.run_job_json: dict = run_job_json

    def execute(self, context: "Context"):
        if self.use_unity_catalog:
            self.assign_service_principal_role()

        job_id = super().execute(context)

        self.run_job_json["job_id"] = job_id
        run_id = self._hook.run_now(self.run_job_json)

        run_page_url = self._hook.get_run_page_url(run_id)

        self.log.info(f"Run submitted with run_id: {run_id}")
        self.log.info(f"View run status, Spark UI, and logs at {run_page_url}")

        context["ti"].xcom_push(key=XCOM_RUN_ID_KEY, value=run_id)
        context["ti"].xcom_push(key=XCOM_RUN_PAGE_URL_KEY, value=run_page_url)

        return job_id

    def assign_service_principal_role(self):
        """
        Passes respective team/environment service principal role to create_job request.
        """
        if not self.team:
            raise Exception("Team name could not be inferred. Please specify your DAG file in your team's respective folder.")
        if not self.env:
            raise Exception("Environment could not be inferred.")
        if self.env != TtdEnvFactory.prod and self.env != TtdEnvFactory.prodTest:
            raise Exception("Databricks connections are not configured for Airflow Dev/Local/Staging. Please use a ProdTest environment.")

        conn = self._hook.get_conn()

        workspace_client = WorkspaceClient(
            host=conn.host,
            client_id=conn.login,
            client_secret=conn.password,
        )

        sp_list = workspace_client.service_principals.list()

        inferred_sp_name = f"scrum_{self.team.lower()}_{str(self.env).lower()}_sp"

        sp_application_id = None
        for sp in sp_list:
            if sp.display_name == inferred_sp_name:
                sp_application_id = sp.application_id
                break

        if sp_application_id is None:
            raise Exception(f"Databricks Service Principal '{inferred_sp_name}' not found. Failing job.")
        self.json["run_as"] = {"service_principal_name": sp_application_id}
