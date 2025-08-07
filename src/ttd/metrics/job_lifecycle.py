from ttd.metrics.metric_db_client import MetricDBClient

JOB_ADDED_SPROC = "metrics.job_added"
JOB_CONCLUDED_SPROC = "metrics.job_concluded"


class JobLifecycleMetricPusher(MetricDBClient):

    def job_added(
        self,
        cluster_id: str,
        run_id: str,
        job_id: str | int,
        jar_name: str,
        class_name: str,
    ):
        parameters = [
            cluster_id,
            run_id,
            str(job_id),
            jar_name,
            class_name,
        ]

        self.execute_sproc(JOB_ADDED_SPROC, parameters)

    def job_terminated(
        self,
        cluster_id: str,
        run_id: str,
        job_id: str | int,
        success: bool,
        fail_state: str | None = None,
        fail_reason: str | None = None,
    ):
        parameters = [
            cluster_id,
            run_id,
            str(job_id),
            success,
            fail_state,
            fail_reason,
        ]

        self.execute_sproc(JOB_CONCLUDED_SPROC, parameters)
