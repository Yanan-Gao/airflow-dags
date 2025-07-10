from dags.forecast.sketches.operators.successful_dow_operator import FindMostRecentSuccessfulDowDates
from dags.forecast.sketches.randomly_sampled_avails.constants import LAST_GOOD_ISO_WEEKDAYS_KEY
from ttd.tasks.op import OpTask

_TASK_ID = "lookup_last_good_iso_weekdays"
_TARGET_TASKS = [
    "CreateContainmentRecordsCluster_cluster_aws_watch_task_CreateContainmentRecords",
    "append_ram_daily_containment_records_from_azure_to_s3_task.transfer_metadata_files"
]


class LookupLastGoodIsoWeekdays(OpTask):

    def __init__(self, dag):
        super().__init__(
            op=FindMostRecentSuccessfulDowDates(
                dag=dag,
                task_id=_TASK_ID,
                xcom_key=LAST_GOOD_ISO_WEEKDAYS_KEY,
                target_tasks=_TARGET_TASKS,
            )
        )
