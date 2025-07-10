import logging
import sys
from collections.abc import Callable
from datetime import date, timedelta, datetime
from typing import Optional

from airflow.models import DagRun
from airflow.utils.state import DagRunState

from datasources.datasources import Datasources
from ttd.cloud_provider import CloudProviders
from ttd.cloud_storages.cloud_storage_builder import CloudStorageBuilder


# One complication is, dates can be delayed due to failed runs and there is no hard
# constraint on how delayed they can be. Data-products team will rerun and USUALLY
# there will be a successful run in the next 2-3 days (we assume).
#
# For example suppose in the run dates in the above example, the run meant to be on 35
# actually finished successfully on 38. We would then have
#       - date=14: covering 14 days  0, 14
#       - date=21: covering 14 days  7, 21
#       - date=28: covering 14 days 14, 28
#       - date=38: covering 14 days 24, 38
#
# Searching for the second-most-recent-date by starting at 38-14=24, will still find
# but now there is a gap. date2=21 spans 7-21 and date1=38 spans 24-38, so days 22-23
# are not covered. It is a 28 day span but there is a 2 day gap.
#
# We won't try to fix this but will instead detect this and error out leaving it for the
# job to be run manually picking a specific user-profiles-date that is acceptable. A better
# implementation might be to pick 3 dates to avoid gaps OR keep going back until we find
# 2 dates that span 28 days fully.
#
def calculate_dates(run_date: date):
    cloud_storage = CloudStorageBuilder(CloudProviders.aws).build()
    most_recent_user_profile_date = (
        Datasources.ctv.upstream_forecast_profile_data
        .check_recent_data_exist(cloud_storage=cloud_storage, ds_date=run_date, max_lookback=14).get()
    )
    second_most_recent_user_profile_date = (
        Datasources.ctv.upstream_forecast_profile_data.check_recent_data_exist(
            cloud_storage=cloud_storage,
            ds_date=most_recent_user_profile_date - timedelta(days=14),
            max_lookback=14,
        ).get()
    )
    logging.info(f'Using user prof 1 date {most_recent_user_profile_date.strftime("%Y-%m-%d")}')
    logging.info(f'Using user prof 2 date {second_most_recent_user_profile_date.strftime("%Y-%m-%d")}')
    assert_dates(second_most_recent_user_profile_date, most_recent_user_profile_date)
    return most_recent_user_profile_date, second_most_recent_user_profile_date


def assert_dates(earlier_date: date, later_date: date):
    earlier_date_span_start = earlier_date - timedelta(days=14)
    later_date_span_start = later_date - timedelta(days=14)

    logging.info(f"Date1 span: {earlier_date_span_start} - {earlier_date}")
    logging.info(f"Date2 span: {later_date_span_start} - {later_date}")
    # Expectation:
    #
    # earlier_date_span.....date1
    #     |            |   later_date_span......date2
    #     |            |       |             |
    #   date=0      date=13  date=14      date=27
    #

    # Basic sanity check
    if later_date <= earlier_date:
        raise Exception(f"later_date = {later_date} is smaller (or equal) to earlier_date = {earlier_date}")

    # Ideally the gap between spans should be 1 day, i.e. the second span starts
    # the day after the first span ends - effectively it is continuous. In practice
    # there are failures and we can end up not such data, so we compromise by allowing
    # up to a week of gap. This is ok - user-data is not a high consistency dataset
    # and there is a lot of fuzziness around cookies/device-ids
    gap_between_spans = later_date_span_start - earlier_date
    if gap_between_spans > timedelta(days=1):
        logging.warning(f"Gap between spans is >= 1 day ({gap_between_spans})")
        if gap_between_spans >= timedelta(days=7):
            Exception(f"Gap between spans is >= 1 day ({gap_between_spans})")

    # earlier_date_span until date2 should span over 28 days at least
    overall_span = later_date - earlier_date_span_start
    if overall_span < timedelta(days=28):
        raise Exception("Dates do not span 28 days")

    logging.info("All user-profiles date checks succeeded")


def get_date() -> date:
    for i, arg in enumerate(sys.argv):
        if arg == "--date":
            if i + 1 < len(sys.argv):
                date_str = sys.argv[i + 1]
                return datetime.strptime(date_str, "%Y-%m-%d")
            raise Exception("No --date arg provided")
    now = datetime.now()
    return date(now.year, now.month, now.day)


def get_conf_or_default(key: str, default: str, **context) -> str:
    if context['dag_run'].conf is None or context['dag_run'].conf.get(key) is None:
        return default
    return context['dag_run'].conf.get(key)


def get_most_recent_successful_dag_run(dag_id: str, key: Callable[[DagRun], datetime] = DagRun.execution_date) -> Optional[datetime]:
    dag_runs: list[DagRun] = DagRun.find(dag_id=dag_id)
    dag_run_times = [key(dag_run) for dag_run in dag_runs if dag_run.state == DagRunState.SUCCESS]
    if len(dag_run_times) == 0:
        return None
    return max(dag_run_times)


def main():
    """This is runnable in airflow-local to make manual testing easy"""
    logging.basicConfig(
        format="%(asctime)s %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S",
        level=logging.INFO,
        handlers=[logging.StreamHandler()],
    )
    (date1, date2) = calculate_dates(get_date())
    logging.info(f"date1 = {date1}, date2 = {date2}")


if __name__ == "__main__":
    main()
