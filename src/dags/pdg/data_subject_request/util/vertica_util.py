import re
from airflow.models import Variable
from datetime import datetime, timedelta
import vertica_python
from typing import Dict, List, Any, Tuple
from airflow import AirflowException
from dags.pdg.data_subject_request.dsr_item import DsrItem

FEEDBACK_VERTICA_HOST = 'bi.useast01.vertica.adsrvr.org'
VERTICA_PORT = 5433

BIDFEEDBACK_LOOKBACK_DAYS = 90
BIDFEEDBACK_INTERVAL_DAYS = 2

# Constants for columns used for indexing when building results
UID2_COLUMN = 'UnifiedId2'
EUID_COLUMN = 'EUID'
TDID_COLUMN = 'TDID'
ZIP_COLUMN = 'Zip'
TTD_CONSENT_COLUMN = 'TTDHasConsentForDataSegmenting'

FEEDBACK_COLUMNS = [
    UID2_COLUMN,
    'Browser',
    'BrowserTDID',
    'City',
    'Country',
    'DeviceAdvertisingId',
    'DeviceMake',
    'DeviceModel',
    'DeviceType',
    'IPAddress',
    'Latitude',
    'Longitude',
    'LogEntryTime',
    'Metro',
    'ReferrerUrl',
    'Region',
    'Site',
    'UserAgent',
    'OSFamily as OperatingSystemFamily',
    'OS as OperatingSystem',
    ZIP_COLUMN,
    # EU-related columns
    EUID_COLUMN,
    TTD_CONSENT_COLUMN,
    # TDID is included for identification, but isn't included in the results
    TDID_COLUMN
]


def _generate_date_ranges(lookback_days, interval_days) -> List[Tuple[str, str]]:
    """
    Generate date ranges from `lookback_days` ago until today in chunks of `interval_days`.
    Args:
        lookback_days (int): How many days back to start.
        interval_days (int): Number of days per range.

    Returns:
        List[Tuple[str, str]]: List of (start, end) datetime tuples.
    """
    date_format = '%Y-%m-%d %H:%M'
    today = datetime.today()
    end_date = datetime.combine(today.date(), datetime.max.time())
    start_date = end_date - timedelta(days=lookback_days)
    start_date = datetime.combine(start_date.date(), datetime.min.time())

    date_ranges = []
    current_start = start_date

    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=interval_days - 1, hours=23, minutes=59), end_date)
        date_ranges.append((current_start.strftime(date_format), current_end.strftime(date_format)))
        current_start = current_end + timedelta(minutes=1)

    return date_ranges


# This is used in conjunction with the _get_feedback_identifiers function below.
# Since we've flattened the lists to make the vertica queries more efficient, we
# need to use this to see which records belong to which individuals after the
# queries have finished running.
def _item_lookup(euid, uid2, tdid, dsr_items: List[DsrItem]):
    for dsr_item in dsr_items:
        if uid2 and dsr_item.email_uid2 == uid2:
            return dsr_item

        if euid and dsr_item.email_euid == euid:
            return dsr_item

        if tdid and tdid in dsr_item.guids:
            return dsr_item

        if uid2 and dsr_item.phone_uid2 == uid2:
            return dsr_item

        if euid and dsr_item.phone_euid == euid:
            return dsr_item
    return None


def _get_full_feedback_query(start_date, end_date, euids, uid2s, tdids):
    euid_str = '\',\''.join(euids)
    uid2_str = '\',\''.join(uid2s)
    tdid_str = '\',\''.join(tdids)
    conditions = []

    if euid_str:
        conditions.append(f"EUID in ('{euid_str}')")

    if uid2_str:
        conditions.append(f"UnifiedId2 in ('{uid2_str}')")

    if tdid_str:
        conditions.append(f"TDID in ('{tdid_str}')")

    if not conditions:
        raise AirflowException('No identifier conditions - unable to query vertica')

    return f"""
        SELECT /*+DEPOT_FETCH(NONE)*/
           {','.join(FEEDBACK_COLUMNS)}
        FROM
            ttd.BidFeedback
        where
            LogEntryTime between '{start_date}' and '{end_date}'
            and (
                {' or '.join(conditions)}
            )
    """


# Create flattened lists of UID2, EUID, and TDID identifiers
# that can be used to query vertica in a single pass.
def _get_feedback_identifiers(dsr_items: List[DsrItem]):
    uid2s = []
    euids = []
    tdids = []
    for dsr_item in dsr_items:
        if dsr_item.email_uid2:
            uid2s.append(dsr_item.email_uid2)
        if dsr_item.email_euid:
            euids.append(dsr_item.email_euid)
        tdids.extend(dsr_item.guids)

        if dsr_item.phone_uid2:
            uid2s.append(dsr_item.phone_uid2)
        if dsr_item.phone_euid:
            euids.append(dsr_item.phone_euid)

    return uid2s, euids, tdids


def query_bidfeedback(dsr_items: List[DsrItem]) -> Dict[str, List[Any]]:
    connection_info = _get_connection_info()

    uid2s, euids, tdids = _get_feedback_identifiers(dsr_items)

    # We need at least one set of IDs to query vertica. This shouldn't happen, but this
    # check will save us from attempting to run a bad query.
    if not uid2s and not euids and not tdids:
        raise Exception("No user identifiers found! Unable to continue")

    # Generate all the queries we need up front. The queries seem to work
    # best using LogEntryTime ranges of two days.
    ranges = _generate_date_ranges(lookback_days=BIDFEEDBACK_LOOKBACK_DAYS, interval_days=BIDFEEDBACK_INTERVAL_DAYS)
    queries = [_get_full_feedback_query(start, end, euids, uid2s, tdids) for start, end in ranges]

    # BidFeedback results - a list of rows keyed by jira ticket number
    results: Dict[str, List[Any]] = {}
    for dsr_item in dsr_items:
        jira_ticket_number = dsr_item.jira_ticket_number
        results[jira_ticket_number] = []
        results[jira_ticket_number].append(_get_column_headers(dsr_item.request_is_eu))

    with vertica_python.connect(**connection_info) as conn:
        cur = conn.cursor()

        for query in queries:
            cur.execute(query)

            for row in cur.fetchall():
                uid2 = row[FEEDBACK_COLUMNS.index(UID2_COLUMN)]
                euid = row[FEEDBACK_COLUMNS.index(EUID_COLUMN)]
                tdid = row[FEEDBACK_COLUMNS.index(TDID_COLUMN)]

                dsr_item = _item_lookup(euid, uid2, tdid, dsr_items)
                if not dsr_item:
                    raise Exception(f'DSR item not found for identifiers[uid2: {uid2}, euid: {euid}, tdid: {tdid}]')

                jira_ticket_number = dsr_item.jira_ticket_number
                results[jira_ticket_number].append(_get_filtered_row(row, dsr_item.request_is_eu))

    return results


def _get_filtered_row(row, is_eu: bool):
    # EU requests get all of the rows listed in the FEEDBACK_COLUMNS list except TDID
    if is_eu:
        return row[0:FEEDBACK_COLUMNS.index(TTD_CONSENT_COLUMN) + 1]

    # Zip is the final column for US and other requests
    return row[0:FEEDBACK_COLUMNS.index(ZIP_COLUMN) + 1]


def _get_column_headers(is_eu: bool):
    headers = FEEDBACK_COLUMNS[0:FEEDBACK_COLUMNS.index(TTD_CONSENT_COLUMN) + 1]\
        if is_eu else FEEDBACK_COLUMNS[0:FEEDBACK_COLUMNS.index(ZIP_COLUMN) + 1]

    # This feels dirty. We need the aliasing in the query, but we don't want it in the output.
    return [re.sub(r"^(\w+\ as\ )(\w+)$", r"\2", header) for header in headers]


def _get_connection_info():
    return {
        'host': FEEDBACK_VERTICA_HOST,
        'port': VERTICA_PORT,
        'user': Variable.get('vertica-user'),
        'password': Variable.get('vertica-password')
    }
