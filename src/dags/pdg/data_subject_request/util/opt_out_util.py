import logging
from datetime import datetime
from typing import Dict, List

import requests
from airflow import AirflowException
from airflow.models import TaskInstance
from ttd.ttdenv import TtdEnvFactory
from dags.pdg.data_subject_request.util import serialization_util
from dags.pdg.data_subject_request.dsr_item import DsrItem
from dags.pdg.data_subject_request.identity import DsrIdentityType
from dags.pdg.data_subject_request.util.prodtest_util import is_prodtest
'''
Note: Two types of opt outs are used here, batch and single.
Ideally, we'd only be using the batch opt out. There's currently an error with the endpoint where it won't return the
invalid UID2s if they are all invalid, just a 400. To handle this, the single opt out is used if we get back a 400
with no json body. This ensures we have the list of unsuccessful opt outs.
Ticket addressing this: https://atlassian.thetradedesk.com/jira/browse/PDG-370
'''

is_prod = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False


def opt_out(task_instance: TaskInstance, **kwargs):
    if is_prodtest:
        return

    dsr_items = serialization_util.deserialize_list(task_instance.xcom_pull(key='dsr_items'), DsrItem)

    uiids: Dict[str, List[str]] = dict()  # uiid_type -> [uiids (of particular type)]
    for dsr_item in dsr_items:
        for identity in dsr_item.identities:
            uiids.setdefault(identity.identity_type, []).append(identity.identifier)

    uid2s = uiids[DsrIdentityType.UID2]
    if uid2s:
        if not is_prod:
            logging.info(f'Opting out UID2s: {uid2s}')
        else:
            batch_opt_out_url = "https://insight.adsrvr.org/track/uid2optout"
            response = requests.post(batch_opt_out_url, json={"uid2Ids": uid2s})

            if response.status_code == 400:
                logging.error("All provided UID2s were invalid.")
                try:
                    error_string = response.json()
                except requests.exceptions.JSONDecodeError:
                    invalid_uid2s = _opt_out_individually(uid2s, 'uid2')
                    error_string = f"Invalid UID2s: {', '.join(invalid_uid2s)}"
                raise AirflowException(error_string)

            failed_uid2s_list = response.json()['UnsuccessfulOptOuts']
            invalid_uid2s_list = response.json()['InvalidUid2s']
            if len(invalid_uid2s_list) > 0:
                logging.warning(f"Invalid UID2s: {invalid_uid2s_list}")
            if len(failed_uid2s_list) > 0:
                logging.error(f"Unsuccessful UID2s: {failed_uid2s_list}")
                raise AirflowException(response.json())

    euids = uiids[DsrIdentityType.EUID]
    if euids:
        if not is_prod:
            logging.info(f'Opting out EUIDs: {euids}')
        else:
            invalid_euids = _opt_out_individually(euids, 'euid')
            if invalid_euids:
                raise AirflowException(f"Invalid EUIDs: {', '.join(invalid_euids)}")


def _opt_out_individually(uiids, uiid_type):
    """
    Currently, UID2 and EUID have optout endpoints.
    See https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/Beacon/TTD.Domain.Beacon.Host/appsettings.json
    """
    invalid_uiid = []
    for uiid in uiids:
        single_opt_out_url = f'https://insight.adsrvr.org/track/{uiid_type}optout?action=dooptout&' \
                             f'{uiid_type}={uiid}&' \
                             f'timestamp={int(datetime.today().timestamp())}'

        response = requests.request("GET", single_opt_out_url)
        if response.status_code != 200:
            invalid_uiid.append(uiid)
    return invalid_uiid
