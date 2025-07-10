import logging
import uuid
from datetime import datetime, timezone

import requests
from airflow.models import TaskInstance
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from dags.pdg.data_subject_request.util import dsr_dynamodb_util
from ttd.ttdenv import TtdEnvFactory
from dags.pdg.data_subject_request.util import serialization_util
from dags.pdg.data_subject_request.dsr_item import DsrItem

openpass_init_url = "https://dsr.internal.myopenpass.com/dsr/delete/init" \
    if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else "https://dsr.internal.stg.myopenpass.com/dsr/delete/init"
openpass_monitor_url = "https://dsr.internal.myopenpass.com/dsr/delete/" \
    if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else "https://dsr.internal.stg.myopenpass.com/dsr/delete/"


def init_openpass_delete(task_instance: TaskInstance, **kwargs):
    """
    Initiates the OpenPass delete process for each email
    Retries IDs with a 500 response up to three times, backing off exponentially
    Pushes a list of generated OpenPass Request IDs to xcom
    https://atlassian.thetradedesk.com/confluence/pages/viewpage.action?spaceKey=EN&title=Epic+Process+Document+-+OpenPass+DSR+Delete+Request
    """

    request_id = task_instance.xcom_pull(key='request_id')
    dsr_items = serialization_util.deserialize_list(task_instance.xcom_pull(key='dsr_items'), DsrItem)
    iso_timestamp = datetime.now(timezone.utc).isoformat()
    openpass_request_ids = []
    sort_key_to_openpass_request_id = {}
    id_to_monitor_url = {}

    for dsr_item in dsr_items:
        openpass_request_id = str(uuid.uuid4())
        request_body_json = {"dsr_id": openpass_request_id, "user_email": dsr_item.email, "dsr_created_on": iso_timestamp}
        response = make_request(request_body_json)

        if response.status_code == 200:
            sort_key = dsr_item.email_sort_key
            if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
                dsr_dynamodb_util.update_items_attribute(request_id, sort_key, 'openpass_request_id', openpass_request_id)
            else:
                logging.info(
                    f'Non prod environment. '
                    f'In prod we would persist pk:{request_id}, sk:{sort_key}, openpass_request_id:{openpass_request_id}'
                )
            sort_key_to_openpass_request_id[sort_key] = openpass_request_id
            openpass_request_ids.append(openpass_request_id)
            id_to_monitor_url[openpass_request_id] = openpass_monitor_url + openpass_request_id
        else:
            logging.error(
                f"Error encountered with openpass id: {openpass_request_id}, tied to email: {dsr_item.email}. "
                f"JSON response from server: {response.json()}"
            )

    task_instance.xcom_push(key='openpass_request_ids', value=openpass_request_ids)
    task_instance.xcom_push(key='sort_key_to_openpass_request_id', value=sort_key_to_openpass_request_id)
    task_instance.xcom_push(key='openpass_id_monitor_urls', value=id_to_monitor_url)


def make_request(request_body_json):
    retry_strategy = Retry(total=3, backoff_factor=2, status_forcelist=[500], allowed_methods=['POST'])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount('https://', adapter)
    return http.post(openpass_init_url, json=request_body_json)


def monitor_openpass_delete(**kwargs):
    """
    Monitors the progress of the requested OpenPass delete by pinging the server for an update on each ID
    https://atlassian.thetradedesk.com/confluence/pages/viewpage.action?spaceKey=EN&title=Epic+Process+Document+-+OpenPass+DSR+Delete+Request
    :return: a boolean - True if all IDs were successfully deleted from OpenPass, False if there are more to check
    """
    ti = kwargs['ti']
    unvalidated_request_ids = ti.xcom_pull(key='openpass_request_ids')
    id_to_monitor_url = ti.xcom_pull(key='openpass_id_monitor_urls')
    request_ids_still_processing = []

    for openpass_request_id in unvalidated_request_ids:
        url = id_to_monitor_url[openpass_request_id]
        response = requests.get(url)
        if response.status_code != 200:
            logging.error(f"Error encountered with openpass id: {openpass_request_id}. "
                          f"JSON response from server: {response.json()}")
        elif response.json()["status"] != "Completed" or response.status_code == 500:
            request_ids_still_processing.append(openpass_request_id)

    if request_ids_still_processing:
        ti.xcom_push(key='openpass_request_ids', value=request_ids_still_processing)
        logging.info(f"Remaining OpenPass IDs to validate: {request_ids_still_processing}")
        return False
    return True


def process_openpass_failure(**kwargs):
    request_id = kwargs['task_instance'].xcom_pull(key='request_id')
    unsuccessful_openpass_ids = kwargs['task_instance'].xcom_pull(key='openpass_request_ids')
    sort_key_to_openpass_request_id = kwargs['task_instance'].xcom_pull(key='sort_key_to_openpass_request_id')

    for sort_key in sort_key_to_openpass_request_id:
        deleted = True
        if sort_key_to_openpass_request_id[sort_key] in unsuccessful_openpass_ids:
            deleted = False

        if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod:
            dsr_dynamodb_util.update_items_attribute(request_id, f'{sort_key}', 'openpass_delete_success', deleted)
        else:
            logging.info(
                f'Non prod environment. '
                f'In prod we would persist pk:{request_id}, sk:{sort_key}, openpass_delete_success:{deleted}'
            )
