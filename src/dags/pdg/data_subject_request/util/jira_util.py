import json
import logging
from typing import List, Dict

import requests
from airflow import AirflowException
from requests.auth import HTTPBasicAuth

from dags.pdg.data_subject_request.util import secrets_manager_util
from dags.pdg.data_subject_request.dsr_item import DsrItem
from ttd.ttdenv import TtdEnvFactory
from ttd.ttdslack import get_slack_client
from io import StringIO

is_prod = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False
is_prodtest = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prodTest else False
is_dev = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.dev else False
base_url = "https://thetradedesk.atlassian.net"
jira_user = "svc_jira_api_prod@thetradedesk.com"

REQUEST_TYPE_ACCESS = "Access"
REQUEST_TYPE_DELETE = "Delete"

TEST_TICKET_KEY = "DSR-172"

jira_open_status = "Open"
jira_done_status = "Done"
jira_in_progress_status = "Work in progress"
jira_pending_status = "PENDING"
jira_ready_for_fulfillment_status = "Ready for Fulfillment"
jira_fulfillment_pending_status = "Fulfillment Pending"
jira_transition_map = {
    jira_open_status: "101",
    jira_pending_status: "121",
    jira_done_status: "131",
    jira_in_progress_status: "111",
    jira_ready_for_fulfillment_status: "91",
    jira_fulfillment_pending_status: "81",
}


def read_jira_dsr_queue(request_type: str, ticket_status: str = jira_open_status) -> List[Dict]:
    field_dsr_email = "customfield_10380"
    field_dsr_phone = "customfield_11793"
    field_dsr_request_type = "customfield_11255"
    field_dsr_region = "customfield_11745"
    field_dsr_request_type_filter = "cf[11255]"

    url = f'{base_url}/rest/api/latest/search/jql'
    jql = f'project = "DSR" and status = "{ticket_status}" and "{field_dsr_request_type_filter}" = "{request_type}"'
    body = {"jql": jql, "fields": ["summary", field_dsr_email, field_dsr_request_type, field_dsr_phone, field_dsr_region, "status"]}

    secrets = secrets_manager_util.get_secrets(secret_name='jira-airflow', region_name='us-west-2')
    headers = {
        'Content-Type': 'application/json',
    }
    auth = HTTPBasicAuth(jira_user, secrets['JIRA_API_KEY'])

    response_json = requests.request("POST", url, headers=headers, auth=auth, data=json.dumps(body)).json()

    dsr_items = []
    for issue in response_json['issues']:
        issue_fields = issue['fields']

        request_type_dict = issue_fields[field_dsr_request_type]
        request_type_value: str
        if request_type_dict is None:
            raise AirflowException(f'Request type cannot be None for jira ticket {issue["key"]}')
        else:
            request_type_value = request_type_dict['value']

        email = issue_fields[field_dsr_email].replace(" ", "")
        if email is None:
            raise AirflowException(f'Email cannot be None for jira ticket {issue["key"]}')

        phone = issue_fields[field_dsr_phone]
        if phone is not None:
            phone = phone.strip()

        region_dict = issue_fields[field_dsr_region]
        region: str
        if region_dict is None:
            raise AirflowException(f'Region cannot be None for jira ticket {issue["key"]}')
        else:
            region = region_dict['value']

        if (is_dev and issue['key'] == TEST_TICKET_KEY) or not is_dev:
            dsr_items.append(DsrItem(issue['key'], email, phone, region, request_type))

    return dsr_items


def transition_jira_status(jira_ticket_numbers: List[str], status: str):
    """
    To query for available transition states, see the `get_ticket_statuses` method in this file.

    :param jira_ticket_numbers: jira ticket numbers to be transitioned to new status
    :param status: the corresponding status you want to transition the ticket numbers to.  See the jira_transition_map
    """
    for ticket_number in jira_ticket_numbers:
        if not _can_update_ticket(ticket_number):
            continue

        url = f"{base_url}/rest/api/latest/issue/{ticket_number}/transitions"

        secrets = secrets_manager_util.get_secrets(secret_name='jira-airflow', region_name='us-west-2')
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        auth = HTTPBasicAuth(jira_user, secrets['JIRA_API_KEY'])

        data = {
            "transition": {
                "id": jira_transition_map[status]
            },
        }
        response = requests.request("POST", url, headers=headers, auth=auth, data=json.dumps(data))
        if response.status_code != 204:
            message = f'Error calling Jira REST API to transition ticket {ticket_number}.'
            logging.error(f'{message} Response from Jira {response}')
            message += f'Jira Link : <https://thetradedesk.atlassian.net/jira/browse/{ticket_number}|Link>.'
            get_slack_client().chat_postMessage(
                channel='#scrum-pdg-alerts', text=f'{message}  Please investigate and manually transition the ticket to In Progress.'
            )


def post_comment_to_ticket(jira_ticket_number: str, comment: str):
    if not _can_update_ticket(jira_ticket_number):
        return

    url = f"{base_url}/rest/api/latest/issue/{jira_ticket_number}/comment"

    secrets = secrets_manager_util.get_secrets(secret_name='jira-airflow', region_name='us-west-2')
    headers = {'Content-Type': 'application/json'}
    auth = HTTPBasicAuth(jira_user, secrets['JIRA_API_KEY'])

    request = {'body': comment}

    requests.request("POST", url, headers=headers, auth=auth, data=json.dumps(request))


# Helper function to post a comment but also tag the list of users that are provided.
def post_comment_to_ticket_tagged(jira_ticket_number: str, comment: str, emails: List[str]):
    if is_prodtest:
        return

    if not emails:
        post_comment_to_ticket(jira_ticket_number, comment)
        return

    user_tags = []
    for email in emails:
        user_info = search_users(email)
        if not user_info:
            logging.warning(f'User not found for email {email}. User will not be tagged in comment!')
            continue
        account_id = user_info['accountId']
        user_tags.append(f'[~accountid:{account_id}]')

    tag_str = ' '.join(user_tags)
    post_comment_to_ticket(jira_ticket_number, f'{tag_str} {comment}')


# Uses the Jira API to search for users by email address. The response information can be
# used to tag users in comments, among other things.
def search_users(email):
    url = f'{base_url}/rest/api/latest/user/search?query={email}'

    secrets = secrets_manager_util.get_secrets(secret_name='jira-airflow', region_name='us-west-2')
    headers = {'Content-Type': 'application/json'}
    auth = HTTPBasicAuth(jira_user, secrets['JIRA_API_KEY'])

    # Users are returned as an array, but when we search by email we
    # should only have a single result.
    user_info = requests.request("GET", url, headers=headers, auth=auth).json()
    if len(user_info) == 0:
        return None
    return user_info[0]


# This isn't used anywhere in the code, but it's helpful to call to get a list
# of possible status codes in the DSR project.  For example:
#   transitions = jira_util.get_ticket_transitions()
#   print(transitions)
def get_ticket_transitions():
    url = f"{base_url}/rest/api/latest/issue/{TEST_TICKET_KEY}/transitions"

    secrets = secrets_manager_util.get_secrets(secret_name='jira-airflow', region_name='us-west-2')
    headers = {'Content-Type': 'application/json'}
    auth = HTTPBasicAuth(jira_user, secrets['JIRA_API_KEY'])

    return requests.request("GET", url, headers=headers, auth=auth).json()


# Uploads an attachment to the specified jira ticket
# params:
#   jira_ticket_number: the ticket number to attach the file to
#   filename: the name of the attachment as it appears on the ticket
#   file_obj: StringIO file to upload
def upload_attachment(jira_ticket_number: str, filename: str, file_obj: StringIO):
    url = f"{base_url}/rest/api/latest/issue/{jira_ticket_number}/attachments"

    secrets = secrets_manager_util.get_secrets(secret_name='jira-airflow', region_name='us-west-2')
    auth = HTTPBasicAuth(jira_user, secrets['JIRA_API_KEY'])

    headers = {
        "X-Atlassian-Token": "no-check",  # Required for file uploads
    }

    files = {"file": (filename, file_obj)}
    requests.post(url, headers=headers, auth=auth, files=files)


def _can_update_ticket(jira_ticket_number) -> bool:
    if is_prod:
        return True

    return is_dev and jira_ticket_number == TEST_TICKET_KEY
