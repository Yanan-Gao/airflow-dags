import logging
from dags.pdg.data_subject_request.util import jira_util, aerospike_util, ses_util
from dags.pdg.data_subject_request.dsr_item import DsrItem
from typing import List


# Performs a "fast check" to determine if the email addresses provided have data
# in our systems.  If data is found, the ticket is transitioned to the state supplied
# by the `jira_next_status` parameter.
# params:
#   dsr_items: list of DsrItem objects based on the jira queue
#   jira_next_status: the jira status to use when data was found
#   jira_notify_users: list of emails to tag on jira ticket comments
# returns: List of DSR Items that will continue through the rest of the delete process
def fast_check(dsr_items: List[DsrItem], jira_next_status: str, jira_notify_users: List[str] = None) -> List[DsrItem]:
    if not dsr_items:
        logging.info('No items provided to fast check')
        return []

    items_for_deletion = []
    aerospike_client = aerospike_util.get_aerospike_client()

    try:
        for dsr_item in dsr_items:
            jira_ticket_number = dsr_item.jira_ticket_number

            if not dsr_item.guids:
                jira_util.post_comment_to_ticket_tagged(
                    jira_ticket_number, "Unable to run Fast Check. There are no IDs associated with this email.", jira_notify_users
                )
                jira_util.transition_jira_status([jira_ticket_number], jira_util.jira_done_status)
                ses_util.send_no_data_email(dsr_item.email)
                continue

            user_exists = any(aerospike_util.key_exists(aerospike_client, guid) for guid in dsr_item.guids)

            if user_exists:
                logging.info(f'Found cold storage segment data for request {jira_ticket_number}')
                jira_util.post_comment_to_ticket(jira_ticket_number, 'Fast Check completed - the user was found in our systems.')
                jira_util.transition_jira_status([jira_ticket_number], jira_next_status)
                items_for_deletion.append(dsr_item)
            else:
                logging.info(f'No cold storage segment data found for request {jira_ticket_number}')
                jira_util.post_comment_to_ticket_tagged(
                    jira_ticket_number, 'Fast Check completed - the user was NOT found in our systems.', jira_notify_users
                )
                jira_util.transition_jira_status([jira_ticket_number], jira_util.jira_done_status)
                ses_util.send_no_data_email(dsr_item.email)
    finally:
        aerospike_client.close()

    return items_for_deletion
