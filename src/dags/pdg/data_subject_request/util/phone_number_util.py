import phonenumbers
import logging
from typing import Optional, List
from dags.pdg.data_subject_request.util import jira_util
from dags.pdg.data_subject_request.dsr_item import DsrItem

JIRA_NOTIFY_USERS = ['james.abendroth@thetradedesk.com', 'brent.kobryn@thetradedesk.com']


# Parses a phone number without specifying the country. Phone numbers must be passed
# using the E164 format which begins with a + and the country code.
# Params: phone - phone number string in the E164 format
# Returns: bool - true if the number was parsed successfully, false otherwise
def is_valid_phone(phone: str) -> bool:
    try:
        return phonenumbers.is_valid_number(phonenumbers.parse(phone, region=None))
    except phonenumbers.NumberParseException:
        return False


def format_phone(phone: str) -> Optional[str]:
    if not is_valid_phone(phone):
        return None
    parsed_number = phonenumbers.parse(phone, None)
    return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)


# The function below takes the dsr_items list that's used throughout the DSAR and DSDR processes.
# It validates the phone numbers that were returned from jira and then does the following:
#   - If the phone is valid, formats the number so it can be used in a UID2 lookup
#   - If the phone is invalid, a comment is left on the ticket and the ticket is removed from processing
def validate_dsr_request_phones(dsr_items: List[DsrItem]) -> List[DsrItem]:
    valid_items = []
    for dsr_item in dsr_items:
        jira_ticket_number = dsr_item.jira_ticket_number
        phone = dsr_item.phone

        # Phone number is currently an optional field
        if not phone:
            valid_items.append(dsr_item)
            logging.info(f'Phone not provided for request {jira_ticket_number}')
            continue

        # Formatting also handles validation
        phone_formatted = format_phone(phone)
        if not phone_formatted:
            logging.info(f'Invalid phone number - removing {jira_ticket_number} from processing.')
            jira_util.post_comment_to_ticket_tagged(
                jira_ticket_number, f'Invalid phone number: {phone}. Please correct the value on this ticket.', JIRA_NOTIFY_USERS
            )
        else:
            dsr_item.phone = phone_formatted
            valid_items.append(dsr_item)

    return valid_items
