import logging
from typing import Dict, List
from airflow import AirflowException
from uid2_client import UnmappedIdentity, IdentityMapClient, IdentityMapInput
from dags.pdg.data_subject_request.util import tdid_hash
from dags.pdg.data_subject_request.dsr_item import DsrItem
from dags.pdg.data_subject_request.identity import DsrIdentity, DsrIdentityType, DsrIdentitySource


def handle_unmapped(unmapped_identities: Dict[str, UnmappedIdentity]):
    reasons_list = [(email, value.get_reason()) for (email, value) in unmapped_identities.items()]
    # Unmapped Reasons: https://unifiedid.com/docs/guides/snowflake_integration#values-for-the-unmapped-column
    if all(reason.lower() == 'optout' for (_, reason) in reasons_list):
        logging.info(f'At least one email has been opted out of EUID or UID2: {reasons_list}')
    elif reasons_list:
        # Raise AirflowException if there are other reasons besides 'optout'
        raise AirflowException(
            f"One or more invalid email addresses found: {reasons_list}. "
            f"See https://unifiedid.com/docs/guides/snowflake_integration#values-for-the-unmapped-column \n"
            f"We do not need to process already opted out IDs, please ignore these. "
        )


# Handles the mapping of human-readable IDs (email, phone) to UID2/EUID
# params:
#   dsr_items: list of DsrItem objects for processing
#   identity_client: UID2 or EUID IdentityMapClient
#   identity_type: IDENTITY_TYPE_UID2 ('uid2') or IDENTITY_TYPE_EUID ('euid')
#   request_id: string (GUID) request_id of the current process
# returns:
#   uiids_string: comma-separated list of strings containing all of the IDs found
def uid2_id_mapping(dsr_items: List[DsrItem], identity_client: IdentityMapClient, identity_type: DsrIdentityType, request_id: str):

    emails_list = [item.email for item in dsr_items]
    phones_list = [item.phone for item in dsr_items if item.phone]

    identity_map = identity_client.generate_identity_map(IdentityMapInput.from_emails(emails_list))
    identity_map_phone = identity_client.generate_identity_map(IdentityMapInput.from_phones(phones_list))

    if not identity_map.is_success() or not identity_map_phone.is_success():
        logging.error(f'Error calling {identity_type.value} service.')
        raise AirflowException(f'Identity service status code: {identity_map.status} (phone status: {identity_map_phone.status})')

    handle_unmapped(identity_map.unmapped_identities)
    handle_unmapped(identity_map_phone.unmapped_identities)

    email_to_ids_mapping = identity_map.mapped_identities
    phone_to_ids_mapping = identity_map_phone.mapped_identities

    opted_out_emails = [email for email in emails_list if email not in email_to_ids_mapping]
    opted_out_phones = [phone for phone in phones_list if phone not in phone_to_ids_mapping]

    all_uiids: List[str] = []
    for dsr_item in dsr_items:
        email = dsr_item.email
        if dsr_item.email not in emails_list:
            raise AirflowException(f"Returned email not found in list of emails: {email}")

        id = email_to_ids_mapping[email]

        if email in opted_out_emails:
            identifier = 'optout'
            tdid = request_id
            salt_bucket = 'optout'
        else:
            identifier = id.get_raw_uid()
            tdid = tdid_hash.tdid_hash(id.get_raw_uid())
            salt_bucket = id.get_bucket_id()

            all_uiids.extend((identifier, tdid))

        sort_key = f'{identifier}#{tdid}' if not dsr_item.email_sort_key else dsr_item.email_sort_key
        identity = DsrIdentity(identity_type, DsrIdentitySource.EMAIL, identifier, tdid, sort_key, salt_bucket)
        dsr_item.add_identity(identity)
        dsr_item.email_sort_key = sort_key

        # Check to see if we have a phone number associated with this request
        if dsr_item.phone:
            phone = dsr_item.phone
            id = phone_to_ids_mapping[phone]

            if phone in opted_out_phones:
                identifier = 'optout'
                tdid = request_id
                salt_bucket = 'optout'
            else:
                identifier = id.get_raw_uid()
                tdid = tdid_hash.tdid_hash(id.get_raw_uid())
                salt_bucket = id.get_bucket_id()

                all_uiids.extend((identifier, tdid))

            sort_key = f'{identifier}#{tdid}' if not dsr_item.phone_sort_key else dsr_item.phone_sort_key
            identity = DsrIdentity(identity_type, DsrIdentitySource.PHONE, identifier, tdid, sort_key, salt_bucket)
            dsr_item.add_identity(identity)
            dsr_item.phone_sort_key = sort_key

    uiids_string = ','.join(all_uiids)
    return uiids_string
