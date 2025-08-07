import logging

from airflow import AirflowException
from typing import Optional, Tuple

from dags.pdg.data_subject_request.util import secrets_manager_util
from dags.pdg.data_subject_request.identity import DsrIdentitySource
from uid2_client import IdentityMapClient, IdentityMapInput
from dags.pdg.data_subject_request.util import tdid_hash
"""
This util aids in communicating with the EUID Service.
We are particularly interested in translating emails to EUIDs.

Currently, EUID service has no docs, however it should work the same as UID2 service.

https://unifiedid.com/docs/endpoints/post-identity-map
https://unifiedid.com/docs/guides/advertiser-dataprovider-guide
"""


def get_euid_and_hashed_guid(identifier: str, identity_source: DsrIdentitySource,
                             euid_client: IdentityMapClient) -> Optional[Tuple[str, str]]:
    match identity_source:
        case DsrIdentitySource.PHONE:
            identity_map_response = euid_client.generate_identity_map(IdentityMapInput.from_phones([identifier]))
        case DsrIdentitySource.EMAIL:
            identity_map_response = euid_client.generate_identity_map(IdentityMapInput.from_emails([identifier]))
        case _:
            raise AirflowException(f'Invalid identity source: {identity_source}')

    mapped_identities = identity_map_response.mapped_identities
    unmapped_identities = identity_map_response.unmapped_identities
    mapped_identity = mapped_identities.get(identifier)
    if mapped_identity is not None:
        euid = mapped_identity.get_raw_uid()
        guid = tdid_hash.tdid_hash(euid)
        return euid, str(guid)
    else:
        unmapped_identity = unmapped_identities.get(identifier)
        logging.info(f'{identity_source} is not mapped -- reason = {unmapped_identity.get_reason()}')
        return None


def create_client() -> IdentityMapClient:
    secrets = secrets_manager_util.get_secrets(secret_name='euid-airflow', region_name='us-west-2')
    return IdentityMapClient(base_url="https://prod.euid.eu", api_key=secrets['EUID_API_KEY'], client_secret=secrets['EUID_API_V2_SECRET'])
