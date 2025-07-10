import logging
from typing import Tuple, Optional
from airflow import AirflowException

from dags.pdg.data_subject_request.util import secrets_manager_util
from dags.pdg.data_subject_request.identity import DsrIdentitySource
from uid2_client import IdentityMapClient, IdentityMapInput
from dags.pdg.data_subject_request.util import tdid_hash
"""
This util aids in communicating with the UnifiedID2 Service.
We are particularly interested in translating emails to uid2s.

https://unifiedid.com/docs/endpoints/post-identity-map
https://unifiedid.com/docs/guides/advertiser-dataprovider-guide
"""


def get_uid2_and_hashed_guid(identifier: str, identity_source: DsrIdentitySource,
                             uid2_client: IdentityMapClient) -> Optional[Tuple[str, str]]:
    match identity_source:
        case DsrIdentitySource.PHONE:
            identity_map_response = uid2_client.generate_identity_map(IdentityMapInput.from_phones([identifier]))
        case DsrIdentitySource.EMAIL:
            identity_map_response = uid2_client.generate_identity_map(IdentityMapInput.from_emails([identifier]))
        case _:
            raise AirflowException(f'Invalid identity source: {identity_source}')

    mapped_identities = identity_map_response.mapped_identities
    unmapped_identities = identity_map_response.unmapped_identities
    mapped_identity = mapped_identities.get(identifier)
    if mapped_identity is not None:
        uid2 = mapped_identity.get_raw_uid()
        guid = tdid_hash.tdid_hash(uid2)
        return uid2, str(guid)
    else:
        unmapped_identity = unmapped_identities.get(identifier)
        logging.info(f'{identity_source} is not mapped -- reason = {unmapped_identity.get_reason()}')
        return None


def create_client() -> IdentityMapClient:
    secrets = secrets_manager_util.get_secrets(secret_name='uid2-airflow', region_name='us-west-2')
    return IdentityMapClient(
        base_url="https://prod.uidapi.com", api_key=secrets['UID2_API_KEY'], client_secret=secrets['UID2_API_V2_SECRET']
    )
