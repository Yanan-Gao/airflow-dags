import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import aerospike
from aerospike import Client
from aerospike import exception as aerospike_ex
import dns.resolver
import msgpack
import logging
from airflow.models import Variable

if TYPE_CHECKING:
    from pandas.core.interchange.dataframe_protocol import DataFrame

COLD_STORAGE_EPOCH = datetime(2018, 12, 1)
TTD_NAMESPACE = 'ttd-coldstorage-onprem'

# https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/Shared/Aerospike/TTD.Domain.Shared.Aerospike.Client/TTDAerospikeV3Client.cs#L277
# These values end up coming through in a dictionary keyed by the numeric values below. For example:
# {'md': {0: True, 8: 0, 9: 'd1d3395e-c1e1-d31a-2128-0dd49b96d78d'}}
METADATA_RECORD_USER_INACTIVE = 0
METADATA_RECORD_INACTIVE_REASON = 8
METADATA_RECORD_TDID = 9

AEROSPIKE_QUERY_POLICY = {
    'total_timeout': 20000,  # Increased timeout
    'socket_timeout': 10000,
    'max_retries': 2,
    'key': aerospike.POLICY_KEY_SEND,  # Explicitly send key
    'serialize_list_map': False,  # Disable complex type serialization
}


def _cold_storage_timestamp_to_datetime(cold_storage_timestamp):
    return COLD_STORAGE_EPOCH + timedelta(hours=cold_storage_timestamp)


def _resolve_consul_url(dns_name: str, port: int = None) -> str:
    try:
        # Set up DNS resolver
        resolver = dns.resolver.Resolver()
        resolver.timeout = 5.0
        resolver.lifetime = 10.0

        # Perform DNS lookup for 'A' records (IPv4 addresses)
        answers = resolver.resolve(dns_name, 'A')

        # Process all IPs in the response
        ips = []
        for answer in answers:
            ip = answer.to_text()
            if port is not None:
                ips.append(f"{ip}:{port}")
            else:
                ips.append(ip)

        aerospike_hosts = ",".join(ips)
        return aerospike_hosts

    except Exception as e:
        logging.error(f"Failed to resolve IPs: {e}")
        return ""


def get_aerospike_client():
    aerospike_user = Variable.get('aerospike-user-id')
    aerospike_password = Variable.get('aerospike-password')
    aerospike_host = Variable.get('aerospike-endpoint-name')
    aerospike_client = _create_aerospike_client(aerospike_host, aerospike_user, aerospike_password)

    if not aerospike_client:
        raise Exception("Unable to connect to aerospike")

    return aerospike_client


def _create_aerospike_client(aerospike_dns: str, username: str, password: str):
    # We split the address into a configuration object of this format: {'hosts' : [ ('10.100.149.150', 3000),
    # ('10.100.155.198', 3000) ]}
    retry_count = 0
    max_retries = 10
    while retry_count < max_retries:
        aerospike_addresses = _resolve_consul_url(aerospike_dns, 3000)
        if aerospike_addresses:
            break
        logging.info('Retrying ...')
        retry_count = retry_count + 1
        time.sleep(5)

    if not aerospike_addresses:
        logging.error(f'Unable to connect to aerospike after {max_retries} tries')
        return None

    hosts = [(host.split(":")[0], int(host.split(":")[1])) for host in aerospike_addresses.split(",")]
    aerospike_config = {
        'hosts': hosts,
        'user': username,
        'password': password,
        'connect_timeout': 60000,
        'policies': {
            'login_timeout_ms': 60000
        },
    }
    logging.info(f'Connecting with seed host: {hosts}')
    client = aerospike.client(aerospike_config).connect()
    return client


def _collect_records_to_dataframe(record) -> "DataFrame":
    import pandas as pd

    data: dict = {"TargetingDataId": [], "Expiration": []}

    for targeting_data_ids in record.keys():
        expiration_date_and_creation_timestamp_list = record[targeting_data_ids]
        data["TargetingDataId"].append(targeting_data_ids)
        data["Expiration"].append(_cold_storage_timestamp_to_datetime(expiration_date_and_creation_timestamp_list[0]))

    # Create a DataFrame from the collected data
    df = pd.DataFrame(data)
    return df


def _decode_aerospike_data(byte_data):
    try:
        # Decode with modified settings to allow ExtType
        decoded = msgpack.unpackb(byte_data, strict_map_key=False, raw=False)

        # Convert to dictionary format
        result = defaultdict(list)

        # Process the decoded data
        for key, values in decoded.items():
            # Handle ExtType keys by converting to integer
            if isinstance(key, msgpack.ExtType):
                key = int.from_bytes(key.data, byteorder='big')

            # Store the values if they're in list format
            if isinstance(values, list):
                result[key] = values

        return dict(result)

    except Exception as e:
        logging.error(f"Error decoding data: {e}")
        return None


def collect_key_to_dataframe(client: Client, guid: str) -> "DataFrame":
    policy = AEROSPIKE_QUERY_POLICY
    policy['deserialize'] = False

    a_key = (TTD_NAMESPACE, None, guid)
    try:
        # Try to get record exists first
        key, exists = client.exists(a_key)
        if not exists:
            return None

        # If record exists, try to get it
        (key, metadata, record) = client.select(a_key, ['ud'], policy)

        if 'ud' not in record:
            return None

        decoded_user_data = _decode_aerospike_data(record['ud'])

        if metadata is not None:
            return _collect_records_to_dataframe(decoded_user_data)
        else:
            return None
    except Exception as e:
        logging.error(f"Error reading record: {e}")
        raise e


def key_exists(client: Client, guid: str) -> bool:
    a_key = (TTD_NAMESPACE, None, guid)
    key, exists = client.exists(a_key)
    return exists


def get_user_metadata(client: Client, guid: str):
    try:
        policy = AEROSPIKE_QUERY_POLICY
        policy['deserialize'] = False
        (key, metadata, record) = client.get((TTD_NAMESPACE, None, guid), policy)
        if record is not None and 'md' in record:
            return {
                'UserOptOut': record['md'][METADATA_RECORD_USER_INACTIVE],
                'UserInactiveReason': record['md'][METADATA_RECORD_INACTIVE_REASON],
                'Guid': record['md'][METADATA_RECORD_TDID]
            }
        else:
            logging.error('Record not found or metadata not on record')
    except aerospike_ex.RecordNotFound:
        logging.info('RecordNotFound exception reading id')
    except Exception as e:
        logging.error(f"Error reading record: {e}")
        raise
    return None
