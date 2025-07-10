# import base64
# import hashlib
# import hmac
# import json
# import logging
# from enum import Enum
# from typing import Dict, List, Callable, NamedTuple, Tuple, Any, Union
#
# import requests
# from airflow import AirflowException
#
# from dags.pdg.data_subject_request.util import secrets_manager_util, sql_util
# from dags.pdg.data_subject_request.util.dsr_dynamodb_util import PARTNER_DSR_ADVERTISER_ID, \
#     PARTNER_DSR_DATA_PROVIDER_ID, PARTNER_DSR_USER_ID_TYPE, PARTNER_DSR_USER_ID_GUID, PARTNER_DSR_USER_ID_RAW
# from dags.pdg.data_subject_request.util.sql_util import sql_batch_size
#
# # From https://partner.thetradedesk.com/v3/portal/data/doc/post-data-advertiser-firstparty
# # The total request size may not exceed 2.5 MB, which is approximately 10,000 IDs.
# # If needed, split large requests into several smaller requests that meet the size requirements.
# max_request_size_bytes = 2.0 * 1024 * 1024  # giving small buffer
#
#
# class DataServerRequestType(NamedTuple):
#     partner_key: str
#     partner_value: str
#     url: str
#     get_segments_callable: Callable[[str, int, int], List[int]]
#
#
# # possible values from
# # https://gitlab.adsrvr.org/thetradedesk/adplatform/blob/479cfa6936e7499bccdb2b7fd94f48ecefa8bc36/TTD/Domain/DataServer/TTD.Domain.DataServer/GenericAdvertiser/GenericDataImportDataHandler.cs#L128
# # and
# # https://gitlab.adsrvr.org/thetradedesk/adplatform/blob/dce1c212cec2080f98703ebeb3c08972871bf7c3/TTD/Domain/Shared/TTD.Domain.Shared.Identity/IdentitySource.cs#L22-L29
# class IdentitySource(Enum):
#     Unknown = 0
#     Tdid = 1
#     MiscDeviceId = 2
#     UnifiedId2 = 3
#     IdentityLinkId = 4
#     EUID = 5
#     Nebula = 6
#     DATId = 9
#     McId = 10
#     ID5 = 11
#     NetId = 12
#     FirstId = 13
#     CoreId = 1015
#
#
# def compute_hmac_sha1(key, json_body):
#     """
#     Computes the hash for the json body which is used in the signature for the request
#     https://partner.thetradedesk.com/v3/portal/data/doc/DataAuthentication
#     """
#     key = key.encode('utf-8')
#     message = json_body.encode('utf-8')
#
#     hmac_sha1 = hmac.new(key, message, hashlib.sha1)
#     digest = hmac_sha1.digest()
#
#     # Convert the digest to a base64-encoded string
#     result = base64.b64encode(digest).decode('utf-8')
#
#     return result
#
#
# def calculate_request_size(data):
#     # Calculate the size of the JSON data in bytes
#     json_body = json.dumps(data)
#     return len(json_body.encode('utf-8')), json_body
#
#
# def handle_response(response) -> bool:
#     if response.status_code == 200:
#         logging.info('Successfully sent request')
#         return False
#     else:
#         return True
#
#
# def send_internal_data_server_import_request(body: Dict[str, any], url: str) -> bool:
#     """
#     Calls data server to remove user ids from advertisers segments
#     """
#     request_size, json_body = calculate_request_size(body)
#     logging.info(f'Request body size: {request_size}')
#     api_key = secrets_manager_util.get_secrets(secret_name='data-server-api-key', region_name='us-west-2')['API_KEY']
#
#     logging.info(f'Sending request to {url}. Request body (truncated to 200 characters): {json_body[:200]}')
#     has_failed_requests = False
#     if request_size < max_request_size_bytes:
#         headers = {
#             "Content-Type": "application/json",
#             "TtdDataset": "DataSubjectRequest",
#             "TtdSignature": compute_hmac_sha1(api_key, json_body)
#         }
#         response = requests.post(url=url, headers=headers, json=json_body)
#         has_failed_requests = handle_response(response)
#
#     else:
#         # Split the Items array into chunks that fit within the maximum size
#         items_chunks = []
#         current_chunk = []
#
#         for item in body['Items']:
#             # If adding the next item exceeds the max request size, start a new chunk
#             items_size_bytes, json_body = calculate_request_size({'Items': current_chunk + [item]})
#             if items_size_bytes > max_request_size_bytes:
#                 if current_chunk:
#                     items_chunks.append(current_chunk)
#                 current_chunk = []
#             current_chunk.append(item)
#
#         if current_chunk:
#             items_chunks.append(current_chunk)
#
#         for items_chunk in items_chunks:
#             partner_key = ""
#             partner_value = ""
#             if "AdvertiserId" in body:
#                 partner_key = "AdvertiserId"
#                 partner_value = body[partner_key]
#             elif "DataProviderId" in body:
#                 partner_key = "DataProviderId"
#                 partner_value = body[partner_key]
#             # Create a new data object with the current chunk of items
#             data_chunk = {
#                 partner_key: partner_value,
#                 "Items": items_chunk
#             }
#             json_body = json.dumps(data_chunk)
#             headers = {
#                 "Content-Type": "application/json",
#                 "TtdDataset": "DataSubjectRequest",
#                 "TtdSignature": compute_hmac_sha1(api_key, json_body)
#             }
#             response = requests.post(url, headers=headers, json=json_body)
#             has_failed_requests = handle_response(response) or has_failed_requests
#
#     return not has_failed_requests
#
#
# def get_type_of_request_data(item: Dict[str, str]) -> DataServerRequestType:
#     """
#     Returns the partner key, url, get segments function for the partner
#     """
#     if PARTNER_DSR_ADVERTISER_ID in item and item[PARTNER_DSR_ADVERTISER_ID]:
#         return DataServerRequestType(partner_key='AdvertiserId',
#                                      partner_value=item[PARTNER_DSR_ADVERTISER_ID],
#                                      url='https://use-ltd-data-int.adsrvr.org/data/internalAdvertiserDataImport',
#                                      get_segments_callable=sql_util.query_advertiser_segments)
#
#     # This order matters.  For the case of external first party
#     # https://partner.thetradedesk.com/v3/portal/data/doc/post-data-advertiser-external
#     # We want to send an Internal Advertiser request where we do not care about the DataProviderId
#     # https://gitlab.adsrvr.org/thetradedesk/adplatform/blob/475cbf2167694caaec847391fba2141031e2f52e/TTD/Domain/DataServer/TTD.Domain.DataServer/InternalApi/InternalDataImportUtils.cs#L27
#     if PARTNER_DSR_DATA_PROVIDER_ID in item and item[PARTNER_DSR_DATA_PROVIDER_ID]:
#         return DataServerRequestType(partner_key='DataProviderId',
#                                      partner_value=item[PARTNER_DSR_DATA_PROVIDER_ID],
#                                      url='https://use-ltd-data-int.adsrvr.org/data/internalThirdPartyDataImport',
#                                      get_segments_callable=sql_util.query_third_party_segments)
#
#     raise AirflowException(f'Unknown type of request for item: {item}')
#
#
# def _get_id_type_and_value(item) -> Union[Tuple[Any, Any], Tuple[str, Any], Tuple[None, None]]:
#     user_id_type = item[PARTNER_DSR_USER_ID_TYPE]
#     user_id_guid = item[PARTNER_DSR_USER_ID_GUID]
#     user_id_raw = item[PARTNER_DSR_USER_ID_RAW]
#     if user_id_type.lower() == IdentitySource.Tdid.name.lower():
#         return user_id_type, user_id_guid
#     if user_id_type.lower() == IdentitySource.MiscDeviceId.name.lower():
#         return 'DAID', user_id_guid
#     if user_id_type.lower() == IdentitySource.UnifiedId2.name.lower():
#         return user_id_type, user_id_raw
#     if user_id_type.lower() == IdentitySource.IdentityLinkId.name.lower():
#         return 'IDL', user_id_raw
#     if user_id_type.lower() == IdentitySource.EUID.name.lower():
#         return user_id_type, user_id_raw
#     if user_id_type.lower() == IdentitySource.ID5.name.lower():
#         return user_id_type, user_id_raw
#     if user_id_type.lower() == IdentitySource.NetId.name.lower():
#         return user_id_type, user_id_raw
#     if user_id_type.lower() == IdentitySource.CoreId.name.lower():
#         return user_id_type, user_id_guid
#     return None, None
#
#
# def build_request_body(items, data_server_request_type, offset) -> Tuple[Dict[str, Any], bool]:
#     segment_ids = data_server_request_type.get_segments_callable(
#         data_server_request_type.partner_value,
#         offset,
#         sql_batch_size)
#
#     request_body = {}
#     if data_server_request_type.partner_key == 'AdvertiserId':
#         request_body['AdvertiserId'] = data_server_request_type.partner_value
#     elif data_server_request_type.partner_key == 'DataProviderId':
#         request_body['DataProviderId'] = data_server_request_type.partner_value
#     else:
#         raise AirflowException(f'Unknown type of request for type: {data_server_request_type} with items: {items}')
#
#     request_body['Items'] = []
#     if segment_ids:
#         has_more_segments = True
#     else:
#         return request_body, False
#
#     for item in items:
#         data_list = []
#         for segment_id in segment_ids:
#             data_list.append({
#                 'Name': str(segment_id),
#                 'TTLInMinutes': 0
#             })
#         id_type, id_value = _get_id_type_and_value(item)
#         if id_type is None or id_value is None:
#             logging.error(f'Unable to extract user id type or value for item: {item}')
#         else:
#             request_body['Items'].append({
#                 id_type: id_value,
#                 'Data': data_list
#             })
#     return request_body, has_more_segments
#
#
# def make_data_server_request(items: List[Dict[str, str]]) -> bool:
#     """
#     Batches calls to data server and makes the call
#     """
#     data_server_request_type = get_type_of_request_data(items[0])
#
#     has_failed_requests = False
#     has_more_segments = True
#     offset = 0
#     while has_more_segments:
#         request_body, has_more_segments = build_request_body(items, data_server_request_type, offset)
#         offset += sql_batch_size
#
#         request_body_items = request_body['Items']
#         if not request_body_items or not request_body_items[0].get('Data', []):
#             logging.info(f'No items or data for request: {request_body}.  Not submitting DataServer request.')
#         else:
#             has_failed_requests = (send_internal_data_server_import_request(request_body, data_server_request_type.url)
#                                    or has_failed_requests)
#     return not has_failed_requests
#
# # example request https://partner.thetradedesk.com/v3/portal/data/doc/post-data-advertiser-firstparty
# # {
# #    "AdvertiserId":"yourAdvertiserId",
# #    "Items":[
# #       {
# #          "TDID":"123e4567-e89b-12d3-a456-426652340000",
# #          "Data":[
# #             {
# #                "Name":"1210",
# #                "TimestampUtc": "2023-11-11T10:11:30+5000",
# #                "TTLInMinutes":43200
# #             },
# #             {
# #                "Name":"1160",
# #                "TimestampUtc": "2023-11-13T09:35:30+5000",
# #                "TTLInMinutes":43200
# #             }
# #          ]
# #       },
# #       ...
# #    ]
# # }
#
# # https://partner.thetradedesk.com/v3/portal/data/doc/post-data-thirdparty
# # {
# #    "DataProviderId":"yourProviderId",
# #    "Items":[
# #       {
# #          "TDID":"123e4567-e89b-12d3-a456-426652340000",
# #          "Data":[
# #             {
# #                "Name":"1210",
# #                "TimestampUtc": "2023-11-11T10:11:30+5000",
# #                "TTLInMinutes":43200
# #             },
# #             {
# #                "Name":"1160",
# #                "TimestampUtc": "2023-11-13T09:35:30+5000",
# #                "TTLInMinutes":43200
# #             }
# #          ]
# #       },
# #       ...
# #    ]
# # }
