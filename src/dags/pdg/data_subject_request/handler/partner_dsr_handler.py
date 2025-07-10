import csv
import io
import logging
import uuid
from typing import Dict, List, Tuple, Set, Union

from airflow import AirflowException
from airflow.models import TaskInstance
from dags.pdg.data_subject_request.handler.vertica_scrub_generator_creator import IdentitySource
from dags.pdg.data_subject_request.util import s3_utils, dsr_dynamodb_util, sql_util
from dags.pdg.data_subject_request.util.dsr_dynamodb_util import PARTNER_DSR_ADVERTISER_ID, \
    PARTNER_DSR_DATA_PROVIDER_ID, \
    PARTNER_DSR_USER_ID_GUID, PARTNER_DSR_REQUEST_ID, PARTNER_DSR_TIMESTAMP, PARTNER_DSR_PARTNER_ID, \
    PARTNER_DSR_TENANT_ID, PARTNER_DSR_USER_ID_RAW, PARTNER_DSR_USER_ID_TYPE


def process_partner_dsr_requests_ready_for_cloud_processing(task_instance: TaskInstance, **kwargs) -> bool:
    items = dsr_dynamodb_util.query_processing_state_gsi_dynamodb_items()
    if len(items) == 0:
        logging.info('No requests to process.')
        return False

    batch_request_id = str(uuid.uuid4())

    task_instance.xcom_push(key='batch_request_id', value=batch_request_id)
    task_instance.xcom_push(key='partner_dsr_requests', value=items)

    return True


def save_tenant_id_and_partner_id(task_instance: TaskInstance, **kwargs) -> bool:
    partner_dsr_requests = task_instance.xcom_pull(key='partner_dsr_requests')

    advertiser_ids, data_provider_ids, partner_to_items_dict = _extract_mappings(partner_dsr_requests)

    tenant_list = sql_util.query_tenant_id(list(advertiser_ids), list(data_provider_ids))
    if not tenant_list:
        logging.error('No tenant information found for items.  Check xcom for partner requests')
        return False
    _update_dynamodb_items_with_tenant_information(partner_to_items_dict, tenant_list)

    return True


def _update_dynamodb_items_with_tenant_information(partner_to_items_dict, tenant_list):
    for tenant_item in tenant_list:
        if PARTNER_DSR_ADVERTISER_ID in tenant_item:
            for partner_item in partner_to_items_dict.get(tenant_item[PARTNER_DSR_ADVERTISER_ID]):
                partner_item.update(tenant_item)
                dsr_dynamodb_util.update_item(partner_item)
        elif PARTNER_DSR_DATA_PROVIDER_ID in tenant_item:
            for partner_item in partner_to_items_dict.get(tenant_item[PARTNER_DSR_DATA_PROVIDER_ID]):
                partner_item.update(tenant_item)
                dsr_dynamodb_util.update_item(partner_item)
        else:
            logging.warning(f'Item: {tenant_item} does not have an advertiser id or data provider id.')


def _extract_mappings(items) -> Tuple[Set[str], Set[str], Dict[str, List[Dict[str, str]]]]:
    advertiser_ids = set()
    data_provider_ids = set()
    partner_to_items_dict: Dict[str, List[Dict[str, str]]] = {}
    for item in items:
        if PARTNER_DSR_ADVERTISER_ID in item and item[PARTNER_DSR_ADVERTISER_ID]:
            advertiser_id = item[PARTNER_DSR_ADVERTISER_ID]
            advertiser_ids.add(advertiser_id)
            if advertiser_id not in partner_to_items_dict.keys():
                partner_to_items_dict[advertiser_id] = [item]
            else:
                partner_to_items_dict.get(advertiser_id, []).append(item)
        elif PARTNER_DSR_DATA_PROVIDER_ID in item and item[PARTNER_DSR_DATA_PROVIDER_ID]:
            if PARTNER_DSR_ADVERTISER_ID in item:
                # we know it is an empty advertiser id so no need to return
                del item[PARTNER_DSR_ADVERTISER_ID]
            data_provider_id = item[PARTNER_DSR_DATA_PROVIDER_ID]
            data_provider_ids.add(data_provider_id)
            if data_provider_id not in partner_to_items_dict.keys():
                partner_to_items_dict[data_provider_id] = [item]
            else:
                partner_to_items_dict.get(data_provider_id, []).append(item)
        else:
            logging.warning(f'Item: {item} does not have an advertiser id or data provider id.')
    return advertiser_ids, data_provider_ids, partner_to_items_dict


def _build_partner_dsr_sort_key(advertiser_id: str, data_provider_id: str, user_id_guid: str) -> str:
    values = []
    # Append non-None values to the list
    if advertiser_id:
        values.append(advertiser_id)
    if data_provider_id:
        values.append(data_provider_id)
    if user_id_guid:
        values.append(user_id_guid)

    return '#'.join(values)


# def remove_users_from_segments(task_instance: TaskInstance, **kwargs) -> bool:
#     """
#     Batches calls to data server and makes the call
#     """
#     request_id = task_instance.xcom_pull(key='request_id')
#
#     has_failed_requests = False
#     last_evaluated_key = None
#     while True:
#         items, last_evaluated_key = dsr_dynamodb_util.query_dynamodb_items(request_id,
#                                                                            last_evaluated_key,
#                                                                            'pk, sk, '
#                                                                            'advertiserId, dataProviderId, userIdGuid, '
#                                                                            'rawUserId, userIdType',
#                                                                            [
#                                                                                (PROCESSING_STATE_ATTRIBUTE_NAME,
#                                                                                 PARTNER_DSR_PROCESSING_READY_FOR_OPT_OUT)
#                                                                            ])
#         logging.info(f'Items query result up to 1 example: {items[:1]}.  LastEvaluatedKey: {last_evaluated_key}')
#         if items is None:
#             logging.info('No items to process')
#             return False
#         else:
#             logging.info(f'Items to be processed {len(items)}')
#
#         for item in items:
#             dsr_dynamodb_util.update_item_attributes(
#                 item['pk'], item['sk'], {
#                     PROCESSING_STATE_ATTRIBUTE_NAME: PARTNER_DSR_PROCESSING_OPT_OUT,
#                 }
#             )
#
#         advertiser_ids, data_provider_ids, partner_to_items_dict = _extract_mappings(items)
#         for partner_requests in partner_to_items_dict.values():
#             has_failed_requests = data_server_util.make_data_server_request(partner_requests) or has_failed_requests
#
#         for item in items:
#             dsr_dynamodb_util.update_item_attributes(
#                 item['pk'], item['sk'], {
#                     PROCESSING_STATE_ATTRIBUTE_NAME: PARTNER_DSR_PROCESSING_READY_FOR_CLOUD,
#                 }
#             )
#
#         # Check if there are more items to retrieve
#         if not last_evaluated_key:
#             break
#
#         logging.info(f'All items in batch successfully processing')
#
#     return not has_failed_requests


def write_to_s3_enriched(task_instance: TaskInstance, **kwargs):
    batch_request_id = task_instance.xcom_pull(key='batch_request_id')
    partner_dsr_requests = dsr_dynamodb_util.query_processing_state_gsi_dynamodb_items()

    advertiser_items = []
    for request in partner_dsr_requests:
        if PARTNER_DSR_ADVERTISER_ID in request and request[PARTNER_DSR_ADVERTISER_ID]:
            reds_required_fields = {
                PARTNER_DSR_REQUEST_ID: request.get(PARTNER_DSR_REQUEST_ID, None),
                PARTNER_DSR_TIMESTAMP: request.get(PARTNER_DSR_TIMESTAMP, None),
                PARTNER_DSR_ADVERTISER_ID: request.get(PARTNER_DSR_ADVERTISER_ID),
                PARTNER_DSR_PARTNER_ID: request.get(PARTNER_DSR_PARTNER_ID, None),
                PARTNER_DSR_TENANT_ID: request.get(PARTNER_DSR_TENANT_ID, None),
                PARTNER_DSR_USER_ID_GUID: request.get(PARTNER_DSR_USER_ID_GUID, None),
                PARTNER_DSR_USER_ID_RAW: request.get(PARTNER_DSR_USER_ID_RAW, None),
                PARTNER_DSR_USER_ID_TYPE: request.get(PARTNER_DSR_USER_ID_TYPE, None)
            }
            advertiser_items.append(reds_required_fields)

    if not advertiser_items:
        logging.info(f'No advertiser DSR were found for batch request id: {batch_request_id}')
        return True

    # Create a CSV file in memory
    csv_file = io.StringIO()
    csv_writer = csv.writer(csv_file)

    # Write the header row
    csv_writer.writerow(advertiser_items[0].keys())

    # Write the data rows
    for item in advertiser_items:
        csv_writer.writerow(item.values())

    csv_content: str = csv_file.getvalue()
    s3_utils.put_enriched_object(csv_content)

    return True


def map_user_id_type_to_value(user_id_type: str, user_id_guid: str, user_id_raw: str) -> Union[str, None]:
    if not user_id_type or (not user_id_guid and not user_id_raw):
        raise AirflowException(
            f'User ID Type: {user_id_type} or '
            f'User Id GUID: {user_id_guid} and User Id Raw: {user_id_raw} are not present'
        )
    if user_id_type.lower() == IdentitySource.Tdid.name.lower():
        return user_id_guid
    if user_id_type.lower() == IdentitySource.UnifiedId2.name.lower():
        return user_id_raw
    if user_id_type.lower() == IdentitySource.EUID.name.lower():
        return user_id_raw
    return None
