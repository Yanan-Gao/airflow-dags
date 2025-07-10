import json
import logging
from datetime import datetime, date
from enum import Enum
from typing import List, Dict, Tuple, Callable

from airflow import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import TaskInstance

from dags.pdg.data_subject_request.config.vertica_config import VerticaTableConfiguration, \
    DSR_VERTICA_TABLE_CONFIGURATIONS, Pii, PARTNER_DSR_VERTICA_TABLE_CONFIGURATIONS_ADVERTISER, ScrubType, \
    VERTICA_SCRUB_IDS_PER_TASK, PARTNER_DSR_VERTICA_TABLE_CONFIGURATIONS_MERCHANT
from dags.pdg.data_subject_request.util.dsr_dynamodb_util import PARTNER_DSR_ADVERTISER_ID, PARTNER_DSR_USER_ID_TYPE, \
    PARTNER_DSR_USER_ID_GUID, PARTNER_DSR_USER_ID_RAW, PARTNER_DSR_MERCHANT_ID
from ttd.ttdenv import TtdEnvFactory
from dags.pdg.data_subject_request.util import serialization_util
from dags.pdg.data_subject_request.dsr_item import DsrItem
from dags.pdg.data_subject_request.identity import DsrIdentityType

is_prod = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False

# https://vault.adsrvr.org/ui/vault/secrets/secret/kv/SCRUM-PDG%2FAirflow%2FConnections%2Fttd-lwdb-dsr-connection/details?namespace=team-secrets&version=1
LWDB_CONNECTION_ID = 'ttd-lwdb-dsr-connection'


class IdentitySource(Enum):
    Unknown = 0
    Tdid = 1
    MiscDeviceId = 2
    UnifiedId2 = 3
    IdentityLinkId = 4
    EUID = 5
    Nebula = 6
    DATId = 9
    McId = 10
    ID5 = 11
    NetId = 12
    FirstId = 13
    CoreId = 1015


class VerticaScrubGeneratorTaskPusher:

    def __init__(self, generate_tasks_creator: Callable[[TaskInstance, date], List[Tuple[VerticaTableConfiguration, str, Dict]]]) -> None:
        self.generate_tasks_creator = generate_tasks_creator

    def push_vertica_scrub_generator_logex_task(self, task_instance: TaskInstance, **kwargs):
        tasks = self.generate_tasks_creator(task_instance, datetime.utcnow().date())

        connection = BaseHook.get_connection(LWDB_CONNECTION_ID)
        hook = connection.get_hook()
        conn = hook.get_conn()
        conn.autocommit(True)
        cursor = conn.cursor()

        table_to_s3_keys_mapping: Dict[str, List[str]] = dict()
        for table_config, log_key, run_data in tasks:
            run_data_json = json.dumps(run_data)
            cursor.callproc('dbo.prc_CreateVerticaScrubGeneratorTask', (log_key, run_data_json, False))
            logging.info(
                f'Stored VerticaScrubGeneratorTask table={table_config.raw_table_name}, log_key={log_key}\n'
                f'omitted run data due to size constraints.  Please query LWDB with the log key to see the run data'
            )

            table_to_s3_keys_mapping.setdefault(table_config.raw_table_name, []).append(log_key)

        logging.info('Stored tasks for all tables')
        cursor.close()
        conn.close()

        task_instance.xcom_push(key='table_to_s3_keys', value=table_to_s3_keys_mapping)


def create_generator_tasks_dsr(task_instance: TaskInstance, start_time: date) \
        -> List[Tuple[VerticaTableConfiguration, str, Dict]]:
    request_id = task_instance.xcom_pull(key='request_id')
    dsr_items = serialization_util.deserialize_list(task_instance.xcom_pull(key='dsr_items'), DsrItem)
    tdids, uid2s, euids = _get_ids(dsr_items)

    tasks = []
    for _, table_config in DSR_VERTICA_TABLE_CONFIGURATIONS.items():
        filter_columns_to_ids = []

        for filter_column in table_config.filter_columns:
            if filter_column == Pii.TDID:
                filter_columns_to_ids.append((filter_column.name, tdids))
            if filter_column == Pii.UnifiedId2:
                filter_columns_to_ids.append((filter_column.name, uid2s))
            if filter_column == Pii.EUID:
                filter_columns_to_ids.append((filter_column.name, euids))

        for special_tdid_column in table_config.tdid_special_transforms:
            filter_columns_to_ids.append((special_tdid_column, tdids))

        for filter_column, ids in filter_columns_to_ids:
            # Break the list of IDs into smaller lists for the vertica scrub. This should result in more
            # vertica scrub tasks with fewer IDs in each task.
            for id_sublist in [ids[i:i + VERTICA_SCRUB_IDS_PER_TASK] for i in range(0, len(ids), VERTICA_SCRUB_IDS_PER_TASK)]:
                (table_config, log_key, run_data) = vertica_scrub_generator_task(
                    request_id, table_config, start_time, filter_column, _filter_exp_dsr(filter_column, id_sublist), "user_dsr/"
                )

                tasks.append((table_config, log_key, run_data))

    return tasks


def extract_vertica_mappings(requests) -> Dict[Tuple[str, bool], List[Dict[str, str]]]:
    partner_dsr_requests: Dict[Tuple[str, bool], List[Dict[str, str]]] = {}
    for request in requests:
        advertiser_id = request.get(PARTNER_DSR_ADVERTISER_ID)
        merchant_id = request.get(PARTNER_DSR_MERCHANT_ID)
        if advertiser_id or merchant_id:
            user_id_type, user_id_value = _map_user_id_type_and_value_to_vertica_pii_column(
                request.get(PARTNER_DSR_USER_ID_TYPE), request.get(PARTNER_DSR_USER_ID_GUID), request.get(PARTNER_DSR_USER_ID_RAW)
            )
            if user_id_type and user_id_value:
                vertica_request_mapping = {'type': user_id_type, 'value': user_id_value}
                if merchant_id:
                    partner_dsr_requests.setdefault((merchant_id, True), []).append(vertica_request_mapping)
                else:
                    partner_dsr_requests.setdefault((advertiser_id, False), []).append(vertica_request_mapping)
            else:
                logging.info(
                    f'For request {request}.\n'
                    f'User ID Type of {user_id_type} is not present in vertica '
                    f'or User Id Value {user_id_value} is invalid'
                )
    logging.info(f'One Example Partner dsr request: {next(iter(partner_dsr_requests), None)}')
    return partner_dsr_requests


def _map_user_id_type_and_value_to_vertica_pii_column(user_id_type: str | None, user_id_guid: str | None, user_id_raw: str | None):
    if not user_id_type or (not user_id_guid and not user_id_raw):
        raise AirflowException('Required IDs are not present')

    # For the case when we dont have the raw value but we have the guid regardless of identity type
    if not user_id_raw and user_id_guid:
        return Pii.TDID.name, user_id_guid

    if user_id_type.lower() == IdentitySource.Tdid.name.lower():
        return Pii.TDID.name, user_id_guid
    if user_id_type.lower() == IdentitySource.MiscDeviceId.name.lower():
        return Pii.DeviceAdvertisingId.name, user_id_guid
    if user_id_type.lower() == IdentitySource.UnifiedId2.name.lower():
        return Pii.UnifiedId2.name, user_id_raw
    if user_id_type.lower() == IdentitySource.IdentityLinkId.name.lower():
        return Pii.IdentityLinkId.name, user_id_raw
    if user_id_type.lower() == IdentitySource.EUID.name.lower():
        return Pii.EUID.name, user_id_raw
    if user_id_type.lower() == IdentitySource.ID5.name.lower():
        return None, None
    if user_id_type.lower() == IdentitySource.NetId.name.lower():
        return None, None
    if user_id_type.lower() == IdentitySource.CoreId.name.lower():
        return None, None

    return None, None


def create_generator_tasks_partner_dsr(task_instance: TaskInstance, start_time: date) \
        -> List[Tuple[VerticaTableConfiguration, str, Dict]]:
    batch_request_id = task_instance.xcom_pull(key='batch_request_id')
    requests = task_instance.xcom_pull(key='partner_dsr_requests')
    partner_dsr_requests = extract_vertica_mappings(requests)

    tasks = []

    for (partner_id, is_merchant), uiids in partner_dsr_requests.items():
        tables = PARTNER_DSR_VERTICA_TABLE_CONFIGURATIONS_ADVERTISER
        if is_merchant:
            tables = PARTNER_DSR_VERTICA_TABLE_CONFIGURATIONS_MERCHANT

        for _, table_config in tables.items():
            uiids_grouped: Dict[str, List[str]] = {}
            for uiid in uiids:
                uiids_grouped.setdefault(uiid['type'], []).append(uiid['value'])

            filter_col_to_uiids = {}
            for filter_column in table_config.filter_columns:
                filter_col_to_uiids[filter_column.name] = uiids_grouped.get(filter_column.name, [])

            for special_tdid_column in table_config.tdid_special_transforms:
                filter_col_to_uiids[special_tdid_column] = uiids_grouped.get(Pii.TDID.name, [])

            for filter_column, ids in filter_col_to_uiids.items():
                if not ids:
                    continue

                # Break the list of IDs into smaller lists for the vertica scrub. This should result in more
                # vertica scrub tasks with fewer IDs in each task.
                for id_sublist in [ids[i:i + VERTICA_SCRUB_IDS_PER_TASK] for i in range(0, len(ids), VERTICA_SCRUB_IDS_PER_TASK)]:
                    (table_config, log_key, run_data) = vertica_scrub_generator_task(
                        batch_request_id,
                        table_config,
                        start_time,
                        filter_column,
                        filter_exp=(
                            _filter_exp_partner_merchant_id(filter_column, partner_id, id_sublist)
                            if is_merchant else _filter_exp_partner_id(filter_column, partner_id, id_sublist)
                        ),
                        log_key_prefix="partner_dsr/"
                    )
                    logging.info(
                        f'Appending the following to the vertica tasks\n'
                        f'table_config: {table_config}\n'
                        f'log_key: {log_key}\n'
                        f'omitting run data, if desired look up run data in LWDB using the log key'
                    )
                    tasks.append((table_config, log_key, run_data))

    return tasks


def vertica_scrub_generator_task(request_id, table_config, start_time, filter_column, filter_exp, log_key_prefix=""):
    scrub_from_str = ((start_time - table_config.retention_period + table_config.start_processing_offset).strftime("%Y-%m-%d"))
    scrub_to_str = (start_time - table_config.stop_processing_offset).strftime("%Y-%m-%d")

    log_key = (f'ttd-data-subject-requests/{log_key_prefix}delete/'
               f'{request_id}/'
               f'{table_config.raw_table_name}/'
               f'{filter_column}')

    scrub_mode, scrub_operation = table_config.scrub_type.value

    run_data = {
        'Mode':
        scrub_mode,
        'Dates':
        f'{scrub_from_str}:{scrub_to_str}',
        'Tables':
        table_config.raw_table_name,
        'FilterExpr':
        filter_exp,
        'Replacements': [] if table_config.scrub_type is ScrubType.DELETE_ROW else [{
            "Column": pii.value.col,
            "Expr": pii.value.scrubbed_value
        } for pii in table_config.pii_columns],
        'PerformScrub':
        is_prod,
    }

    return table_config, log_key, run_data


def _filter_exp_dsr(filter_col, uiids):
    uiid_ls = ','.join(f"'{uiid}'" for uiid in uiids)
    return f"{filter_col} in ({uiid_ls})"


def _filter_exp_partner_id(filter_col, partner_id, uiids):
    return f"AdvertiserId = '{partner_id}' and {_filter_exp_dsr(filter_col, uiids)}"


def _filter_exp_partner_merchant_id(filter_col, merchant_id, uiids):
    return f"MerchantId = '{merchant_id}' and {_filter_exp_dsr(filter_col, uiids)}"


def _get_ids(dsr_items) -> Tuple[List[str], List[str], List[str]]:
    tdids = []
    uid2s = []
    euids = []

    for dsr_item in dsr_items:
        for identity in dsr_item.identities:
            if identity.identity_type == DsrIdentityType.UID2:
                uid2s.append(identity.identifier)
            if identity.identity_type == DsrIdentityType.EUID:
                euids.append(identity.identifier)
            tdids.append(identity.tdid)

    return tdids, uid2s, euids
