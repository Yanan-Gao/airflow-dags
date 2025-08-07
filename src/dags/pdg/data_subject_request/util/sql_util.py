import logging
from textwrap import dedent
from typing import List, Tuple, Dict

from airflow import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import TaskInstance
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from dags.pdg.data_subject_request.util.dsr_dynamodb_util import PARTNER_DSR_PARTNER_ID, PARTNER_DSR_ADVERTISER_ID, \
    PARTNER_DSR_TENANT_ID, PARTNER_DSR_DATA_PROVIDER_ID
from ttd.ttdenv import TtdEnvFactory
from dags.pdg.data_subject_request.util import serialization_util
from dags.pdg.data_subject_request.util.prodtest_util import is_prodtest, is_dag_operation_enabled
from dags.pdg.data_subject_request.dsr_item import DsrItem

is_prod = True if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else False

sql_batch_size = 1000

# https://vault.adsrvr.org/ui/vault/secrets/secret/kv/SCRUM-PDG%2FAirflow%2FConnections%2Fttd-global-dsr-connection/details?namespace=team-secrets&version=2
TTD_GLOBAL_CONNECTION_ID = 'ttd-global-dsr-connection'

sql_connection_provisioning = 'provisioning_replica'  # provdb-bi.adsrvr.org
db_name_provisioning = 'Provisioning'

# Mapping **MUST** be in sync with
# https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/Shared/TTD.Domain.Shared.Identity/IdentitySource.cs
IDENTITY_SOURCE_ID = {'uid2': 3, 'euid': 5}


def persist_to_ttdglobal_deleterequest(task_instance: TaskInstance, **kwargs):
    if is_prodtest and not is_dag_operation_enabled():
        return

    connection = BaseHook.get_connection(TTD_GLOBAL_CONNECTION_ID)
    sql_hook = connection.get_hook()
    connection = sql_hook.get_conn()
    connection.autocommit(True)
    cursor = connection.cursor()

    dsr_items = serialization_util.deserialize_list(task_instance.xcom_pull(key='dsr_items'), DsrItem)

    if not is_prod:
        logging.info("Non prod environment. Logging SQL that would be ran in prod.")

    for dsr_item in dsr_items:
        jira_ticket_number = dsr_item.jira_ticket_number

        for identity in dsr_item.identities:
            sql = dedent(
                f"""
                exec support.prc_AddDeletionRequest
                @TDID = '{identity.tdid}',
                @TicketNumber = '{jira_ticket_number}',
                @RawValue = '{identity.identifier}',
                @SaltBucket = '{identity.salt_bucket}',
                @IdentitySourceId = {IDENTITY_SOURCE_ID[identity.identity_type]}
                """
            )

            if is_prod:
                try:
                    cursor.execute(sql)
                except Exception as e:
                    if 'Violation of PRIMARY KEY constraint' in str(e):
                        logging.warning(f"We have already processed this TDID.  Duplicate key violation occurred: {e}")
                    else:
                        raise e
            else:
                logging.info(f"To be executed t-sql: {sql}")


def query_advertiser_segments(advertiser_id: str, offset=0, batch_size=sql_batch_size) -> List[int]:
    """
    Queries Provisioning DB for Advertisers' Segments
    We first check to see if the Advertiser is designated as a Master Advertiser
    If so, we query for all children advertisers which have segment data shared with
    Then we query for all segment ids associated with the advertiser(s)
    """
    if not advertiser_id:
        raise AirflowException('Advertiser id cannot be empty')

    sql_hook = MsSqlHook(mssql_conn_id=sql_connection_provisioning, schema=db_name_provisioning)
    connection = sql_hook.get_conn()
    cursor = connection.cursor()

    sql = dedent(
        f"""
        SELECT DISTINCT TargetingDataId
        FROM dbo.TargetingData TD
        WHERE TD.DataOwnerId = '{advertiser_id}'
        ORDER BY TargetingDataId
        OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY
        """
    )

    logging.info(f"Executing t-sql: {sql}")
    cursor.execute(sql)
    segment_ids_tuple: List[Tuple[int]] = cursor.fetchall()  # returns tuple [(1,),(2,)]
    logging.info(f"Result of query (up to 5): {segment_ids_tuple[:5]}")

    return [segment_id for (segment_id, ) in segment_ids_tuple]


def query_third_party_segments(data_provider_id: str, offset=0, batch_size=sql_batch_size) -> List[int]:
    """
    Queries segment data from third party providers.
    First we check to ensure the third party provider is not treated as a First Party provider.
    Then we query for the Third Party's segment data.
    """
    if not data_provider_id:
        raise AirflowException('Data Provider id cannot be empty')

    sql_hook = MsSqlHook(mssql_conn_id=sql_connection_provisioning, schema=db_name_provisioning)
    connection = sql_hook.get_conn()
    cursor = connection.cursor()

    sql = dedent(
        f"""
        IF NOT EXISTS (SELECT 1
                       FROM dbo.OfflineDataProvider
                       WHERE TreatAsFirstParty = 1
                        AND OfflineDataProviderId = '{data_provider_id}')
            SELECT DISTINCT TargetingDataId
            FROM dbo.TargetingData
            WHERE DataOwnerId = '{data_provider_id}'
            ORDER BY TargetingDataId
            OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY;
        ELSE
            PRINT 'OfflineDataProviderId is designated as a first party provider.  No segments will be returned.'
        """
    )

    logging.info(f"Executing t-sql: {sql}")
    cursor.execute(sql)
    segment_ids_tuple: List[Tuple[int]] = cursor.fetchall()  # returns tuple [(1,),(2,)]
    logging.info(f"Result of query (up to 5): {segment_ids_tuple[:5]}")

    return [segment_id for (segment_id, ) in segment_ids_tuple]


def query_tenant_id(advertiser_ids: List[str], data_provider_ids: List[str]) -> List[Dict[str, str]]:
    """
    Queries for the tenant id and/or partner id associated with the data provider or advertiser
    """
    results = []

    sql_hook = MsSqlHook(mssql_conn_id=sql_connection_provisioning, schema=db_name_provisioning)
    connection = sql_hook.get_conn()
    cursor = connection.cursor()

    if advertiser_ids:
        quoted_advertiser_ids = [f"'{id_}'" for id_ in advertiser_ids if id_]
        sql_in_clause = ','.join(quoted_advertiser_ids)
        advertiser_sql = dedent(
            f"""
            SELECT A.AdvertiserId, A.PartnerId, PG.TenantId
            FROM Advertiser as A
                     JOIN dbo.Partner AS P ON P.PartnerId = A.PartnerId
                     JOIN dbo.PartnerGroup AS PG ON PG.PartnerGroupId = P.PartnerGroupId
            WHERE A.AdvertiserId IN ({sql_in_clause});
            """
        )
        logging.info(f"Executing advertiser t-sql: {advertiser_sql}")
        cursor.execute(advertiser_sql)
        advertiser_results = cursor.fetchall()
        logging.info(f"Result of advertisers query: {advertiser_results}")
        for advertiser_row in advertiser_results:
            results.append({
                PARTNER_DSR_ADVERTISER_ID: advertiser_row[0],
                PARTNER_DSR_PARTNER_ID: advertiser_row[1],
                PARTNER_DSR_TENANT_ID: advertiser_row[2]
            })

    if data_provider_ids:
        quoted_data_provider_ids = [f"'{id_}'" for id_ in data_provider_ids if id_]
        sql_in_clause = ','.join(quoted_data_provider_ids)
        data_provider_sql = dedent(
            f"""
            SELECT TPD.ThirdPartyDataProviderId, T.TenantId
            FROM dbo.ThirdPartyDataProvider AS TPD
                    JOIN
                 dbo.Tenant AS T ON TPD.CloudServiceId = T.CloudServiceId
            WHERE TPD.ThirdPartyDataProviderId IN ({sql_in_clause})
            """
        )
        logging.info(f"Executing data provider t-sql: {data_provider_sql}")
        cursor.execute(data_provider_sql)
        data_provider_results = cursor.fetchall()
        logging.info(f"Result of advertisers query: {data_provider_results}")
        for data_provider_row in data_provider_results:
            results.append({PARTNER_DSR_DATA_PROVIDER_ID: data_provider_row[0], PARTNER_DSR_TENANT_ID: data_provider_row[1]})

    logging.info(f"Result of tenant queries: {results}")
    return results
