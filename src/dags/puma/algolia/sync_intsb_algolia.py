"""
Daily Airflow DAG to copy all provisioning data from int-sb
and copy it into Algolia
"""

from datetime import datetime, timedelta

from algoliasearch.search.client import SearchClientSync
from airflow.models import Variable
import pymssql

from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

from ttd.tasks.op import OpTask
from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import puma
from ttd.workers.worker import Workers
from ttd.task_service.k8s_pod_resources import TaskServicePodResources

job_name = 'sync_intsb_algolia'
job_description = 'Sync Int-sb version of Algolia with int-sb database'
dag = TtdDag(
    dag_id=job_name,
    start_date=datetime(2024, 1, 1, 0, 0, 0, 0),
    run_only_latest=True,
    schedule_interval="5 0 * * *",
    slack_channel=puma.alarm_channel,
    tags=[puma.name, puma.jira_team],
    retries=3,
    retry_delay=(timedelta(minutes=15))
)

adag = dag.airflow_dag


class AlgoliaSettings:

    def __init__(self, application_id, write_api_key, index_name):
        self.application_id = application_id
        self.write_api_key = write_api_key
        self.index_name = index_name


class AlgoliaUserObject:

    def __init__(self):
        self.objectID = None
        self.PlatformPartnerIds = []
        self.PlatformPartnerNames = []
        self.PlatformAdvertiserIds = []
        self.PlatformAdvertiserNames = []
        self.PlatformAccessGroupIds = []
        self.PlatformAccessGroupNames = []
        self.PlatformRoles = []
        self.PlatformPrimaryAccessPartnerId = None
        self.PlatformPrimaryAccessPartnerName = None


def fetch_and_add_to_platform_attributes(conn, query, user_dict, key):
    cursor = conn.cursor()
    cursor.execute(query)

    for row in cursor.fetchall():
        user_id, id, name = row
        algolia_user = user_dict.get(user_id, AlgoliaUserObject())
        algolia_user.objectID = user_id
        if key == "AccessGroup":
            algolia_user.PlatformAccessGroupIds.append(id)
            algolia_user.PlatformAccessGroupNames.append(name)
        elif key == "Partner":
            algolia_user.PlatformPartnerIds.append(id)
            algolia_user.PlatformPartnerNames.append(name)
        elif key == "PrimaryAccessPartner":
            algolia_user.PlatformPrimaryAccessPartnerId = id
            algolia_user.PlatformPrimaryAccessPartnerName = name
        elif key == "Advertiser":
            algolia_user.PlatformAdvertiserIds.append(id)
            algolia_user.PlatformAdvertiserNames.append(name)
        elif key == "Role":
            algolia_user.PlatformRoles.append(name)
        user_dict[user_id] = algolia_user

    print(f"Grabbed {key} Information")


def get_db_connection(conn_name):
    conn_info = BaseHook.get_connection(conn_name)
    server = conn_info.host
    user = conn_info.login
    password = conn_info.password
    database = conn_info.schema
    return pymssql.connect(server=server, user=user, password=password, database=database)


def sync_algolia_task():
    algolia_settings = AlgoliaSettings(
        application_id=Variable.get("int_sb_algolia_applicationid"),
        write_api_key=Variable.get("int_sb_algolia_write_api_key"),
        index_name=Variable.get("int_sb_algolia_index_name")
    )

    client = SearchClientSync(algolia_settings.application_id, algolia_settings.write_api_key)

    access_group_query = (
        "SELECT uag.UserId, ag.AccessGroupId, ag.AccessGroupName "
        "FROM usermanagement.UserAccessGroup uag "
        "LEFT JOIN usermanagement.AccessGroup ag "
        "ON uag.AccessGroupId = ag.AccessGroupId;"
    )

    partner_query = (
        "SELECT uug.UserId, puga.PartnerId, p.PartnerName "
        "FROM [dbo].[UserUserGroup] uug "
        "INNER JOIN [dbo].[PartnerUserGroupAuthorization] puga "
        "ON uug.UserGroupId = puga.UserGroupId "
        "INNER JOIN [dbo].[Partner] p "
        "ON p.PartnerId = puga.PartnerId "
        "WHERE uug.UserGroupId NOT IN ( "
        "'tdadmingroup', "
        "'tdadminlitegroup',"
        "'ttdadmingroupwm',"
        "'ttdagileservices',"
        "'ttdagileserviceswm'"
        ");"
    )

    pap_query = (
        "SELECT uug.UserId, p.PartnerId, p.PartnerName "
        "FROM [dbo].[UserUserGroup] uug "
        "INNER JOIN [dbo].[UserGroup] ug "
        "ON uug.UserGroupId = ug.UserGroupId "
        "INNER JOIN [dbo].[Partner] p "
        "ON p.PartnerId = ug.PartnerId;"
    )

    advertiser_query = (
        "SELECT uug.UserId, auga.AdvertiserId, p.AdvertiserName "
        "FROM [dbo].[UserUserGroup] uug "
        "INNER JOIN [dbo].[AdvertiserUserGroupAuthorization] auga "
        "ON uug.UserGroupId = auga.UserGroupId "
        "INNER JOIN [dbo].[Advertiser] p "
        "ON p.AdvertiserId = auga.AdvertiserId "
        "WHERE uug.UserGroupId NOT IN ( "
        "'tdadmingroup', "
        "'tdadminlitegroup',"
        "'ttdadmingroupwm',"
        "'ttdagileservices',"
        "'ttdagileserviceswm'"
        ");"
    )

    role_query = (
        "SELECT upg.[UserId], upg.[PermissionGroupId], pg.[PermissionGroupName] "
        "FROM [Provisioning].[dbo].[UserPermissionGroup] upg "
        "LEFT JOIN [Provisioning].[dbo].[PermissionGroup] pg ON upg.[PermissionGroupId] = pg.[PermissionGroupId]"
    )

    conn = get_db_connection("IntSb_Provisioning")
    user_dict: dict[str, AlgoliaUserObject] = {}

    fetch_and_add_to_platform_attributes(conn, access_group_query, user_dict, "AccessGroup")

    fetch_and_add_to_platform_attributes(conn, partner_query, user_dict, "Partner")

    fetch_and_add_to_platform_attributes(conn, pap_query, user_dict, "PrimaryAccessPartner")

    fetch_and_add_to_platform_attributes(conn, advertiser_query, user_dict, "Advertiser")

    fetch_and_add_to_platform_attributes(conn, role_query, user_dict, "Role")

    conn.close()

    try:
        client.partial_update_objects(
            index_name="UserManagementPortalUsers", objects=[user.__dict__ for user in user_dict.values()], create_if_not_exists=True
        )

        print("Partial update succeeded.")

    except Exception as e:
        print(f"Partial update failed: {e}")
        raise


sync_algolia_task = OpTask(
    op=PythonOperator(
        task_id='sync_algolia_task',
        python_callable=sync_algolia_task,
        provide_context=True,
        queue=Workers.k8s.queue,
        pool=Workers.k8s.pool,
        executor_config=TaskServicePodResources.large().as_executor_config(),
        dag=adag,
    )
)

dag >> sync_algolia_task
