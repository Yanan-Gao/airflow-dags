"""
Task to populate microtargeting.ExternalUser table in Provisioning with data from changelog.ActivityRollup.
"""

import logging
import typing
from datetime import datetime, timedelta

from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

from ttd import ttdprometheus
from ttd.el_dorado.v2.base import TtdDag
from ttd.slack.slack_groups import pdg
from ttd.tasks.op import OpTask
from ttd.ttdenv import TtdEnvFactory


def choose(prod, test):
    return prod if TtdEnvFactory.get_from_system() == TtdEnvFactory.prod else test


dag = TtdDag(
    dag_id='microtargeting-externaluser-merge-activityrollup',
    start_date=datetime(2024, 7, 1, 0, 0),
    schedule_interval=choose(prod=timedelta(days=1), test=None),
    slack_channel=choose(prod='#scrum-pdg-alarms', test=None),
    tags=[pdg.name, pdg.jira_team, 'Microtargeting'],
    max_active_runs=1,  # don't run multiple sprocs at the same time
)

adag = dag.airflow_dag  # MUST be explicitly defined for Airflow to parse this file as a DAG


def get_db_connection():
    """
    Connects to Provisioning DB, either Production or Internal Sandbox based on the Airflow environment.

    Connections are defined in Vault in SCRUM-PDG/Airflow/Connections/.
    See https://thetradedesk.gitlab-pages.adsrvr.org/teams/dataproc/docs/airflow/ttd_tooling/secrets_layer/.

    :return: SQL connection
    """
    name = choose(
        # Production ProvDB
        prod='ttd_microtargeting-provdb-int-connection',
        # Internal Sandbox
        test='ttd_microtargeting-dip-provdb-connection'
    )
    connection = BaseHook.get_connection(name)
    hook = connection.get_hook()
    conn = hook.get_conn()
    conn.autocommit(True)
    return conn


def merge_activityrollup(prev_execution_date_success: typing.Optional[datetime], **kwargs) -> None:
    """Executes microtargeting.prc_ExternalUser_MergeActivityRollup sproc.

    The sproc updates microtargeting.ExternalUser table with changes from changelog.ActivityRollUp."""

    # If we don't have previous run or previous run was on Saturday do a full merge.
    # See PDG-1422 for details, we occasionally find skipped rows and need to do a full merge.
    if not prev_execution_date_success or prev_execution_date_success.weekday() == 5:
        sync_from = '2023-12-01 00:00:00'
    else:
        sync_from_dt = prev_execution_date_success - timedelta(hours=3)
        sync_from = sync_from_dt.strftime("%Y-%m-%d %H:%M:%S")

    sql = f'exec [microtargeting].[prc_ExternalUser_MergeActivityRollup_v2] @syncFrom = "{sync_from}"'
    logging.info(f'Executing `{sql}`')

    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute(sql)
    cursor.close()
    connection.close()


def monitor_coverage(**kwargs) -> None:
    """Measure and upload Prometheus metrics."""
    connection = get_db_connection()

    def _exec(query):
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result

    def _get_single_count(rows):
        return rows[0][0]

    # from https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/1d15b16fd8d2fa148d1c35f175e97f130fe81a08/DB/Provisioning/ProvisioningDB/Schema%20Objects/Functions/microtargeting.fn_Enum_ExternalUserSourceId.function.sql
    source_id_map = {
        0: "Unknown",
        1: "ActivityRollup_UI",
        2: "ActivityRollup_API",
    }

    externaluser_adgroup_count_by_sourceid = ttdprometheus.get_or_register_gauge(
        job=adag.dag_id,
        name='microtargeting_externaluser_adgroup_count_by_sourceid',
        description='Microtargeting - Number of unique AdGroupId covered in microtargeting.ExternalUser',
        labels=['source']
    )
    sql = 'select SourceId, count(distinct AdGroupId) from [microtargeting].[ExternalUser] group by SourceId;'
    for (source_id, count) in _exec(sql):
        source_name = source_id_map.get(source_id, "Unknown")
        externaluser_adgroup_count_by_sourceid.labels(source=source_name).set(count)

    externaluser_adgroup_count = ttdprometheus.get_or_register_gauge(
        job=adag.dag_id,
        name='microtargeting_externaluser_adgroup_count',
        description='Microtargeting - Number of unique AdGroupId covered in microtargeting.ExternalUser',
        labels=[]
    )
    sql = 'select count(distinct AdGroupId) from [microtargeting].[ExternalUser]'
    externaluser_adgroup_count.set(_get_single_count(_exec(sql)))

    active_political_adgroup_count = ttdprometheus.get_or_register_gauge(
        job=adag.dag_id,
        name='microtargeting_active_political_adgroup_count',
        description='Microtargeting - Number of unique AdGroupId in microtargeting.vw_ActivePoliticalAdGroups',
        labels=[]
    )
    sql = 'select count(distinct AdGroupId) from [microtargeting].[vw_ActivePoliticalAdGroups]'
    active_political_adgroup_count.set(_get_single_count(_exec(sql)))

    active_political_adgroup_without_contact_count = ttdprometheus.get_or_register_gauge(
        job=adag.dag_id,
        name='microtargeting_active_political_adgroup_without_contact_count',
        description='Microtargeting - Number of unique AdGroupId in microtargeting.vw_ActivePoliticalAdGroups'
        ' without contacts details in microtargeting.ExternalUser',
        labels=[]
    )
    sql = '''
        select count(distinct vw_ActivePoliticalAdGroups.AdGroupId)
        from [microtargeting].[vw_ActivePoliticalAdGroups]
            left join [microtargeting].[ExternalUser] on ExternalUser.AdGroupId = vw_ActivePoliticalAdGroups.AdGroupId
        where ExternalUser.AdGroupId is null
    '''
    active_political_adgroup_without_contact_count.set(_get_single_count(_exec(sql)))

    active_political_adgroup_with_missed_contact_count = ttdprometheus.get_or_register_gauge(
        job=adag.dag_id,
        name='microtargeting_active_political_adgroup_with_missed_contact_count',
        description='Microtargeting - Number of unique AdGroupId in microtargeting.vw_ActivePoliticalAdGroups with '
        'entries in the ActivityRollup but no contact details in microtargeting.ExternalUser post merge',
        labels=[]
    )
    # The 'where' clause below should match with filtering done in microtargeting.prc_ExternalUser_MergeActivityRollup
    sql = '''
        select
            count(distinct actag.AdGroupId)
        from changelog.ActivityRollup activity
            inner join dbo.AdGroup ag on activity.OwnerAdGroupId = ag.AdGroupId
            inner join dbo.Campaign cmp on ag.CampaignId = cmp.CampaignId
            inner join microtargeting.vw_ActivePoliticalAdGroups actag on actag.AdGroupId = activity.OwnerAdGroupId
        where activity.SourceId in (0,1)
            and activity.ModifiedByUser <> 'UI'
            and activity.ModifiedByUser not like '%thetradedesk.com'
            and ( activity.ChangeTimeUtc > dateadd( hour, -51, getutcdate() ) or activity.ChangeTimeUtc < cmp.StartDate or ag.CreatedAt > cmp.StartDate )
            and not exists(select 1 from microtargeting.ExternalUser eu where eu.AdGroupId = actag.AdGroupId and eu.SourceId = activity.SourceId + 1)
    '''
    active_political_adgroup_with_missed_contact_count.set(_get_single_count(_exec(sql)))

    active_hec_adgroup_without_contact_count = ttdprometheus.get_or_register_gauge(
        job=adag.dag_id,
        name='microtargeting_active_hec_adgroup_without_contact_count',
        description='Microtargeting - Number of unique HEC AdGroupIds without contact details in microtargeting.ExternalUser',
        labels=[]
    )
    sql = '''
            select count(distinct ag.AdGroupId)
            from [datapolicies].[dataPolicyApplicableContext] dpac
                inner join [dbo].[vw_ActiveAdGroups] ag on dpac.AdvertiserId = ag.AdvertiserId
                left join [microtargeting].[ExternalUser] eu on ag.AdGroupId = eu.AdGroupId
            where dpac.DataPolicyId = 4
                and eu.AdGroupId is null
        '''
    active_hec_adgroup_without_contact_count.set(_get_single_count(_exec(sql)))

    active_hec_adgroup_with_missed_contact_count = ttdprometheus.get_or_register_gauge(
        job=adag.dag_id,
        name='microtargeting_active_hec_adgroup_with_missed_contact_count',
        description='Microtargeting - Number of unique HEC AdGroupIds with '
        'entries in the ActivityRollup but no contact details in microtargeting.ExternalUser post merge',
        labels=[]
    )
    # The 'where' clause below should match with filtering done in microtargeting.prc_ExternalUser_MergeActivityRollup
    sql = '''
            select
                count(distinct actag.AdGroupId)
            from changelog.ActivityRollup activity
                inner join dbo.AdGroup ag on activity.OwnerAdGroupId = ag.AdGroupId
                inner join dbo.Campaign cmp on ag.CampaignId = cmp.CampaignId
                inner join dbo.vw_ActiveAdGroups actag on activity.OwnerAdGroupId = actag.AdGroupId
                inner join datapolicies.dataPolicyApplicableContext dpac on actag.AdvertiserId = dpac.AdvertiserId
            where dpac.DataPolicyId = 4
                and activity.SourceId in (0,1)
                and activity.ModifiedByUser <> 'UI'
                and activity.ModifiedByUser not like '%thetradedesk.com'
                and ( activity.ChangeTimeUtc > dateadd( hour, -51, getutcdate() ) or activity.ChangeTimeUtc < cmp.StartDate or ag.CreatedAt > cmp.StartDate )
                and not exists(select 1 from microtargeting.ExternalUser eu where eu.AdGroupId = actag.AdGroupId and eu.SourceId = activity.SourceId + 1)
        '''
    active_hec_adgroup_with_missed_contact_count.set(_get_single_count(_exec(sql)))

    ttdprometheus.push_all(adag.dag_id)


merge_activityrollup = OpTask(
    op=PythonOperator(
        dag=adag,
        task_id='merge_activityrollup',
        python_callable=merge_activityrollup,
        provide_context=True,
    )
)

monitor_coverage = OpTask(
    op=PythonOperator(
        dag=adag,
        task_id='monitor_coverage',
        python_callable=monitor_coverage,
        provide_context=True,
    )
)

dag >> merge_activityrollup >> monitor_coverage
