"""
Daily DAG to copy Samba campaign details into S3.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.el_dorado.v2.base import TtdDag
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask

pipeline_name = "ctv-sambaau-data-to-s3"
job_start_date = datetime(2024, 7, 16, 0, 0)
job_schedule_interval = timedelta(days=1)

sql_connection = 'provisioning_replica'  # provdb-bi.adsrvr.org
db_name = 'Provisioning'

s3_bucket = "samba-partner-ttd"
s3_base_path = "to-samba/campaigns/"

# DAG
dag_pipeline: TtdDag = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    retries=1,
    retry_delay=timedelta(hours=1),
    depends_on_past=False,
    run_only_latest=False,
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    tags=["irr"]
)
dag: DAG = dag_pipeline.airflow_dag

####################################################################################################################
# Steps
####################################################################################################################

samba_campaign_query = """
        with campaign_am
         as (select cmp.CampaignId,
                    cf.CampaignFlightId,
                    a.AdvertiserId,
                    a.AdvertiserRevenueGroupId,
                    b.BrandName,
                    cf.StartDateInclusiveUTC,
                    cf.EndDateExclusiveUTC,
                    b.IrrProviderId
             from dbo.Campaign cmp
                      inner join iSpot.Campaign ic
                                 on cmp.CampaignId = ic.CampaignId
                      inner join dbo.CampaignFlight cf
                                 on cmp.CampaignId = cf.CampaignId
                      inner join iSpot.Brand b
                                 on ic.BrandId = b.BrandId
                      inner join dbo.Advertiser a
                                 on cmp.AdvertiserId = a.AdvertiserId
             where ic.IsEnabled = 1
               and b.IrrProviderId = 4
               and cmp.IsBuildInProgress = 0
             group by cmp.CampaignId,
                      cf.CampaignFlightId,
                      a.AdvertiserId,
                      a.AdvertiserRevenueGroupId,
                      b.BrandName,
                      cf.StartDateInclusiveUTC,
                      cf.EndDateExclusiveUTC,
                      b.IrrProviderId),
        account_manager_details
         as (select cmp.CampaignId,
                    cmp.CampaignFlightId,
                    u.LoweredEmail as Email,
                    cont.FirstName + ' ' + cont.LastName as AccountManager,
                    row_number() over (partition by cmp.AdvertiserId,
                        cmp.CampaignId,
                        cmp.BrandName,
                        cmp.StartDateInclusiveUTC,
                        cmp.EndDateExclusiveUTC
                        order by tea.TTDEmployeeAssignmentTypeId
                        ) as order_num
             from InternalBusiness.internal.TTDEmployeeAssignment tea
                      inner join campaign_am cmp
                                 on tea.AdvertiserRevenueGroupId = cmp.AdvertiserRevenueGroupId
                      inner join UserProfile.dbo.[User] u
                                 on tea.TTDEmployeeId = u.UserId
                      inner join UserProfile.dbo.[Contact] cont
                                 on u.UserId = cont.UserId
             where tea.EndDate is null
               and tea.TTDEmployeeRoleId = 0
               and tea.TTDEmployeeAssignmentTypeId in (0, 2, 3)),
        primary_account_manager_details
         as (select *
             from account_manager_details
             where order_num = 1)

    select
        c.AdvertiserId,
        c.CampaignId,
        c.BrandName,
        coalesce(pamd.AccountManager, 'Grace Zhao') as AccountManager,
        coalesce(pamd.Email, 'grace.zhao@thetradedesk.com') as Email,
        c.StartDateInclusiveUTC,
        c.EndDateExclusiveUTC
    from
        campaign_am c
    left join
        primary_account_manager_details pamd
        on c.CampaignId = pamd.CampaignId and c.CampaignFlightId = pamd.CampaignFlightId
    """

start_task = OpTask(op=EmptyOperator(task_id='Start', dag=dag))


def load_and_write_samba_data_to_s3(**context):
    sql_hook = MsSqlHook(mssql_conn_id=sql_connection, schema=db_name)
    campaignDetails = sql_hook.get_pandas_df(samba_campaign_query)
    outputDf_str = campaignDetails.to_csv()

    aws_storage = AwsCloudStorage(conn_id='aws_default')
    aws_storage.load_string(
        string_data=outputDf_str,
        key=s3_base_path + context['data_interval_start'].strftime("%Y-%m-%d") + '.csv',
        bucket_name=s3_bucket,
        replace=True
    )


write_samba_data_to_s3 = OpTask(
    op=PythonOperator(
        dag=dag,
        python_callable=load_and_write_samba_data_to_s3,
        provide_context=True,
        task_id="load_and_write_samba_data_to_s3",
    )
)

end_task = OpTask(op=EmptyOperator(task_id='End', dag=dag))

start_task >> write_samba_data_to_s3 >> end_task
