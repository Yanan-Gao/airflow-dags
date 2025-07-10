from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

from ttd.eldorado.base import TtdDag
from ttd.slack.slack_groups import AUDAUTO
from ttd.tasks.op import OpTask
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.operators.dummy_operator import DummyOperator
from dags.audauto.utils import utils
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.operators.dataset_check_sensor import DatasetCheckSensor

start_date = datetime(2025, 5, 12, 1, 0)

msck_repair_tables = ['cpa.dailyattributionevents', 'roas.dailyattributionevents']
two_partition_keys_tables = [
    ('audauto_shared.bidfeedback', "`date`='{{ ds_nodash }}'", 'hour', [f"'{i:02d}'" for i in range(24)]),
    ('audauto_shared.rtb_bidrequest_cleanfile', "`date`='{{ ds_nodash }}'", 'hour', [f"'{i:02d}'" for i in range(24)]),
    ('audauto_shared.rtb_clicktracker_cleanfile', "`date`='{{ ds_nodash }}'", 'hour', [f"'{i:02d}'" for i in range(24)]),
    ('audauto_shared.rtb_conversiontracker_cleanfile', "`date`='{{ ds_nodash }}'", 'hour', [f"'{i:02d}'" for i in range(24)]),
    ('audauto_shared.rtb_conversiontracker_verticaload', "`date`='{{ ds_nodash }}'", 'hour', [f"'{i:02d}'" for i in range(24)]),
    ('rsm.density_feature', "`date`='{{ ds_nodash }}'", 'split', [f"{i}" for i in range(10)]),
    ('rsm.tdid2seedid', "`date`='{{ ds_nodash }}'", 'split', [f"{i}" for i in range(10)]),
    ('rsm.tdid2seedid_raw', "`date`='{{ ds_nodash }}'", 'split', [f"{i}" for i in range(10)]),
    ('rsm.tdid_emb', "`date`='{{ ds_nodash }}'", 'split', [f"{i}" for i in range(10)]),
    ('roas.trainset', "`date`='{{ ds_nodash }}'", 'split', ["'train'", "'val'"]),
    ('roas.trainset_click', "`date`='{{ ds_nodash }}'", 'split', ["'train'", "'val'"]),
    ('roas.trainset_click_userdata', "`date`='{{ ds_nodash }}'", 'split', ["'train'", "'val'"]),
    ('roas.trainset_userdata', "`date`='{{ ds_nodash }}'", 'split', ["'train'", "'val'"]),
    ('cpa.trainset', "`date`='{{ ds_nodash }}'", 'split', ["'train'", "'val'"]),
    ('cpa.trainset_inc', "`date`='{{ ds_nodash }}'", 'split', ["'train'", "'val'"]),
    ('cpa.trainset_click', "`date`='{{ ds_nodash }}'", 'split', ["'train'", "'val'"]),
    ('cpa.trainset_click_inc', "`date`='{{ ds_nodash }}'", 'split', ["'train'", "'val'"]),
    ('cpa.trainset_click_userdata', "`date`='{{ ds_nodash }}'", 'split', ["'train'", "'val'"]),
    ('cpa.trainset_click_userdata_inc', "`date`='{{ ds_nodash }}'", 'split', ["'train'", "'val'"]),
    ('cpa.trainset_userdata', "`date`='{{ ds_nodash }}'", 'split', ["'train'", "'val'"]),
    ('cpa.trainset_userdata_inc', "`date`='{{ ds_nodash }}'", 'split', ["'train'", "'val'"]),
]

daily_tables = [
    ('audauto_shared.adgroup', "`date`='{{ ds_nodash }}'"),
    ('audauto_shared.advertiser', "`date`='{{ ds_nodash }}'"),
    ('audauto_shared.attributedevent', "`date`='{{ ds }}'"),
    ('audauto_shared.attributedeventresult', "`date`='{{ ds }}'"),
    (
        'audauto_shared.bidsimpressions',
        "`year`='{{data_interval_start.strftime(\"%Y\")}}', `month`='{{data_interval_start.strftime(\"%m\")}}', `day`='{{data_interval_start.strftime(\"%d\")}}'"
    ),
    ('audauto_shared.campaign', "`date`='{{ ds_nodash }}'"),
    ('audauto_shared.campaignconversionreportingcolumn', "`date`='{{ ds_nodash }}'"),
    ('audauto_shared.campaignflight', "`date`='{{ ds_nodash }}'"),
    ('audauto_shared.campaignroigoal', "`date`='{{ ds_nodash }}'"),
    ('audauto_shared.campaignseed', "`date`='{{ ds_nodash }}'"),
    ('audauto_shared.clicktracker', "`date`='{{ ds }}'"),
    ('audauto_shared.conversiontracker', "`date`='{{ ds }}'"),
    ('audauto_shared.eventtracker', "`date`='{{ ds }}'"),
    ('audauto_shared.partner', "`date`='{{ ds_nodash }}'"),
    ('audauto_shared.seeninbiddingdevice', "`date`='{{ ds_nodash }}'"),
    ('audauto_shared.targetingdata', "`date`='{{ ds_nodash }}'"),
    ('audauto_shared.thirdpartydata', "`date`='{{ ds_nodash }}'"),
    ('audauto_shared.distributedalgosadvertiserrestrictionstatus', "`date`='{{ ds_nodash }}'"),
    ('cpa.cvrforscaling', "`date`='{{ ds_nodash }}'"),
    ('cpa.dailyadgrouppolicy', "`date`='{{ ds_nodash }}'"),
    ('cpa.dailyadgrouppolicymapping', "`date`='{{ ds_nodash }}'"),
    ('cpa.dailyattribution', "`date`='{{ ds_nodash }}'"),
    ('cpa.dailybidfeedback', "`date`='{{ ds_nodash }}'"),
    ('cpa.dailybidsimpressions', "`date`='{{ ds_nodash }}'"),
    ('cpa.dailyclick', "`date`='{{ ds_nodash }}'"),
    ('cpa.dailyconversion', "`date`='{{ ds_nodash }}'"),
    ('cpa.sampledimpressionforscoring', "`date`='{{ ds_nodash }}'"),
    ('cpa.scored', "`date`='{{ ds_nodash }}'"),
    ('roas.dailyadgrouppolicy', "`date`='{{ ds_nodash }}'"),
    ('roas.dailyadgrouppolicymapping', "`date`='{{ ds_nodash }}'"),
    ('roas.dailyattribution', "`date`='{{ ds_nodash }}'"),
    ('roas.dailybidfeedback', "`date`='{{ ds_nodash }}'"),
    ('roas.dailybidsimpressions', "`date`='{{ ds_nodash }}'"),
    ('roas.dailyclick', "`date`='{{ ds_nodash }}'"),
    ('roas.dailyexchangerate', "`date`='{{ ds_nodash }}'"),
    ('rsm.seedids', "`date`='{{ ds_nodash }}'"),
    ('rsm.seedpopulationscore', "`date`='{{ ds_nodash }}'"),
]

ttd_dag = TtdDag(
    dag_id="perf-automation-metastore-update",
    start_date=start_date,
    schedule_interval='0 3 * * *',
    dag_tsg='https://atlassian.thetradedesk.com/confluence/x/qdkMCQ',
    retries=1,
    retry_delay=timedelta(hours=1),
    # max_active_runs=3,
    slack_channel="#dev-perf-auto-alerts-cpa-roas",
    slack_tags=AUDAUTO.team.sub_team,
    enable_slack_alert=True,
    run_only_latest=False,
    tags=["AUDAUTO"]
)
adag = ttd_dag.airflow_dag

adgroup_data = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external",
    data_name="thetradedesk.db/provisioning/adgroup",
    version=1,
    date_format="date=%Y%m%d",
    env_aware=False,
    success_file=None,
)

campaignflight_data = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external",
    data_name="thetradedesk.db/provisioning/campaignflight",
    version=1,
    date_format="date=%Y%m%d",
    env_aware=False,
    success_file=None,
)

dataset_sensor = OpTask(
    op=DatasetCheckSensor(
        task_id="data_available",
        datasets=[adgroup_data, campaignflight_data],
        ds_date="{{data_interval_start.to_datetime_string()}}",
        poke_interval=60 * 10,
        timeout=60 * 60 * 12,
    )
)


def delete_s3_objects(**kwargs):
    s3_path = kwargs['s3_path']
    utils.delete_s3_objects_with_prefix(s3_path=s3_path)


clean_up_adgroup = OpTask(
    op=PythonOperator(
        task_id='clean_up_adgroup',
        provide_context=True,
        python_callable=delete_s3_objects,
        op_kwargs={'s3_path': 's3://thetradedesk-mlplatform-us-east-1/features/feature_store/prod/online/adgroup_exclusion_list'},
        dag=adag,
    )
)

gen_adgroup_exclusion_list = OpTask(
    op=AWSAthenaOperator(
        task_id='gen_adgroup_exclusion_list',
        output_location='s3://thetradedesk-useast-data-import/temp/athena-output',
        query='''insert into audauto_shared.adgroup_exclusion_list
        with ag as (
        select adgroupid, partnerid,campaignid,advertiserid from audauto_shared.adgroup a where date='{{ ds_nodash }}' and isenabled=true
        ),
        active_campaigns as (
            select distinct c.campaignid  from audauto_shared.campaignflight c
            where date='{{ ds_nodash }}' and EndDateExclusiveUTC > timestamp '{{ ds }}' and IsDeleted=false
        ),
        active_ag as (
            select adgroupid, partnerid, advertiserid
            from ag join active_campaigns on (ag.campaignid=active_campaigns.campaignid)
        ),
        ad_excluded as (
            select adgroupid from active_ag join audauto_shared.partner_exclusion_list pel
            on (active_ag.partnerid = pel.partnerid)
        ),
        medhealth as (
            select distinct AdvertiserId from audauto_shared.distributedalgosadvertiserrestrictionstatus where date='{{ ds_nodash }}' and IsRestricted=1 and categorypolicy='Health'
        ),
        ad_medhealth as (
            select adgroupid from active_ag join medhealth on (active_ag.advertiserid=medhealth.advertiserid)
        ),
        unioned as (
            select adgroupid from ad_excluded union select adgroupid from ad_medhealth
        )
        select adgroupid from unioned;
            ''',
        retries=3,
        sleep_time=60,
        aws_conn_id='aws_default',
        database='youjunyuan'
    )
)

pre_op = dataset_sensor
for tbl, partition in daily_tables:
    op = OpTask(
        op=AWSAthenaOperator(
            task_id=f'add_partitions_{tbl}',
            output_location='s3://thetradedesk-useast-data-import/temp/athena-output',
            query=f'''alter table {tbl} add IF NOT EXISTS partition ({partition});
                ''',
            retries=3,
            sleep_time=60,
            aws_conn_id='aws_default',
            database='youjunyuan'
        )
    )
    pre_op >> op
    pre_op = op

for tbl in msck_repair_tables:
    op = OpTask(
        op=AWSAthenaOperator(
            task_id=f'add_partitions_{tbl}',
            output_location='s3://thetradedesk-useast-data-import/temp/athena-output',
            query=f'''msck repair table {tbl};
                ''',
            retries=3,
            sleep_time=60,
            aws_conn_id='aws_default',
            database='youjunyuan'
        )
    )
    pre_op >> op
    pre_op = op

for tbl, daily_partition, hour_partition_name, hour_partition_values in two_partition_keys_tables:
    dummy_end = OpTask(op=DummyOperator(task_id=f"dummy_end_{tbl}", dag=ttd_dag.airflow_dag))
    for hour_partition_value in hour_partition_values:
        op = OpTask(
            op=AWSAthenaOperator(
                task_id=f'add_partitions_{tbl}_{hour_partition_value}'.replace("'", ''),
                output_location='s3://thetradedesk-useast-data-import/temp/athena-output',
                query=f'''alter table {tbl} add IF NOT EXISTS partition ({daily_partition}, {hour_partition_name}={hour_partition_value});
                    ''',
                retries=3,
                sleep_time=60,
                aws_conn_id='aws_default',
                database='youjunyuan'
            )
        )
        pre_op >> op >> dummy_end
    pre_op = dummy_end

ttd_dag >> dataset_sensor
pre_op >> clean_up_adgroup >> gen_adgroup_exclusion_list
