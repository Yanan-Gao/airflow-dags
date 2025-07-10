from airflow import DAG
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta

from dags.cmo.utils.experian_partner_config import ExperianPartnerConfig
from dags.cmo.utils.fleet_batch_config import EmrInstanceClasses, EmrInstanceSizes, \
    getFleetInstances, getMasterFleetInstances
from dags.cmo.utils.pipeline_config import PipelineConfig
from datasources.datasources import Datasources
from ttd.cloud_storages.aws_cloud_storage import AwsCloudStorage
from ttd.el_dorado.v2.base import TtdDag
from ttd.eldorado.aws.emr_cluster_task import EmrClusterTask
from ttd.eldorado.aws.emr_job_task import EmrJobTask
from ttd.slack.slack_groups import CMO
from ttd.tasks.op import OpTask

job_start_date = datetime(2024, 10, 1, 0, 0)
job_schedule_interval = "0 8 1 * *"  # monthly job

pipeline_name = "ctv-experian-crosswalk"  # IMPORTANT: add "dev-" prefix when testing

crosswalk_job_prefix = "ctv-experian-crosswalk_"

pipeline_config = PipelineConfig()

experian_data = Datasources.experian()

################################################################################################
# DAG
################################################################################################
dag: TtdDag = TtdDag(
    dag_id=pipeline_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    slack_channel=CMO.team.alarm_channel,
    slack_tags=CMO.team.sub_team,
    retries=1,
    retry_delay=timedelta(hours=2),
    tags=["experian"]
)
adag: DAG = dag.airflow_dag


################################################################################################
# Pipeline Dependencies
################################################################################################
def check_crosswalk_data(partner_name: str, logical_date: str):
    logical_date_dt = datetime.strptime(logical_date, "%Y-%m-%d %H:%M:%S%z")
    month = logical_date_dt.strftime("%Y%m")
    key = f'experian/upload/crosswalk/TheTradeDesk_{partner_name}_Crosswalk_{month}.psv.gz'
    aws_storage = AwsCloudStorage(conn_id='aws_default')

    if aws_storage.check_for_key(key, bucket_name='thetradedesk-useast-data-import'):
        print(f's3://thetradedesk-useast-data-import/{key} exists')
        return True
    print(f's3://thetradedesk-useast-data-import/{key} does not exists')
    return False


################################################################################################
# Will run for each experian partner: tivo and fwm, but not for dtv (it has its own DAG)
################################################################################################
partner_configs = [ExperianPartnerConfig('tivo', 'Tivo_Expanded')]
all_partner = ','.join([p.name for p in partner_configs])

for partner in partner_configs:
    crosswalk_sensor = OpTask(
        op=PythonSensor(
            task_id='experian_crosswalk_sensor_' + partner.name + '_' + partner.file_upload_partner_name,
            python_callable=check_crosswalk_data,
            op_kwargs={
                'partner_name': partner.file_upload_partner_name,
                'logical_date': '{{ logical_date }}'
            },
            poke_interval=60 * 60 * 2,  # poke every 2 hours
            timeout=60 * 60 * 24,  # wait for a whole day
            mode='reschedule',
            dag=adag,
        )
    )

    crosswalk = EmrClusterTask(
        name=crosswalk_job_prefix + partner.name + '_' + partner.file_upload_partner_name,
        master_fleet_instance_type_configs=getMasterFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX),
        cluster_tags={
            "Team": CMO.team.jira_team,
        },
        core_fleet_instance_type_configs=
        getFleetInstances(EmrInstanceClasses.GeneralPurpose, EmrInstanceSizes.OneX, instance_capacity=4, ebs_size=32),
        emr_release_label=pipeline_config.emr_release_label,
        additional_application_configurations=pipeline_config.get_cluster_additional_configurations()
    )
    crosswalk.add_parallel_body_task(
        EmrJobTask(
            name='ExperianCrosswalk',
            class_name="jobs.ctv.experian.ExperianCrosswalk",
            eldorado_config_option_pairs_list=[("date", '{{ logical_date.strftime(\"%Y-%m-%d\") }}'),
                                               ("experianBasePath", "s3a://thetradedesk-useast-data-import/experian/upload"),
                                               ("crosswalkOutputPath", experian_data.crosswalk_output(partner.name).get_root_path()),
                                               ("partner", partner.name), ("partnerNameInFile", partner.file_upload_partner_name),
                                               ("allPartnersSplitByComma", all_partner)],
            timeout_timedelta=timedelta(minutes=90),
            executable_path=pipeline_config.jar,
            additional_args_option_pairs_list=pipeline_config.get_step_additional_configurations()
        )
    )

    # Structure
    # Add crosswalk steps for each provider
    dag >> crosswalk_sensor >> crosswalk
