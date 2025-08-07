from datetime import timedelta, datetime

from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ec2.emr_instance_types.memory_optimized.r5 import R5
from ttd.el_dorado.v2.base import TtdDag
from ttd.el_dorado.v2.emr import EmrClusterTask, EmrJobTask
from ttd.eldorado.emr_cluster_scaling_properties import EmrClusterScalingProperties
from ttd.eldorado.fleet_instance_types import EmrFleetInstanceTypes
from ttd.interop.vertica_import_operators import VerticaImportFromCloud, LogTypeFrequency
from ttd.slack.slack_groups import INVENTORY_MARKETPLACE
from ttd.ttdenv import TtdEnvFactory

###########################################
#   Job Configs
###########################################

job_name = 'cac-statistics-pipeline'
job_slack_channel = '#scrum-invmkt-alarms'
job_schedule_interval = timedelta(hours=24)
job_start_date = datetime(2024, 8, 13)
job_environment = TtdEnvFactory.get_from_system()
s3_bucket = 'ttd-campaign-audience-composition'
s3_env_prefix = 'prod' if job_environment == TtdEnvFactory.prod else 'test'
invmkt = INVENTORY_MARKETPLACE().team

jar_path = 's3://ttd-build-artefacts/eldorado/release-spark-3/main-spark-3/latest/el-dorado-assembly.jar'
class_name = 'jobs.campaignaudiencecomposition.CampaignAudienceComposition'
active_running_jobs = 1

sourceTypes = ['BidFeedbacks', 'Clicks']
campaignsTypes = ['PG']  # NonPG is also supported but is ignored for now
gating_type_ids = {'BidFeedbacks': 2000052, 'Clicks': 2000053}
vertica_import_enabled = True
log_type_frequency = LogTypeFrequency.DAILY

# Date and Partition configs
run_date = '{{ dag_run.logical_date.strftime("%Y-%m-%d") }}'
run_date_prefix_format = '{{ dag_run.logical_date.strftime(\"%Y%m%d\") }}'

####################################################################################################################
# DAG
####################################################################################################################

# The top-level dag
dag: TtdDag = TtdDag(
    dag_id=job_name,
    start_date=job_start_date,
    schedule_interval=job_schedule_interval,
    max_active_runs=active_running_jobs,
    slack_channel=job_slack_channel,
    retries=0,
    tags=[invmkt.jira_team, 'campaign-audience-composition'],
    default_args={
        # If depends_on_past=True then future task instances will block until previous instances
        # are successful, i.e. needs human intervention. Use False only if the workflow doesn't
        # depend on output from previous instances.
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "email": None,
        "owner": invmkt.jira_team,
        "retries": 0,
        "start_date": job_start_date,
    }
)

####################################################################################################################
# clusters
####################################################################################################################

base_ebs_size = 4000
instance_type = R5.r5_12xlarge()  # EmrInstanceTypes.r5d_12xlarge()

instance_types = [
    instance_type.with_ebs_size_gb(base_ebs_size).with_fleet_weighted_capacity(1),
]
cac_on_demand_weighted_capacity = 5
cluster_params = instance_type.calc_cluster_params(instances=cac_on_demand_weighted_capacity, parallelism_factor=5)

master_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=[instance_type.with_ebs_size_gb(base_ebs_size).with_fleet_weighted_capacity(1)], on_demand_weighted_capacity=1
)

core_fleet_instance_type_configs = EmrFleetInstanceTypes(
    instance_types=instance_types, on_demand_weighted_capacity=cac_on_demand_weighted_capacity
)

cluster_name = "invmkt_campaign_audience_composition"
cac_cluster_task = EmrClusterTask(
    name=cluster_name,
    emr_release_label=AwsEmrVersions.AWS_EMR_SPARK_3_2_1,
    master_fleet_instance_type_configs=master_fleet_instance_type_configs,
    core_fleet_instance_type_configs=core_fleet_instance_type_configs,
    cluster_tags={"Team": invmkt.jira_team},
    enable_prometheus_monitoring=True,
    cluster_auto_terminates=True,
    environment=job_environment,
    additional_application_configurations=[{
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "true"
        }
    }],
    managed_cluster_scaling_config=EmrClusterScalingProperties(
        cac_on_demand_weighted_capacity * 2, cac_on_demand_weighted_capacity, cac_on_demand_weighted_capacity * 2,
        cac_on_demand_weighted_capacity * 2
    )
)

####################################################################################################################
# Spark configs: Set automatically; except for shuffle.partitions
####################################################################################################################
spark_configs = [("conf", "spark.sql.shuffle.partitions=20000"), ("conf", "spark.driver.maxResultSize=0"),
                 ("conf", "spark.sql.autoBroadcastJoinThreshold=-1")]

####################################################################################################################
# steps
####################################################################################################################

coldstorage_aerospike_host = "{{ macros.ttd_extras.resolve_consul_url('ttd-coldstorage-onprem.aerospike.service.vaf.consul', limit=1) }}"

job_steps = []
for sourceType in sourceTypes:
    for campaignType in campaignsTypes:
        config_list = [
            ('aerospikeHostName', coldstorage_aerospike_host),
            ('aerospikePort', 4333),
            ('redisHost', "campaign-audience-composition-redis.hoonr9.0001.use1.cache.amazonaws.com"),
            ('redisPort', 6379),
            ('globalQpsLimit', 25000),
            ('threadPoolSize', 32),
            ('bucketSizeSec', 15),
            ('maxResultQueueSize', 1024),
            ('lookupPartitionNum', 295),
            ('maxResultQueueSize', 1024),
            ('CACSource', sourceType),
            ('campaignsType', campaignType),
            ('MaxSampleSizePerCampaign', 5000),
            ('histogramBuckets', "10,100,1000,2000,5000"),
            ('numOutputFiles', 10),
            ('date', run_date),
        ]
        if campaignType == 'PG':  # We only want look up with cross vendor for ctv PGs for scale purposes
            config_list.append(('xDVendorIds', "10"))

        job_step = EmrJobTask(
            name=f"cac_{sourceType}_{campaignType}",
            class_name=class_name,  # no class name means its a .sh, will use jar path as script path
            executable_path=jar_path,
            configure_cluster_automatically=True,
            additional_args_option_pairs_list=spark_configs,
            eldorado_config_option_pairs_list=config_list,
            timeout_timedelta=timedelta(hours=12),
            cluster_specs=cac_cluster_task.cluster_specs
        )
        job_steps.append(job_step)
        cac_cluster_task.add_parallel_body_task(job_step)

# Airflow only recognizes top-level dag objects, so extract the underlying dag we generated.
dag >> cac_cluster_task
adag = dag.airflow_dag

####################################################################################################################
# VerticaImportFromCloud
####################################################################################################################
for sourceType in sourceTypes:
    vertica_import_task = VerticaImportFromCloud(
        dag=adag,
        subdag_name=sourceType,
        gating_type_id=gating_type_ids[sourceType],
        log_type_frequency=log_type_frequency,
        vertica_import_enabled=vertica_import_enabled,
        job_environment=job_environment,
    )

    cac_cluster_task >> vertica_import_task
