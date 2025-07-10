from dags.datperf.date_partitioned_dataset import DatePartitionedDataset
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourGeneratedDataset
from ttd.datasets.rtb_datalake_dataset import RtbDatalakeDataset

# A few notes:
# - Url Format is
#   bucket
#   /path_prefix
#   /env=***        | if env_aware=True/default. If false, this is skipped
#   /data_name
#   /version_str    | if version=None, this is skipped
#   /date_str       | Using date_format. Default is `date=yyyymmdd`
#   /hour_str       | Using hour_format. Default is `hour=hh`
#
# - An HourGeneratedDataset is checked for the whole day by the DatasetCheckSensor.
#   If you want it to check only for the specified hour, you need to add
#   `.with_check_type("hour")`

# =======================================
# GERONIMO
# =======================================

# Sample S3 Url: s3://thetradedesk-mlplatform-us-east-1/features/data/koav4/v=1/prod/bidsimpressions/
#                   year=2024/month=06/day=02/hourPart=23/_SUCCESS
geronimo_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="features/data/koav4/v=1",
    env_aware=True,
    data_name="bidsimpressions",
    version=None,
    date_format="year=%Y/month=%m/day=%d",
    hour_format="hourPart={hour}"
)

# =======================================
# PLUTUS DATASETS
# =======================================

# S3 Url: s3://ttd-identity/datapipeline/prod/pcresultsgeronimo/v=3/date=20240703/hour=12/_SUCCESS
plutus_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="ttd-identity", path_prefix="datapipeline", env_aware=True, data_name="pcresultsgeronimo", version=3, hour_format="hour={hour}"
)

# S3 Url: s3://thetradedesk-mlplatform-us-east-1/features/data/plutus/v=1/prod/singledayprocessed/rubicon/
#                   year=2024/month=01/day=07/_SUCCESS
plutus_etl_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="features/data/plutus/v=1",
    env_aware=True,
    data_name="singledayprocessed/rubicon",
    version=None,  # Version comes before env currently
    date_format="year=%Y/month=%m/day=%d",
)

# S3 Url: s3://thetradedesk-useast-logs-2/predictiveclearingresults/collected/2024/07/03/12/
pc_results_log_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-useast-logs-2",
    path_prefix="predictiveclearingresults",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-logs-2/lostbidrequest/cleansed/2024/07/03/12/
lost_bidrequest_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-useast-logs-2",
    path_prefix="lostbidrequest",
    env_aware=False,
    data_name="cleansed",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
)

# =======================================
# VERTICA DATASETS
# =======================================

# S3 Url: s3://ttd-vertica-backups/ExportProductionAdGroupBudgetSnapshot/VerticaAws/date=20240703/hour=12/_SUCCESS
production_adgroup_budget_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="ttd-vertica-backups",
    path_prefix="ExportProductionAdGroupBudgetSnapshot",
    env_aware=False,
    data_name="VerticaAws",
    version=None
)

# S3 Url: s3://ttd-vertica-backups/ExportProductionVolumeControlBudgetSnapshot/VerticaAws/date=20240703/hour=12/_SUCCESS
production_volumecontrol_budget_dataset = HourGeneratedDataset(
    bucket="ttd-vertica-backups",
    path_prefix="ExportProductionVolumeControlBudgetSnapshot",
    data_name="VerticaAws",
    version=None,
    env_aware=False,
)

# S3 Url: s3://ttd-vertica-backups/ExportProductionCampaignBudgetSnapshot/VerticaAws/date=20240703/hour=12/_SUCCESS
production_campaign_budget_dataset = HourGeneratedDataset(
    bucket="ttd-vertica-backups",
    path_prefix="ExportProductionCampaignBudgetSnapshot",
    data_name="VerticaAws",
    version=None,
    env_aware=False,
)

# s3://ttd-identity/datapipeline/prod/internalauctionresultslog/v=1/date=20240718/hour=0/
ial_dataset = HourGeneratedDataset(
    bucket="ttd-identity",
    path_prefix="datapipeline",
    data_name="internalauctionresultslog",
    hour_format="hour={hour}",
    version=1,
    env_aware=True,
    success_file=None
)

# S3 Url: s3://ttd-vertica-backups/ExportPlatformReport/VerticaAws/date=20240703/hour=12/_SUCCESS
platformreport_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="ttd-vertica-backups",
    path_prefix="ExportPlatformReport",
    data_name="VerticaAws",
    version=None,
    env_aware=False,
    hour_format="hour={hour:0>2d}"
)

# S3 Url: s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/env=prod/aggregate-pacing-statistics/v=2/metric=throttle_metric_parquet/date=20240703/
campaignthrottlemetric_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="model_monitor/mission_control/env=prod/aggregate-pacing-statistics/v=2",
    data_name="metric=throttle_metric_parquet",
    version=None,
    env_aware=False
)

# S3 Url: s3://thetradedesk-mlplatform-us-east-1/model_monitor/mission_control/env=prod/aggregate-pacing-statistics/v=2/metric=throttle_metric_campaign_parquet/date=20240703/
adgroupthrottlemetric_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="model_monitor/mission_control/env=prod/aggregate-pacing-statistics/v=2",
    data_name="metric=throttle_metric_campaign_parquet",
    version=None,
    env_aware=False
)

# S3 Url: s3://thetradedesk-mlplatform-us-east-1/model_monitor/model_metrics/dev/kongming/v=1/source=outofsample/pipeline=prod/
kongming_oos_dataset: DateGeneratedDataset = DatePartitionedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="model_monitor/model_metrics/dev/kongming/v=1/source=outofsample",
    data_name="pipeline=prod",
    version=None,
    env_aware=False,
    partitions=['id_type=CampaignId', 'userdataoptin=0'],
    success_file=None
)

kongming_oos_optin_dataset: DateGeneratedDataset = DatePartitionedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="model_monitor/model_metrics/dev/kongming/v=1/source=outofsample",
    data_name="pipeline=prod",
    version=None,
    env_aware=False,
    partitions=['id_type=CampaignId', 'userdataoptin=1'],
    success_file=None
)

kongming_oosattribution_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="data/prod/kongming/outofsampleattributionset/v=1",
    data_name="delay=3D",
    version=None,
    env_aware=False,
    success_file=None,
    date_format="date=%Y%m%d"
)

# S3 Url: s3://ttd-vertica-backups/ExportPlatformReport/VerticaAws/date=20240703/hour=12/_SUCCESS
rtbplatformreport_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="ttd-vertica-backups",
    path_prefix="ExportPlatformReport",
    data_name="VerticaAws",
    version=None,
    env_aware=False,
    hour_format="hour={hour:0>2d}"
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/vertica/VerticaKoaVolumeControlBudgetExportToS3/v=1/date=202040101/_SUCCESS
verticakoabudget_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/vertica",
    data_name="VerticaKoaVolumeControlBudgetExportToS3",
    version=1,
    env_aware=False,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/vertica/VerticaKoaCampaignSpendTargets/v=2/date=202040101/_SUCCESS
campaignspendtargets_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/vertica",
    data_name="VerticaKoaCampaignSpendTargets",
    version=2,
    env_aware=False,
    success_file=None
)

# =======================================
# PROVISIONING DATASETS
# =======================================

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/privatecontract/v=1/
#               date=20240703/privatecontract_2024-07-03T02-00-14.parquet
privatecontract_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    env_aware=False,
    data_name="privatecontract",
    version=1,
    success_file=None,
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/adformat/v=1/
#               date=20240703/adformat_2024-07-03T03-12-31.parquet
adformat_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    env_aware=False,
    data_name="adformat",
    version=1,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/adgroup/v=1/
#               date=20240703/
adgroup_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    env_aware=False,
    data_name="adgroup",
    version=1,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/campaign/v=1/
#               date=20240703/
campaign_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    env_aware=False,
    data_name="campaign",
    version=1,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/roigoaltype/v=1/
#               date=20240703/
roigoaltype_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    env_aware=False,
    data_name="roigoaltype",
    version=1,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/advertiser/v=1/
#               date=20240703/
advertiser_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    env_aware=False,
    data_name="advertiser",
    version=1,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/campaignroigoal/advertiser/v=1/
#               date=20240703/
campaignroigoal_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    env_aware=False,
    data_name="campaignroigoal",
    version=1,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/partnergroup/advertiser/v=1/
#               date=20240703/
partnergroup_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    env_aware=False,
    data_name="partnergroup",
    version=1,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/partner/advertiser/v=1/
#               date=20240703/
partner_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    env_aware=False,
    data_name="partner",
    version=1,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/currencyexchangerate/advertiser/v=1/
#               date=20240703/
currencyexchangerate_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    env_aware=False,
    data_name="currencyexchangerate",
    version=1,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/supplyvendor/v=1/
#               date=20240703/
supplyvendor_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    env_aware=False,
    data_name="supplyvendor",
    version=1,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/campaignflight/v=1/date=20240703/
campaignflight_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="campaignflight",
    version=1,
    success_file=None,
    env_aware=False
)

# S3 Url: s3a://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/viewabilitysettings/v=1/
viewsettings_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="viewabilitysettings",
    version=1,
    env_aware=False,
    success_file=None
)

# S3 Url: s3a://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/viewabilitysettings/v=1/
vcr_etl_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-mlplatform-us-east-1",
    path_prefix="models",
    data_name="vcr-prediction/v=1/hashed_featureset",
    version=None,
    env_aware=True,
    success_file=None,
    date_format="date=%Y%m%d"
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/geosegment/v=1
geosegment_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="geosegment",
    version=1,
    env_aware=False,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/flattenedstandardlocation/v=1
flattenedstandardlocation_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="flattenedstandardlocation",
    version=1,
    env_aware=False,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/country/v=1
country_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="country",
    version=1,
    env_aware=False,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/region/v=1
region_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="region",
    version=1,
    env_aware=False,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/metro/v=1
metro_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="metro",
    version=1,
    env_aware=False,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/city/v=1
city_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="city",
    version=1,
    env_aware=False,
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-qubole/warehouse.external/thetradedesk.db/provisioning/zip/v=1
zip_dataset: DateGeneratedDataset = DateGeneratedDataset(
    bucket="thetradedesk-useast-qubole",
    path_prefix="warehouse.external/thetradedesk.db/provisioning",
    data_name="zip",
    version=1,
    env_aware=False,
    success_file=None
)

# =======================================
# Parquet DATASETS
# =======================================

# S3 Url: s3://thetradedesk-useast-logs-2/budgetrvaluecalculationresult/collected/2024/12/09/23
r_value_calculation_result_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-useast-logs-2",
    path_prefix="budgetrvaluecalculationresult",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-logs-2/budgetvolumecontrolcalculationresult/collected/2024/12/09/23
volume_control_calculation_result_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-useast-logs-2",
    path_prefix="budgetvolumecontrolcalculationresult",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-logs-2/budgetadgroupcalculationresult/collected/2024/12/09/23
adgroup_calculation_result_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-useast-logs-2",
    path_prefix="budgetadgroupcalculationresult",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-logs-2/budgetvirtualadgroupcalculationresult/collected/2024/12/01/24/
virtual_adgroup_calculation_result_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-useast-logs-2",
    path_prefix="budgetvirtualadgroupcalculationresult",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-logs-2/budgetcampaigncalculationresult/collected/2024/12/09/23
campaign_calculation_result_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-useast-logs-2",
    path_prefix="budgetcampaigncalculationresult",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-logs-2/budgetvirtualcampaigncalculationresult/collected/2024/12/01/24/
virtual_campaign_calculation_result_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-useast-logs-2",
    path_prefix="budgetvirtualcampaigncalculationresult",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
)

# S3 Url: s3://ttd-datapipe-data/parquet/rtb_bidfeedback_cleanfile/v=5/
rtb_bidfeedback_v5: RtbDatalakeDataset = RtbDatalakeDataset(
    bucket="ttd-datapipe-data",
    azure_bucket="ttd-datapipe-data@eastusttdlogs",
    path_prefix="parquet",
    data_name="rtb_bidfeedback_cleanfile",
    version=5,
    success_file="_SUCCESS-sx-%Y%m%d-%H",
    env_aware=True,
    eldorado_class="BidFeedbackDataSetV5",
    oss_bucket="ttd-datapipe-data",
)

# S3 Url: s3://ttd-datapipe-data/parquet/rtb_bidrequest_cleanfile/v=5/
rtb_bidrequest_v5: RtbDatalakeDataset = RtbDatalakeDataset(
    bucket="ttd-datapipe-data",
    azure_bucket="ttd-datapipe-data@eastusttdlogs",
    path_prefix="parquet",
    data_name="rtb_bidrequest_cleanfile",
    version=5,
    success_file="_SUCCESS-sx-%Y%m%d-%H",
    env_aware=True,
    eldorado_class="BidFeedbackDataSetV5",
    oss_bucket="ttd-datapipe-data",
)

# S3 Url: s3://thetradedesk-useast-logs-2/budgetadgroupcalculationresult/collected/2024/12/01/24/
adgroup_budget_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-useast-logs-2",
    path_prefix="budgetadgroupcalculationresult",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
)

# S3 Url: s3://thetradedesk-useast-logs-2/budgetcampaigncalculationresult/collected/2024/12/01/24/
campaign_budget_dataset: HourGeneratedDataset = HourGeneratedDataset(
    bucket="thetradedesk-useast-logs-2",
    path_prefix="budgetcampaigncalculationresult",
    env_aware=False,
    data_name="collected",
    version=None,
    date_format="%Y/%m/%d",
    hour_format="{hour:0>2d}",
    success_file=None
)
