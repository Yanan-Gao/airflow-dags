# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: forecasting_hist.proto
# Protobuf Python Version: 5.29.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2  # noqa # pylint: disable=unused-import

_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'forecasting_hist.proto')
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x16\x66orecasting_hist.proto\x12\x11ttd.forecast_hist\x1a\x1fgoogle/protobuf/timestamp.proto\"y\n DeploymentNameWithRamAddressList\x12U\n\x1c\x64\x65ployment_ram_web_addresses\x18\x01 \x03(\x0b\x32/.ttd.forecast_hist.DeploymentNameWithRamAddress\"[\n\x1c\x44\x65ploymentNameWithRamAddress\x12\x17\n\x0f\x64\x65ployment_name\x18\x01 \x01(\t\x12\"\n\x1a\x64\x65ployment_ram_web_address\x18\x02 \x01(\t\"\xa5\x01\n\x15ReturnAndStoreOptions\x12+\n\x1eshould_upload_to_cloud_storage\x18\x01 \x01(\x08H\x00\x88\x01\x01\x12\"\n\x15should_return_results\x18\x02 \x01(\x08H\x01\x88\x01\x01\x42!\n\x1f_should_upload_to_cloud_storageB\x18\n\x16_should_return_results\"/\n\x0e\x44\x65ploymentList\x12\x1d\n\x15productionDeployments\x18\x01 \x03(\t\"r\n\x0f\x46orecastRequest\x12\x35\n)OBSOLETE_forecasting_adgroup_execution_id\x18\x01 \x01(\x03\x42\x02\x18\x01\x12(\n forecasting_adgroup_execution_id\x18\x02 \x01(\t\"\x8d\x02\n\x12\x46orecastRequestRaw\x12.\n\ncreated_at\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x1f\n\x17\x66orecasting_ad_group_id\x18\x02 \x01(\t\x12\x15\n\rforecast_hash\x18\x03 \x01(\t\x12 \n\x13\x66orecast_deployment\x18\x04 \x01(\tH\x00\x88\x01\x01\x12\x1f\n\x12request_identifier\x18\x05 \x01(\tH\x01\x88\x01\x01\x12\x1d\n\x15\x66orecast_request_json\x18\x06 \x01(\tB\x16\n\x14_forecast_deploymentB\x15\n\x13_request_identifier\"*\n\x07\x41\x64Group\x12\x1f\n\x17\x66orecasting_ad_group_id\x18\x01 \x01(\t\"q\n\x0c\x46orecastList\x12\x36\n*OBSOLETE_forecasting_adgroup_execution_ids\x18\x01 \x03(\x03\x42\x02\x18\x01\x12)\n!forecasting_adgroup_execution_ids\x18\x02 \x03(\t\"\xcb\x14\n\x0c\x46orecastData\x12\x1e\n\x11\x61vails_result_min\x18\x01 \x01(\x03H\x00\x88\x01\x01\x12\x1e\n\x11\x61vails_result_max\x18\x02 \x01(\x03H\x01\x88\x01\x01\x12#\n\x16impressions_result_min\x18\x03 \x01(\x03H\x02\x88\x01\x01\x12#\n\x16impressions_result_max\x18\x04 \x01(\x03H\x03\x88\x01\x01\x12\x1d\n\x10spend_result_min\x18\x05 \x01(\x03H\x04\x88\x01\x01\x12\x1d\n\x10spend_result_max\x18\x06 \x01(\x03H\x05\x88\x01\x01\x12\'\n\x1a\x66requency_users_result_min\x18\x07 \x01(\x01H\x06\x88\x01\x01\x12\'\n\x1a\x66requency_users_result_max\x18\x08 \x01(\x01H\x07\x88\x01\x01\x12(\n\x1b\x66requency_people_result_min\x18\t \x01(\x01H\x08\x88\x01\x01\x12(\n\x1b\x66requency_people_result_max\x18\n \x01(\x01H\t\x88\x01\x01\x12+\n\x1e\x66requency_household_result_min\x18\x0b \x01(\x01H\n\x88\x01\x01\x12+\n\x1e\x66requency_household_result_max\x18\x0c \x01(\x01H\x0b\x88\x01\x01\x12#\n\x16reach_users_result_min\x18\r \x01(\x03H\x0c\x88\x01\x01\x12#\n\x16reach_users_result_max\x18\x0e \x01(\x03H\r\x88\x01\x01\x12$\n\x17reach_people_result_min\x18\x0f \x01(\x03H\x0e\x88\x01\x01\x12$\n\x17reach_people_result_max\x18\x10 \x01(\x03H\x0f\x88\x01\x01\x12\'\n\x1areach_household_result_min\x18\x11 \x01(\x03H\x10\x88\x01\x01\x12\'\n\x1areach_household_result_max\x18\x12 \x01(\x03H\x11\x88\x01\x01\x12.\n\ncreated_at\x18\x13 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x1f\n\x17\x66orecasting_ad_group_id\x18\x14 \x01(\t\x12\x18\n\x0b\x61verage_bid\x18\x15 \x01(\x01H\x12\x88\x01\x01\x12\x1a\n\x12\x63onfiguration_tags\x18\x16 \x03(\t\x12\x30\n\ndimensions\x18! \x03(\x0b\x32\x1c.ttd.forecast_hist.Dimension\x12!\n\x19\x66orecast_duration_in_days\x18\" \x01(\x05\x12\x15\n\rforecast_hash\x18# \x01(\t\x12-\n bid_above_floor_price_percentage\x18$ \x01(\x01H\x13\x88\x01\x01\x12\x16\n\x0e\x65xecution_tags\x18% \x03(\t\x12\x14\n\x07user_id\x18& \x01(\tH\x14\x88\x01\x01\x12\x16\n\tcall_type\x18\' \x01(\tH\x15\x88\x01\x01\x12@\n\x12session_attributes\x18( \x03(\x0b\x32$.ttd.forecast_hist.SessionAttributes\x12 \n\x13\x66orecast_deployment\x18) \x01(\tH\x16\x88\x01\x01\x12\x36\n\nconfidence\x18, \x01(\x0b\x32\x1d.ttd.forecast_hist.ConfidenceH\x17\x88\x01\x01\x12\x1f\n\x12request_identifier\x18- \x01(\tH\x18\x88\x01\x01\x12=\n\x07metrics\x18. \x03(\x0b\x32,.ttd.forecast_hist.ForecastData.MetricsEntry\x12\x34\n\trelevance\x18/ \x01(\x0b\x32\x1c.ttd.forecast_hist.RelevanceH\x19\x88\x01\x01\x12\x14\n\x07seed_id\x18\x30 \x01(\tH\x1a\x88\x01\x01\x12-\n forecasting_adgroup_execution_id\x18\x31 \x01(\tH\x1b\x88\x01\x01\x12T\n\x11haystack_coverage\x18\x32 \x01(\x0e\x32\x34.ttd.forecast_hist.ForecastData.HaystackCoverageTypeH\x1c\x88\x01\x01\x12K\n\x0f\x64\x61g_runner_type\x18\x33 \x01(\x0e\x32-.ttd.forecast_hist.ForecastData.DagRunnerTypeH\x1d\x88\x01\x01\x12\x1a\n\radvertiser_id\x18\x34 \x01(\tH\x1e\x88\x01\x01\x1a.\n\x0cMetricsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x01:\x02\x38\x01\"Y\n\x14HaystackCoverageType\x12\x08\n\x04None\x10\x00\x12\x17\n\x13PartialFrequencyMap\x10\x01\x12\x14\n\x10\x46ullFrequencyMap\x10\x02\x12\x08\n\x04\x46ull\x10\x03\"L\n\rDagRunnerType\x12\x10\n\x0cSketchesOnly\x10\x00\x12\x17\n\x13SketchesAndHaystack\x10\x01\x12\x10\n\x0cHaystackOnly\x10\x02\x42\x14\n\x12_avails_result_minB\x14\n\x12_avails_result_maxB\x19\n\x17_impressions_result_minB\x19\n\x17_impressions_result_maxB\x13\n\x11_spend_result_minB\x13\n\x11_spend_result_maxB\x1d\n\x1b_frequency_users_result_minB\x1d\n\x1b_frequency_users_result_maxB\x1e\n\x1c_frequency_people_result_minB\x1e\n\x1c_frequency_people_result_maxB!\n\x1f_frequency_household_result_minB!\n\x1f_frequency_household_result_maxB\x19\n\x17_reach_users_result_minB\x19\n\x17_reach_users_result_maxB\x1a\n\x18_reach_people_result_minB\x1a\n\x18_reach_people_result_maxB\x1d\n\x1b_reach_household_result_minB\x1d\n\x1b_reach_household_result_maxB\x0e\n\x0c_average_bidB#\n!_bid_above_floor_price_percentageB\n\n\x08_user_idB\x0c\n\n_call_typeB\x16\n\x14_forecast_deploymentB\r\n\x0b_confidenceB\x15\n\x13_request_identifierB\x0c\n\n_relevanceB\n\n\x08_seed_idB#\n!_forecasting_adgroup_execution_idB\x14\n\x12_haystack_coverageB\x12\n\x10_dag_runner_typeB\x10\n\x0e_advertiser_idJ\x04\x08\x17\x10!J\x04\x08*\x10+J\x04\x08+\x10,\"B\n\x0e\x45xperimentName\x12\x1c\n\x0f\x65xperiment_name\x18\x01 \x01(\tH\x00\x88\x01\x01\x42\x12\n\x10_experiment_name\"\x85\x02\n\x14\x45xperimentDefinition\x12\x1c\n\x0f\x65xperiment_name\x18\x01 \x01(\tH\x00\x88\x01\x01\x12[\n\x13\x63ontrol_test_groups\x18\x02 \x03(\x0b\x32>.ttd.forecast_hist.ExperimentDefinition.ControlTestGroupsEntry\x12\x16\n\tis_active\x18\x03 \x01(\x08H\x01\x88\x01\x01\x1a\x38\n\x16\x43ontrolTestGroupsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x42\x12\n\x10_experiment_nameB\x0c\n\n_is_active\"Y\n\x0e\x45xperimentList\x12G\n\x16\x65xperiment_definitions\x18\x01 \x03(\x0b\x32\'.ttd.forecast_hist.ExperimentDefinition\"u\n\x11SessionAttributes\x12\x1b\n\x0e\x61ttribute_name\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x1c\n\x0f\x61ttribute_value\x18\x02 \x01(\tH\x01\x88\x01\x01\x42\x11\n\x0f_attribute_nameB\x12\n\x10_attribute_value\"{\n\rForecastReply\x12\x35\n)OBSOLETE_forecasting_adgroup_execution_id\x18\x01 \x01(\x03\x42\x02\x18\x01\x12-\n\x04\x64\x61ta\x18\" \x01(\x0b\x32\x1f.ttd.forecast_hist.ForecastDataJ\x04\x08\x02\x10\"\"\x1b\n\x07TagData\x12\x10\n\x08tag_name\x18\x01 \x01(\t\")\n\nTagRequest\x12\x1b\n\x13\x66orecasting_tags_id\x18\x01 \x01(\x03\"\"\n\x0eTagRequestName\x12\x10\n\x08tag_name\x18\x01 \x01(\t\"Q\n\x08TagReply\x12\x1b\n\x13\x66orecasting_tags_id\x18\x01 \x01(\x03\x12(\n\x04\x64\x61ta\x18\x04 \x01(\x0b\x32\x1a.ttd.forecast_hist.TagData\"\x95\x01\n\x15\x46orecastTagConnection\x12\x35\n)OBSOLETE_forecasting_adgroup_execution_id\x18\x01 \x01(\x03\x42\x02\x18\x01\x12\x1b\n\x13\x66orecasting_tags_id\x18\x02 \x01(\x03\x12(\n forecasting_adgroup_execution_id\x18\x03 \x01(\t\"g\n\tDimension\x12+\n#forecasting_dimension_descriptor_id\x18\x01 \x01(\x03\x12-\n\tbid_lists\x18\x02 \x03(\x0b\x32\x1a.ttd.forecast_hist.BidList\"\xbc\x01\n\x07\x42idList\x12I\n\x0f\x61\x64justment_type\x18\x01 \x01(\x0e\x32\x30.ttd.forecast_hist.BidList.BidListAdjustmentType\x12\x0f\n\x07hash_id\x18\x02 \x01(\t\x12\x0f\n\x07vectors\x18\x03 \x03(\t\"D\n\x15\x42idListAdjustmentType\x12\r\n\tEXCLUSION\x10\x00\x12\r\n\tINCLUSION\x10\x01\x12\r\n\tOPTIMIZED\x10\x02\"{\n\tTimeframe\x12.\n\x05start\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x00\x88\x01\x01\x12,\n\x03\x65nd\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x01\x88\x01\x01\x42\x08\n\x06_startB\x06\n\x04_end\")\n\x05\x43ount\x12\r\n\x05total\x18\x01 \x01(\x03\x12\x11\n\tno_avails\x18\x02 \x01(\x03\"\xcb\x01\n\x14TimeframeWithSorting\x12J\n\x11sortingPreference\x18\x01 \x01(\x0e\x32/.ttd.forecast_hist.TimeframeWithSorting.OrderBy\x12/\n\ttimeframe\x18\x02 \x01(\x0b\x32\x1c.ttd.forecast_hist.Timeframe\"6\n\x07OrderBy\x12\x13\n\x0fTOTAL_FORECASTS\x10\x00\x12\x16\n\x12NO_AVAIL_FORECASTS\x10\x01\"N\n\x13\x45ntityForecastsList\x12\x37\n\x08\x65ntities\x18\x01 \x03(\x0b\x32%.ttd.forecast_hist.EntityNumForecasts\"I\n\x12\x45ntityNumForecasts\x12\n\n\x02id\x18\x01 \x01(\t\x12\'\n\x05\x63ount\x18\x02 \x01(\x0b\x32\x18.ttd.forecast_hist.Count\"e\n\x19\x46orecastConfigurationList\x12H\n\x16\x66orecastConfigurations\x18\x01 \x03(\x0b\x32(.ttd.forecast_hist.ForecastConfiguration\"Z\n\x15\x46orecastConfiguration\x12\x12\n\nadgroup_id\x18\x01 \x01(\t\x12-\n\tbid_lists\x18\x02 \x03(\x0b\x32\x1a.ttd.forecast_hist.BidList\"\xa3\x01\n\x0f\x43onfidenceIssue\x12\x0c\n\x04type\x18\x01 \x01(\t\x12\x10\n\x08severity\x18\x02 \x01(\t\x12@\n\x07\x64\x65tails\x18\x03 \x03(\x0b\x32/.ttd.forecast_hist.ConfidenceIssue.DetailsEntry\x1a.\n\x0c\x44\x65tailsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"X\n\nConfidence\x12\x16\n\x0eissue_detected\x18\x01 \x01(\x08\x12\x32\n\x06issues\x18\x02 \x03(\x0b\x32\".ttd.forecast_hist.ConfidenceIssue\"\xdd\x01\n\x1f\x44\x42RetrievalRequestWithTimeFrame\x12/\n\ttimeframe\x18\x01 \x01(\x0b\x32\x1c.ttd.forecast_hist.Timeframe\x12&\n\x1eshould_upload_to_cloud_storage\x18\x02 \x01(\x08\x12\x1d\n\x15should_return_results\x18\x03 \x01(\x08\x12%\n\x18number_of_forecast_limit\x18\x04 \x01(\x05H\x00\x88\x01\x01\x42\x1b\n\x19_number_of_forecast_limit\"\xca\x05\n\x18SanityCheckConfiguration\x12\x12\n\nqueue_task\x18\x01 \x01(\x08\x12_\n\x16sanity_check_task_name\x18\x02 \x01(\x0e\x32?.ttd.forecast_hist.SanityCheckConfiguration.SanityCheckTaskName\x12P\n\x0btask_config\x18\x03 \x03(\x0b\x32;.ttd.forecast_hist.SanityCheckConfiguration.TaskConfigEntry\x1a\x31\n\x0fTaskConfigEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\xb3\x03\n\x13SanityCheckTaskName\x12\x19\n\x15\x43onfidenceSanityCheck\x10\x00\x12\x1a\n\x16\x46orecastingConsistency\x10\x01\x12\x16\n\x12OverlapSanityCheck\x10\x02\x12\x13\n\x0fMockSanityCheck\x10\x03\x12\x1c\n\x18\x43ountryCensusSanityCheck\x10\x04\x12\x18\n\x14MockConsistencyCheck\x10\x05\x12%\n!AdditionalBidListConsistencyCheck\x10\x06\x12!\n\x1d\x42idListSubsetConsistencyCheck\x10\x07\x12\x1e\n\x1a\x44\x65viceTagsConsistencyCheck\x10\x08\x12\x1c\n\x18TimeSpanConsistencyCheck\x10\t\x12\x1d\n\x19\x46requencyConsistencyCheck\x10\n\x12\x17\n\x13GeoConsistencyCheck\x10\x0b\x12$\n PrivateContractsConsistencyCheck\x10\x0c\x12\x1a\n\x16SnP500ConsistencyCheck\x10\r\"\xe2\x01\n\x11SanityCheckResult\x12\x0c\n\x04pass\x18\x01 \x01(\x08\x12\x1e\n\x16sanity_check_task_name\x18\x02 \x01(\t\x12\x16\n\x0eresult_details\x18\x03 \x01(\t\x12\x19\n\x11sanity_check_name\x18\x04 \x01(\t\x12\x15\n\rcheck_filters\x18\x05 \x01(\t\x12\x19\n\x11passing_threshold\x18\x06 \x01(\x01\x12\x18\n\x10\x61\x63tual_threshold\x18\x07 \x01(\x01\x12 \n\x18summary_under_test_count\x18\x08 \x01(\x04\"\xc5\x01\n\x15SanityCheckTaskResult\x12?\n\x06status\x18\x01 \x01(\x0e\x32/.ttd.forecast_hist.SanityCheckTaskResult.Status\x12\x41\n\x13sanity_check_result\x18\x02 \x03(\x0b\x32$.ttd.forecast_hist.SanityCheckResult\"(\n\x06Status\x12\x08\n\x04Pass\x10\x00\x12\x08\n\x04\x46\x61il\x10\x01\x12\n\n\x06Queued\x10\x02\"\xc1\x01\n\tRelevance\x12\x1e\n\x16relevance_distribution\x18\x01 \x03(\x01\x12\x0f\n\x07seed_id\x18\x02 \x01(\t\x12@\n\x12session_attributes\x18\x06 \x03(\x0b\x32$.ttd.forecast_hist.SessionAttributes\x12;\n\x14relevance_confidence\x18\x07 \x01(\x0b\x32\x1d.ttd.forecast_hist.ConfidenceJ\x04\x08\x03\x10\x06\x32\x81\x0c\n\x12\x46orecastingHistory\x12R\n\rStoreForecast\x12\x1f.ttd.forecast_hist.ForecastData\x1a .ttd.forecast_hist.ForecastReply\x12X\n\x10RetrieveForecast\x12\".ttd.forecast_hist.ForecastRequest\x1a .ttd.forecast_hist.ForecastReply\x12Z\n\x1bRetrieveForecastsForAdGroup\x12\x1a.ttd.forecast_hist.AdGroup\x1a\x1f.ttd.forecast_hist.ForecastList\x12\x81\x01\n\x1dRetrieveForecastsWithAudience\x12\x32.ttd.forecast_hist.DBRetrievalRequestWithTimeFrame\x1a,.ttd.forecast_hist.ForecastConfigurationList\x12o\n\x16\x45xecuteSanityCheckTask\x12+.ttd.forecast_hist.SanityCheckConfiguration\x1a(.ttd.forecast_hist.SanityCheckTaskResult\x12l\n\x18\x41\x64\x64\x46orecastingExperiment\x12\'.ttd.forecast_hist.ExperimentDefinition\x1a\'.ttd.forecast_hist.ExperimentDefinition\x12k\n\x1cRetrieveAllActiveExperiments\x12(.ttd.forecast_hist.ReturnAndStoreOptions\x1a!.ttd.forecast_hist.ExperimentList\x12`\n\x12\x41\x63tivateExperiment\x12!.ttd.forecast_hist.ExperimentName\x1a\'.ttd.forecast_hist.ExperimentDefinition\x12\x62\n\x14\x44\x65\x61\x63tivateExperiment\x12!.ttd.forecast_hist.ExperimentName\x1a\'.ttd.forecast_hist.ExperimentDefinition\x12l\n\x1dRetrieveProductionDeployments\x12(.ttd.forecast_hist.ReturnAndStoreOptions\x1a!.ttd.forecast_hist.DeploymentList\x12`\n\x18\x41\x64\x64ProductionDeployments\x12!.ttd.forecast_hist.DeploymentList\x1a!.ttd.forecast_hist.DeploymentList\x12\x63\n\x1bRemoveProductionDeployments\x12!.ttd.forecast_hist.DeploymentList\x1a!.ttd.forecast_hist.DeploymentList\x12\x85\x01\n!SetDeploymentRamServiceWebAddress\x12/.ttd.forecast_hist.DeploymentNameWithRamAddress\x1a/.ttd.forecast_hist.DeploymentNameWithRamAddress\x12\x8d\x01\n,RetrieveActiveDeploymentsWithRamWebAddresses\x12(.ttd.forecast_hist.ReturnAndStoreOptions\x1a\x33.ttd.forecast_hist.DeploymentNameWithRamAddressListBS\n\'com.thetradedesk.forecast.hist.contract\xaa\x02\'TTD.Forecasting.HistoryService.Contractb\x06proto3'
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'forecasting_hist_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR'
             ]._serialized_options = b'\n\'com.thetradedesk.forecast.hist.contract\252\002\'TTD.Forecasting.HistoryService.Contract'
    _globals['_FORECASTREQUEST'].fields_by_name['OBSOLETE_forecasting_adgroup_execution_id']._loaded_options = None
    _globals['_FORECASTREQUEST'].fields_by_name['OBSOLETE_forecasting_adgroup_execution_id']._serialized_options = b'\030\001'
    _globals['_FORECASTLIST'].fields_by_name['OBSOLETE_forecasting_adgroup_execution_ids']._loaded_options = None
    _globals['_FORECASTLIST'].fields_by_name['OBSOLETE_forecasting_adgroup_execution_ids']._serialized_options = b'\030\001'
    _globals['_FORECASTDATA_METRICSENTRY']._loaded_options = None
    _globals['_FORECASTDATA_METRICSENTRY']._serialized_options = b'8\001'
    _globals['_EXPERIMENTDEFINITION_CONTROLTESTGROUPSENTRY']._loaded_options = None
    _globals['_EXPERIMENTDEFINITION_CONTROLTESTGROUPSENTRY']._serialized_options = b'8\001'
    _globals['_FORECASTREPLY'].fields_by_name['OBSOLETE_forecasting_adgroup_execution_id']._loaded_options = None
    _globals['_FORECASTREPLY'].fields_by_name['OBSOLETE_forecasting_adgroup_execution_id']._serialized_options = b'\030\001'
    _globals['_FORECASTTAGCONNECTION'].fields_by_name['OBSOLETE_forecasting_adgroup_execution_id']._loaded_options = None
    _globals['_FORECASTTAGCONNECTION'].fields_by_name['OBSOLETE_forecasting_adgroup_execution_id']._serialized_options = b'\030\001'
    _globals['_CONFIDENCEISSUE_DETAILSENTRY']._loaded_options = None
    _globals['_CONFIDENCEISSUE_DETAILSENTRY']._serialized_options = b'8\001'
    _globals['_SANITYCHECKCONFIGURATION_TASKCONFIGENTRY']._loaded_options = None
    _globals['_SANITYCHECKCONFIGURATION_TASKCONFIGENTRY']._serialized_options = b'8\001'
    _globals['_DEPLOYMENTNAMEWITHRAMADDRESSLIST']._serialized_start = 78
    _globals['_DEPLOYMENTNAMEWITHRAMADDRESSLIST']._serialized_end = 199
    _globals['_DEPLOYMENTNAMEWITHRAMADDRESS']._serialized_start = 201
    _globals['_DEPLOYMENTNAMEWITHRAMADDRESS']._serialized_end = 292
    _globals['_RETURNANDSTOREOPTIONS']._serialized_start = 295
    _globals['_RETURNANDSTOREOPTIONS']._serialized_end = 460
    _globals['_DEPLOYMENTLIST']._serialized_start = 462
    _globals['_DEPLOYMENTLIST']._serialized_end = 509
    _globals['_FORECASTREQUEST']._serialized_start = 511
    _globals['_FORECASTREQUEST']._serialized_end = 625
    _globals['_FORECASTREQUESTRAW']._serialized_start = 628
    _globals['_FORECASTREQUESTRAW']._serialized_end = 897
    _globals['_ADGROUP']._serialized_start = 899
    _globals['_ADGROUP']._serialized_end = 941
    _globals['_FORECASTLIST']._serialized_start = 943
    _globals['_FORECASTLIST']._serialized_end = 1056
    _globals['_FORECASTDATA']._serialized_start = 1059
    _globals['_FORECASTDATA']._serialized_end = 3694
    _globals['_FORECASTDATA_METRICSENTRY']._serialized_start = 2689
    _globals['_FORECASTDATA_METRICSENTRY']._serialized_end = 2735
    _globals['_FORECASTDATA_HAYSTACKCOVERAGETYPE']._serialized_start = 2737
    _globals['_FORECASTDATA_HAYSTACKCOVERAGETYPE']._serialized_end = 2826
    _globals['_FORECASTDATA_DAGRUNNERTYPE']._serialized_start = 2828
    _globals['_FORECASTDATA_DAGRUNNERTYPE']._serialized_end = 2904
    _globals['_EXPERIMENTNAME']._serialized_start = 3696
    _globals['_EXPERIMENTNAME']._serialized_end = 3762
    _globals['_EXPERIMENTDEFINITION']._serialized_start = 3765
    _globals['_EXPERIMENTDEFINITION']._serialized_end = 4026
    _globals['_EXPERIMENTDEFINITION_CONTROLTESTGROUPSENTRY']._serialized_start = 3936
    _globals['_EXPERIMENTDEFINITION_CONTROLTESTGROUPSENTRY']._serialized_end = 3992
    _globals['_EXPERIMENTLIST']._serialized_start = 4028
    _globals['_EXPERIMENTLIST']._serialized_end = 4117
    _globals['_SESSIONATTRIBUTES']._serialized_start = 4119
    _globals['_SESSIONATTRIBUTES']._serialized_end = 4236
    _globals['_FORECASTREPLY']._serialized_start = 4238
    _globals['_FORECASTREPLY']._serialized_end = 4361
    _globals['_TAGDATA']._serialized_start = 4363
    _globals['_TAGDATA']._serialized_end = 4390
    _globals['_TAGREQUEST']._serialized_start = 4392
    _globals['_TAGREQUEST']._serialized_end = 4433
    _globals['_TAGREQUESTNAME']._serialized_start = 4435
    _globals['_TAGREQUESTNAME']._serialized_end = 4469
    _globals['_TAGREPLY']._serialized_start = 4471
    _globals['_TAGREPLY']._serialized_end = 4552
    _globals['_FORECASTTAGCONNECTION']._serialized_start = 4555
    _globals['_FORECASTTAGCONNECTION']._serialized_end = 4704
    _globals['_DIMENSION']._serialized_start = 4706
    _globals['_DIMENSION']._serialized_end = 4809
    _globals['_BIDLIST']._serialized_start = 4812
    _globals['_BIDLIST']._serialized_end = 5000
    _globals['_BIDLIST_BIDLISTADJUSTMENTTYPE']._serialized_start = 4932
    _globals['_BIDLIST_BIDLISTADJUSTMENTTYPE']._serialized_end = 5000
    _globals['_TIMEFRAME']._serialized_start = 5002
    _globals['_TIMEFRAME']._serialized_end = 5125
    _globals['_COUNT']._serialized_start = 5127
    _globals['_COUNT']._serialized_end = 5168
    _globals['_TIMEFRAMEWITHSORTING']._serialized_start = 5171
    _globals['_TIMEFRAMEWITHSORTING']._serialized_end = 5374
    _globals['_TIMEFRAMEWITHSORTING_ORDERBY']._serialized_start = 5320
    _globals['_TIMEFRAMEWITHSORTING_ORDERBY']._serialized_end = 5374
    _globals['_ENTITYFORECASTSLIST']._serialized_start = 5376
    _globals['_ENTITYFORECASTSLIST']._serialized_end = 5454
    _globals['_ENTITYNUMFORECASTS']._serialized_start = 5456
    _globals['_ENTITYNUMFORECASTS']._serialized_end = 5529
    _globals['_FORECASTCONFIGURATIONLIST']._serialized_start = 5531
    _globals['_FORECASTCONFIGURATIONLIST']._serialized_end = 5632
    _globals['_FORECASTCONFIGURATION']._serialized_start = 5634
    _globals['_FORECASTCONFIGURATION']._serialized_end = 5724
    _globals['_CONFIDENCEISSUE']._serialized_start = 5727
    _globals['_CONFIDENCEISSUE']._serialized_end = 5890
    _globals['_CONFIDENCEISSUE_DETAILSENTRY']._serialized_start = 5844
    _globals['_CONFIDENCEISSUE_DETAILSENTRY']._serialized_end = 5890
    _globals['_CONFIDENCE']._serialized_start = 5892
    _globals['_CONFIDENCE']._serialized_end = 5980
    _globals['_DBRETRIEVALREQUESTWITHTIMEFRAME']._serialized_start = 5983
    _globals['_DBRETRIEVALREQUESTWITHTIMEFRAME']._serialized_end = 6204
    _globals['_SANITYCHECKCONFIGURATION']._serialized_start = 6207
    _globals['_SANITYCHECKCONFIGURATION']._serialized_end = 6921
    _globals['_SANITYCHECKCONFIGURATION_TASKCONFIGENTRY']._serialized_start = 6434
    _globals['_SANITYCHECKCONFIGURATION_TASKCONFIGENTRY']._serialized_end = 6483
    _globals['_SANITYCHECKCONFIGURATION_SANITYCHECKTASKNAME']._serialized_start = 6486
    _globals['_SANITYCHECKCONFIGURATION_SANITYCHECKTASKNAME']._serialized_end = 6921
    _globals['_SANITYCHECKRESULT']._serialized_start = 6924
    _globals['_SANITYCHECKRESULT']._serialized_end = 7150
    _globals['_SANITYCHECKTASKRESULT']._serialized_start = 7153
    _globals['_SANITYCHECKTASKRESULT']._serialized_end = 7350
    _globals['_SANITYCHECKTASKRESULT_STATUS']._serialized_start = 7310
    _globals['_SANITYCHECKTASKRESULT_STATUS']._serialized_end = 7350
    _globals['_RELEVANCE']._serialized_start = 7353
    _globals['_RELEVANCE']._serialized_end = 7546
    _globals['_FORECASTINGHISTORY']._serialized_start = 7549
    _globals['_FORECASTINGHISTORY']._serialized_end = 9086
# @@protoc_insertion_point(module_scope)
