from ttd.datasets.date_generated_dataset import DateGeneratedDataset


def get_frequency_map_result(
    avail_stream: str = "deviceSampled",
    id_type: str = "tdid",
    mapping_type: str = "targetingData",
    lookback_days: int = 1,
) -> DateGeneratedDataset:
    return DateGeneratedDataset(
        bucket="thetradedesk-useast-qubole",
        path_prefix="frequency-maps",
        data_name=f"resultsV3/{avail_stream}/{id_type}/{mapping_type}/{lookback_days}",
        version=None,
        success_file=None,
        env_aware=True,
    )


def get_audience_number_to_data_group_numbers(avail_stream: str, id_type: str) -> DateGeneratedDataset:
    return DateGeneratedDataset(
        bucket="thetradedesk-useast-qubole",
        path_prefix="frequency-maps",
        data_name=f"truthset/targetingData/audiences/audienceNumberToDataGroupNumbers/{avail_stream}/{id_type}",
        version=None,
        success_file=None,
        env_aware=True,
    )


def get_data_group_number_to_targeting_data_ids(avail_stream: str, id_type: str) -> DateGeneratedDataset:
    return DateGeneratedDataset(
        bucket="thetradedesk-useast-qubole",
        path_prefix="frequency-maps",
        data_name=f"truthset/targetingData/dataGroups/dataGroupNumberToTargetingDataIds/{avail_stream}/{id_type}",
        version=None,
        success_file=None,
        env_aware=True,
    )


def get_id_to_audiences_frequency(avail_stream: str, id_type: str) -> DateGeneratedDataset:
    return DateGeneratedDataset(
        bucket="thetradedesk-useast-qubole",
        path_prefix="frequency-maps",
        data_name=f"truthset/targetingData/audiences/idToAudiencesFrequency/{avail_stream}/{id_type}",
        version=None,
        success_file=None,
        env_aware=True,
    )
