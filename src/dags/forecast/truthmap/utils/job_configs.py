from typing import Union

from dags.forecast.truthmap.utils.constants.mapping_type import MappingType


def get_base_truth_map_combination_generation_config(avail_sampling_rate: int, avail_stream: str, combination_list_name: str,
                                                     id_type: str) -> list[Union[tuple[str, str], tuple[str, int]]]:
    # see TruthMapCombinationListGenerationCombinationConfig in etl-based-forecasts
    return [("combinationListNames", combination_list_name), ("idType", id_type), ("targetDate", "{{ ds }}"),
            ("availSamplingRate", avail_sampling_rate), ("availStream", avail_stream)]


def enhance_base_truth_map_combination_generation_config_with_partials(base_config, combination_list_name: str
                                                                       ) -> list[Union[tuple[str, str], tuple[str, int]]]:
    if _has_partials(combination_list_name):
        return base_config + [("generatePartialCombinations", "true")]
    return base_config


def get_base_truth_map_mapping_config(
    avail_stream: str, combination_list_name: str, combination_list_type: str, combination_mapping_type: str, id_type: str
) -> list[tuple[str, str]]:
    # see TruthMapMappingConfig in etl-based-forecasts
    return [("availStream", avail_stream), ("combinationListName", combination_list_name), ("combinationListType", combination_list_type),
            ("idType", id_type), ("mappingTypes", combination_mapping_type), ("targetDate", "{{ ds }}")]


def enhance_base_truth_map_mapping_config_with_partials(base_config, combination_list_name: str) -> list[tuple[str, str]]:
    if _has_partials(combination_list_name):
        return base_config + [("hasPartials", "true")]
    return base_config


def _has_partials(combination_list_name: str) -> bool:
    return combination_list_name == MappingType.INTERSECT_COMBINATION_LIST_NAME
