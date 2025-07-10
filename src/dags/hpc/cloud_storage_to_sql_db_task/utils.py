from ttd.datasets.dataset import Dataset
from typing import List, Dict, Any, Union


def get_dataset_prefix(dataset: Dataset, dataset_partitioning_args: Dict[str, Any]) -> str:
    parsed_partitioning_args = dataset.parse_partitioning_args(**dataset_partitioning_args)
    prefix = dataset._get_full_key(**parsed_partitioning_args)
    formatted_prefix = prefix if prefix.endswith('/') else prefix + '/'
    return formatted_prefix


def convert_bool_to_config_str(x: bool) -> str:
    return str(x).lower()


class CloudStorageToSqlColumn:

    def __init__(self, source_column_name: str, destination_column_name: str):
        self.source = source_column_name
        self.destination = destination_column_name


def column_mapping_to_string(column_mapping: List[CloudStorageToSqlColumn]) -> str:
    return ','.join([f'{col_map.source}:{col_map.destination}' for col_map in column_mapping])


class ConsistencyCheckCondition:

    def __init__(self, column: str, value: Union[str, int, float]):
        self.column = column
        self.value = value


def consistency_check_conditions_to_string(conditions: List[ConsistencyCheckCondition]) -> str:
    conditions_list = []

    for condition in conditions:
        if isinstance(condition.value, str):
            # we wrap the string values in single quotes, which makes it easier to use them in SQL
            conditions_list.append(f"{condition.column}='{condition.value}'")
        else:
            conditions_list.append(f"{condition.column}={condition.value}")

    conditions_as_str = ','.join(conditions_list)

    return conditions_as_str
