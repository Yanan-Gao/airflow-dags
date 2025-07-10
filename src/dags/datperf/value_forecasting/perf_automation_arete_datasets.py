from typing import Literal, List

from ttd.datasets.dataset import Dataset
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.ttdenv import TtdEnv

MLPLATFORM_S3_BUCKET = "thetradedesk-mlplatform-us-east-1"
DATASET_NAME_PREFIX = "value-pacing-forecasting"


def build_dataset(dataset_type: Literal['data', 'models'], dataset_name: str, env: TtdEnv = None) -> Dataset:
    full_name = f"{DATASET_NAME_PREFIX}/{dataset_name}"
    dataset = DateGeneratedDataset(bucket=MLPLATFORM_S3_BUCKET, path_prefix=dataset_type, data_name=full_name, version=None, env_aware=True)
    if env is not None:
        dataset = dataset.with_env(env)
    return dataset


def build_datasets(names: List[str], dataset_type: Literal['data', 'models'], env: TtdEnv = None) -> List[Dataset]:
    return [build_dataset(dataset_type, d, env) for d in names]
