from ttd.semver import SemverVersion
from enum import Enum
from typing import List, Optional


class DatabricksRuntimeSpecification():
    label: str
    python_version: SemverVersion
    spark_version: SemverVersion
    ml: bool
    gpu: bool

    def __init__(self, label: str, python_version: SemverVersion, spark_version: SemverVersion, ml=False, gpu=False) -> None:
        self.label = label
        self.python_version = python_version
        self.spark_version = spark_version
        self.ml = ml
        self.gpu = gpu


class DatabricksRuntimeVersion(Enum):
    DB_10_4 = DatabricksRuntimeSpecification("10.4.x-scala2.12", SemverVersion(3, 8, 10), SemverVersion(3, 2, 1))
    DB_11_3 = DatabricksRuntimeSpecification("11.3.x-scala2.12", SemverVersion(3, 9, 19), SemverVersion(3, 3, 0))
    DB_12_2 = DatabricksRuntimeSpecification("12.2.x-scala2.12", SemverVersion(3, 9, 19), SemverVersion(3, 3, 2))
    DB_13_3 = DatabricksRuntimeSpecification("13.3.x-scala2.12", SemverVersion(3, 10, 12), SemverVersion(3, 4, 1))
    DB_14_3 = DatabricksRuntimeSpecification("14.3.x-scala2.12", SemverVersion(3, 10, 12), SemverVersion(3, 5, 0))
    DB_15_4 = DatabricksRuntimeSpecification("15.4.x-scala2.12", SemverVersion(3, 11, 0), SemverVersion(3, 5, 0))

    ML_CPU_DB_10_4 = DatabricksRuntimeSpecification("10.4.x-cpu-ml-scala2.12", SemverVersion(3, 8, 10), SemverVersion(3, 2, 1), ml=True)
    ML_CPU_DB_11_3 = DatabricksRuntimeSpecification("11.3.x-cpu-ml-scala2.12", SemverVersion(3, 9, 19), SemverVersion(3, 3, 0), ml=True)
    ML_CPU_DB_12_2 = DatabricksRuntimeSpecification("12.2.x-cpu-ml-scala2.12", SemverVersion(3, 9, 19), SemverVersion(3, 3, 2), ml=True)
    ML_CPU_DB_13_3 = DatabricksRuntimeSpecification("13.3.x-cpu-ml-scala2.12", SemverVersion(3, 10, 12), SemverVersion(3, 4, 1), ml=True)
    ML_CPU_DB_14_3 = DatabricksRuntimeSpecification("14.3.x-cpu-ml-scala2.12", SemverVersion(3, 10, 12), SemverVersion(3, 5, 0), ml=True)
    ML_CPU_DB_15_4 = DatabricksRuntimeSpecification("15.4.x-cpu-ml-scala2.12", SemverVersion(3, 11, 0), SemverVersion(3, 5, 0), ml=True)

    ML_GPU_DB_10_4 = DatabricksRuntimeSpecification(
        "10.4.x-gpu-ml-scala2.12", SemverVersion(3, 8, 10), SemverVersion(3, 2, 1), ml=True, gpu=True
    )
    ML_GPU_DB_11_3 = DatabricksRuntimeSpecification(
        "11.3.x-gpu-ml-scala2.12", SemverVersion(3, 9, 19), SemverVersion(3, 3, 0), ml=True, gpu=True
    )
    ML_GPU_DB_12_2 = DatabricksRuntimeSpecification(
        "12.2.x-gpu-ml-scala2.12", SemverVersion(3, 9, 19), SemverVersion(3, 3, 2), ml=True, gpu=True
    )
    ML_GPU_DB_13_3 = DatabricksRuntimeSpecification(
        "13.3.x-gpu-ml-scala2.12", SemverVersion(3, 10, 12), SemverVersion(3, 4, 1), ml=True, gpu=True
    )
    ML_GPU_DB_14_3 = DatabricksRuntimeSpecification(
        "14.3.x-gpu-ml-scala2.12", SemverVersion(3, 10, 12), SemverVersion(3, 5, 0), ml=True, gpu=True
    )
    ML_GPU_DB_15_4 = DatabricksRuntimeSpecification(
        "15.4.x-gpu-ml-scala2.12", SemverVersion(3, 11, 0), SemverVersion(3, 5, 0), ml=True, gpu=True
    )


def candidate_runtime_versions(spark_version: SemverVersion,
                               python_version: Optional[SemverVersion],
                               ml=False,
                               gpu=False) -> List[DatabricksRuntimeSpecification]:
    candidates = []
    for db_version in DatabricksRuntimeVersion:
        if db_version.value.ml == ml and db_version.value.gpu == gpu and db_version.value.spark_version == spark_version:
            if python_version is not None:
                if python_version.is_compatible_with(db_version.value.python_version):
                    candidates.append(db_version.value)
            else:
                candidates.append(db_version.value)

    return candidates
