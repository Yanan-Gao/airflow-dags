from ttd.semver import SemverVersion
from enum import Enum


class PythonVersionSpecificationException(Exception):
    pass


class PythonVersions(Enum):
    PYTHON_3_7 = SemverVersion(3, 7, 16)
    PYTHON_3_8 = SemverVersion(3, 8, 10)
    PYTHON_3_9 = SemverVersion(3, 9, 19)
    PYTHON_3_10 = SemverVersion(3, 10, 12)
    PYTHON_3_11 = SemverVersion(3, 11, 0)
