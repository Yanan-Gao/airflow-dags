from __future__ import annotations

from typing import List, Optional, TYPE_CHECKING

from ttd.semver import SemverVersion

if TYPE_CHECKING:
    from ttd.ec2.emr_instance_type import EmrInstanceType


class EmrVersion(SemverVersion):

    def __init__(self, emr_version: str):
        version_parts = emr_version.removeprefix("emr-").split(".")
        if len(version_parts) != 3:
            raise ValueError("Invalid EmrVersion version string")
        super().__init__(int(version_parts[0]), int(version_parts[1]), int(version_parts[2]))

    def __str__(self):
        return f"emr-{self.major}.{self.minor}.{self.patch}"

    def __repr__(self):
        return f"EmrVersion(major={self.major}, minor={self.minor}, patch={self.patch})"

    def filter_by_minimum_emr_versions(self, instance_types: List[EmrInstanceType]) -> List[EmrInstanceType]:
        return [
            instance_type for instance_type in instance_types if self.check_for_minimum_emr_versions(instance_type.minimum_emr_versions)
        ]

    def check_for_minimum_emr_versions(self, min_versions: Optional[List[EmrVersion]]) -> bool:
        """
        Compares if this version is higher than any of the provided versions that has the same major version as this one.
        Args:
            min_versions:
        """

        if min_versions is None:
            return True

        versions = [v for v in min_versions if v.major == self.major]
        if len(versions) != 1:
            raise ValueError("More then one EMR Version instances matched in the list")
        return versions[0] <= self
