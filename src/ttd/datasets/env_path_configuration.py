from abc import ABC
from typing import Optional, Dict


class EnvPathConfiguration(ABC):
    """
    Base class to specify env path configuration in the Dataset class.
    Migrated datasets are created with env={test}|{prod} prepended to their path prefix.
    Path prefix is not modified for existing datasets (non migrated datasets).
    Migrating datasets have two path configurations -- an original and a new. The original path is built on the bucket and path_prefix.
    The new path configuration is built on the new_bucket and new_path_prefix provided through the MigratingDatasetPathConfiguration class.

    Migrating datasets are written to both the new and original paths when: the source dataset is of type Migrating and exists on both paths.
    For all other source dataset configurations, the destination Migrating dataset is only written to the path that the source dataset is on.
    So, if the source is Existing, the Migrating dataset will be written to its original path.
    If the source is Migrated, the Migrating dataset will be written to its new path
    If the source is Migrating and exists only on one of its paths (original or new), the Migrating will be written to that env path.
    """

    pass


class ExistingDatasetPathConfiguration(EnvPathConfiguration):
    """
    Env path configuration for existing datasets. Path prefix is not modified for existing datasets.
    """

    pass


class MigratedDatasetPathConfiguration(EnvPathConfiguration):
    """
    Env path configuration for migrated datasets. Path prefix is modified to prepend env={test}|{prod} at the front.
    """

    pass


class MigratingDatasetPathConfiguration(EnvPathConfiguration):
    """
    Env path configuration for Migrating datasets.
    Takes in a new bucket, new path prefix, new_buckets_for_other_regions, and smart_read bool, which is set to True by default.
    Migrating datasets will be written to new_bucket/env={env}/new_path_prefix,
    and in special cases also written to their original path (see explanation in EnvPathConfiguration class for special cases).
    smart_read is enabled by default, and should be disabled in the case that upon a write to a Dataset, existing data is modified, rather than overwritten.
    In such a case, smart_read will not return the correct data, as it will only be reading data from the new path
    -- it will assume that the data at the new path prefix is the complete set of data to fetch.
    """

    def __init__(
        self,
        new_bucket: str,
        new_path_prefix: str,
        smart_read: bool = True,
        new_buckets_for_other_regions: Optional[Dict[str, str]] = None,
    ):
        """
        Migrating Dataset configuration
        :param new_bucket: new bucket in which dataset is to exist
        :param new_path_prefix: new path prefix
        :param smart_read: if set to True, reads to the dataset will check both the new and old path, with precedence for the new path.
        :param new_buckets_for_other_regions: optionally, mapping of buckets for other regions.
        """
        self.new_bucket = new_bucket
        self.new_path_prefix = new_path_prefix
        self.new_buckets_for_other_regions = new_buckets_for_other_regions
        self.smart_read = smart_read
