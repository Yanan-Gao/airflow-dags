from dags.pdg.task_utils import choose
from ttd.datasets.static_dataset import StaticDataset
from ttd.datasets.time_dataset import TimeDataset

source_geo_store_diff_cache_data_dataset: TimeDataset = TimeDataset(
    bucket="ttd-geo",
    path_prefix="env=prod",  # test is not kept up to date so we should use prod data always
    env_aware=False,
    data_name="GeoStoreNg/GeoStoreGeneratedData",
    path_postfix="DiffCacheData",
    success_file=None,
    version=None
)

dest_geo_store_diff_cache_data_dataset: TimeDataset = TimeDataset(
    bucket="ttd-geo",
    azure_bucket="ttd-microtargeting-va9@ttdmicrotargeting",
    path_prefix=f"env={choose('prod', 'test')}",
    env_aware=False,
    data_name="GeoStoreGeneratedData",
    path_postfix="DiffCacheData",
    success_file=None,
    version=None
)

source_geo_store_s2_cell_to_full_and_partial_matches_dataset: TimeDataset = TimeDataset(
    bucket="ttd-geo",
    path_prefix="env=prod",  # test is not kept up to date so we should use prod data always
    env_aware=False,
    data_name="GeoStoreNg/GeoStoreGeneratedData",
    path_postfix="S2CellToFullAndPartialMatches",
    success_file=None,
    version=None
)

dest_geo_store_s2_cell_to_full_and_partial_matches_dataset: TimeDataset = TimeDataset(
    bucket="ttd-geo",
    azure_bucket="ttd-microtargeting-va9@ttdmicrotargeting",
    path_prefix=f"env={choose('prod', 'test')}",
    env_aware=False,
    data_name="GeoStoreGeneratedData",
    path_postfix="S2CellToFullAndPartialMatches",
    success_file=None,
    version=None
)

source_us_zip_adjacency_dataset: StaticDataset = StaticDataset(
    bucket="ttd-geo",
    path_prefix="env=prod",  # test is not kept up to date so we should use prod data always
    env_aware=False,
    data_name="USZipAdjacency",
    version=None
)

dest_us_zip_adjacency_dataset: StaticDataset = StaticDataset(
    bucket="ttd-geo",
    azure_bucket="ttd-microtargeting-va9@ttdmicrotargeting",
    path_prefix=f"env={choose('prod', 'test')}",
    env_aware=False,
    data_name="USZipAdjacency",
    version=None
)
