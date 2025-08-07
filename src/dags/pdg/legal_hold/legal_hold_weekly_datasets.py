import calendar

from ttd.cloud_provider import CloudProviders, CloudProvider
from ttd.datasets.dataset import Dataset
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.identity_graphs.identity_graphs import IdentityGraphs


class LegalHoldSourceDestinationDatasetWeekly:

    def __init__(
        self,
        dataset_name: str,
        source_dataset: Dataset,
        dest_dataset: Dataset,
        cloud_provider: CloudProvider,
        weekday: int,
        num_partitions: int = 1,
        max_threads: int = 20
    ):
        self.dataset_name = dataset_name
        self.source_dataset = source_dataset
        self.dest_dataset = dest_dataset
        self.cloud_provider = cloud_provider
        self.weekday = weekday
        self.num_partitions = num_partitions  # for large datasets to use multiple transfer tasks
        self.max_threads = max_threads


source_graph_bucket = "thetradedesk-useast-data-import"
dest_bucket = "ttd-datapipe-file-hold"

source_tapad_apac = DateGeneratedDataset(
    bucket=source_graph_bucket,
    path_prefix="sxd-etl/universal",
    data_name="tapad/apac",
    date_format="%Y-%m-%d/success",
    version=None,
    env_aware=False
)

source_tapad_eur = DateGeneratedDataset(
    bucket=source_graph_bucket,
    path_prefix="sxd-etl/universal",
    data_name="tapad/eur",
    date_format="%Y-%m-%d/success",
    version=None,
    env_aware=False
)

source_tapad_na = DateGeneratedDataset(
    bucket=source_graph_bucket,
    path_prefix="sxd-etl/universal",
    data_name="tapad/na",
    date_format="%Y-%m-%d/success",
    version=None,
    env_aware=False
)

source_tapad_lookuptable = DateGeneratedDataset(
    bucket=source_graph_bucket,
    path_prefix="sxd-etl/universal",
    data_name="tapad/lookupTable",
    date_format="%Y-%m-%d/lookupTable",
    version=None,
    env_aware=False
)

source_id5_iav2 = DateGeneratedDataset(
    bucket=source_graph_bucket,
    path_prefix="sxd-etl/universal",
    data_name="id5-iav2",
    date_format="%Y-%m-%d/success",
    version=None,
    env_aware=False
)

source_emetriq_iav2 = DateGeneratedDataset(
    bucket=source_graph_bucket,
    path_prefix="sxd-etl/universal",
    data_name="emetriq-iav2",
    date_format="%Y-%m-%d/success",
    version=None,
    env_aware=False
)

identity_graphs = IdentityGraphs()

# Dataset with weekday of generation
weekly_datasets = [
    (identity_graphs.ttd_graph.v2.standard_input.persons_capped_for_hot_cache.dataset, calendar.SATURDAY),
    (identity_graphs.ttd_graph.v2.standard_input.households_capped_for_hot_cache.dataset, calendar.SATURDAY),
    (identity_graphs.ttd_graph.v2.standard_input.singleton_persons.dataset, calendar.SATURDAY),
    (identity_graphs.ttd_graph.v1.standard_input.persons_capped_for_hot_cache.dataset, calendar.SATURDAY),
    (identity_graphs.ttd_graph.v1.standard_input.households_capped_for_hot_cache.dataset, calendar.SATURDAY),
    (identity_graphs.identity_alliance.v2.default_input.persons_capped_for_hot_cache_and_with_dats.dataset, calendar.SATURDAY),
    (identity_graphs.identity_alliance.v2.default_input.households_capped_for_hot_cache_and_with_dats.dataset, calendar.SATURDAY),
    (identity_graphs.identity_alliance.v2.based_on_ttd_graph_v1.persons_capped_for_hot_cache_and_with_dats.dataset, calendar.SATURDAY),
    (identity_graphs.identity_alliance.v2.based_on_ttd_graph_v1.households_capped_for_hot_cache_and_with_dats.dataset, calendar.SATURDAY),
    (identity_graphs.live_ramp_graph.v1.merged.persons_capped_for_hot_cache.dataset,
     calendar.MONDAY), (source_tapad_apac, calendar.TUESDAY), (source_tapad_eur, calendar.TUESDAY), (source_tapad_na, calendar.TUESDAY),
    (source_tapad_lookuptable, calendar.TUESDAY), (source_id5_iav2, calendar.WEDNESDAY), (source_emetriq_iav2, calendar.WEDNESDAY)
]

dest_weekly_datasets = [
    DateGeneratedDataset(
        bucket=dest_bucket,
        path_prefix=dataset.path_prefix,
        data_name=dataset.data_name,
        date_format=dataset.date_format,
        version=None,
        env_aware=False,
    ) for (dataset, weekday) in weekly_datasets
]

legal_hold_weekly_datasets = []

for (dataset, weekday), dest_dataset in zip(weekly_datasets, dest_weekly_datasets):
    legal_hold_weekly_datasets.append(
        LegalHoldSourceDestinationDatasetWeekly(
            dataset_name=f"{dataset.data_name}_aws".replace("/", "_"),
            source_dataset=dataset,
            dest_dataset=dest_dataset,
            cloud_provider=CloudProviders.aws,
            weekday=weekday
        )
    )
