from dags.cmo.utils.pipeline_config import PipelineConfig
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.ttdenv import TtdEnv, TtdEnvFactory


class AcrPipelineConfig(PipelineConfig):

    def __init__(
        self,
        provider: str,
        country: str,
        run_date: str,
        data_delay: int,
        enriched_dataset: DateGeneratedDataset,
        jar: str = None,
        env: TtdEnv = TtdEnvFactory.get_from_system(),
        use_crosswalk: bool = False,
        frequency_aggregation_version: int = 5,  # frequency aggregation dataset version
        segment_enriched_version: int = 3,  # segment enriched dataset version
        gen_date: str = None,
        provider_graph_aggregate_version: int = 1,  # provider graph aggregate dataset version
        tdid_mapper_ds_version: int = 3,

        # Audience specific settings
        raw_data_path: str = None,
    ):
        self.provider = provider
        self.country = country
        self.run_date = run_date
        self.data_delay = data_delay
        self.enriched_dataset = enriched_dataset
        self.enriched_path_root = enriched_dataset.get_root_path()
        self.env = env
        self.use_crosswalk = "true" if use_crosswalk else "false"
        self.frequency_aggregation_version = frequency_aggregation_version
        self.segment_enriched_version = segment_enriched_version
        self.gen_date = gen_date
        self.raw_data_path = raw_data_path
        self.provider_graph_aggregate_version = provider_graph_aggregate_version
        self.tdid_mapper_ds_version = tdid_mapper_ds_version

        # Spark 3 configuration
        self.jar = jar or 's3://ttd-build-artefacts/tv-data/prod/latest/jars/ltv-data-jobs.jar'
        self.emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_5
