from dags.cmo.utils.pipeline_config import PipelineConfig
from ttd.aws.emr.aws_emr_versions import AwsEmrVersions
from ttd.ttdenv import TtdEnv, TtdEnvFactory


class LtvClassifierConfig(PipelineConfig):

    def __init__(
        self,
        provider: str,
        country: str,
        enrichment_date: str,
        run_date: str,
        window: int = 7,
        debug: str = "false",
        jar: str = None,
        env: TtdEnv = TtdEnvFactory.get_from_system()
    ):
        self.provider: str = provider
        self.enrichment_date = enrichment_date
        self.run_date = run_date
        self.country = country
        self.window = window
        self.model_path = f's3://ttd-ctv/light-tv/{country}/provider={provider}/fit/{env.dataset_write_env}'
        self.score_path = f's3://ttd-ctv/light-tv/{country}/provider={provider}/score/{env.dataset_write_env}'

        self.env = env
        self.debug = debug

        # Spark 3 configuration
        self.jar = jar or 's3://ttd-build-artefacts/tv-data/prod/latest/jars/ltv-data-jobs.jar'
        self.emr_release_label = AwsEmrVersions.AWS_EMR_SPARK_3_5

    def save_model_path(self, **context):
        return context['data_interval_end'].strftime("%Y%m%d")

    def get_saved_model_path(self) -> str:
        date = self.get_saved_model_date
        return f'{self.model_path}/{date}/'

    def get_saved_model_date(self) -> str:
        return '{{ task_instance.xcom_pull(task_ids="save_model_path", include_prior_dates=True) }}'

    def get_execution_model_path(self) -> str:
        date = '{{ data_interval_end.strftime("%Y%m%d") }}'
        return f'{self.model_path}/{date}/'

    def get_score_path(self) -> str:
        date = '{{ data_interval_end.strftime("%Y%m%d") }}'
        return f'{self.score_path}/{date}/'
