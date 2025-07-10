from datasources.datasource import Datasource
from ttd.datasets.date_generated_dataset import DateGeneratedDataset


class FrequencyMapDataSources(Datasource):
    targetingDataEmbeddingKLLSketch: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-qubole",
        path_prefix="frequency-maps",
        data_name=
        "vectorLevelUserEmbeddingLookBackAggregate/availStream=deviceSampled/idType=tdid/dailyFreqCap=48/mapType=targetingData/sketchType=KLL/lookBackDays=7",
        version=None,
        success_file="_userEmbeddingVersion",
    )
