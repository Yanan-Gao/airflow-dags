from ttd.datasets.date_external_dataset import DateExternalDataset
from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.env_path_configuration import MigratedDatasetPathConfiguration


class ContentLibraryDatasources:
    gracenote_title_taxonomy_external_data: DateExternalDataset = DateExternalDataset(
        bucket="s3-data-import-bucket-s3bucket-zvhg29nip3jb",
        path_prefix="gracenote-import/raw",
        data_name="t=gracenote",
        date_format="v=%Y-%m-%d"
    )

    gracenote_title_taxonomy_data: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-content-library",
        path_prefix="taxonomy",
        data_name="GracenoteContent",
        version=1,
        date_format="date=%Y-%m-%d",
        env_path_configuration=MigratedDatasetPathConfiguration()
    )
