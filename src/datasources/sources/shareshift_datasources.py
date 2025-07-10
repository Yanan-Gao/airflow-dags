from ttd.datasets.date_generated_dataset import DateGeneratedDataset


class ShareshiftDataType:
    Impression = 'impressions'
    Network = 'networks'
    Demographic = 'demos'
    Weight = 'weights'
    Correction = 'corrections'


class ShareshiftDatasource:
    bucket = "thetradedesk-useast-data-import"
    path_prefix = "rShiny/shareshift"
    date_format = "monthdate=%Y%m"

    def __init__(self, country, provider):
        self.country = country
        self.provider = provider

    def get_shareshift_dataset(self, data_type, version=None):
        """
        Produce DateGeneratedDataset for each data type
        @param data_type: DataType
        @param version: version of the dataset. If not None then there will be a trailing `v={version}` between data_name and date partition str
        @return: DateGeneratedDataset
        """
        return DateGeneratedDataset(
            bucket=self.bucket,
            path_prefix=self.path_prefix,
            data_name=f"country={self.country}/provider={self.provider}/{data_type}",
            version=version,
            date_format=self.date_format
        )

    def get_full_s3_path(self, data_type, env_str, version=None):
        """
        Produce full s3 path, not including date_format
        @param data_type: DataType
        @param env_str: str, "prod", "test" etc (because these datasets are environment aware)
        @param version: version of the dataset
        @return: str, path key
        """
        version_str = "" if version is None else f"/v={version}"
        return f"s3://{self.bucket}/{self.path_prefix}/{env_str}/{self.get_shareshift_dataset(data_type, version).data_name}{version_str}"
