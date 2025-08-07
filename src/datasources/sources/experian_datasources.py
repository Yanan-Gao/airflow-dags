from ttd.datasets.date_generated_dataset import DateGeneratedDataset


class ExperianDatasources:
    experian_bucket = "thetradedesk-useast-data-import"
    experian_path = "s3://thetradedesk-useast-data-import/experian"
    experian_processed_path = "s3://thetradedesk-useast-data-import/experian/processed"

    def crosswalk_output(self, provider: str):
        return DateGeneratedDataset(
            bucket=self.experian_bucket,
            path_prefix="experian/processed",
            data_name=f"cross-walk/prov={provider}",
            version=1,
        )
