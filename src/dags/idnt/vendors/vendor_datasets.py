from ttd.datasets.date_generated_dataset import DateGeneratedDataset
from ttd.datasets.hour_dataset import HourGeneratedDataset
from typing import List


class VendorDatasets:
    throtle: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-data-import",
        path_prefix="sxd-etl/deterministic",
        data_name="throtle",
        date_format="%Y-%m-%d",
        version=None,
        env_aware=False,
    )

    truedata: DateGeneratedDataset = DateGeneratedDataset(
        bucket="thetradedesk-useast-data-import",
        path_prefix="sxd-etl/deterministic",
        data_name="truedata",
        date_format="%Y-%m-%d",
        version=None,
        env_aware=False,
    )

    @staticmethod
    def get_uid_vendor_dataset(vendor_name: str) -> DateGeneratedDataset:
        return DateGeneratedDataset(
            bucket="thetradedesk-useast-data-import",
            path_prefix="sxd-etl/uid2",
            data_name=f"vendor={vendor_name}",
            date_format="date=%Y-%m-%d",
            version=None,
            env_aware=False,
        )

    @staticmethod
    def get_mobilewalla_raw_data(region: str) -> DateGeneratedDataset:
        return DateGeneratedDataset(
            bucket='thetradedesk-useast-data-import',
            path_prefix="mobilewalla/dids",
            data_name=region,
            version=None,
            date_format="%Y-%m-%d",
            env_aware=False,
            success_file="_SUCCESS"
        )

    @staticmethod
    def get_bidfeedback_eu_raw_data() -> List[HourGeneratedDataset]:
        all_bidfeedback_eu_raw_data = []
        datacenters = [
            "82",  # datacenter IE1
            "96"  # datacenter DE2
        ]

        for d in datacenters:
            all_bidfeedback_eu_raw_data.append(
                HourGeneratedDataset(
                    bucket='thetradedesk-useast-logs-2',
                    path_prefix="unmaskedandunhashedbidfeedback",
                    data_name="collected",
                    version=None,
                    date_format="%Y/%m/%d",
                    hour_format="{hour:0>2d}/" + d,
                    env_aware=False,
                    success_file=None
                )
            )

        return all_bidfeedback_eu_raw_data
