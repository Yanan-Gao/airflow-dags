from ttd.datasets.date_generated_dataset import DateGeneratedDataset


def get_currency_exchange_rate() -> DateGeneratedDataset:
    return DateGeneratedDataset(
        bucket="thetradedesk-useast-qubole",
        path_prefix="warehouse.external",
        data_name="thetradedesk.db/provisioning/currencyexchangerate",
        version=1,
        success_file=None,
        env_aware=False,
    )


def get_deal_metadata_floor_price() -> DateGeneratedDataset:
    return DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline",
        data_name="markets/availsmetadata",
        version=None,
        success_file=None,
        env_aware=True,
        date_format="%Y-%m-%d",
    )


def get_private_contract() -> DateGeneratedDataset:
    return DateGeneratedDataset(
        bucket="thetradedesk-useast-qubole",
        path_prefix="warehouse.external",
        data_name="thetradedesk.db/provisioning/privatecontract",
        version=1,
        success_file=None,
        env_aware=False,
    )


def get_supply_vendor_bidding() -> DateGeneratedDataset:
    return DateGeneratedDataset(
        bucket="thetradedesk-useast-qubole",
        path_prefix="warehouse.external",
        data_name="thetradedesk.db/provisioning/supplyvendorbidding",
        version=1,
        success_file=None,
        env_aware=False,
    )


def get_supply_vendor_deal() -> DateGeneratedDataset:
    return DateGeneratedDataset(
        bucket="thetradedesk-useast-qubole",
        path_prefix="warehouse.external",
        data_name="thetradedesk.db/provisioning/supplyvendordeal",
        version=1,
        success_file=None,
        env_aware=False,
    )
