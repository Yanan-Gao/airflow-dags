from ttd.datasets.date_external_dataset import DateExternalDataset


class DatPrdDatasources:
    owdi_demo_export_data: DateExternalDataset = DateExternalDataset(
        bucket="ttd-datprd-us-east-1",
        path_prefix="application/radar/data/export/general",
        data_name="Demographic",
        date_format="Date=%Y-%m-%d"
    )
