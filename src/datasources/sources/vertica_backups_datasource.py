from ttd.datasets.hour_dataset import HourGeneratedDataset


class VerticaBackupsDatasource:
    bucket = "ttd-vertica-backups"

    rtb_platformreport_vertica_backups_export: HourGeneratedDataset = HourGeneratedDataset(
        bucket=bucket,
        path_prefix="ExportPlatformReport",
        data_name="VerticaAws",
        date_format="date=%Y%m%d",
        hour_format="hour={hour:0>2d}",
        version=None,
        success_file="_SUCCESS",
        env_aware=False
    )
