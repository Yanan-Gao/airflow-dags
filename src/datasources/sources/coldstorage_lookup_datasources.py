from ttd.datasets.hour_dataset import HourGeneratedDataset, HourExternalDataset


class ColdstorageLookupDataSources:
    avails_targetingdata: HourGeneratedDataset = HourGeneratedDataset(
        bucket="thetradedesk-useast-qubole",
        path_prefix="warehouse.external/thetradedesk.db/ttd",
        data_name="coldstoragetargetingdata/app=sampledavails",
        date_format="date=%Y%m%d",
    )

    avails_sampled: HourExternalDataset = HourExternalDataset(
        bucket="thetradedesk-useast-logs-2",
        path_prefix="avails",
        # It's weired. But avails doesn't follow current patter. To use this settings will work.
        data_name="cleansed",
        date_format="%Y/%m/%d",
        hour_format="{hour:0>2d}",
        success_file=None,
        version=None,
    )


# local adhoc test code
if __name__ == "__main__":
    # from airflow.hooks.S3_hook import S3Hook
    # hook = S3Hook(aws_conn_id='aws_default', verify=None)
    print(ColdstorageLookupDataSources.avails_targetingdata.get_root_path())
    print(ColdstorageLookupDataSources.avails_targetingdata.get_dataset_path())
