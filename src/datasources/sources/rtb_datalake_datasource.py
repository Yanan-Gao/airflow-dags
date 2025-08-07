from ttd.datasets.rtb_datalake_dataset import RtbDatalakeDataset


class RtbDatalakeDatasource:
    bucket = "ttd-datapipe-data"
    azure_bucket = "ttd-datapipe-data@eastusttdlogs"
    oss_bucket = "ttd-datapipe-data"

    rtb_bidfeedback_v5: RtbDatalakeDataset = RtbDatalakeDataset(
        bucket=bucket,
        azure_bucket=azure_bucket,
        path_prefix="parquet",
        data_name="rtb_bidfeedback_cleanfile",
        version=5,
        success_file="_SUCCESS-sx-%Y%m%d-%H",
        env_aware=True,
        eldorado_class="BidFeedbackDataSetV5",
        oss_bucket=oss_bucket,
    ).with_check_type(check_type="hour")

    rtb_bidfeedback_verticaload_v1: RtbDatalakeDataset = RtbDatalakeDataset(
        bucket=bucket,
        azure_bucket=azure_bucket,
        path_prefix="parquet",
        data_name="rtb_bidfeedback_verticaload",
        version=1,
        success_file="_SUCCESS-sx-%Y%m%d-%H",
        env_aware=True,
        eldorado_class="BidFeedbackVerticaLoadDataSet",
        oss_bucket=oss_bucket,
    ).with_check_type(check_type="hour")

    rtb_bidrequest_v5: RtbDatalakeDataset = RtbDatalakeDataset(
        bucket=bucket,
        azure_bucket=azure_bucket,
        path_prefix="parquet",
        data_name="rtb_bidrequest_cleanfile",
        version=5,
        success_file="_SUCCESS-sx-%Y%m%d-%H",
        env_aware=True,
        eldorado_class="BidRequestDataSetV5",
        oss_bucket=oss_bucket,
    ).with_check_type(check_type="hour")

    rtb_clicktracker_v5: RtbDatalakeDataset = RtbDatalakeDataset(
        bucket=bucket,
        azure_bucket=azure_bucket,
        path_prefix="parquet",
        data_name="rtb_clicktracker_cleanfile",
        version=5,
        success_file="_SUCCESS-sx-%Y%m%d-%H",
        env_aware=True,
        eldorado_class="ClickTrackerDataSetV5",
        oss_bucket=oss_bucket,
    ).with_check_type(check_type="hour")

    rtb_clicktracker_verticaload_v1: RtbDatalakeDataset = RtbDatalakeDataset(
        bucket=bucket,
        azure_bucket=azure_bucket,
        path_prefix="parquet",
        data_name="rtb_clicktracker_verticaload",
        version=1,
        success_file="_SUCCESS-sx-%Y%m%d-%H",
        env_aware=True,
        eldorado_class="ClickTrackerVerticaLoadDataSet",
        oss_bucket=oss_bucket,
    ).with_check_type(check_type="hour")

    rtb_conversiontracker_v5: RtbDatalakeDataset = RtbDatalakeDataset(
        bucket=bucket,
        path_prefix="parquet",
        data_name="rtb_conversiontracker_cleanfile",
        version=5,
        success_file="_SUCCESS-sx-%Y%m%d-%H",
        env_aware=True,
        eldorado_class="ConversionTrackerDataSetV5",
    ).with_check_type(check_type="hour")

    rtb_conversiontracker_verticaload_v4: RtbDatalakeDataset = RtbDatalakeDataset(
        bucket=bucket,
        path_prefix="parquet",
        data_name="rtb_conversiontracker_verticaload",
        version=4,
        success_file="_SUCCESS-sx-%Y%m%d-%H",
        env_aware=True,
        eldorado_class="ConversionTrackerVerticaLoadDataSetV4",
    ).with_check_type(check_type="hour")

    rtb_attributedevent_verticaload_v1: RtbDatalakeDataset = RtbDatalakeDataset(
        bucket=bucket,
        path_prefix="parquet",
        data_name="rtb_attributedevent_verticaload",
        version=1,
        success_file="_SUCCESS-sx-%Y%m%d-%H",
        env_aware=True,
        eldorado_class="AttributedEventVerticaLoadDataSet",
    ).with_check_type(check_type="hour")

    rtb_attributedeventresult_verticaload_v1: RtbDatalakeDataset = RtbDatalakeDataset(
        bucket=bucket,
        path_prefix="parquet",
        data_name="rtb_attributedeventresult_verticaload",
        version=1,
        success_file="_SUCCESS-sx-%Y%m%d-%H",
        env_aware=True,
        eldorado_class="AttributedEventResultVerticaLoadDataSet",
    ).with_check_type(check_type="hour")

    rtb_attributedeventdataelement_verticaload_v1: RtbDatalakeDataset = RtbDatalakeDataset(
        bucket=bucket,
        path_prefix="parquet",
        data_name="rtb_attributedeventdataelement_verticaload",
        version=1,
        success_file="_SUCCESS-sx-%Y%m%d-%H",
        env_aware=True,
        eldorado_class="AttributedEventDataElementVerticaLoadDataSet",
    ).with_check_type(check_type="hour")

    rtb_eventtracker_verticaload_v4: RtbDatalakeDataset = RtbDatalakeDataset(
        bucket=bucket,
        path_prefix="parquet",
        data_name="rtb_eventtracker_verticaload",
        version=4,
        success_file="_SUCCESS-sx-%Y%m%d-%H",
        env_aware=True,
        eldorado_class="EventTrackerVerticaLoadDataSetV4",
    ).with_check_type(check_type="hour")

    rtb_videoevent_v5: RtbDatalakeDataset = RtbDatalakeDataset(
        bucket=bucket,
        azure_bucket=azure_bucket,
        path_prefix="parquet",
        data_name="rtb_videoevent_cleanfile",
        version=5,
        success_file="_SUCCESS-sx-%Y%m%d-%H",
        env_aware=True,
        eldorado_class="VideoEventDataSetV5",
        oss_bucket=oss_bucket,
    ).with_check_type(check_type="hour")

    rtb_controlbidrequest_v1: RtbDatalakeDataset = RtbDatalakeDataset(
        bucket=bucket,
        path_prefix="parquet",
        data_name="rtb_controlbidrequest_cleanfile",
        version=1,
        success_file="_SUCCESS-sx-%Y%m%d-%H",
        env_aware=True,
        eldorado_class="ControlBidRequestDataSetV1",
    ).with_check_type(check_type="hour")

    @classmethod
    def get_by_logname(cls, logname: str) -> RtbDatalakeDataset:
        if logname == "BidFeedback":
            return cls.rtb_bidfeedback_v5
        if logname == "BidFeedbackVerticaLoad":
            return cls.rtb_bidfeedback_verticaload_v1
        if logname == "BidRequest":
            return cls.rtb_bidrequest_v5
        if logname == "ClickTracker":
            return cls.rtb_clicktracker_v5
        if logname == "ClickTrackerVerticaLoad":
            return cls.rtb_clicktracker_verticaload_v1
        if logname == "ControlBidRequest":
            return cls.rtb_controlbidrequest_v1
        if logname == "ConversionTracker":
            return cls.rtb_conversiontracker_v5
        if logname == "ConversionTrackerVerticaLoad":
            return cls.rtb_conversiontracker_verticaload_v4
        if logname == "EventTrackerVerticaLoad":
            return cls.rtb_eventtracker_verticaload_v4
        if logname == "VideoEvent":
            return cls.rtb_videoevent_v5
        if logname == "AttributedEventVerticaLoad":
            return cls.rtb_attributedevent_verticaload_v1
        if logname == "AttributedEventResultVerticaLoad":
            return cls.rtb_attributedeventresult_verticaload_v1
        if logname == "AttributedEventDataElementVerticaLoad":
            return cls.rtb_attributedeventdataelement_verticaload_v1
