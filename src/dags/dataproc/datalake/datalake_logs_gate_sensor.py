from time import time

from airflow.utils.context import Context

from dags.dataproc.datalake.datalake_parquet_utils import LogNames
from ttd.cloud_provider import CloudProvider
from ttd.metrics.metric_pusher import MetricPusher
from ttd.sensors.error_handling_sql_sensor import ErrorHandlingSqlSensor


class DatalakeLogsGateSensor(ErrorHandlingSqlSensor):

    def __init__(self, logname: str, cloud_provider: CloudProvider, *args, **kwargs):
        self._logname = logname
        self._cloud_provider = cloud_provider
        super().__init__(conn_id="lwdb", sql=self._get_query(logname), *args, **kwargs)

    def post_execute(self, context: Context, result=None):
        metric_pusher = MetricPusher()

        data_interval_start = context['data_interval_start']
        time_to_open = int(time() - context['data_interval_end'].int_timestamp)

        metric_pusher.push(
            name="datalake_lwf_gate_open",
            value=time_to_open,
            labels={
                'job': self._logname,
                'cloud_provider': str(self._cloud_provider)
            },
            timestamp=data_interval_start,
        )

    def _get_query(self, logname: str) -> str:
        return (
            "declare @HourlyTaskId int = " + self._get_enum_for_logname(logname) + ";" + """
            declare @temp as table (a datetime, b datetime2, IsCompleted bit);
            insert into @temp
               exec dbo.prc_GetHourlyTaskCompletedStatus
                   @HourlyTaskId=@HourlyTaskId,
                   @HourlyWindowStartTime='{{ execution_date.strftime(\"%Y-%m-%d %H:00\") }}';
            select IsCompleted from @temp;"""
        )

    def _get_enum_for_logname(self, logname: str) -> str:
        if logname == LogNames.bidfeedback:
            return "dbo.fn_Enum_Task_HourlyCleanseBidFeedbackCompleted()"
        if logname == LogNames.bidfeedbackverticaload:
            return "dbo.fn_Enum_Task_HourlyCleanseBidFeedbackCompleted()"
        if logname == LogNames.bidrequest:
            return "dbo.fn_enum_Task_HourlyCleanseBidRequestCompleted()"
        if logname == LogNames.clicktracker:
            return "dbo.fn_enum_Task_HourlyCleanseClickTrackerCompleted()"
        if logname == LogNames.clicktrackerverticaload:
            return "dbo.fn_enum_Task_HourlyCleanseClickTrackerCompleted()"
        if logname == LogNames.controlbidrequest:
            return "dbo.fn_enum_Task_HourlyCleanseBidRequestCompleted()"
        if logname == LogNames.conversiontracker:
            return "dbo.fn_enum_Task_HourlyCleanseConversionTrackerCompleted()"
        if logname == LogNames.conversiontrackerverticaload:
            return "dbo.fn_enum_Task_HourlyCleanseConversionTrackerCompleted()"
        if logname == LogNames.eventtrackerverticaload:
            return "dbo.fn_enum_Task_HourlyCleanseEventTrackerCompleted()"
        if logname == LogNames.videoevent:
            return "dbo.fn_enum_Task_HourlyCleanseVideoEventCompleted()"
        if (logname == LogNames.attributedeventverticaload or logname == LogNames.attributedeventresultverticaload
                or logname == LogNames.attributedeventdataelementverticaload):
            return "dbo.fn_enum_Task_HourlySingleLogAttributionCompleted()"
        raise NotImplementedError
