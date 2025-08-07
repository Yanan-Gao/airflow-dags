from datetime import datetime, timedelta
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import CMO

registry = TaskServiceDagRegistry(globals())

registry.register_dag(
    TaskServiceDagFactory(
        task_name="CtvBrandImportTask",
        task_config_name="CtvBrandImportTaskConfig",
        scrum_team=CMO.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval="0 */1 * * *",
        resources=TaskServicePodResources.medium(),
    )
)

registry.register_dags([
    TaskServiceDagFactory(
        task_name="SambaTvAcrDataIngestionTask",
        task_config_name="SambaTvAcrDataIngestionTaskConfig",
        scrum_team=CMO.team,
        start_date=datetime.now() - timedelta(hours=7),
        job_schedule_interval="0 */6 * * *",  # every 6 hours
        resources=TaskServicePodResources.medium(),
        slack_tags=CMO.team.sub_team,
        configuration_overrides={"SambaTvAcrDataIngestion.Country": "AU"},
        telnet_commands=[
            "invokeMethod SambaTvAcrDataIngestionTask.CleanCustomFoldersToBeSynced",
            "changeField SambaTvAcrDataIngestionTask.EnableCustomFoldersToBeSynced true",
            "invokeMethod SambaTvAcrDataIngestionTask.AddCustomFoldersToBeSynced the-trade-desk/match-file-outputs/ sambatv-ttd/au-match-file-v2/",
            "invokeMethod SambaTvAcrDataIngestionTask.AddCustomFoldersToBeSynced the-trade-desk/report=TTDAUEXP134/ sambatv-ttd/au-feeds/"
        ],
        task_name_suffix="AU"
    ),
    TaskServiceDagFactory(
        task_name="SambaTvAcrDataIngestionTask",
        task_config_name="SambaTvAcrDataIngestionTaskConfig",
        scrum_team=CMO.team,
        start_date=datetime.now() - timedelta(hours=7),
        job_schedule_interval="0 */6 * * *",  # every 6 hours
        resources=TaskServicePodResources
        .custom(request_cpu="1", request_memory="2Gi", limit_memory="4Gi", limit_ephemeral_storage="4Gi", request_ephemeral_storage="1Gi"),
        slack_tags=CMO.team.sub_team,
        configuration_overrides={"SambaTvAcrDataIngestion.Country": "CA"},
        telnet_commands=[
            "invokeMethod SambaTvAcrDataIngestionTask.CleanCustomFoldersToBeSynced",
            "changeField SambaTvAcrDataIngestionTask.EnableCustomFoldersToBeSynced true",
            "invokeMethod SambaTvAcrDataIngestionTask.AddCustomFoldersToBeSynced the-trade-desk-ca/match-file-outputs/ sambatv-ttd/CA/ca-match-file/",
            "invokeMethod SambaTvAcrDataIngestionTask.AddCustomFoldersToBeSynced the-trade-desk-ca/report=TTDCAEXP1331/ sambatv-ttd/CA/ca-feeds/"
        ],
        task_name_suffix="CA",
        retry_delay=timedelta(hours=2)  # wait for 2 hours and then retry
    ),
])
registry.register_dag(
    TaskServiceDagFactory(
        task_name="OmnichannelRetargetingSettingTask",
        task_config_name="OmnichannelRetargetingSettingTaskConfig",
        task_name_suffix="SimpleRetargeting",
        scrum_team=CMO.team,
        start_date=datetime.now() - timedelta(days=1),
        job_schedule_interval="0 */6 * * *",  # every 6 hour
        resources=TaskServicePodResources.medium(),
        configuration_overrides={
            "OmnichannelRetargetingSettingTask.OmnichannelGroupIds":
            "159,230" + ",292,295,299,301,302,378" + ",409" + ",147" + ",423" + ",437" + ",466,467" + ",473" + ",501" + ",556" + ",611" +
            ",635" + ",509",
            "OmnichannelRetargetingSettingTask.RetargetingWindowInMinutes":
            "43200",
            "OmnichannelRetargetingSettingTask.BidAdjustment":
            "2",
            "OmnichannelRetargetingSettingTask.UserId":
            "vildup7",
        },
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="OmnichannelPathOptimizationSettingTask",
        task_config_name="OmnichannelPathOptimizationSettingTaskConfig",
        task_name_suffix="PathOptimization",
        scrum_team=CMO.team,
        start_date=datetime.now() - timedelta(days=1),
        job_schedule_interval="0 */1 * * *",  # every hour
        resources=TaskServicePodResources.medium(),
        configuration_overrides={
            "OmnichannelPathOptimizationSettingTask.UserId":
            "vildup7",
            "OmnichannelPathOptimizationSettingTask.CounterResetIntervalInMinutes":
            "43200",
            "OmnichannelPathOptimizationSettingTask.OmnichannelGroupIds":
            "237,291,294,297,298,300,304,395,464,465,469,472,474,485,522,523,601,609,610,611,612,624,641,656,708,710,711,733,745,761,778,788,789,798,800,801,806"
            +
            ",823,824,825,826,834,835,836,837,841,855,860,864,865,903,926,930,934,944,945,946,967,941,942,943,953,965,970,980,986,991,992,993,1023,1029,1031,1032,1039,1040,1057,1071,1076,1078"
            +
            ",1255,1256,1257,1258,1259,1260,1261,1262,1263,1264,1265,1266,1267,1268,1269,1270,1271,1272,1273,1274,1275,1276,1277,1278,1279,1280,1281,1282,1283,1284,1285,1286,1287,1288,1289,1290"
            +
            ",1291,1292,1293,1294,1295,1296,1297,1298,1299,1300,1301,1302,1303,1304,1305,1306,1307,1308,1309,1310,1311,1312,1313,1314,1315,1316,1317,1318,1319,1320,1321,1322,1323,1324,1330,1331"
            + ",1335,1340,1343,1344,1349,1354"
        },
    )
)
