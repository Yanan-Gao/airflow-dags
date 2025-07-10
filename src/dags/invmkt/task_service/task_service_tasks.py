from datetime import datetime, timedelta
# from airflow import DAG
from ttd.task_service.task_service_dag import TaskServiceDagFactory, TaskServiceDagRegistry
from ttd.task_service.k8s_pod_resources import TaskServicePodResources
from ttd.slack.slack_groups import INVENTORY_MARKETPLACE

registry = TaskServiceDagRegistry(globals())
registry.register_dag(
    TaskServiceDagFactory(
        task_name="CommitmentPrioritizationTask",
        scrum_team=INVENTORY_MARKETPLACE.team,
        task_config_name="CommitmentPrioritizationTaskConfig",
        start_date=datetime(2023, 7, 28),
        job_schedule_interval="0 0 * * *",
        resources=TaskServicePodResources.large(),
        retries=4,
        telnet_commands=[
            "try changeField CommitmentDetailsDataAccess.CommitmentDetailsProvisioningQueryTimeoutInSeconds 10800",
            "try changeField BidLineThrottlingLimits.DefaultMaxBidLinesPerCampaign 400",
            "try changeField BidLineThrottlingLimits.MaxBidLinesPerAdvertiser 70000",
            "changeField CommitmentCampaignAutoOptIn.EnableTestingModeSwitch.Enabled false",
            "try changeField CommitmentCampaignAutoOptInDataAccess.GetEligibleForAutoOptinQueryTimeoutInSeconds 10800",
            "try changeField CommitmentCampaignAutoOptInDataAccess.GetNoLongerEligibleForAutoOptinQueryTimeoutInSeconds 10800",
            "try changeField CommitmentPrioritizationTask.BatchQueryLimit 1000",
            "try changeField CampaignVcPromotionBatch.ApplyCampaignVCPromotionsTimeoutInSeconds 3600",
            "try changeField BidFactorPromotionBatch.ApplyBidFactorPromotionsTimeoutInSeconds 3600",
            "try changeField CampaignVcPromotionPrioritizationApplier.RemovePreviousCampaignVCPromotionsTimeoutInSeconds 3600",
            "try changeField BidFactorPromotionPrioritizationApplier.RemovePreviousBidFactorPromotionsTimeoutInSeconds 3600",
            "try changeField CampaignVcPromotionPrioritizationApplier.EmptyCommitmentCampaignOptimizationsQueueTimeoutInSeconds 3600",
            "try changeField BidFactorPromotionPrioritizationApplier.EmptyCommitmentBidFactorOptimizationsQueueTimeoutInSeconds 3600",
            "try changeField CommitmentPrioritizationTask.VerticaClusterOverride USEast01",
            "try changeField CommitmentDetailsDataAccess.IgnoreAfterDaysNoBids 2",
            "changeField HillClimberAlgorithm.UseSpendComputationQueryV2 true",
            "changefield ValueAlgoBidFactorAlgorithm.MapVCPromotionToBidFactor true",
            "changeField CommitmentPrioritizationTask.IsValueAlgoPrioritizationEnabled true",
            "changefield CommitmentPrioritizationTask.IsCrossPartnerPrioritizationEnabled true",
        ],
        task_execution_timeout=timedelta(hours=24),
        dag_tsg="https://atlassian.thetradedesk.com/confluence/display/EN/Commitment+Prioritization+-+Task+Service+Failures",
    )
)

registry.register_dag(
    TaskServiceDagFactory(
        task_name="PublisherMetadataImportTask",
        task_config_name="PublisherMetadataImportConfig",
        scrum_team=INVENTORY_MARKETPLACE.team,
        start_date=datetime.now() - timedelta(hours=3),
        job_schedule_interval=None,  # Means Manual
        resources=TaskServicePodResources.medium(),
        retries=0,
        configuration_overrides={"WriteMetricsToDatabase": "false"},
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="MktsVerticaConsistencyCheckTask",
        task_config_name="SellerDetailDailyCheckConfig",
        task_name_suffix="SellerDetailDailyCheck",
        scrum_team=INVENTORY_MARKETPLACE.team,
        start_date=datetime(2024, 10, 9),
        job_schedule_interval="0 10 * * *",
        resources=TaskServicePodResources.medium(),
        dag_tsg="https://atlassian.thetradedesk.com/confluence/display/EN/Consistency+Check+Inconsistencies",
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="MktsVerticaConsistencyCheckTask",
        task_name_suffix="CacV2ImpressionsCheck",
        task_config_name="CacV2ImpressionsCheckConfig",
        scrum_team=INVENTORY_MARKETPLACE.team,
        start_date=datetime(2023, 9, 5),
        job_schedule_interval="0 12 * * *",
        resources=TaskServicePodResources.medium(),
        dag_tsg="https://atlassian.thetradedesk.com/confluence/display/EN/Consistency+Check+Inconsistencies",
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="MktsVerticaConsistencyCheckTask",
        task_name_suffix="CacV2ClicksCheck",
        task_config_name="CacV2ClicksCheckConfig",
        scrum_team=INVENTORY_MARKETPLACE.team,
        start_date=datetime(2023, 9, 5),
        job_schedule_interval="0 12 * * *",
        resources=TaskServicePodResources.medium(),
        dag_tsg="https://atlassian.thetradedesk.com/confluence/display/EN/Consistency+Check+Inconsistencies",
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="MktsVerticaConsistencyCheckTask",
        task_config_name="PrivateContractDailyCheckConfig",
        task_name_suffix="PrivateContractDailyCheck",
        scrum_team=INVENTORY_MARKETPLACE.team,
        start_date=datetime(2023, 9, 28),
        job_schedule_interval="0 10 * * *",
        resources=TaskServicePodResources.medium(),
        dag_tsg="https://atlassian.thetradedesk.com/confluence/display/EN/Consistency+Check+Inconsistencies",
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="MktsVerticaConsistencyCheckTask",
        task_config_name="CommitmentHistoryCheckConfig",
        task_name_suffix="CommitmentHistoryCheck",
        scrum_team=INVENTORY_MARKETPLACE.team,
        start_date=datetime(2023, 9, 28),
        job_schedule_interval="0 10 * * *",
        resources=TaskServicePodResources.medium(),
        dag_tsg="https://atlassian.thetradedesk.com/confluence/display/EN/Consistency+Check+Inconsistencies",
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="RCDealTargetingTask",
        task_config_name="RCDealTargetingConfig",
        scrum_team=INVENTORY_MARKETPLACE.team,
        start_date=datetime(2024, 4, 1),
        job_schedule_interval="0 0 * * *",
        resources=TaskServicePodResources.medium(),
        retries=5,
        task_execution_timeout=timedelta(hours=24),
        configuration_overrides={"RCDealTargetingTask.AdGroupClassificationRatioForA3": "0.9"}
    )
)
registry.register_dag(
    TaskServiceDagFactory(
        task_name="AlwaysOnDealsTask",
        scrum_team=INVENTORY_MARKETPLACE.team,
        task_config_name="AlwaysOnDealsTaskConfig",
        start_date=datetime(2024, 11, 11),
        job_schedule_interval="*/15 * * * *",
        resources=TaskServicePodResources.large(),
        retries=4,
        task_execution_timeout=timedelta(hours=2)
    )
)
