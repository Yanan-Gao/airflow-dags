from dags.dist.budget.alerting.budget_slack_alert_processor import EntityAlert

CPM_PROXIMITY_ALERTS = [
    EntityAlert(
        name="adgroup_cpm_too_high_above_target_cpm",
        sql="""
            select
                CampaignId,
                AdGroupId,
                CampaignFlightId,
                RealizedToTargetCpmProximity,
                RealizedCpmForTargetComparison,
                TargetCpmInAdvertiserCurrency
            from {{ad_group_cpm_proximity}}
            where RealizedToTargetCpmProximity > 1
            and Date = '{{run_time}}'
            and CpmTargetMode = 'PartnerCpm'
            order by RealizedToTargetCpmProximity desc;
                """,
        channel="#taskforce-budget-metrics-alarms",
        metric_name='RealizedToTargetCpmProximity',
        threshold='1',
        entity_name='adgroup'
    )
]
