from dags.dist.budget.alerting.budget_slack_alert_processor import GlobalAlert, EntityAlert

BUDGET_DELIVERY_ALERTS = [
    GlobalAlert(
        name="overall_overspend",
        sql="""
                select 100*sum(OverdeliveryInUSD)/sum(TotalAdvertiserCostFromPerformanceReportInUSD) as overdelivery_as_percent_of_total_cost
                from {{throttle_metric_campaign_daily}}
                where date = '{{run_time}}'
                having 100*sum(OverdeliveryInUSD)/sum(TotalAdvertiserCostFromPerformanceReportInUSD)  > 0.15;
                """,
        channel="#taskforce-budget-metrics-alarms",
        metric_name='overdelivery_as_percent_of_total_cost',
        threshold='0.15'
    ),
    EntityAlert(
        name="campaign_large_overspend",
        sql="""
                select CampaignId, CampaignFlightId, HasImpressionBudget, IsValuePacing, IsProgrammaticGuaranteed,
                HasPaceASAPAdGroups, HasAudienceImpressionBudget, IsUsingAllowanceSystem,
                sum(OverdeliveryInUSD) as total_overdelivery_usd,
                sum(TotalAdvertiserCostFromPerformanceReportInUSD) as total_advertiser_cost_usd,
                max(EstimatedBudgetInUSD) as daily_budget_usd
                from {{throttle_metric_campaign_daily}}
                where date = '{{run_time}}'
                group by all
                having total_overdelivery_usd > 1000
                order by total_overdelivery_usd desc;
                """,
        channel="#taskforce-budget-metrics-alarms",
        metric_name='total_overdelivery_usd',
        threshold='1000 $',
        entity_name='campaign'
    ),
    EntityAlert(
        name="adgroup_large_overspend",
        sql="""
                select CampaignId,AdGroupId, CampaignFlightId, HasImpressionBudget, IsValuePacing, IsProgrammaticGuaranteed,
                HasPaceASAPAdGroups, HasAudienceImpressionBudget, IsUsingAllowanceSystem,
                sum(AdGroupOverdeliveryInUSD) as total_overdelivery_usd,
                sum(TotalAdvertiserCostFromPerformanceReportInUSD) as total_advertiser_cost_usd,
                max(AdGroupEstimatedBudgetInUSD) as daily_budget_usd
                from {{throttle_metric_adgroup_daily}}
                where date = '{{run_time}}'
                group by all
                having total_overdelivery_usd > 1000
                order by total_overdelivery_usd desc;
               """,
        channel="#taskforce-budget-metrics-alarms",
        metric_name='total_overdelivery_usd',
        threshold='1000 $',
        entity_name='adgroup'
    )
]
