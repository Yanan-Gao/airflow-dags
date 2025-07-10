from dags.dist.budget.alerting.budget_slack_alert_processor import EntityAlert

BUDGET_R_VALUE_CONTROL_ALERTS = [
    EntityAlert(
        name="r_value_computation_not_performed",
        sql="""
                with pacing_calculations_in_day as (
                    select
                        CampaignId,
                        CampaignFlightId,
                        coalesce(VirtualCampaignId, -1) as VirtualCampaignId,
                        AdGroupId,
                        count(distinct CalculationTime) as CalculationCount
                    from {{volume_control_calculation_results}}
                    where true
                        and IsValuePacing
                        and date(CalculationTime) = '{{run_time}}'
                        and KeepRate > 0
                    group by CampaignId, CampaignFlightId, VirtualCampaignId, AdGroupId
                ), actively_pacing_ad_groups as (
                    select
                        CampaignId,
                        CampaignFlightId,
                        VirtualCampaignId,
                        AdGroupId
                    from pacing_calculations_in_day
                    where CalculationCount >= 480
                ), r_value_calculations_in_day as (
                    select
                        CampaignId,
                        CampaignFlightId,
                        coalesce(VirtualCampaignId, -1) as VirtualCampaignId,
                        AdGroupId,
                        count(distinct CalculationTime) as CalculationCount
                    from {{r_value_calculation_results}}
                    where true
                        and date(CalculationTime) = '{{run_time}}'
                        and (RValueCalculationStatus > 0 or CalculationStatus != '')
                    group by CampaignId, CampaignFlightId, VirtualCampaignId, AdGroupId
                ), pacing_ad_groups_r_value_calculation_count as (
                    select
                        p.CampaignId,
                        p.CampaignFlightId,
                        p.VirtualCampaignId,
                        p.AdGroupId,
                        coalesce(CalculationCount, 0) as JustifiedRValueCalculationCount
                    from actively_pacing_ad_groups p
                        left join r_value_calculations_in_day r on p.CampaignId = r.CampaignId and p.CampaignFlightId = r.CampaignFlightId and p.VirtualCampaignId = r.VirtualCampaignId and p.AdGroupId = r.AdGroupId
                    )
                select
                    CampaignId,
                    CampaignFlightId,
                    case when VirtualCampaignId = -1 then null else VirtualCampaignId end as VirtualCampaignId,
                    AdGroupId,
                    1 as unjustified_lack_of_r_value_computations
                from pacing_ad_groups_r_value_calculation_count
                where JustifiedRValueCalculationCount <= 0
                order by CampaignId, CampaignFlightId, VirtualCampaignId, AdGroupId;
                """,
        channel="#dev-valuepacing-alarms",
        metric_name='unjustified_lack_of_r_value_computations',
        threshold='0',
        entity_name='adgroup'
    ),
]
