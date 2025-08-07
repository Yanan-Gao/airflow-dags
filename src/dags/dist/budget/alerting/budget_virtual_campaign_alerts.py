from dags.dist.budget.alerting.budget_slack_alert_processor import EntityAlert, GlobalAlert

BUDGET_VIRTUAL_CAMPAIGN_ALERTS = [
    GlobalAlert(
        name="overall_virtual_underdelivery",
        sql="""
            SELECT Sum(UnderdeliveryInUSD) / sum(TotalAdvertiserCostFromPerformanceReportInUSD) * 100 as overall_virtual_underdelivery_as_percent_of_total_cost
            FROM {{throttle_metric_virtual_campaign_daily}}
            where ReportDate = '{{run_time}}' and CampaignThrottlemetric < 0.8
            having 100*sum(UnderdeliveryInUSD)/sum(TotalAdvertiserCostFromPerformanceReportInUSD)  > 4;
                """,
        channel="#taskforce-budget-split-alarms",
        metric_name='overall_virtual_underdelivery_as_percent_of_total_cost',
        threshold='4'
    ),
    GlobalAlert(
        name="overall_virtual_overdelivery",
        sql="""
            SELECT Sum(OverdeliveryInUSD) / sum(TotalAdvertiserCostFromPerformanceReportInUSD) * 100 as overall_virtual_overdelivery_as_percent_of_total_cost
            FROM {{throttle_metric_virtual_campaign_daily}}
            where ReportDate = '{{run_time}}'
            having 100*sum(OverdeliveryInUSD)/sum(TotalAdvertiserCostFromPerformanceReportInUSD)  > 0.15;
                """,
        channel="#taskforce-budget-split-alarms",
        metric_name='overall_virtual_overdelivery_as_percent_of_total_cost',
        threshold='0.15'
    ),
    GlobalAlert(
        name="overall_parent_overdelivery",
        sql="""
            select 100*sum(OverdeliveryInUSD)/sum(TotalAdvertiserCostFromPerformanceReportInUSD) as overall_parent_overdelivery_as_percent_of_total_cost
            from {{throttle_metric_campaign_daily}}
            where date = '{{run_time}}' and HaveActiveExperiment = true
            having 100*sum(OverdeliveryInUSD)/sum(TotalAdvertiserCostFromPerformanceReportInUSD)  > 0.15;
                """,
        channel="#taskforce-budget-metrics-alarms",
        metric_name='overall_parent_overdelivery_as_percent_of_total_cost',
        threshold='0.15'
    ),
    GlobalAlert(
        name="overall_parent_underdelivery",
        sql="""
            SELECT Sum(UnderdeliveryInUSD) / sum(TotalAdvertiserCostFromPerformanceReportInUSD) * 100 as overall_parent_underdelivery_as_percent_of_total_cost
            FROM {{throttle_metric_campaign_daily}}
            where date = '{{run_time}}' and CampaignThrottlemetric < 0.8 and HaveActiveExperiment = true
            having 100*sum(UnderdeliveryInUSD)/sum(TotalAdvertiserCostFromPerformanceReportInUSD)  > 4;
                """,
        channel="#taskforce-budget-metrics-alarms",
        metric_name='overall_parent_underdelivery_as_percent_of_total_cost',
        threshold='4'
    ),
    EntityAlert(
        name="virtual_campaign_calculated_cap_calculation_correctness",
        sql="""
            WITH ParentCampaign AS (
            SELECT
                CalculationTime,
                CampaignId,
                CampaignFlightId,
                HaveActiveExperiment,
                CalculatedDailyCapInAdvertiserCurrency,
                CalculatedDailyCapInImpressions,
                CalculatedDailyCapInAudienceImpressions
            FROM
                {{campaign_calculation_results}}
            WHERE
                IsLeader = true
                AND HaveActiveExperiment = true
                AND DATE(CalculationTime) = '{{run_time}}'
            ORDER BY
                CalculationTime DESC
            ),
            VirtualCampaign AS (
            SELECT
                CalculationTime,
                CampaignId,
                CampaignFlightId,
                calculatedDailyCapInAdvertiserCurrency,
                COALESCE(calculatedDailyCapInImpressions, 0) AS calculatedDailyCapInImpressions,
                COALESCE(calculatedDailyCapInAudienceImpressions, 0) AS calculatedDailyCapInAudienceImpressions,
                AudienceRatio,
                COALESCE(ParentSpendAtStartOfExperimentIfFirstDayInAdvertiserCurrency, 0) AS ParentSpendAtStartOfExperimentIfFirstDayInAdvertiserCurrency,
                COALESCE(ParentSpendAtStartOfExperimentIfFirstDayInImpressions, 0) AS ParentSpendAtStartOfExperimentIfFirstDayInImpressions,
                COALESCE(ParentSpendAtStartOfExperimentIfFistDayInAudienceImpresssions, 0) AS ParentSpendAtStartOfExperimentIfFirstDayInAudienceImpressions
            FROM
                {{virtual_campaign_calculation_results}}
            WHERE
                IsLeader = true
                AND DATE(CalculationTime) = '{{run_time}}'
            ORDER BY
                CalculationTime DESC
            ),
            AlignmentResult AS (
            SELECT
                pc.CalculationTime,
                pc.CampaignId,
                pc.CampaignFlightId,
                vc.AudienceRatio,
                pc.CalculatedDailyCapInAdvertiserCurrency,
                vc.ParentSpendAtStartOfExperimentIfFirstDayInAdvertiserCurrency,
                vc.ParentSpendAtStartOfExperimentIfFirstDayInImpressions,
                vc.ParentSpendAtStartOfExperimentIfFirstDayInAudienceImpressions,
                vc.calculatedDailyCapInAdvertiserCurrency,
                ABS(vc.AudienceRatio *
                    GREATEST(0, pc.CalculatedDailyCapInAdvertiserCurrency - vc.ParentSpendAtStartOfExperimentIfFirstDayInAdvertiserCurrency)
                    - vc.calculatedDailyCapInAdvertiserCurrency) AS VirtualCampaignDailyCapCalculationDiff,
                ABS(vc.AudienceRatio *
                    GREATEST(0, pc.calculatedDailyCapInImpressions - vc.ParentSpendAtStartOfExperimentIfFirstDayInImpressions)
                    - vc.calculatedDailyCapInImpressions) AS VirtualCampaignDailyCapInImpressionsCalculationDiff,
                ABS(vc.AudienceRatio *
                    GREATEST(0, pc.calculatedDailyCapInAudienceImpressions - vc.ParentSpendAtStartOfExperimentIfFirstDayInAudienceImpressions)
                    - vc.calculatedDailyCapInAudienceImpressions) AS VirtualCampaignDailyCapInAudienceImpressionsCalculationDiff
            FROM
                ParentCampaign pc
            INNER JOIN
                VirtualCampaign vc
                ON pc.CalculationTime = vc.CalculationTime
                AND pc.CampaignId = vc.CampaignId
                AND pc.CampaignFlightId = vc.CampaignFlightId
            )
            SELECT
                CampaignId,
                CampaignFlightId,
                VirtualCampaignDailyCapCalculationDiff,
                VirtualCampaignDailyCapInImpressionsCalculationDiff,
                VirtualCampaignDailyCapInAudienceImpressionsCalculationDiff
            FROM AlignmentResult
            GROUP BY ALL
            HAVING MAX(
                    GREATEST(
                        VirtualCampaignDailyCapCalculationDiff,
                        CASE
                            WHEN VirtualCampaignDailyCapInImpressionsCalculationDiff > 2 THEN VirtualCampaignDailyCapInImpressionsCalculationDiff
                            ELSE 0
                        END,
                       VirtualCampaignDailyCapInAudienceImpressionsCalculationDiff
                    )
                ) > 0.00001;
           """,
        channel="#taskforce-budget-split-alarms",
        metric_name='virtual_campaign_calculated_cap_calculation_correctness',
        threshold='0.00001',
        entity_name='campaign'
    ),
    EntityAlert(
        name="virtual_adgroup_calculated_cap_calculation_correctness",
        sql="""
            WITH ParentCampaign AS (
            SELECT
                CalculationTime,
                CampaignId,
                CampaignFlightId,
                adgroupId,
                HaveActiveExperiment,
                CalculatedDailyMinimumInAdvertiserCurrency,
                CalculatedDailyCapInAdvertiserCurrency,
                CalculatedDailyCapInImpressions,
                CalculatedDailyCapInAudienceImpressions
            FROM {{ad_group_calculation_results}}
            WHERE
                IsLeader = true
                AND HaveActiveExperiment = true
                AND DATE(CalculationTime) = '{{run_time}}'
            ORDER BY
                CalculationTime DESC
            ),
            VirtualCampaign AS (
            SELECT
                CalculationTime,
                CampaignId,
                CampaignFlightId,
                adgroupId,
                CalculatedDailyMinimumInAdvertiserCurrency,
                CalculatedDailyCapInAdvertiserCurrency,
                CalculatedDailyCapInImpressions,
                CalculatedDailyCapInAudienceImpressions,
                AudienceRatio,
                COALESCE(ParentSpendAtStartOfExperimentIfFirstDayInAdvertiserCurrency, 0) AS ParentSpendAtStartOfExperimentIfFirstDayInAdvertiserCurrency,
                COALESCE(ParentSpendAtStartOfExperimentIfFirstDayInImpressions, 0) AS ParentSpendAtStartOfExperimentIfFirstDayInImpressions,
                COALESCE(ParentSpendAtStartOfExperimentIfFirstDayInAudienceImpressions, 0) AS ParentSpendAtStartOfExperimentIfFirstDayInAudienceImpressions
            FROM {{virtual_ad_group_calculation_results}}
            WHERE
                IsLeader = true
                AND DATE(CalculationTime) = '{{run_time}}'
            ORDER BY
                CalculationTime DESC
            ),
            AlignmentResult AS (
            SELECT
                pc.CalculationTime,
                pc.CampaignId,
                pc.CampaignFlightId,
                pc.AdGroupId,
                vc.AudienceRatio,
                pc.CalculatedDailyMinimumInAdvertiserCurrency AS ParentDailyMinimumInAdvertiserCurrency,
                vc.CalculatedDailyMinimumInAdvertiserCurrency AS VirtualDailyMinimumInAdvertiserCurrency,
                pc.CalculatedDailyCapInAdvertiserCurrency AS ParentDailyCapInAdvertiserCurrency,
                vc.ParentSpendAtStartOfExperimentIfFirstDayInAdvertiserCurrency,
                vc.ParentSpendAtStartOfExperimentIfFirstDayInImpressions,
                vc.ParentSpendAtStartOfExperimentIfFirstDayInAudienceImpressions,
                vc.CalculatedDailyCapInAdvertiserCurrency AS VirtualDailyCapInAdvertiserCurrency,
                pc.CalculatedDailyCapInImpressions AS ParentDailyCapInImpressions,
                vc.CalculatedDailyCapInImpressions AS VirtualDailyCapInImpressions,
                pc.CalculatedDailyCapInAudienceImpressions AS ParentDailyCapInAudienceImpressions,
                vc.CalculatedDailyCapInAudienceImpressions AS VirtualDailyCapInAudienceImpressions,
                ABS(vc.AudienceRatio * GREATEST(0, pc.CalculatedDailyCapInAdvertiserCurrency - vc.ParentSpendAtStartOfExperimentIfFirstDayInAdvertiserCurrency) - vc.CalculatedDailyCapInAdvertiserCurrency) AS VirtualAdGroupDailyCapCalculationDiff,
                ABS(vc.AudienceRatio * GREATEST(0, pc.CalculatedDailyCapInImpressions - vc.ParentSpendAtStartOfExperimentIfFirstDayInImpressions) - vc.CalculatedDailyCapInImpressions) AS VirtualAdGroupDailyCapInImpressionsCalculationDiff,
                ABS(vc.AudienceRatio * GREATEST(0, pc.CalculatedDailyCapInAudienceImpressions - vc.ParentSpendAtStartOfExperimentIfFirstDayInAudienceImpressions) - vc.CalculatedDailyCapInAudienceImpressions) AS VirtualAdGroupDailyCapInAudienceImpressionsCalculationDiff,
                ABS(vc.AudienceRatio * GREATEST(0, pc.CalculatedDailyMinimumInAdvertiserCurrency - vc.ParentSpendAtStartOfExperimentIfFirstDayInAdvertiserCurrency) - vc.CalculatedDailyMinimumInAdvertiserCurrency) AS VirtualAdGroupDailyMinimumCalculationDiff
            FROM
                ParentCampaign pc
            INNER JOIN
                VirtualCampaign vc
            ON
                pc.CalculationTime = vc.CalculationTime
                AND pc.CampaignId = vc.CampaignId
                AND pc.CampaignFlightId = vc.CampaignFlightId
                AND pc.adgroupId = vc.adgroupId
            )
            SELECT
                CampaignId,
                CampaignFlightId,
                AdGroupId,
                VirtualAdGroupDailyCapCalculationDiff,
                VirtualAdGroupDailyCapInImpressionsCalculationDiff,
                VirtualAdGroupDailyCapInAudienceImpressionsCalculationDiff,
                VirtualAdGroupDailyMinimumCalculationDiff
            FROM AlignmentResult
            GROUP BY ALL
            HAVING MAX(
                    GREATEST(
                        VirtualAdGroupDailyCapCalculationDiff,
                        CASE
                            WHEN VirtualAdGroupDailyCapInImpressionsCalculationDiff > 1 THEN VirtualAdGroupDailyCapInImpressionsCalculationDiff
                            ELSE 0
                        END,
                        CASE
                            WHEN VirtualAdGroupDailyCapInAudienceImpressionsCalculationDiff > 1 THEN VirtualAdGroupDailyCapInAudienceImpressionsCalculationDiff
                            ELSE 0
                        END
                    )
                ) > 0.00001;
           """,
        channel="#taskforce-budget-split-alarms",
        metric_name='virtual_adgroup_calculated_cap_calculation_correctness',
        threshold='0.00001',
        entity_name='adgroup'
    ),
    EntityAlert(
        name="virtual_campaign_spend_alignment_check",
        sql="""
            WITH ParentCampaign AS (
            SELECT
                CalculationTime,
                CampaignId,
                CampaignFlightId,
                HaveActiveExperiment,
                CalculatedDailyCapInAdvertiserCurrency,
                CalculatedDailyCapInImpressions,
                CalculatedDailyCapInAudienceImpressions,
                DailyAdvertiserSpendInAdvertiserCurrency,
                DailySpendInAudienceImpressions,
                DailySpendInImpressions,
                VarianceAdjustedDailySpendInAdvertiserCurrency AS DailyAdvertiserSpendInAdvertiserCurrencyAdjusted,
                VarianceAdjustedDailySpendInImpressions AS DailySpendInImpressionsAdjusted,
                VarianceAdjustedDailySpendInAudienceImpressions AS DailySpendInAudienceImpressionsAdjusted
            FROM
                {{campaign_calculation_results}}
            WHERE
                IsLeader = true
                AND HaveActiveExperiment = true
                AND DATE(CalculationTime) = '{{run_time}}'
            ORDER BY
                CalculationTime DESC
        ),
        VirtualCampaign AS (
            SELECT
                CalculationTime,
                CampaignId,
                CampaignFlightId,
                SUM(CalculatedDailyCapInAdvertiserCurrency) AS aggregatedCalculatedDailyCapInAdvertiserCurrency,
                SUM(CalculatedDailyCapInImpressions) AS aggregatedCalculatedDailyCapInImpressions,
                SUM(CalculatedDailyCapInAudienceImpressions) AS aggregatedCalculatedDailyCapInAudienceImpressions,
                SUM(RebalancedDailySpendInAdvertiserCurrency) AS aggregatedRebalancedDailyAdvertiserSpendInAdvertiserCurrency,
                SUM(RebalancedDailySpendInImpressions) AS aggregatedRebalancedDailyAdvertiserSpendInImpressions,
                SUM(RebalancedDailySpendInAudienceImpressions) AS aggregatedRebalancedDailyAdvertiserSpendInAudienceImpressions,
                MAX(COALESCE(ParentSpendAtStartOfExperimentIfFirstDayInAdvertiserCurrency, 0)) AS TotalParentSpendIfFirstDay,
                MAX(COALESCE(ParentSpendAtStartOfExperimentIfFirstDayInImpressions, 0)) AS TotalParentSpendIfFirstDayInImpressions,
                MAX(COALESCE(ParentSpendAtStartOfExperimentIfFistDayInAudienceImpresssions, 0)) AS TotalParentSpendIfFirstDayInAudienceImpressions,
                MAX(AudienceRatio) AS AudienceRatio
            FROM
                {{virtual_campaign_calculation_results}}
            WHERE
                IsLeader = true
                AND DATE(CalculationTime) = '{{run_time}}'
            GROUP BY
                CalculationTime, CampaignId, CampaignFlightId
            ORDER BY
                CalculationTime DESC
        ),
        AlignmentResult AS (
            SELECT
                pc.CalculationTime,
                pc.CampaignId,
                pc.CampaignFlightId,
                pc.DailyAdvertiserSpendInAdvertiserCurrency,
                ABS(GREATEST(0, DailySpendInAudienceImpressionsAdjusted - TotalParentSpendIfFirstDayInAudienceImpressions) - aggregatedRebalancedDailyAdvertiserSpendInAudienceImpressions) AS RebalancedDailySpendInAudienceImpressionsDiff,
                ABS(GREATEST(0, DailySpendInImpressionsAdjusted - TotalParentSpendIfFirstDayInImpressions) - aggregatedRebalancedDailyAdvertiserSpendInImpressions) AS RebalancedDailySpendInImpressionsDiff,
                ABS(GREATEST(0, DailyAdvertiserSpendInAdvertiserCurrencyAdjusted - TotalParentSpendIfFirstDay) - aggregatedRebalancedDailyAdvertiserSpendInAdvertiserCurrency) AS RebalancedDailySpendDiff,
                ABS(GREATEST(0, CalculatedDailyCapInAdvertiserCurrency - TotalParentSpendIfFirstDay) - aggregatedCalculatedDailyCapInAdvertiserCurrency) AS CalculatedDailyCapDiff,
                ABS(GREATEST(0, CalculatedDailyCapInImpressions - TotalParentSpendIfFirstDayInImpressions) - aggregatedCalculatedDailyCapInImpressions) AS CalculatedDailyCapInImpressionDiff,
                ABS(GREATEST(0, CalculatedDailyCapInAudienceImpressions - TotalParentSpendIfFirstDayInAudienceImpressions) - aggregatedCalculatedDailyCapInAudienceImpressions) AS CalculatedDailyCapInAudienceImpressionDiff
            FROM
                ParentCampaign pc
            INNER JOIN
                VirtualCampaign vc
            ON
                pc.CalculationTime = vc.CalculationTime
                AND pc.CampaignId = vc.CampaignId
                AND pc.CampaignFlightId = vc.CampaignFlightId
        )
        SELECT
            CampaignId,
            CampaignFlightId,
            SUM(RebalancedDailySpendDiff),
            SUM(DailyAdvertiserSpendInAdvertiserCurrency),
            SUM(CalculatedDailyCapDiff),
            (SUM(RebalancedDailySpendDiff) / SUM(DailyAdvertiserSpendInAdvertiserCurrency) * 100) AS SpendDiffPercentage
        FROM
            AlignmentResult
        WHERE
            DailyAdvertiserSpendInAdvertiserCurrency > 1
            AND (RebalancedDailySpendDiff > 0.0001
            OR CalculatedDailyCapDiff > 0.00001)
        GROUP BY ALL
        HAVING
            SpendDiffPercentage > 0.1
        ORDER BY
            SpendDiffPercentage DESC;
           """,
        channel="#taskforce-budget-split-alarms",
        metric_name='virtual_campaign_spend_alignment_check',
        threshold='0.1',
        entity_name='campaign'
    ),
    EntityAlert(
        name="virtual_adgroup_spend_alignment_check",
        sql="""
            WITH ParentAdGroup AS (
            SELECT
                CalculationTime,
                CampaignId,
                AdGroupId,
                CampaignFlightId,
                PacingAllowedCount,
                FoundCount,
                WarmupFoundCount,
                BidCount,
                CalculatedDailyCapInAdvertiserCurrency,
                CalculatedDailyCapInImpressions,
                CalculatedDailyCapInAudienceImpressions,
                CalculatedDailyMinimumInAdvertiserCurrency,
                DailyAdvertiserSpendInAdvertiserCurrency,
                VarianceAdjustedDailySpendInAdvertiserCurrency AS DailyAdvertiserSpendInAdvertiserCurrencyAdjusted,
                VarianceAdjustedDailySpendInImpressions AS DailySpendInImpressionsAdjusted,
                VarianceAdjustedDailySpendInAudienceImpressions AS DailySpendInAudienceImpressionsAdjusted,
                DailySpendInAudienceImpressions,
                DailySpendInImpressions
            FROM
                {{ad_group_calculation_results}}
            WHERE
                IsLeader = true
                AND HaveActiveExperiment = true
                AND DATE(CalculationTime) = '{{run_time}}'
            ORDER BY
                CalculationTime DESC
        ),
        VirtualAdGroup AS (
            SELECT
                CalculationTime,
                CampaignId,
                AdGroupId,
                CampaignFlightId,
                SUM(CalculatedDailyMinimumInAdvertiserCurrency) AS aggregatedCalculatedDailyMinimumInAdvertiserCurrency,
                SUM(PacingAllowedCount) AS aggregatedPacingAllowedCount,
                SUM(FoundCount) AS aggregatedFoundCount,
                SUM(WarmupFoundCount) AS aggregatedWarmup,
                SUM(BidCount) AS aggregatedBidCount,
                SUM(CalculatedDailyCapInAdvertiserCurrency) AS aggregatedCalculatedDailyCapInAdvertiserCurrency,
                SUM(CalculatedDailyCapInImpressions) AS aggregatedCalculatedDailyCapInImpressions,
                SUM(CalculatedDailyCapInAudienceImpressions) AS aggregatedCalculatedDailyCapInAudienceImpressions,
                SUM(RebalancedDailySpendInAdvertiserCurrency) AS aggregatedRebalancedDailyAdvertiserSpendInAdvertiserCurrency,
                SUM(RebalancedDailySpendInImpressions) AS aggregatedRebalancedDailyAdvertiserSpendInImpressions,
                SUM(RebalancedDailySpendInAudienceImpressions) AS aggregatedRebalancedDailyAdvertiserSpendInAudienceImpressions,
                MAX(COALESCE(ParentSpendAtStartOfExperimentIfFirstDayInAdvertiserCurrency, 0)) AS TotalParentSpendIfFirstDay,
                MAX(COALESCE(ParentSpendAtStartOfExperimentIfFirstDayInImpressions, 0)) AS TotalParentSpendIfFirstDayInImpressions,
                MAX(COALESCE(ParentSpendAtStartOfExperimentIfFirstDayInAudienceImpressions, 0)) AS TotalParentSpendIfFirstDayInAudienceImpressions,
                MAX(AudienceRatio) AS AudienceRatio
            FROM
                {{virtual_ad_group_calculation_results}}
            WHERE
                IsLeader = true
                AND DATE(CalculationTime) = '{{run_time}}'
            GROUP BY
                CalculationTime, CampaignId, AdGroupId, CampaignFlightId
            ORDER BY
                CalculationTime DESC
        ),
        AlignmentResult AS (
            SELECT
                pc.CalculationTime,
                pc.CampaignId,
                pc.AdGroupId,
                pc.CampaignFlightId,
                pc.DailyAdvertiserSpendInAdvertiserCurrency,
                pc.DailyAdvertiserSpendInAdvertiserCurrencyAdjusted AS DailyAdvertiserSpendInAdvertiserCurrencyAdjusted,
                ABS(PacingAllowedCount - vc.aggregatedPacingAllowedCount) AS PacingAllowedCountDiff,
                ABS(FoundCount - vc.aggregatedFoundCount) AS FoundCountDiff,
                ABS(WarmupFoundCount - vc.aggregatedWarmup) AS WarmupFoundCountDiff,
                ABS(BidCount - vc.aggregatedBidCount) AS BidCountDiff,
                ABS(GREATEST(0, CalculatedDailyCapInAdvertiserCurrency - TotalParentSpendIfFirstDay) - aggregatedCalculatedDailyCapInAdvertiserCurrency) AS CalculatedDailyCapDiff,
                ABS(GREATEST(0, CalculatedDailyCapInImpressions - TotalParentSpendIfFirstDayInImpressions) - aggregatedCalculatedDailyCapInImpressions) AS CalculatedDailyCapInImpressionsDiff,
                ABS(GREATEST(0, CalculatedDailyCapInAudienceImpressions - TotalParentSpendIfFirstDayInAudienceImpressions) - aggregatedCalculatedDailyCapInAudienceImpressions) AS CalculatedDailyCapInAudienceImpressionDiff,
                ABS(GREATEST(0, CalculatedDailyMinimumInAdvertiserCurrency - TotalParentSpendIfFirstDay) - aggregatedCalculatedDailyMinimumInAdvertiserCurrency) AS CalculatedDailyMinimumDiff,
                ABS(GREATEST(0, DailySpendInAudienceImpressionsAdjusted - TotalParentSpendIfFirstDayInAudienceImpressions) - aggregatedRebalancedDailyAdvertiserSpendInAudienceImpressions) AS RebalancedDailySpendInAudienceImpressionsDiff,
                ABS(GREATEST(0, DailySpendInImpressionsAdjusted - TotalParentSpendIfFirstDayInImpressions) - aggregatedRebalancedDailyAdvertiserSpendInImpressions) AS RebalancedDailySpendInImpressionsDiff,
                ABS(GREATEST(0, DailyAdvertiserSpendInAdvertiserCurrencyAdjusted - TotalParentSpendIfFirstDay) - aggregatedRebalancedDailyAdvertiserSpendInAdvertiserCurrency) AS RebalancedDailySpendDiff
            FROM
                ParentAdGroup pc
            INNER JOIN
                VirtualAdGroup vc
            ON
                pc.CalculationTime = vc.CalculationTime
                AND pc.CampaignId = vc.CampaignId
                AND pc.AdGroupId = vc.AdGroupId
                AND pc.CampaignFlightId = vc.CampaignFlightId
        )
        SELECT
            CampaignId,
            AdGroupId,
            CampaignFlightId,
            SUM(DailyAdvertiserSpendInAdvertiserCurrency),
            SUM(PacingAllowedCountDiff),
            SUM(FoundCountDiff),
            SUM(WarmupFoundCountDiff),
            SUM(CalculatedDailyCapDiff),
            SUM(CalculatedDailyMinimumDiff),
            SUM(RebalancedDailySpendDiff),
            (SUM(RebalancedDailySpendDiff) / SUM(DailyAdvertiserSpendInAdvertiserCurrency) * 100) AS SpendDiffPercentage
        FROM
            AlignmentResult
        WHERE
            DailyAdvertiserSpendInAdvertiserCurrency > 1
            AND (PacingAllowedCountDiff > 0
            OR FoundCountDiff > 0
            OR WarmupFoundCountDiff > 0
            OR BidCountDiff > 0
            OR CalculatedDailyCapDiff > 0.00001
            OR CalculatedDailyMinimumDiff > 0.00001
            OR RebalancedDailySpendDiff > 0.001)
        GROUP BY ALL
        HAVING
            SpendDiffPercentage > 0.1
        ORDER BY
            SpendDiffPercentage DESC;
           """,
        channel="#taskforce-budget-split-alarms",
        metric_name='virtual_adgroup_spend_alignment_check',
        threshold='0.1',
        entity_name='adgroup'
    ),
    EntityAlert(
        name="virtual_campaign_overdelivery",
        sql="""
            select CampaignId, CampaignFlightId, VirtualCampaignId, HasImpressionBudget, IsValuePacing, IsProgrammaticGuaranteed,
                HasPaceASAPAdGroups, HasAudienceImpressionBudget, HasHardMinimum,
                sum(OverdeliveryInUSD) as total_overdelivery_usd,
                sum(TotalAdvertiserCostFromPerformanceReportInUSD) as total_advertiser_cost_usd,
                max(EstimatedBudgetInUSD) as daily_budget_usd
            FROM {{throttle_metric_virtual_campaign_daily}}
            where ReportDate = '{{run_time}}'
            group by all
            having total_overdelivery_usd  > 100;
           """,
        channel="#taskforce-budget-split-alarms",
        metric_name='virtual_campaign_overdelivery',
        threshold='100 $',
        entity_name='campaign'
    ),
    EntityAlert(
        name="virtual_campaign_underdelivery",
        sql="""
            select CampaignId, CampaignFlightId, VirtualCampaignId, HasImpressionBudget, IsValuePacing, IsProgrammaticGuaranteed,
                HasPaceASAPAdGroups, HasAudienceImpressionBudget, HasHardMinimum,
                sum(UnderdeliveryInUSD) as total_underdelivery_usd,
                sum(TotalAdvertiserCostFromPerformanceReportInUSD) as total_advertiser_cost_usd,
                max(EstimatedBudgetInUSD) as daily_budget_usd
            FROM {{throttle_metric_virtual_campaign_daily}}
            where ReportDate = '{{run_time}}' and CampaignThrottlemetric < 0.8
            group by all
            having total_underdelivery_usd  > 100;
           """,
        channel="#taskforce-budget-split-alarms",
        metric_name='virtual_campaign_underdelivery',
        threshold='100 $',
        entity_name='campaign'
    ),
    EntityAlert(
        name="virtual_adgroup_hard_min_underdelivery",
        sql="""
            select CampaignId, CampaignFlightId, VirtualCampaignId, IsValuePacing,
                sum(AdGroupHardMinimumUnderdeliveryInUSD) as total_hard_min_underdelivery_usd,
                max(AdGroupEstimatedBudgetInUSD) as daily_budget_usd
            FROM {{throttle_metric_virtual_adroup_hardmin_daily}}
            where ReportDate = '{{run_time}}' and AdGroupThrottleMetric < 0.8
            group by all
            having total_hard_min_underdelivery_usd  > 10;
           """,
        channel="#taskforce-budget-split-alarms",
        metric_name='virtual_adgroup_hard_min_underdelivery',
        threshold='10 $',
        entity_name='adgroup'
    ),
    EntityAlert(
        name="parent_campaign_large_overspend",
        sql="""
                select CampaignId, CampaignFlightId, HasImpressionBudget, IsValuePacing, IsProgrammaticGuaranteed,
                HasPaceASAPAdGroups, HasAudienceImpressionBudget, IsUsingAllowanceSystem,
                sum(OverdeliveryInUSD) as total_overdelivery_usd,
                sum(TotalAdvertiserCostFromPerformanceReportInUSD) as total_advertiser_cost_usd,
                max(EstimatedBudgetInUSD) as daily_budget_usd
                from {{throttle_metric_campaign_daily}}
                where date = '{{run_time}}' and HaveActiveExperiment = true
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
        name="non_virtual_spend_on_campaign_with_active_experiment",
        sql="""
with campaign_experiments as (
    select
        ReportHourUTC,
        CampaignId,
        CampaignFlightId,
        AdGroupId,
        sum(m.PartnerCostCPMInUSDSum/1000) as total_virtual_partner_cost_usd
    from
        {{cpm_metrics}} m
    join
        {{budgetsplitexperimentinfo}} e using(ExperimentId)
where
    date_trunc('DAY', ReportHourUTC) = '{{run_time}}'
  and VirtualCampaignId is not null
  and (ExperimentEndDate is null or ReportHourUTC < ActualExperimentEndDate - INTERVAL 4 HOURS)
  and (ReportHourUTC > ExperimentStartDate + INTERVAL 4 HOURS)
group by all
    )
select
    m.AdGroupId,
    m.CampaignId,
    100 * (SUM(m.PartnerCostCPMInUSDSum/1000) / a.total_virtual_partner_cost_usd) AS non_virtual_percent_spend_for_virtual_campaigns,
    sum(m.PartnerCostCPMInUSDSum/1000) as total_non_virtual_partner_cost_usd,
    a.total_virtual_partner_cost_usd,
    sum(ImpressionCount) as total_non_virtual_impressions
from
    campaign_experiments a
join
   {{cpm_metrics}} m using (CampaignId, AdGroupId, ReportHourUTC, CampaignFlightId)
where
    m.VirtualCampaignId is null and date_trunc('DAY', m.ReportHourUTC) = '{{run_time}}'
GROUP BY
    ALL
having
    100 * (SUM(m.PartnerCostCPMInUSDSum/1000) / a.total_virtual_partner_cost_usd) > 5;
            """,
        channel="#taskforce-budget-split-alarms",
        metric_name='non_virtual_percent_spend_for_virtual_campaigns',
        threshold='5 %',
        entity_name='adgroup'
    )
]
