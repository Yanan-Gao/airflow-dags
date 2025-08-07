from datetime import datetime, timedelta
import random
from ttd.el_dorado.v2.base import TtdDag
from ttd.operators.final_dag_status_check_operator import FinalDagStatusCheckOperator
from ttd.tasks.op import OpTask
from airflow.operators.python import PythonOperator
from dags.datasrvc.consistency_checker.vertica_cluster import VERTICA_CLUSTERS_TO_CHECK, US_AWS_VERTICA_CLUSTERS
from airflow.hooks.base_hook import BaseHook

import logging
import vertica_python

logger = logging.getLogger(__name__)

dag_name = "budget_variance_compliance_dag"

run_date = "{{ data_interval_start }}"


def run_vertica_query(**context):
    aws_cluster = US_AWS_VERTICA_CLUSTERS[0]
    cluster_info = VERTICA_CLUSTERS_TO_CHECK[aws_cluster]
    conn = BaseHook.get_connection(cluster_info.conn_id)
    conn_info = {'host': conn.host, 'port': conn.port, 'user': conn.login, 'password': conn.password, 'database': conn.schema}

    # Randomly pick a date in the last 8 days
    days_ago = random.randint(2, 8)
    run_date = context["data_interval_start"]
    random_date = run_date - timedelta(days=days_ago)
    maxdate_str = random_date.strftime('%Y-%m-%d')
    print(f"Max date: {maxdate_str}")
    # Substitute into query
    query = f"""
        -- Get all Partners that spent that day
        select PartnerId
        from ttdprotected.BidFeedback
        where date(LogEntryTime) = date('{maxdate_str}')
        group by 1
        order by 1;
    """

    with vertica_python.connect(**conn_info) as connection:
        print(f'Find Partner Query:{query}')
        cursor = connection.cursor()
        cursor.execute(query)
        partner_rows = cursor.fetchall()
        print(f"Results for {run_date}:", partner_rows)

    if not partner_rows:
        raise ValueError(f"No partners found for date {maxdate_str}")

    # Step 3: Pick a random PartnerId
    partner_id = random.choice(partner_rows)[0]

    # Step 4: Inject both into final query
    final_query = f"""
        -- Using maxdate: '{maxdate_str}', partnerId: '{partner_id}'
        -- Budget Server has a true up system called the BudgetVarianceTask.
        -- This works by looking at the difference between vertica spend tables (reports.CumulativePerformanceReport, reports.vw_PerformanceReport and ttdprotected.BidFeedback),
        -- and the budget spend data in Vertica (budget.PartnerBudgetSnapshot). The difference between these values is then sent across to budget server,
        -- and applied so budgets data is inline with the vertica spend data.
        -- This variance adjustment is then written back to the budget tables in vertica (budget.PartnerBudgetSnapshot) during subsequent backups.
        -- The below query looks at the the difference between the vertica spend tables, and the vertica budget tables + the variance adjustment. Which should end up being 0.
        -- This uses the two production budget server machines (use-bud001-budgetserver, use-bud002-budgetserver)
        with CurrentBudgetCheckpoint as (
        -- Find the budget checkpoint with the latest date earlier than out max date. This is the subsequent backup for the variance adjustments, and we will backtrack from here to find the date of the live spend.
        select timestampadd('day', -3, CheckpointEnd)::date as CumulativeDate, -- The date to use for the CumulativePerformanceReport table. Use a date from 3 days earlier, to assure it will be up to date.
                LogFileId
            from budget.vw_CompleteBudgetCheckpoints
            where CheckpointEnd < '{maxdate_str}'
            order by checkpointend desc
            limit 8 ),
        CurrentPartnerSnapshot as (
            select PartnerId,
                    CheckpointEnd,
                    CurrentLifeTimePartnerAdjustmentInUSD,
                    CheckpointEnd as VarianceDate, -- This is the date in which the CurrentLifeTimePartnerAdjustmentInUSD was uploaded back to the budget vertica logs
                    VarianceBackupTime --This is the time of the original comparison. Ie The time when the vertica spend tables and the budget tables were compared, to generate this adjustment.
            from budget.PartnerBudgetSnapshot p
            inner join CurrentBudgetCheckpoint cbc
                on p.LogFileId = cbc.LogFileId
            where PartnerId = '{partner_id}' or '{partner_id}' is null
        ),
        ComparisonBudgetCheckpoint as (
            -- Find the budget log files associated with the original comparison.
            select bc.LogFileId,
                    bc.CheckpointEnd as ComparisonDate
            from budget.vw_CompleteBudgetCheckpoints bc
            inner join CurrentPartnerSnapshot cps
                on bc.CheckpointEnd = cps.VarianceBackupTime
            order by bc.CheckpointEnd desc
            limit 8
        ),
        ComparisonPartnerSnapshot as (
            select PartnerId,
                CheckpointEnd,
                LifeTimePartnerSpendInUSD --This is the original spend according to budget server, used in the
            from budget.PartnerBudgetSnapshot p
            inner join ComparisonBudgetCheckpoint cbc
                on p.LogFileId = cbc.LogFileId
        ),
        BudgetAdjustedSpend as (
            -- Get the combined values as per budget server.
            select cb.PartnerId,
                    cb.CheckpointEnd,
                    LifeTimePartnerSpendInUSD as LifeTimePartnerSpendInUSDAsPerBudget,
                    CurrentLifeTimePartnerAdjustmentInUSD as CurrentLifeTimePartnerAdjustmentInUSDAsPerBudgyet,
                    LifeTimePartnerSpendInUSD + CurrentLifeTimePartnerAdjustmentInUSD as CombinedPartnerCostAsPerBudget,
                    VarianceBackupTime,
                    VarianceDate
            from CurrentPartnerSnapshot cb
            inner join ComparisonPartnerSnapshot ab
                on cb.PartnerId = ab.PartnerId),
        Advertisers as
        (
            -- As advertisers can changes Partners over their lifetime. The PartnerId for a particular advertiser may be incorrect in reports.CumulativePerformanceReport.
            -- As such, we grab the latest Partner according to the provisioning2 tables (which comes from Provisioning.)
            select AdvertiserId, PartnerId from provisioning2.Advertiser
            where PartnerId = '{partner_id}' or '{partner_id}' is null
        ),
        CurrentBudgetCheckpointCumulativeDate as
        (
            select max(CumulativeDate) as CumulativeDate
            from CurrentBudgetCheckpoint
        ),
        ComparisonBudgetCheckpointDate as
        (
            select max(ComparisonDate) as ComparisonDate
            from ComparisonBudgetCheckpoint
        ),
        Cumulative as (
            -- The CumulativePerformanceReport contains lifetime spend up to a certain date. For the Advertisers associated with our particular Partner, lets grab the lifetime spend
            -- up to the CumulativeDate calculated earlier.
            select adv.PartnerId,
                    sum(CumulativePartnerCostInUsd) as CumulativePartnerCostInUsd
            from reports.CumulativePerformanceReport
            inner join Advertisers adv
                on reports.CumulativePerformanceReport.AdvertiserId = adv.AdvertiserId
            inner join CurrentBudgetCheckpointCumulativeDate cbc
                on ReportDateUtc = CumulativeDate
            group by 1 ),
        RecentFeedback as (
            -- Now lets grab any recent feedback that's came in since the CumulativeDate, up to the ComparisonDate (The date we calculated the earlier adjustments).
            select adv.PartnerId,
                sum(PartnerCostInUsd) as PartnerCostInUsd
            from ttdprotected.BidFeedback
            inner join Advertisers adv
                on ttdprotected.BidFeedback.AdvertiserId = adv.AdvertiserId
            inner join CurrentBudgetCheckpointCumulativeDate cbc
                on LogEntryTime >= timestampadd('day', 1, CumulativeDate)
            inner join ComparisonBudgetCheckpointDate cobc
                on LogEntryTime <= ComparisonDate
            where LogEntryTime >= timestampadd('day', -10, '{maxdate_str}')::date
            group by 1
        ), ReportSpend as (
            -- Now lets get the total spend according to the vertica spend tables.
            select c.PartnerId,
                CumulativePartnerCostInUsd + PartnerCostInUsd as PartnerCostInUsd
            from cumulative c
            left join recentfeedback rf -- We need to left join because if partner has no spend recently, the recentfeedback will be empty
                on c.PartnerId = rf.PartnerId )
        -- Lastly, let's get the final values in a single query, and look at the difference between them.
        select rs.PartnerId,
                VarianceBackupTime, -- The time this comparison was down.
                PartnerCostInUsd as PartnerCostInUsdAsPerVertica,  -- The spend according to the Vertica spend tables.
                LifeTimePartnerSpendInUSDAsPerBudget, -- The lifetime spend according to the budget tables (not taking in any adjustments).
                CurrentLifeTimePartnerAdjustmentInUSDAsPerBudgyet, -- The adjustment we got from a subsequent backup.
                CombinedPartnerCostAsPerBudget, -- The spend + adjustments from budget server.
                VarianceDate, -- The time that CurrentLifeTimePartnerAdjustmentInUSDAsPerBudgyet was pulled from
                round(CombinedPartnerCostAsPerBudget - PartnerCostInUsd, 2) as Difference -- The difference between the Vertica spend tables, and the spend + adjustments according to budget server.
        from ReportSpend rs
        inner join BudgetAdjustedSpend bas
            on rs.PartnerId = bas.PartnerId
        where rs.PartnerId = '{partner_id}' or '{partner_id}' is null
    """

    with vertica_python.connect(**conn_info) as connection:
        print(f'Final Query:{final_query}')
        cursor = connection.cursor()
        cursor.execute(final_query)

        # Get column names from cursor.description
        column_names = [desc[0] for desc in cursor.description]
        result = cursor.fetchall()
        print(f"Results for {run_date}:")

        for row_idx, row in enumerate(result):
            row_dict = dict(zip(column_names, row))
            for col_name, value in row_dict.items():
                print(f"  {col_name}: {value}")

        # Check if "Difference" column exists and is greater than 1
        if "Difference" in row_dict and row_dict["Difference"] > 1:
            raise Exception(f"Row {row_idx}: Difference too large! Value = {row_dict['Difference']}")


###############################################################################
# DAG Definition
###############################################################################
# The top-level dag
# Runs once a day
budget_variance_compliance_dag = TtdDag(
    dag_id=dag_name,
    start_date=datetime(2025, 4, 15),
    schedule_interval='0 10 * * *',
    dag_tsg='https://thetradedesk.atlassian.net/wiki/x/FoALC',
    retries=3,
    max_active_runs=1,
    retry_delay=timedelta(minutes=5),
    slack_channel="#taskforce-budget-metrics-alarms",
    slack_alert_only_for_prod=True,
    tags=["DIST"],
)

dag = budget_variance_compliance_dag.airflow_dag

###############################################################################
# Tasks
###############################################################################

reboot_task = OpTask(
    op=PythonOperator(
        task_id='execute_variance_compliance_query',
        python_callable=run_vertica_query,
        provide_context=True,
        dag=dag,
    )
)

###############################################################################
# Final DAG Status Check
###############################################################################

final_dag_status_step = OpTask(op=FinalDagStatusCheckOperator(dag=dag))

###############################################################################
# DAG Dependencies
###############################################################################
budget_variance_compliance_dag >> final_dag_status_step
