Src = """
select
    cast(Date as date) as Day,
    PartnerId,
    sum(TTDCostInUSD) as TTDUSD,
    sum(PartnerCostInUSD) as PartnerUSD
from Reporting.Reports.vw_RtiAdvertiserRollupDaily
where Date >= '{StartDateUtcInclusive}'
and Date < '{EndDateUtcExclusive}'
group by cast(Date as date), PartnerId
"""

SrcDB = 'ttdglobal_int_metric'

Dst = """
insert into ttd_dpsr.metrics_SpendStats{TableSuffix} (
    day,
    partner_id,
    TTDCostInUSD,
    PartnerCostinUSD
)
values (%s,%s,%s,%s)
"""
