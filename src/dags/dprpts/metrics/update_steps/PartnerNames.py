Pre = 'truncate table ttd_dpsr.metrics_ConsumerNamesStage{TableSuffix}'

Src = 'select PartnerId, PartnerName from Provisioning.dbo.Partner'

Dst = """
insert into ttd_dpsr.metrics_ConsumerNamesStage{TableSuffix} (
    CustomerId,
    CustomerKindId,
    CustomerName
)
values (%s,3,%s)
"""

Post = """
insert into ttd_dpsr.metrics_ConsumerNames{TableSuffix} (
    CustomerId,
    CustomerKindId,
    CustomerName
)
select
    mcns.CustomerId,
    mcns.CustomerKindId,
    mcns.CustomerName
from ttd_dpsr.metrics_ConsumerNamesStage{TableSuffix} mcns
where not exists (
        select mcn.CustomerName from ttd_dpsr.metrics_ConsumerNames{TableSuffix} mcn
            where mcn.CustomerId = mcns.CustomerId and mcn.CustomerKindId = mcns.CustomerKindId
    )
"""
