Pre = 'truncate table ttd_dpsr.metrics_PTGNamesStage{TableSuffix}'

Src = """
select
    ptg.PhysicalTableGroupId,
    ptg.PhysicalTableGroupName
from myreports.PhysicalTableGroup ptg
"""

Dst = """
insert into ttd_dpsr.metrics_PTGNamesStage{TableSuffix} (
    physical_table_group,
    group_name
)
values (%s,%s)
"""

Post = """
insert into ttd_dpsr.metrics_PTGNames{TableSuffix} (
    physical_table_group,
    group_name
)
select
    physical_table_group,
    group_name
from ttd_dpsr.metrics_PTGNamesStage{TableSuffix} ptgns
where not exists (
    select ptgn.physical_table_group
    from ttd_dpsr.metrics_PTGNames{TableSuffix} ptgn
    where ptgn.physical_table_group = ptgns.physical_table_group
)
"""
