Post = [
    """
with violations as (select analyze_constraints('ttd_dpsr.metrics_ConsumerNames{TableSuffix}'))
select distinct ('Constraint violation in ' || "Table Name" || '.' || "Column Names")::int from violations
""", """
with violations as (select analyze_constraints('ttd_dpsr.metrics_ExecutionTraits{TableSuffix}'))
select distinct ('Constraint violation in ' || "Table Name" || '.' || "Column Names")::int from violations
""", """
with violations as (select analyze_constraints('ttd_dpsr.metrics_ExecutionStats{TableSuffix}'))
select distinct ('Constraint violation in ' || "Table Name" || '.' || "Column Names")::int from violations
""", """
with violations as (select analyze_constraints('ttd_dpsr.metrics_SLADelayClasses{TableSuffix}'))
select distinct ('Constraint violation in ' || "Table Name" || '.' || "Column Names")::int from violations
""", """
with violations as (select analyze_constraints('ttd_dpsr.metrics_SLAViolationStatsClasses{TableSuffix}'))
select distinct ('Constraint violation in ' || "Table Name" || '.' || "Column Names")::int from violations
""", """
with violations as (select analyze_constraints('ttd_dpsr.metrics_ExecutionResolution{TableSuffix}'))
select distinct ('Constraint violation in ' || "Table Name" || '.' || "Column Names")::int from violations
""", """
with violations as (select analyze_constraints('ttd_dpsr.metrics_ExecutionStats{TableSuffix}'))
select distinct ('Constraint violation in ' || "Table Name" || '.' || "Column Names")::int from violations
""", """
with violations as (select analyze_constraints('ttd_dpsr.metrics_ExecutionRSPTG{TableSuffix}'))
select distinct ('Constraint violation in ' || "Table Name" || '.' || "Column Names")::int from violations
""", """
with violations as (select analyze_constraints('ttd_dpsr.metrics_PTGNames{TableSuffix}'))
select distinct ('Constraint violation in ' || "Table Name" || '.' || "Column Names")::int from violations
""", """
with violations as (select analyze_constraints('ttd_dpsr.metrics_ScheduleConsumers{TableSuffix}'))
select distinct ('Constraint violation in ' || "Table Name" || '.' || "Column Names")::int from violations
""", """
with violations as (select analyze_constraints('ttd_dpsr.metrics_SpendStats{TableSuffix}'))
select distinct ('Constraint violation in ' || "Table Name" || '.' || "Column Names")::int from violations
""", """
with violations as (select analyze_constraints('ttd_dpsr.metrics_ScheduleAttributes{TableSuffix}'))
select distinct ('Constraint violation in ' || "Table Name" || '.' || "Column Names")::int from violations
""", """
with violations as (select analyze_constraints('ttd_dpsr.metrics_ExecutionDepClasses{TableSuffix}'))
select distinct ('Constraint violation in ' || "Table Name" || '.' || "Column Names")::int from violations
""", """
with violations as (select analyze_constraints('ttd_dpsr.metrics_ExecutionStateHistory{TableSuffix}'))
select distinct ('Constraint violation in ' || "Table Name" || '.' || "Column Names")::int from violations
""", """
with violations as (select analyze_constraints('ttd_dpsr.metrics_ExecutionErrors{TableSuffix}'))
select distinct ('Constraint violation in ' || "Table Name" || '.' || "Column Names")::int from violations
"""
]
