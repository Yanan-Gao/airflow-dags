select /*+label(RawDataCloudHourlyConsistencyCheck_Airflow)*/ * from (
with bidrequest_grouped as (
    select date_trunc('hour', LogEntryTime) as Date, partnerid, count(*) as RowCount
    from ttd.bidrequest 
    where LogEntryTime >= '{startDate}' and LogEntryTime< '{endDate}'
    group by 1,2),
bidfeedback_grouped as (
    select date_trunc('hour', LogEntryTime) as Date, partnerid, count(*) as RowCount
    from ttd.bidfeedback 
    where LogEntryTime >= '{startDate}' and LogEntryTime< '{endDate}'
    group by 1,2),
clicktracker_grouped as (
    select date_trunc('hour', LogEntryTime) as Date, partnerid, count(*) as RowCount
    from ttd.clicktracker 
    where LogEntryTime >= '{startDate}' and LogEntryTime< '{endDate}'
    group by 1,2),
conversiontracker_grouped as (
    select date_trunc('hour', LogEntryTime) as Date, partnerid, count(*) as RowCount
    from ttd.conversiontracker 
    where LogEntryTime >= '{startDate}' and LogEntryTime< '{endDate}'
    group by 1,2),
videoevent_grouped as (
    select date_trunc('hour', LogEntryTime) as Date, partnerid, count(*) as RowCount
    from ttd.videoevent 
    where LogEntryTime >= '{startDate}' and LogEntryTime< '{endDate}'
    group by 1,2)
select 'bidrequest' as Source, Date, TenantName, SUM(RowCount) as RowCount from bidrequest_grouped
join provisioning2.partner using (partnerid)
join provisioning2.partnergroup using (partnergroupid)
join provisioning2.tenant using (tenantid)
group by 1,2,3
union all
select 'bidfeedback' as Source, Date, TenantName, SUM(RowCount) as RowCount from bidfeedback_grouped
join provisioning2.partner using (partnerid)
join provisioning2.partnergroup using (partnergroupid)
join provisioning2.tenant using (tenantid)
group by 1,2,3
union all
select 'clicktracker' as Source, Date, TenantName, SUM(RowCount) as RowCount from clicktracker_grouped
join provisioning2.partner using (partnerid)
join provisioning2.partnergroup using (partnergroupid)
join provisioning2.tenant using (tenantid)
group by 1,2,3
union all
select 'conversiontracker' as Source, Date, TenantName, SUM(RowCount) as RowCount from conversiontracker_grouped
join provisioning2.partner using (partnerid)
join provisioning2.partnergroup using (partnergroupid)
join provisioning2.tenant using (tenantid)
group by 1,2,3
union all
select 'videoevent' as Source, Date, TenantName, SUM(RowCount) as RowCount from videoevent_grouped
join provisioning2.partner using (partnerid)
join provisioning2.partnergroup using (partnergroupid)
join provisioning2.tenant using (tenantid)
group by 1,2,3
) labeled