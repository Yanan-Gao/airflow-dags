import pymssql
from textwrap import dedent
from typing import List
from airflow.models import Variable


def _get_provdb_connection():
    server = 'provdb.adsrvr.org'
    user = 'ttd_readonly'
    password = Variable.get('ttd-readonly-password')
    database = 'provisioning'

    return pymssql.connect(server, user, password, database)


def query_for_additional_provisioning_data(targeting_data_ids: List[int]):
    """
    Queries Provisioning DB for segment information associated with the
    targeting_data_ids that are passed in.
    """
    results = []

    with _get_provdb_connection() as conn:
        # Define your batch size to keep IN clauses manageable (e.g., 1000 IDs per query)
        batch_size = 10000
        for i in range(0, len(targeting_data_ids), batch_size):
            batch_ids = targeting_data_ids[i:i + batch_size]

            sql = dedent(
                f"""
            select distinct
                case
                    when (b.[Name] like 'Data Alliance%' and (tpd.ThirdPartyDataProviderId in ('thetradedesk', 'q-alliance', 'ttd_internal', 'ttdeskpat',
                        'ttddatasupp', 'ttdgeofence', 'uspolitical', 'ttdbcsds',
                        'adbrain', 'crosswise', 'idlink', 'tapad', 'ttd-cai',
                        'ttdoffline', 'ccdata'))) then ''
                    when (b.[Name] like 'Data Alliance%' and (tpd.ThirdPartyDataProviderId not in ('thetradedesk', 'q-alliance', 'ttd_internal', 'ttdeskpat',
                        'ttddatasupp', 'ttdgeofence', 'uspolitical', 'ttdbcsds',
                        'adbrain', 'crosswise', 'idlink', 'tapad', 'ttd-cai',
                        'ttdoffline', 'ccdata'))) then tpd.ThirdPartyDataProviderId
                    else b.[Name]
                end as [Provider],
                tpd.FullPath as [Full Path]
            from
                dbo.TargetingData td
                left join dbo.ThirdPartyData tpd ON tpd.TargetingDataId = td.TargetingDataId
                inner join dbo.TargetingDataUniques v1 on tpd.TargetingDataId = v1.TargetingDataId
                inner join dbo.TargetingDataUniquesV2 v2 on v1.TargetingDataId = v2.TargetingDataId
                inner join dbo.ExpandedThirdPartyDataRate ex on ex.ThirdPartyDataId = tpd.ThirdPartyDataId
                inner join dbo.DataRate2 r2 on ex.DataRateId = r2.DataRateId
                inner join dbo.DataRateCard drc on drc.DataRateCardId = r2.DataRateCardId
                inner join dbo.ThirdPartyDataBrand b on drc.ThirdPartyDataBrandId = b.ThirdPartyDataBrandId
            where
                td.TargetingDataId IN ({','.join(map(str, batch_ids))}) and
                DataOwnerTypeId = 2 and
                tpd.ThirdPartyDataProviderId is not null
                and Buyable = 1
                and ex.DataRateId is not null
                and drc.isLatest = 1
                and r2.BaseDataRateId is null
                and ((v1.uniques > 0 ) OR (v2.uniques > 0 ))"""
            )

            with conn.cursor() as cursor:
                cursor.execute(sql)
                results.extend(cursor.fetchall())

    return results


def get_cross_device_vendors_changed_count() -> int:
    """
    Returns: count of cross device vendors that are new and not in our list, or are in our list and have been disabled.
      This count will be emitted as a metric for use in an alert.
    """
    sql = dedent(
        """
        --Currently enabled Cross Device vendor IDs
        declare @crossDeviceVendorIds table(CrossDeviceVendorId int)
        insert into @crossDeviceVendorIds(CrossDeviceVendorId)
        values
            (1),
            (4),
            (6),
            (8),
            (10),
            (11)

        /*
          This count should be 0 unless:
          1) A new vendor has been enabled and added to the list, or
          2) An existing vendor has been disabled
        */
        select
            (select
                count(*)
            from
                dbo.CrossDeviceVendor
            where
                IsEnabled=1 and
                CrossDeviceVendorId not in (select CrossDeviceVendorId from @crossDeviceVendorIds))
            +
            (select
                count(*)
            from
                dbo.CrossDeviceVendor
            where
                IsEnabled=0 and
                CrossDeviceVendorId in (select CrossDeviceVendorId from @crossDeviceVendorIds))
        """
    )

    with _get_provdb_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            count_row = cursor.fetchone()
            if not count_row:
                return -1
            return count_row[0]
