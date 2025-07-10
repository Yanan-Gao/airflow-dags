import json
import re
from datetime import datetime
from enum import Enum

from typing import List, Dict
import logging

from dags.datasrvc.reds.exposurefeed import ExposureFeed
from dags.datasrvc.reds.feed_utils import get_delimiter_from_sql_query, replace_placeholders

exposure_feeds_existing_sql = '''
WITH FeedLastStateChangeDate AS (
    SELECT
        fce.FeedDefinitionId, fce.FeedChangeEventTypeId,
        MAX(fce.EventTime) AS LastEventDate
    FROM
        exposure.FeedChangeEvent fce
    WHERE fce.FeedChangeEventTypeId in (exposure.fn_Enum_FeedChangeEventType_Disabled(), exposure.fn_Enum_FeedChangeEventType_Deleted())
    GROUP BY
        fce.FeedDefinitionId, fce.FeedChangeEventTypeId
),
FeedAdvertiser AS (
    SELECT
        fdp.FeedDefinitionId,
        fdp.Value AS AdvertiserId
    FROM
        exposure.FeedDefinitionParameter fdp
        JOIN exposure.ParameterDefinition pd ON pd.ParameterDefinitionId = fdp.ParameterDefinitionId
    WHERE
        pd.ParameterKey = 'AdvertiserList'
),
FeedPartner AS (
     SELECT
        fdp.FeedDefinitionId,
        fdp.Value AS PartnerId
    FROM
        exposure.FeedDefinitionParameter fdp
        JOIN exposure.ParameterDefinition pd ON pd.ParameterDefinitionId = fdp.ParameterDefinitionId
    WHERE
        pd.ParameterKey = 'PartnerList'
),
FeedCampaign AS (
     SELECT
        fdp.FeedDefinitionId,
        fdp.Value AS CampaignId
    FROM
        exposure.FeedDefinitionParameter fdp
        JOIN exposure.ParameterDefinition pd ON pd.ParameterDefinitionId = fdp.ParameterDefinitionId
    WHERE
        pd.ParameterKey = 'CampaignList'
),
FeedSchemaVersion AS (
    SELECT
        fd.FeedDefinitionId,
        'sv=' + cs.Version + '-' + CAST(cs.SchemaLevel AS VARCHAR(10)) AS SchemaVersion
    FROM
        exposure.FeedDefinition fd
        JOIN exposure.FeedColumnSet fcs ON fcs.FeedDefinitionId = fd.FeedDefinitionId
        JOIN exposure.ColumnSet cs ON cs.ColumnSetId = fcs.ColumnSetId
    WHERE
        fd.DestinationLocation like '%<schemaversion>%'
)
SELECT
    f.FeedDefinitionId,
    fs.FeedStatusName,
    coalesce(fp.PartnerId, f.PartnerId) AS PartnerId,
    fa.AdvertiserId,
    fc.CampaignId,
    f.DestinationLocation,
    fdt.FeedDestinationTypeName,
    f.Version,
    f.RetentionPeriodInDays,
    FORMAT(f.StartDate, 'yyyy-MM-dd') AS StartDate,
    FORMAT(f.EndDate, 'yyyy-MM-dd') AS EndDate,
    FORMAT(fsd.LastEventDate, 'yyyy-MM-dd') AS LastStateChangeDate,
    f.ExportSql,
    r.RecipientPath,
    ft.FeedTypeName,
    fsv.SchemaVersion
FROM exposure.FeedDefinition f
    JOIN exposure.FeedDestinationType fdt ON fdt.FeedDestinationTypeId = f.FeedDestinationTypeId
    LEFT JOIN FeedLastStateChangeDate fsd ON (
        fsd.FeedDefinitionId = f.FeedDefinitionId
        and ((f.FeedStatusId = exposure.fn_Enum_FeedStatus_Disabled() and fsd.FeedChangeEventTypeId = exposure.fn_Enum_FeedChangeEventType_Disabled())
        or (f.FeedStatusId = exposure.fn_Enum_FeedStatus_Archived() and fsd.FeedChangeEventTypeId = exposure.fn_Enum_FeedChangeEventType_Deleted()))
    )
    LEFT JOIN FeedAdvertiser fa ON fa.FeedDefinitionId = f.FeedDefinitionId
    LEFT JOIN FeedPartner fp ON fp.FeedDefinitionId = f.FeedDefinitionId
    LEFT JOIN FeedCampaign fc ON fc.FeedDefinitionId = f.FeedDefinitionId
    LEFT JOIN exposure.Recipient r ON r.RecipientId = f.RecipientId
    LEFT JOIN exposure.FeedType ft ON ft.FeedTypeId = f.FeedTypeId
    LEFT JOIN FeedSchemaVersion fsv ON fsv.FeedDefinitionId = f.FeedDefinitionId
    JOIN exposure.DataSource ds ON ds.DataSourceId = f.DataSourceId
    JOIN exposure.FeedStatus fs ON fs.FeedStatusId = f.FeedStatusId
WHERE ds.DataSourceProviderId = 5 -- only need to scrub data from Snowflake
and (f.FeedStatusId = exposure.fn_Enum_FeedStatus_Active()
    or (F.FeedStatusId = exposure.fn_Enum_FeedStatus_Completed() and f.EndDate > DATEADD(DAY, -f.RetentionPeriodInDays, GETUTCDATE()))
    or (F.FeedStatusId in (exposure.fn_Enum_FeedStatus_Archived(), exposure.fn_Enum_FeedStatus_Disabled())
            and fsd.LastEventDate IS NOT NULL and fsd.LastEventDate > DATEADD(DAY, -f.RetentionPeriodInDays, GETUTCDATE())))
'''


class Delimiter(str, Enum):
    TAB = '\t'
    COMMA = ','


class ExposureFeedDsdr(ExposureFeed):

    def __init__(
        self, feed_id, feed_status, partner_id, advertiser_id, campaign_id, feed_destination, feed_destination_type_name, version,
        retention_period, start_date, enable_date, last_change_date, export_sql, recipient, feed_type, schema_version, *args, **kwargs
    ) -> None:
        super().__init__(
            feed_id, partner_id, advertiser_id, feed_destination, feed_destination_type_name, version, retention_period, start_date,
            enable_date, campaign_id, recipient, feed_type, schema_version
        )
        self.feed_status = feed_status
        self.last_change_date = datetime.strptime(last_change_date, '%Y-%m-%d') if last_change_date else None
        self.export_sql = export_sql
        self.delimiter = get_delimiter_from_str(get_delimiter_from_sql_query(self.export_sql))
        self._columns: List[str] = list()
        self._advertiser_ids = self.advertiser_id.lower().split(',') if self.advertiser_id else list()

    @classmethod
    def all_feeds_with_columns(cls, conn):
        cursor = conn.cursor()
        cursor.execute(exposure_feeds_existing_sql)
        all_feeds = [cls(*row) for row in cursor]
        return enrich_feed_columns(conn, all_feeds)

    @staticmethod
    def _split_location(template):
        parts = re.split(r'(/date=<date>)|(/<date>)', template, 1)
        bucket, path = parts[0].split('/', 1)
        subpath_with_filename = parts[-1]
        return bucket, path + '/', subpath_with_filename

    @property
    def prefix_before_date(self):
        _, path, _ = self._split_location(self.feed_destination)
        return replace_placeholders(path, self.feed_configs_before_date_partition)

    @property
    def include(self):
        # replace for java pattern matching
        override_variables = {'partitionkey': r'([a-zA-Z]+|\{partitionkey\})'}
        feed_configs = {**self.feed_configs, **override_variables}
        _, _, subpath_with_filename = self._split_location(self.feed_destination)
        return '.*' + replace_placeholders(subpath_with_filename.lower(), feed_configs)

    @property
    def columns(self):
        return self._columns

    def set_columns(self, columns):
        self._columns = columns

    @property
    def destination_type(self):
        return self.feed_destination_type_name

    @property
    def status(self):
        return self.feed_status

    def dsdr_scrubbable(self, id_graph: dict, user_id_columns, storage='s3') -> bool:
        if not self.columns:
            logging.warning(f'Feed: {self.feed_id}, partnerId: {self.partner_id}, has no schema defined, skipping scrub')
            return False

        cols_lower = [i.lower() for i in self.columns]

        # special case: no partner id provided, retail feed
        if not self.partner_id:
            return any(col in cols_lower for col in user_id_columns)

        # Check if partner_id is in idGraph and if advertiser_id either exists and matches or doesn't exist
        partner_id_lower = self.partner_id.lower()
        if (partner_id_lower in id_graph and (not self.advertiser_id or any(adv_id in self._advertiser_ids
                                                                            for adv_id in id_graph[partner_id_lower]))
                and any(col in cols_lower for col in user_id_columns)):
            return True

        return False

    def to_dsdr_work_item(self, override_bucket: str = None) -> str:
        options = {
            "FeedId": self.feed_id,
            "FeedVersion": self.version,
            "FeedType": "exfeed",
            "Prefix": self.prefix_before_date,
            "PartnerId": self.partner_id,
            "AdvertiserIds": self._advertiser_ids,
            "Include": self.include,
            "ParentBucket": override_bucket if override_bucket is not None else self.bucket,
            "DestinationBucket": override_bucket if override_bucket is not None else self.bucket,
            "Delimiter": self.delimiter.name,
            "Schema": self.columns
        }
        return json.dumps(options)

    def to_dict(self):
        return {
            "feed_id": self.feed_id,
            "feed_status": self.status,
            "partner_id": self.partner_id,
            "advertiser_id": self.advertiser_id,
            "campaign_id": self.campaign_id,
            "feed_destination": self.feed_destination,
            "feed_destination_type_name": self.feed_destination_type_name,
            "version": self.version,
            "retention_period": self.retention_period,
            'start_date': self.start_date.strftime('%Y-%m-%d') if self.start_date else None,
            'enable_date': self.enable_date.strftime('%Y-%m-%d') if self.enable_date else None,
            'last_change_date': self.last_change_date.strftime('%Y-%m-%d') if self.last_change_date else None,
            'export_sql': self.export_sql,
            'columns': self.columns,
            'recipient': self.recipient,
            'feed_type': self.feed_type,
            "schema_version": self.schema_version
        }

    @classmethod
    def from_dict(cls, data):
        result = cls(
            data['feed_id'], data['feed_status'], data['partner_id'], data['advertiser_id'], data['campaign_id'], data['feed_destination'],
            data['feed_destination_type_name'], data['version'], data['retention_period'], data['start_date'], data['enable_date'],
            data['last_change_date'], data['export_sql'], data['recipient'], data['feed_type'], data['schema_version']
        )
        result.set_columns(data['columns'])
        return result


def enrich_feed_columns(conn, feeds: List[ExposureFeedDsdr]) -> List[ExposureFeedDsdr]:
    feed_definition_ids = [feed.feed_id for feed in feeds]
    ids_formatted = ','.join(map(str, feed_definition_ids))
    feed_columns_sql = f'''
        SELECT fdc.FeedDefinitionId, coalesce(fdc.ExternalName, dsc.DefaultColumnAlias, cd.ColumnName)
        FROM exposure.FeedDefinitionColumn fdc
        JOIN exposure.ColumnDefinition cd ON fdc.ColumnDefinitionId = cd.ColumnDefinitionId
        JOIN exposure.FeedDefinition fd ON fd.FeedDefinitionId = fdc.FeedDefinitionId
        JOIN exposure.DataSourceColumn dsc ON dsc.ColumnDefinitionId = cd.ColumnDefinitionId
            AND dsc.DataSourceId = fd.DataSourceId
        WHERE fdc.FeedDefinitionId IN ({ids_formatted})
        ORDER BY fdc.FeedDefinitionId, fdc.OrderIndex
    '''
    feed_id_to_columns: Dict[str, List[str]] = dict()
    cursor = conn.cursor()
    cursor.execute(feed_columns_sql)
    for row in cursor:
        if row[0] not in feed_id_to_columns:
            feed_id_to_columns[row[0]] = list()
        feed_id_to_columns[row[0]].append(row[1].lower())

    for feed in feeds:
        if feed.feed_id in feed_id_to_columns:
            feed.set_columns(feed_id_to_columns[feed.feed_id])
        else:
            raise ValueError(f"No columns found for feed with feedId={feed.feed_id}")
    return feeds


def get_delimiter_from_str(value: str) -> Delimiter:
    if value == '\t' or value == '\\t':
        return Delimiter.TAB
    elif value == ',':
        return Delimiter.COMMA
    else:
        raise ValueError(f"Delimiter {value} is not supported.")
