"""
Exposure Feed definition
"""
import logging
from datetime import datetime
import re
from dags.datasrvc.reds.feed_utils import split_location_by_date, replace_placeholders

all_exposure_feeds_sql = '''
WITH FeedEnableDate AS (
    SELECT
        fce.FeedDefinitionId,
        MIN(fce.EventTime) AS MinEnableDate
    FROM
        exposure.FeedChangeEvent fce
    WHERE
        fce.FeedChangeEventTypeId = exposure.fn_Enum_FeedChangeEventType_Enabled()
    GROUP BY
        fce.FeedDefinitionId
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
    fp.PartnerId,
    fa.AdvertiserId,
    f.DestinationLocation,
    fdt.FeedDestinationTypeName,
    f.Version,
    f.RetentionPeriodInDays,
    FORMAT(f.StartDate, 'yyyy-MM-dd') AS StartDate,
    FORMAT(fed.MinEnableDate, 'yyyy-MM-dd') AS EnableDate,
    fc.CampaignId,
    r.RecipientPath,
    ft.FeedTypeName,
    fsv.SchemaVersion
FROM exposure.FeedDefinition f
    JOIN exposure.FeedDestinationType fdt ON fdt.FeedDestinationTypeId = f.FeedDestinationTypeId
    LEFT JOIN FeedEnableDate fed ON fed.FeedDefinitionId = f.FeedDefinitionId
    LEFT JOIN FeedAdvertiser fa ON fa.FeedDefinitionId = f.FeedDefinitionId
    LEFT JOIN FeedPartner fp ON fp.FeedDefinitionId = f.FeedDefinitionId
    LEFT JOIN FeedCampaign fc ON fc.FeedDefinitionId = f.FeedDefinitionId
    LEFT JOIN exposure.Recipient r ON r.RecipientId = f.RecipientId
    LEFT JOIN exposure.FeedType ft ON ft.FeedTypeId = f.FeedTypeId
    LEFT JOIN FeedSchemaVersion fsv ON fsv.FeedDefinitionId = f.FeedDefinitionId
'''

get_retention_target_query = all_exposure_feeds_sql + '''
WHERE fed.MinEnableDate < DATEADD(DAY, -f.RetentionPeriodInDays, '{execution_date}')
    and fed.MinEnableDate IS NOT NULL;
'''

get_all_non_template_query = all_exposure_feeds_sql + '''
WHERE f.IsTemplate = 0;
'''


class ExposureFeed:

    def __init__(self, *args) -> None:
        self.feed_id = args[0]
        self.partner_id = args[1]
        self.advertiser_id = args[2]
        self.feed_destination = args[3]
        self.feed_destination_type_name = args[4]
        self.version = args[5]
        self.retention_period = int(args[6])
        self.start_date = datetime.strptime(args[7], '%Y-%m-%d') if args[7] else None
        self.enable_date = datetime.strptime(args[8], '%Y-%m-%d') if args[8] else None
        self.campaign_id = args[9]
        self.recipient = args[10]
        self.feed_type = args[11].lower() if isinstance(args[11], str) else args[11]
        self.schema_version = args[12]

    @classmethod
    def all(cls, conn):
        cursor = conn.cursor()
        cursor.execute(get_all_non_template_query)
        return [cls(*row) for row in cursor]

    def to_dict(self):
        return {
            'feed_id': self.feed_id,
            'partner_id': self.partner_id,
            'advertiser_id': self.advertiser_id,
            'feed_destination': self.feed_destination,
            'feed_destination_type_name': self.feed_destination_type_name,
            'version': self.version,
            'retention_period': self.retention_period,
            'start_date': self.start_date.strftime('%Y-%m-%d') if self.start_date else None,
            'enable_date': self.enable_date.strftime('%Y-%m-%d') if self.enable_date else None,
            'campaign_id': self.campaign_id,
            'recipient': self.recipient,
            'feed_type': self.feed_type,
            "schema_version": self.schema_version
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            data['feed_id'], data['partner_id'], data['advertiser_id'], data['feed_destination'], data['feed_destination_type_name'],
            data['version'], data['retention_period'], data['start_date'], data['enable_date'], data['campaign_id'], data['recipient'],
            data['feed_type'], data['schema_version']
        )

    @property
    def destination_date_prefix(self):
        return split_location_by_date(self.prefix)[0]

    def populate_destination_date_prefix(self, allowed_placeholders=None, raise_on_unresolved=False, **variables):
        # Merge feed configs with external variables
        feed_configs = {**self.feed_configs_before_date_partition, **variables}
        # Validate that no placeholders remain unresolved
        # Only raise on unresolved if:
        #  1) raise_on_unresolved is enabled by the caller
        #  2) and this isn’t a 'test' bucket,
        #  3) and the template contains '<date>' (required by our retention logic)
        raise_unresolved_flag = raise_on_unresolved and ('test' not in self.bucket) and ('<date>' in self.prefix)

        try:
            return replace_placeholders(self.destination_date_prefix, feed_configs, allowed_placeholders, raise_unresolved_flag)
        except Exception as e:
            logging.error(f"Error occurred in feed_id {self.feed_id}: {e}")
            raise

    def populate_destination(self, allowed_placeholders=None, raise_on_unresolved=False, **variables):
        # Merge feed configs with external variables
        feed_configs = {**self.feed_configs, **variables}
        # Validate that no placeholders remain unresolved
        # Raise an exception if passed-in raise_on_unresolved is True and the feed is not a test feed
        raise_unresolved_flag = raise_on_unresolved and ('test' not in self.bucket)

        try:
            return replace_placeholders(self.prefix, feed_configs, allowed_placeholders, raise_unresolved_flag)
        except Exception as e:
            logging.error(f"Error occurred in feed_id {self.feed_id}: {e}")
            raise

    @property
    def feed_configs(self):
        feed_configs = {
            'starttime': '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}',
            'endtime': '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}',
            'processedtime': '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}',
            'date': '[0-9]{4}-[0-9]{2}-[0-9]{2}',
            'hour': '[0-9]{2}',
            'hash': '[a-z0-9]{32}',
            'partitionkey': '([a-zA-Z]+|{partitionkey})'
        }
        return {**self.feed_configs_before_date_partition, **feed_configs}

    @property
    def feed_configs_before_date_partition(self):
        feed_configs = {
            'feedid': self.feed_id,
            'partnerid': self.partner_id,
            'advertiserid': self.advertiser_id,
            'campaignid': self.campaign_id,
            'version': self.version,
            'recipient': self.recipient,
            'feedtype': self.feed_type,
            'schemaversion': self.schema_version
        }
        return feed_configs

    @property
    def bucket(self):
        variables = {
            'bucket': 'thetradedesk-useast-partner-datafeed',
        }
        bucket = self.feed_destination.split('/')[0]

        if re.match('^<.+>$', bucket):
            return replace_placeholders(bucket, variables)
        else:
            return bucket

    @property
    def prefix(self):
        return self.feed_destination.split('/', 1)[1]

    @property
    def destination_path_regex(self):
        variables = {
            'bucket': self.bucket,
            'feedid': str(self.feed_id),
            'partnerid': self.partner_id,
            'advertiserid': self.advertiser_id,
            'campaignid': self.campaign_id,
            'version': str(self.version),
            'starttime': r'\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}',
            'endtime': r'\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}',
            'processedtime': r'\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}',
            'hour': r'\d{1,2}',
            'date': r'\d{4}-\d{2}-\d{2}',
            'hash': r'[a-z0-9]{32}',
            'partitionkey': '([a-zA-Z]+|{partitionkey})',
            'recipient': self.recipient,
            'feedtype': self.feed_type,
            'schemaversion': self.schema_version
        }
        return replace_placeholders(self.feed_destination, variables)

    def __eq__(self, other):
        if not isinstance(other, ExposureFeed):
            return False
        return (
            self.feed_id == other.feed_id and self.partner_id == other.partner_id and self.advertiser_id == other.advertiser_id
            and self.feed_destination == other.feed_destination and self.feed_destination_type_name == other.feed_destination_type_name
            and self.version == other.version and self.retention_period == other.retention_period and self.start_date == other.start_date
            and self.enable_date == other.enable_date and self.campaign_id == other.campaign_id and self.recipient == other.recipient
            and self.feed_type == other.feed_type and self.schema_version == other.schema_version
        )
