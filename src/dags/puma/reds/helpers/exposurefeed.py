"""
Exposure Feed definition
"""
import logging
from datetime import datetime
import re
import dags.puma.reds.helpers.feed_utils as feed_utils

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
    ft.FeedTypeName
FROM exposure.FeedDefinition f
    JOIN exposure.FeedDestinationType fdt ON fdt.FeedDestinationTypeId = f.FeedDestinationTypeId
    LEFT JOIN FeedEnableDate fed ON fed.FeedDefinitionId = f.FeedDefinitionId
    LEFT JOIN FeedAdvertiser fa ON fa.FeedDefinitionId = f.FeedDefinitionId
    LEFT JOIN FeedPartner fp ON fp.FeedDefinitionId = f.FeedDefinitionId
    LEFT JOIN FeedCampaign fc ON fc.FeedDefinitionId = f.FeedDefinitionId
    LEFT JOIN exposure.Recipient r ON r.RecipientId = f.RecipientId
    LEFT JOIN exposure.FeedType ft ON ft.FeedTypeId = f.FeedTypeId
WHERE
    f.DestinationLocation IS NOT NULL
    AND f.IsTemplate = 0
    AND fdt.FeedDestinationTypeName = 'S3'
'''

get_retention_target_query = all_exposure_feeds_sql + '''
WHERE fed.MinEnableDate < DATEADD(DAY, -f.RetentionPeriodInDays, '{execution_date}')
    and fed.MinEnableDate IS NOT NULL;
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

    @classmethod
    def all(cls, conn):
        cursor = conn.cursor()
        cursor.execute(all_exposure_feeds_sql)
        return [cls(*row) for row in cursor]

    @classmethod
    def get_exposure_feed_ids(cls, conn):
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT f.FeedDefinitionId
            FROM exposure.FeedDefinition f
            WHERE f.DestinationLocation IS NOT NULL
            AND f.FeedDestinationTypeId = 0
            AND f.IsTemplate = 0
            """
        )
        rows = cursor.fetchall()
        feed_ids = [row[0] for row in rows]
        return feed_ids

    @classmethod
    def log_feed_access_batch(cls, conn, values):
        sql = """
        MERGE INTO exposure.FeedAccessHistory AS target
        USING (VALUES (%s, %s, %s, %s)) AS source (FeedDefinitionId, LastAccessedDateTime, AccountId, PrincipalId)
        ON target.FeedDefinitionId = source.FeedDefinitionId
        WHEN MATCHED AND source.LastAccessedDateTime > target.LastAccessedDateTime THEN
            UPDATE SET
                LastAccessedDateTime = source.LastAccessedDateTime,
                AccountId = source.AccountId,
                PrincipalId = source.PrincipalId
        WHEN NOT MATCHED THEN
            INSERT (FeedDefinitionId, LastAccessedDateTime, AccountId, PrincipalId)
            VALUES (source.FeedDefinitionId, source.LastAccessedDateTime, source.AccountId, source.PrincipalId);
        """
        cursor = conn.cursor()
        cursor.executemany(sql, values)
        conn.commit()

    @property
    def destination_date_prefix(self):
        return feed_utils.split_location_by_date(self.prefix)[0]

    def populate_destination_date_prefix(self, allowed_placeholders=None, raise_on_unresolved=False, **variables):
        # Merge feed configs with external variables
        feed_configs = {**self.feed_configs_before_date_partition, **variables}
        return self.populate_destination_by_configs(self.destination_date_prefix, feed_configs, allowed_placeholders, raise_on_unresolved)

    def populate_destination(self, allowed_placeholders=None, raise_on_unresolved=False, **variables):
        # Merge feed configs with external variables
        feed_configs = {**self.feed_configs, **variables}
        return self.populate_destination_by_configs(self.prefix, feed_configs, allowed_placeholders, raise_on_unresolved)

    def populate_destination_by_configs(self, prefix, feed_configs, allowed_placeholders=None, raise_on_unresolved=False):
        # Validate that no placeholders remain unresolved
        # Raise an exception if passed-in raise_on_unresolved is True and the feed is not a test feed
        raise_unresolved_flag = raise_on_unresolved and ('test' not in self.bucket)
        try:
            return feed_utils.replace_placeholders(prefix, feed_configs, allowed_placeholders, raise_unresolved_flag)
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
            'feedtype': self.feed_type
        }
        return feed_configs

    @property
    def bucket(self):
        variables = {
            'bucket': 'thetradedesk-useast-partner-datafeed',
        }
        bucket = self.feed_destination.split('/')[0]

        if re.match('^<.+>$', bucket):
            return feed_utils.replace_placeholders(bucket, variables)
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
            'feedtype': self.feed_type
        }
        return self.populate_destination_by_configs(self.feed_destination, variables, raise_on_unresolved=True)
