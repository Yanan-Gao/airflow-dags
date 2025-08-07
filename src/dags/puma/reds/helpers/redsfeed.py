"""
Helper code for handling REDS feeds information from Provisioning
"""
from collections import namedtuple
import re

Config = namedtuple(
    'Config',
    'feed_id, status, destination_type, destination_location, partner_id, advertiser_id, feed_type_name, version, concatenation_interval'
)


class RedsFeed(Config):

    def __new__(cls, *args):
        return super(RedsFeed, cls).__new__(cls, *args)

    @classmethod
    def all(cls, conn):
        # Our 'AdvertiserId' here may be NULL. This would cause problems with '_get_path' except that we have upstream
        # validation that ensures that we don't use the '<advertiserid>' placeholder unless there is an associated
        # 'AdvertiserId'.
        sql = """
            select distinct
                rf.FeedId,
                rfs.FeedStatusName,
                rfdt.FeedDestinationTypeName,
                rf.DestinationLocation,
                rf.PartnerId,
                rfa.AdvertiserId,
                rft.FeedTypeName,
                rf.Version,
                rf.ConcatenationIntervalInMinutes
            from reds.Feed rf
                left outer join (select FeedId, min(AdvertiserId) AdvertiserId from reds.FeedAdvertiser group by FeedId) rfa on rfa.FeedId = rf.FeedId
                join reds.FeedType rft on rft.FeedTypeId = rf.FeedTypeId
                join reds.FeedStatus rfs on rfs.FeedStatusId = rf.FeedStatusId
                join reds.FeedDestinationType rfdt on rfdt.FeedDestinationTypeId = rf.FeedDestinationTypeId
            where
                rf.DestinationLocation IS NOT NULL
                AND rfdt.FeedDestinationTypeName = 'S3'
                """
        cursor = conn.cursor()
        cursor.execute(sql)
        return [cls(*row) for row in cursor]

    @classmethod
    def get_reds_feed_ids(cls, conn):
        sql = """
            select distinct rf.FeedId
            from reds.Feed rf
            where rf.DestinationLocation IS NOT NULL
            AND rf.FeedDestinationTypeId = 0
            """
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        feed_ids = [row[0] for row in rows]
        return feed_ids

    @classmethod
    def log_feed_access_batch(cls, conn, values):
        sql = """
        MERGE INTO reds.FeedAccessHistory AS target
        USING (VALUES (%s, %s, %s, %s)) AS source (FeedId, LastAccessedDateTime, AccountId, PrincipalId)
        ON target.FeedId = source.FeedId
        WHEN MATCHED AND source.LastAccessedDateTime > target.LastAccessedDateTime THEN
            UPDATE SET
                LastAccessedDateTime = source.LastAccessedDateTime,
                AccountId = source.AccountId,
                PrincipalId = source.PrincipalId
        WHEN NOT MATCHED THEN
            INSERT (FeedId, LastAccessedDateTime, AccountId, PrincipalId)
            VALUES (source.FeedId, source.LastAccessedDateTime, source.AccountId, source.PrincipalId);
        """
        cursor = conn.cursor()
        cursor.executemany(sql, values)
        conn.commit()

    @staticmethod
    def _replace_placeholders(template, variables):

        def replace(match):
            return variables[match.group(1)]

        regex = re.compile(r'<([^>]*)>')  # Match values inside angle braces
        return regex.sub(replace, template)

    @staticmethod
    def _split_location(template):
        parts = template.partition('/date=<date>')
        bucket, path = parts[0].split('/', 1)
        subpath, file = ''.join(parts[1:]).strip('/').rsplit('/', 1)
        return bucket, path, subpath, file

    @property
    def bucket(self):
        variables = {
            'bucket': 'thetradedesk-useast-partner-datafeed',
        }

        bucket, _, _, _ = self._split_location(self.destination_location)
        return self._replace_placeholders(bucket, variables)

    def _get_path(self):
        variables = {
            'feedid': str(self.feed_id),
            'partnerid': self.partner_id,
            'advertiserid': self.advertiser_id,
            'feedtype': self.feed_type_path,
            'version': str(self.version),
        }

        _, path, _, _ = self._split_location(self.destination_location)
        return self._replace_placeholders(path, variables)

    @property
    def source_path(self):
        return self._get_path()

    @property
    def destination_path_regex(self):
        variables = {
            'bucket': self.bucket,
            'feedid': str(self.feed_id),
            'partnerid': self.partner_id,
            'advertiserid': self.advertiser_id,
            'feedtype': self.feed_type_path,
            'version': str(self.version),
            'starttime': r'\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}',
            'endtime': r'\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}',
            'processedtime': r'\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}',
            'hour': r'\d{1,2}',
            'date': r'\d{4}-\d{2}-\d{2}',
            'hash': '[a-f0-9]{32}',
            'index': r'\d{1}'
        }

        if self.has_concat:
            bucket, _, subpath, _ = self._split_location(self.destination_location)
            regex = re.compile(r'\(([^\)]*)\)')  # Match values inside parentheses
            file = ''.join(regex.findall(self.grouping_regex)) + '<index>.gz'
            return self._replace_placeholders(f"{bucket}/{self.destination_path}/{subpath}/{file}", variables)

        return self._replace_placeholders(self.destination_location, variables)

    @property
    def destination_path_regex_always_unconcatenated(self):
        variables = {
            'bucket': self.bucket,
            'feedid': str(self.feed_id),
            'partnerid': self.partner_id,
            'advertiserid': self.advertiser_id,
            'feedtype': self.feed_type_path,
            'version': str(self.version),
            'starttime': r'\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}',
            'endtime': r'\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}',
            'processedtime': r'\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}',
            'hour': r'\d{1,2}',
            'date': r'\d{4}-\d{2}-\d{2}',
            'hash': '[a-f0-9]{32}',
            'index': r'\d{1}'
        }

        return self._replace_placeholders(self.destination_location, variables)

    # List of older hard-coded overrides that are exceptions to the general newer aggregation naming
    _destination_overrides = {
        'choozle/redf5aggregated': 'Choozle/redf5aggregated',
        'groupm - connect - multiple - uk - gbp/redf5aggregated': 'GroupM - Connect - Multiple - UK - GBP/redf5aggregated',
        'groupm - magic moments - multiple - de - eur/redf5aggregated': 'GroupM - Magic Moments - Multiple - DE - EUR/redf5aggregated',
        'groupm - maxus - ibot - us - usd/redf5aggregated': 'GroupM - Maxus - IBOT - US - USD/redf5aggregated',
        'groupm - maxus - multiple - us - usd/redf5aggregated': 'GroupM - Maxus - Multiple - US - USD/redf5aggregated',
        'groupm - maxus - nestle - ca - cad/redf5aggregated': 'GroupM - Maxus - Nestle - CA - CAD/redf5aggregated',
        'groupm - maxus - sc johnson - us - usd/redf5aggregated': 'GroupM - Maxus - SC Johnson - US - USD/redf5aggregated',
        'groupm - mec - colgate - singapore - sgd/redf5aggregated': 'GroupM - MEC - Colgate - Singapore - SGD/redf5aggregated',
        'groupm - mec - epc - us - usd/redf5aggregated': 'GroupM - MEC - EPC - US - USD/redf5aggregated',
        'groupm - mec - godaddy - ca - usd/redf5aggregated': 'GroupM - MEC - GoDaddy - CA - USD/redf5aggregated',
        'groupm - mec - multiple - singapore - usd/redf5aggregated': 'GroupM - MEC - Multiple - Singapore - USD/redf5aggregated',
        'groupm - mec - multiple - uk - gbp/redf5aggregated': 'GroupM - MEC - Multiple - UK - GBP/redf5aggregated',
        'groupm - mec - multiple - uk - usd/redf5aggregated': 'GroupM - MEC - Multiple - UK - USD/redf5aggregated',
        'groupm - mec - multiple - us - usd/redf5aggregated': 'GroupM - MEC - Multiple - US - USD/redf5aggregated',
        'groupm - mediacom - multiple - de - eur /redf5aggregated': 'GroupM - Mediacom - Multiple - DE - EUR /redf5aggregated',
        'groupm - mediacom - multiple - singapore - usd/redf5aggregated': 'GroupM - Mediacom - Multiple - Singapore - USD/redf5aggregated',
        'groupm - mediacom - multiple - uk - usd/redf5aggregated': 'GroupM - MediaCom - Multiple - UK - USD/redf5aggregated',
        'groupm - mediacom - multiple - us - usd/redf5aggregated': 'GroupM - Mediacom - Canon - US - USD/redf5aggregated',
        'groupm - mediacom - sony - us - usd/redf5aggregated': 'GroupM - Mediacom - Sony - US - USD/redf5aggregated',
        'groupm - mindshare - booking.com - uk - eur/redf5aggregated': 'GroupM - Mindshare - Booking.com - UK - EUR/redf5aggregated',
        'groupm - mindshare - booking.com - us - usd/redf5aggregated': 'GroupM - Mindshare - Booking.com - US - USD/redf5aggregated',
        'groupm - mindshare - bp - us - usd/redf5aggregated': 'GroupM - Mindshare - BP - US - USD/redf5aggregated',
        'groupm - mindshare - domino\'s - us - usd/redf5aggregated': 'GroupM - Mindshare - Domino\'s - US - USD/redf5aggregated',
        'groupm - mindshare - general mills - us - usd/redf5aggregated': 'GroupM - Mindshare - General Mills - US - USD/redf5aggregated',
        'groupm - mindshare - hsbc - apac - usd/redf5aggregated': 'GroupM - Mindshare - Multiple - Singapore - USD/redf5aggregated',
        'groupm - mindshare - ihg - us - usd/redf5aggregated': 'GroupM - Mindshare - Multiple - US - USD/redf5aggregated',
        'groupm - mindshare - kimberly clark - ca - cad/redf5aggregated': 'GroupM - Mindshare - Kimberly Clark - CA - CAD/redf5aggregated',
        'groupm - mindshare - kimberly clark - hong kong - usd/redf5aggregated':
        'GroupM - Mindshare - Kimberly Clark - Hong Kong - USD/redf5aggregated',
        'groupm - mindshare - kimberly clark - singapore - usd/redf5aggregated':
        'GroupM - Mindshare - Kimberly Clark - Singapore - USD/redf5aggregated',
        'groupm - mindshare - kimberly clark - uk - eur/redf5aggregated': 'GroupM - Mindshare - Kimberly Clark - UK - EUR/redf5aggregated',
        'groupm - mindshare - kimberly clark - uk - gbp/redf5aggregated': 'GroupM - Mindshare - Kimberly Clark - UK - GBP/redf5aggregated',
        'groupm - mindshare - kimberly clark - us - usd/redf5aggregated': 'GroupM - Mindshare - Kimberly Clark - US - USD/redf5aggregated',
        'groupm - mindshare - kimberly clark - vietnam - usd/redf5aggregated':
        'GroupM - Mindshare - Kimberly Clark - Vietnam - USD/redf5aggregated',
        'groupm - mindshare - multiple - de - eur /redf5aggregated': 'GroupM - Mindshare - Multiple - DE - EUR /redf5aggregated',
        'groupm - mindshare - multiple - jp - jpy/redf5aggregated': 'GroupM - Mindshare - Multiple - JP - JPY/redf5aggregated',
        'groupm - mindshare - unilever - apac - usd/redf5aggregated': 'GroupM - Mindshare - Unilever - APAC - USD/redf5aggregated',
        'groupm - mindshare - unilever - ca - cad/redf5aggregated': 'GroupM - Mindshare - Unilever - CA - CAD/redf5aggregated',
        'groupm - mindshare - unilever - malaysia - usd/redf5aggregated': 'GroupM - Mindshare - Unilever - Malaysia - USD/redf5aggregated',
        'groupm - mindshare - unilever - philippines - usd/redf5aggregated':
        'GroupM - Mindshare - Unilever - Philippines - USD/redf5aggregated',
        'groupm - mindshare - unilever - singapore - usd/redf5aggregated':
        'GroupM - Mindshare - Unilever - Singapore - USD/redf5aggregated',
        'groupm - mindshare - unilever - south africa - usd/redf5aggregated':
        'GroupM - Mindshare - Unilever - South Africa - USD/redf5aggregated',
        'groupm - mindshare - unilever - thailand -thb/redf5aggregated': 'GroupM - Mindshare - Unilever - Thailand -THB/redf5aggregated',
        'groupm - mindshare - unilever - uk - eur/redf5aggregated': 'GroupM - Mindshare - Unilever - UK - EUR/redf5aggregated',
        'groupm - mindshare - unilever - uk - gbp/redf5aggregated': 'GroupM - Mindshare - Unilever - UK - GBP/redf5aggregated',
        'groupm - mindshare - unilever - us - usd/redf5aggregated': 'GroupM - Mindshare - Unilever - US - USD/redf5aggregated',
        'groupm - mindshare - unilever - vietnam - usd/redf5aggregated': 'GroupM - Mindshare - Unilever - Vietnam - USD/redf5aggregated',
        'groupm - mindshare - usmc - us - usd/redf5aggregated': 'GroupM - Mindshare - USMC - US - USD/redf5aggregated',
        'groupm - multiple - muliple - au - aud/redf5aggregated': 'GroupM - Multiple - Muliple - AU - AUD/redf5aggregated',
        'groupm - mec - colgate - us - usd/redf5aggregated': 'GroupM - MEC - Colgate - US - USD/redf5aggregated',
        'groupm - mec - chevron - us - usd/redf5aggregated': 'GroupM - MEC - Chevron - US - USD/redf5aggregated',
        'groupm – catalyst – multiple - ca - cad/redf5aggregated': 'GroupM – Catalyst – Multiple - CA - CAD/redf5aggregated',
        'light reaction (xaxis) de/redf5aggregated': 'Light Reaction (Xaxis) DE/redf5aggregated',
        'light reaction (xaxis) th/redf5aggregated': 'Light Reaction (Xaxis) TH/redf5aggregated',
        'omgprogrammatic_uk_gbp/redf5aggregated': 'OMGProgrammatic_UK_GBP/redf5aggregated',
        'xaxis hk (mec)/redf5aggregated': 'Xaxis HK (MEC)/redf5aggregated',
    }

    @property
    def destination_path(self):
        path = self._get_path() + 'aggregated'
        return self._destination_overrides.get(path, path)

    @property
    def grouping_regex(self):

        def wrap(s):
            return f'({s})'

        variables = {
            'feedid': wrap(self.feed_id),
            'partnerid': self.partner_id,
            'advertiserid': self.advertiser_id,
            'feedtype': wrap(self.feed_type_path),
            'version': wrap(self.version),
            'date': wrap('<date>'),
            'hour': wrap('<hour>'),
            'starttime': '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}',
            'endtime': '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}',
            'processedtime': '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}',
            'hash': '[a-f0-9]{32}',
        }

        _, _, subpath, file = self._split_location(self.destination_location)
        return '.*' + self._replace_placeholders(f'{subpath}/{file}', variables)

    def get_date_hour_grouping_regex(self, dt):
        variables = {
            'date': str(dt.date()),
            'hour': str(dt.hour),
        }

        return self._replace_placeholders(self.grouping_regex, variables)

    @property
    def include(self):

        def wrap(s):
            return f'({s})'

        variables = {
            'feedid': wrap(self.feed_id),
            'partnerid': self.partner_id,
            'advertiserid': self.advertiser_id,
            'feedtype': wrap(self.feed_type_path),
            'version': wrap(self.version),
            'date': wrap('<date>'),
            'hour': wrap('<hour>'),
            'starttime': '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}',
            'endtime': '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}',
            'processedtime': '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}',
            'hash': '[a-f0-9]{32}',
        }
        _, _, _, file = self._split_location(self.destination_location)
        return '.*' + self._replace_placeholders(f'{file}', variables)

    @property
    def concat_prefix(self):
        variables = {
            'feedid': self.feed_id,
            'partnerid': self.partner_id,
            'advertiserid': self.advertiser_id,
            'feedtype': self.feed_type_path,
            'version': self.version,
            'date': '<date>',
            'hour': '<hour>',
            'index': '<index>',
        }

        _, _, subpath, _ = self._split_location(self.destination_location)

        regex = re.compile(r'\(([^\)]*)\)')  # Match values inside parentheses
        file = ''.join(regex.findall(self.grouping_regex)) + '<index>.gz'

        return self._replace_placeholders(f'{self.destination_path}/{subpath}/{file}', variables)

    def get_date_hour_concat_prefix(self, dt):
        variables = {
            'date': str(dt.date()),
            'hour': str(dt.hour),
            'index': '<index>',
        }

        return self._replace_placeholders(self.concat_prefix, variables)

    @property
    def feed_type_path(self):
        if self.feed_type_name == 'GdprConsent':
            return 'consent'
        return self.feed_type_name.lower()

    # Returns whether the concatenation job should run on it
    @property
    def has_concat(self):
        return self.destination_type == 'S3' \
            and self.feed_type_name in {'Conversions', 'Impressions', 'Bids', 'Clicks', 'VideoEvents'} \
            and self.concatenation_interval is not None
