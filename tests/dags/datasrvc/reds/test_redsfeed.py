from dags.datasrvc.reds.redsfeed import RedsFeed
import unittest
from datetime import datetime, timedelta


class TestRedsFeed(unittest.TestCase):
    feed_id = 123
    status = 'Active'
    destination_type = 'S3'
    destination_location = '<bucket>/<partnerid>/redf5/date=<date>/hour=<hour>/<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz'
    partner_id = '123456ab'
    advertiser_id = '123456cd'
    feed_type_name = 'Impressions'
    version = 1
    concatenation_interval = 60
    has_header = 1
    legacy_column_ordering = 0
    schema = 'abc,def,ghi,'
    unscrubbed_pii_days = 99
    retention_period_in_days = 464
    min_enable_date = datetime(year=2019, month=1, day=1)
    max_disable_date = datetime(year=2020, month=1, day=1)

    row = [
        feed_id, status, destination_type, destination_location, partner_id, advertiser_id, feed_type_name, version, concatenation_interval,
        has_header, legacy_column_ordering, schema, unscrubbed_pii_days, retention_period_in_days, min_enable_date, max_disable_date
    ]
    feed = RedsFeed(*row)

    def test_base_getters(self):
        self.assertEqual(self.feed.feed_id, self.feed_id)
        self.assertEqual(self.feed.status, self.status)
        self.assertEqual(self.feed.destination_type, self.destination_type)
        self.assertEqual(self.feed.destination_location, self.destination_location)
        self.assertEqual(self.feed.partner_id, self.partner_id)
        self.assertEqual(self.feed.advertiser_id, self.advertiser_id)
        self.assertEqual(self.feed.feed_type_name, self.feed_type_name)
        self.assertEqual(self.feed.version, self.version)
        self.assertEqual(self.feed.concatenation_interval, self.concatenation_interval)
        self.assertEqual(self.feed.has_header, self.has_header)
        self.assertEqual(self.feed.legacy_column_ordering, self.legacy_column_ordering)
        self.assertEqual(self.feed.schema, self.schema)

    def test_split_location(self):
        bucket, path, subpath, file = RedsFeed._split_location(self.destination_location)
        self.assertEqual(bucket, '<bucket>')
        self.assertEqual(path, '<partnerid>/redf5')
        self.assertEqual(subpath, 'date=<date>/hour=<hour>')
        self.assertEqual(file, '<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz')

    def test_split_location_subpath(self):
        destination_location = '<bucket>/<partnerid>/redf5/date=<date>/hour=<hour>/<feedtype>/<partnerid>/<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz'
        bucket, path, subpath, file = RedsFeed._split_location(destination_location)
        self.assertEqual(bucket, '<bucket>')
        self.assertEqual(path, '<partnerid>/redf5')
        self.assertEqual(subpath, 'date=<date>/hour=<hour>/<feedtype>/<partnerid>')
        self.assertEqual(file, '<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz')

    def test_bucket_default(self):
        self.assertEqual(self.feed.bucket, 'thetradedesk-useast-partner-datafeed')

    def test_bucket_override(self):
        destination_location = 'thetradedesk-useast-partner-datafeed-test2/<partnerid>/redf5/date=<date>/hour=<hour>/<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz'
        row = [
            self.feed_id, self.status, self.destination_type, destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, self.schema,
            self.unscrubbed_pii_days, self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        self.assertEqual(feed.bucket, 'thetradedesk-useast-partner-datafeed-test2')

    def test_source_path_success(self):
        self.assertEqual(self.feed.source_path, '123456ab/redf5')

    def test_source_path_failure(self):
        destination_location = '<bucket>/<fakeid>/redf5/date=<date>/hour=<hour>/<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz'
        row = [
            self.feed_id, self.status, self.destination_type, destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, self.schema,
            self.unscrubbed_pii_days, self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        with self.assertRaises(KeyError):
            _ = feed.source_path

    def test_destination_path_success(self):
        self.assertEqual(self.feed.destination_path, '123456ab/redf5aggregated')

    def test_destination_path_failure(self):
        destination_location = '<bucket>/<fakeid>/redf5/date=<date>/hour=<hour>/<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz'
        row = [
            self.feed_id, self.status, self.destination_type, destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, self.schema,
            self.unscrubbed_pii_days, self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        with self.assertRaises(KeyError):
            _ = feed.destination_path

    def test_destination_path_replacement(self):
        destination_location = '<bucket>/choozle/redf5/date=<date>/hour=<hour>/<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz'
        row = [
            self.feed_id, self.status, self.destination_type, destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, self.schema,
            self.unscrubbed_pii_days, self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        self.assertEqual(feed.destination_path, 'Choozle/redf5aggregated')

    def test_grouping_regex_success(self):
        self.assertEqual(
            self.feed.grouping_regex,
            '.*date=(<date>)/hour=(<hour>)/(impressions)_123456ab_V5_1_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[a-f0-9]{32}.log.gz'
        )

    def test_grouping_regex_failure(self):
        destination_location = '<bucket>/<partnerid>/redf5/date=<date>/hour=<hour>/<feedtype>_<fakeid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz'
        row = [
            self.feed_id, self.status, self.destination_type, destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, self.schema,
            self.unscrubbed_pii_days, self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        with self.assertRaises(KeyError):
            _ = feed.grouping_regex

    def test_date_hour_grouping_regex(self):
        date_hour_grouping_regex = self.feed.get_date_hour_grouping_regex(datetime(2021, 1, 2, 3, 4, 5))
        self.assertEqual(
            date_hour_grouping_regex,
            '.*date=(2021-01-02)/hour=(3)/(impressions)_123456ab_V5_1_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[a-f0-9]{32}.log.gz'
        )

    def test_concat_prefix_success(self):
        self.assertEqual(self.feed.concat_prefix, '123456ab/redf5aggregated/date=<date>/hour=<hour>/<date><hour>impressions<index>.gz')

    def test_concat_prefix_success_subpath(self):
        destination_location = '<bucket>/<partnerid>/redf5/date=<date>/hour=<hour>/<feedtype>/<partnerid>/<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz'
        row = [
            self.feed_id, self.status, self.destination_type, destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, self.schema,
            self.unscrubbed_pii_days, self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        self.assertEqual(
            feed.concat_prefix,
            '123456ab/redf5aggregated/date=<date>/hour=<hour>/impressions/123456ab/<date><hour>impressionsimpressions<index>.gz'
        )

    def test_concat_prefix_failure(self):
        destination_location = '<bucket>/<partnerid>/redf5/date=<date>/hour=<hour>/<feedtype>/<fakeid>/<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz'
        row = [
            self.feed_id, self.status, self.destination_type, destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, self.schema,
            self.unscrubbed_pii_days, self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        with self.assertRaises(KeyError):
            _ = feed.concat_prefix

    def test_date_hour_concat_prefix(self):
        date_hour_concat_prefix = self.feed.get_date_hour_concat_prefix(datetime(2021, 1, 2, 3, 4, 5))
        self.assertEqual(date_hour_concat_prefix, '123456ab/redf5aggregated/date=2021-01-02/hour=3/2021-01-023impressions<index>.gz')

    def test_has_concat_true(self):
        self.assertTrue(self.feed.has_concat)

    def test_has_concat_false_not_s3(self):
        row = [
            self.feed_id, self.status, 'Snowflake', self.destination_location, self.partner_id, self.advertiser_id, self.feed_type_name,
            self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, self.schema, self.unscrubbed_pii_days,
            self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        self.assertFalse(feed.has_concat)

    def test_has_concat_false_not_concat(self):
        row = [
            self.feed_id, self.status, self.destination_type, self.destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, None, self.has_header, self.legacy_column_ordering, self.schema, self.unscrubbed_pii_days,
            self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        self.assertFalse(feed.has_concat)

    def test_has_concat_false_gdpr_consent_type(self):
        row = [
            self.feed_id, self.status, self.destination_type, self.destination_location, self.partner_id, self.advertiser_id, 'GdprConsent',
            self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, self.schema, self.unscrubbed_pii_days,
            self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        self.assertFalse(feed.has_concat)

    def test_has_concat_true_video_events_type(self):
        row = [
            self.feed_id, self.status, self.destination_type, self.destination_location, self.partner_id, self.advertiser_id, 'VideoEvents',
            self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, self.schema, self.unscrubbed_pii_days,
            self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        self.assertTrue(feed.has_concat)

    def test_schema_with_header(self):
        self.assertEqual(self.feed.schema_list, ['abc', 'def', 'ghi'])

    def test_schema_without_header(self):
        row = [
            self.feed_id, self.status, self.destination_type, self.destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval, 0, self.legacy_column_ordering, self.schema,
            self.unscrubbed_pii_days, self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        self.assertEqual(feed.schema_list, ['abc', 'def', 'ghi'])

    def test_header_present(self):
        self.assertEqual(self.feed.header, ['abc', 'def', 'ghi'])

    def test_header_absent(self):
        row = [
            self.feed_id, self.status, self.destination_type, self.destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval, 0, self.legacy_column_ordering, self.schema,
            self.unscrubbed_pii_days, self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        self.assertIsNone(feed.header)

    def test_header_legacy(self):
        row = [
            self.feed_id, self.status, self.destination_type, self.destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval, self.has_header, 1, self.schema, self.unscrubbed_pii_days,
            self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        self.assertIsNone(feed.header)

    def test_has_scrub_true(self):
        self.assertTrue(self.feed.has_scrub)

    def test_has_scrub_false_not_s3(self):
        row = [
            self.feed_id, self.status, 'Snowflake', self.destination_location, self.partner_id, self.advertiser_id, self.feed_type_name,
            self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, self.schema, self.unscrubbed_pii_days,
            self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        self.assertFalse(feed.has_scrub)

    def test_has_scrub_false_video_event_type(self):
        row = [
            self.feed_id, self.status, self.destination_type, self.destination_location, self.partner_id, self.advertiser_id, 'VideoEvents',
            self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, self.schema, self.unscrubbed_pii_days,
            self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        self.assertFalse(feed.has_scrub)

    def test_GdprConsent_feed_type_name(self):
        row = [
            self.feed_id, self.status, self.destination_type, self.destination_location, self.partner_id, self.advertiser_id, "GdprConsent",
            self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, self.schema, self.unscrubbed_pii_days,
            self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        self.assertEqual(feed.feed_type_path, "consent")
        self.assertEqual(feed.concat_prefix, '123456ab/redf5aggregated/date=<date>/hour=<hour>/<date><hour>consent<index>.gz')

    def test_dsdr_scrubbale_return_true(self):
        row = [
            self.feed_id, self.status, self.destination_type, self.destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, 'abc,tdid',
            self.unscrubbed_pii_days, self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        ids = {f'{feed.partner_id}': {f'{feed.advertiser_id}': [1, 2, 3]}}
        id_cols = ['tdid', 'combinedidentifier', 'originalid', 'deviceadvertisingid']
        self.assertTrue(feed.dsdr_scrubbable(ids, id_cols))

    def test_dsdr_scrubbale_return_false_id_graph_not_match(self):
        row = [
            self.feed_id, self.status, self.destination_type, self.destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, 'abc,tdid',
            self.unscrubbed_pii_days, self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        ids = {"not_exists_partner": {f'{feed.advertiser_id}': [1, 2, 3]}}
        id_cols = ['tdid', 'combinedidentifier', 'originalid', 'deviceadvertisingid']
        self.assertFalse(feed.dsdr_scrubbable(ids, id_cols))

    def test_dsdr_scrubbale_return_false_disabled_102_days_ago(self):
        row = [
            self.feed_id, 'Disabled', self.destination_type, self.destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, 'abc,tdid',
            self.unscrubbed_pii_days, self.retention_period_in_days, self.min_enable_date,
            datetime.now() - timedelta(days=102)
        ]
        feed = RedsFeed(*row)

        ids = {f'{feed.partner_id}': {f'{feed.advertiser_id}': [1, 2, 3]}}
        id_cols = ['tdid', 'combinedidentifier', 'originalid', 'deviceadvertisingid']
        self.assertFalse(feed.dsdr_scrubbable(ids, id_cols))

    def test_dsdr_scrubbale_return_false_has_no_id_col(self):
        row = [
            self.feed_id, self.status, self.destination_type, self.destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, 'abc,tdid',
            self.unscrubbed_pii_days, self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        ids = {f'{feed.partner_id}': {f'{feed.advertiser_id}': [1, 2, 3]}}
        id_cols = ['tdid1', 'combinedidentifier11', 'originalid', 'deviceadvertisingid']
        self.assertFalse(feed.dsdr_scrubbable(ids, id_cols))

    def test_split_location_azure(self):
        row = [
            self.feed_id, self.status, 'Azure',
            'partnerdatafeed/<partnerid>/reds/<feedtype>/<version>/date=<date>/hour=<hour>/<feedtype>_<partnerid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz',
            self.partner_id, self.advertiser_id, self.feed_type_name, self.version, self.concatenation_interval, self.has_header,
            self.legacy_column_ordering, 'abc,tdid', self.unscrubbed_pii_days, self.retention_period_in_days, self.min_enable_date,
            self.max_disable_date
        ]
        feed = RedsFeed(*row)

        self.assertEqual(feed.bucket, 'partnerdatafeed')
        source_path = feed.source_path
        self.assertEqual(source_path, '123456ab/reds/impressions/1')

    def test_normal_destination_path_regex(self):
        destination_location = '<bucket>/<partnerid>/redf5/date=<date>/hour=<hour>/<feedtype>/<partnerid>/<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz'
        row = [
            self.feed_id, self.status, self.destination_type, destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, None, self.has_header, self.legacy_column_ordering, self.schema, self.unscrubbed_pii_days,
            self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)

        self.assertEqual(
            feed.destination_path_regex,
            r'thetradedesk-useast-partner-datafeed/123456ab/redf5/date=\d{4}-\d{2}-\d{2}/hour=\d{1,2}/impressions/123456ab/impressions_123456ab_V5_1_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_[a-f0-9]{32}.log.gz'
        )

    def test_concatenated_destination_path_regex(self):
        destination_location = '<bucket>/<partnerid>/redf5/date=<date>/hour=<hour>/<partnerid>/<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz'
        row = [
            self.feed_id, self.status, self.destination_type, destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval, self.has_header, self.legacy_column_ordering, self.schema,
            self.unscrubbed_pii_days, self.retention_period_in_days, self.min_enable_date, self.max_disable_date
        ]
        feed = RedsFeed(*row)
        self.assertEqual(
            feed.destination_path_regex,
            r'thetradedesk-useast-partner-datafeed/123456ab/redf5aggregated/date=\d{4}-\d{2}-\d{2}/hour=\d{1,2}/123456ab/\d{4}-\d{2}-\d{2}\d{1,2}impressions\d{1}.gz'
        )


if __name__ == '__main__':
    unittest.main()
