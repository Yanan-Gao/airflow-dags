import unittest
from dags.datasrvc.reds.feed_utils import get_expired_date, replace_placeholders, split_location_by_date, \
    get_delimiter_from_sql_query, get_retention_start_date, get_latest_date_to_delete, get_dates_to_delete, \
    check_unresolved_placeholders
from datetime import datetime, timedelta


class TestFeedUtils(unittest.TestCase):

    def test_get_retention_start_date(self):
        # Arrange
        retention_period = 99
        start_date = datetime(2021, 12, 30)

        enable_date_before_start_date = datetime(2021, 11, 20)
        enable_date_equal_to_start_date = datetime(2021, 12, 30)
        enable_date_after_start_date = datetime(2022, 2, 15)

        # Act
        retention_start_date = get_retention_start_date(enable_date_before_start_date, start_date, retention_period)
        retention_start_date2 = get_retention_start_date(enable_date_equal_to_start_date, start_date, retention_period)
        retention_start_date3 = get_retention_start_date(enable_date_after_start_date, start_date, retention_period)

        # Assert
        self.assertEqual(start_date + timedelta(days=(retention_period + 1)), retention_start_date)
        self.assertEqual(start_date + timedelta(days=(retention_period + 1)), retention_start_date2)
        self.assertEqual(enable_date_after_start_date + timedelta(days=(retention_period + 1)), retention_start_date3)

    def test_get_latest_date_to_delete(self):
        # Arrange
        retention_period = 99
        current_date = datetime(2024, 6, 3)

        # Act
        latest_date_to_delete = get_latest_date_to_delete(current_date, retention_period)

        # Assert
        self.assertEqual(current_date - timedelta(days=(retention_period + 1)), latest_date_to_delete)

    def test_get_dates_to_delete_not_expired_enable_date_after_start_date(self):
        # Test case where the current date is before retention start date, enable_date > start_date
        # Arrange
        current_date = datetime(2024, 6, 3)
        enable_date = current_date - timedelta(days=98)
        start_date = current_date - timedelta(days=120)
        retention_period = 99

        # Assert
        self.assertIsNone(get_dates_to_delete(current_date, enable_date, start_date, retention_period))
        # print(get_dates_to_delete(current_date, enable_date, start_date, retention_period))

    def test_get_dates_to_delete_not_expired_enable_date_before_start_date(self):
        # Test case where the current date is before retention start date, enable_date < start_date
        # Arrange
        current_date = datetime(2024, 6, 3)
        enable_date = current_date - timedelta(days=120)
        start_date = current_date - timedelta(days=98)
        retention_period = 99

        # Assert
        self.assertIsNone(get_dates_to_delete(current_date, enable_date, start_date, retention_period))
        # print(get_dates_to_delete(current_date, enable_date, start_date, retention_period))

    def test_get_dates_to_delete_expired_retention_start_date(self):
        # Test case where current date is the first day of retention
        # Arrange
        current_date = datetime(2024, 6, 13)
        enable_date = datetime(2024, 3, 5)
        start_date = current_date - timedelta(days=120)
        retention_period = 99

        # Assert
        self.assertEqual(
            get_dates_to_delete(current_date, enable_date, start_date, retention_period),
            (start_date, current_date - timedelta(days=(retention_period + 1)))
        )
        # print(get_dates_to_delete(current_date, enable_date, start_date, retention_period))

    def test_get_dates_to_delete_expired_not_retention_start_date(self):
        # Test case where current date is not the first day of retention
        # Arrange
        current_date = datetime(2024, 6, 3)
        enable_date = current_date - timedelta(days=110)
        start_date = current_date - timedelta(days=120)
        retention_period = 99

        # Assert
        self.assertEqual(
            get_dates_to_delete(current_date, enable_date, start_date, retention_period),
            (current_date - timedelta(days=(retention_period + 1)), current_date - timedelta(days=(retention_period + 1)))
        )
        # print(get_dates_to_delete(current_date, enable_date, start_date, retention_period))

    def test_get_expired_date_not_expired(self):
        # Arrange
        retention_period = 99
        now = datetime(2022, 4, 30, 0, 0, 0)
        start_date = datetime(2021, 12, 30, 0, 0, 0)

        enable_date = datetime(2022, 1, 21, 0, 0, 0)
        enable_date2 = datetime(2022, 2, 15, 0, 0, 0)

        # Act
        expired_date = get_expired_date(now, enable_date, start_date, retention_period)
        expired_date2 = get_expired_date(now, enable_date2, start_date, retention_period)

        # Assert
        self.assertEqual(None, expired_date)
        self.assertEqual(None, expired_date2)

    def test_get_expired_date_expired_backfill_feed(self):
        # Arrange
        retention_period = 99
        now = datetime(2022, 4, 30, 0, 0, 0)
        start_date = datetime(2021, 12, 1, 0, 0, 0)

        enable_date = datetime(2022, 1, 20, 0, 0, 0)
        enable_date2 = datetime(2022, 1, 5, 0, 0, 0)

        # Act
        expired_date = get_expired_date(now, enable_date, start_date, retention_period)
        expired_date2 = get_expired_date(now, enable_date2, start_date, retention_period)  # 15 days expired

        # Assert
        self.assertEqual(2021, expired_date.year)
        self.assertEqual(12, expired_date.month)
        self.assertEqual(1, expired_date.day)

        self.assertEqual(2021, expired_date2.year)
        self.assertEqual(12, expired_date2.month)
        self.assertEqual(16, expired_date2.day)

    def test_get_expired_date_expired_ongoing_feed(self):
        # Arrange
        retention_period = 99
        now = datetime(2022, 4, 30, 0, 0, 0)
        start_date = datetime(2021, 1, 20, 0, 0, 0)
        enable_date = datetime(2022, 1, 20, 0, 0, 0)

        # Act
        expired_date = get_expired_date(now, enable_date, start_date, retention_period)

        # Assert
        self.assertEqual(2021, expired_date.year)
        self.assertEqual(1, expired_date.month)
        self.assertEqual(20, expired_date.day)

    def test_get_expired_date_not_expired_future_feed(self):
        # Arrange
        retention_period = 99
        now = datetime(2022, 4, 30, 0, 0, 0)
        start_date = datetime(2022, 6, 20, 0, 0, 0)
        enable_date = datetime(2022, 1, 15, 0, 0, 0)

        # Act
        expired_date = get_expired_date(now, enable_date, start_date, retention_period)

        # Assert
        self.assertEqual(None, expired_date)

    def test_replace_placeholders(self):
        # Arrange
        templates = [
            ('liveramp/exposure/brand/<partnerId>/<version>/date=<date>', 'liveramp/exposure/brand/ttdpartner/1/date=<date>'),
            (
                'liveramp/exposure/brand/<partnerid>/advertiser/<advertiserid>/<version>/date=<date>',
                'liveramp/exposure/brand/ttdpartner/advertiser//1/date=<date>'
            ),
        ]
        variables = {'feedid': '5', 'partnerid': 'ttdpartner', 'version': '1', 'advertiserid': None}

        # Act && Assert
        for i in range(0, len(templates)):
            populated_template = replace_placeholders(templates[i][0], variables)
            self.assertEqual(populated_template, templates[i][1])

    def test_check_unresolved_placeholders_no_placeholders(self):
        """
        Test that check_unresolved_placeholders passes when no unresolved placeholders are found.
        """
        path = "liveramp/exposure/ncs/partnerid1/advertisers/advertiserid1/campaigns//1/date=[0-9]{4}-[0-9]{2}-[0-9]{2}/hour=[0-9]{2}/([a-zA-Z]+|{partitionkey})_imps__v1_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_100_[a-z0-9]{32}.log.gz"

        try:
            check_unresolved_placeholders(path, raise_on_unresolved=True)
        except ValueError:
            self.fail("check_unresolved_placeholders raised ValueError unexpectedly when no placeholders were present.")

    def test_check_unresolved_placeholders_with_unresolved(self):
        """
        Test that check_unresolved_placeholders raises a ValueError when unresolved placeholders are found.
        """
        path = "<recipient>/example/exposure/<partnerid>/advertisers/advertiserid1"

        with self.assertRaises(ValueError) as context:
            check_unresolved_placeholders(path, raise_on_unresolved=True)

        # Check if the correct placeholder is mentioned in the error message
        self.assertIn("<recipient>", str(context.exception))
        self.assertIn("<partnerid>", str(context.exception))

    def test_check_unresolved_placeholders_with_unresolved_no_raise(self):
        """
        Test that check_unresolved_placeholders passes when unresolved placeholders are found with no raise.
        """
        path = "<recipient>/example/exposure/<partnerid>/advertisers/advertiserid1"

        try:
            check_unresolved_placeholders(path, raise_on_unresolved=False)
        except ValueError:
            self.fail("check_unresolved_placeholders raised ValueError unexpectedly when no raise.")

    def test_check_unresolved_placeholders_with_allowed_placeholders(self):
        """
        Test that check_unresolved_placeholders passes when allowed_placeholders are found.
        """
        path = "example/exposure/<partnerid>/advertisers/advertiserid1/date=<date>"

        try:
            check_unresolved_placeholders(path, ['<partnerid>', '<date>'], True)
        except ValueError:
            self.fail("check_unresolved_placeholders raised ValueError unexpectedly when valid placeholders were present.")

    def test_split_location_by_date(self):
        # Arrange
        template = 'target/impressions/<date>/<processedtime>.csv.gz'
        template_2 = 'liveramp/exposure/brand/<partnerid>/advertisers/<advertiserid>/<version>/date=<date>/hour=<hour>/<partitionkey>_impressions_<advertiserid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz'
        template_3 = 'GroupM/LADDReports/<partnerid>/SiteReport/<partnerid>_CustomSiteReport_{ReportEndDateExclusive:yyyyMMdd}.gz'

        # Act
        date_prefix = split_location_by_date(template)
        date_prefix_2 = split_location_by_date(template_2)
        date_prefix_3 = split_location_by_date(template_3)

        # Assert
        self.assertEqual('target/impressions/<date>', date_prefix[0])
        self.assertEqual('/<processedtime>.csv.gz', date_prefix[1])

        self.assertEqual('liveramp/exposure/brand/<partnerid>/advertisers/<advertiserid>/<version>/date=<date>', date_prefix_2[0])
        self.assertEqual(
            '/hour=<hour>/<partitionkey>_impressions_<advertiserid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz',
            date_prefix_2[1]
        )

    def test_get_delimiter_from_sql_query(self):
        # Arrange
        query1 = '''
        FILE_FORMAT =
        (
            TYPE = CSV
            COMPRESSION = GZIP
            FIELD_OPTIONALLY_ENCLOSED_BY = NONE
            NULL_IF = ('')
            EMPTY_FIELD_AS_NULL = FALSE
            FIELD_DELIMITER = '\t')
        '''

        query2 = '''
        COPY INTO @EXPOSURE_FEEDS_STAGE
        FROM
        (
            [SQLQUERY]
        )
        PARTITION BY (to_varchar(TimeStamp, 'YYYY-MM-DD'))
        FILE_FORMAT =
        (
            TYPE = CSV
            COMPRESSION = GZIP
            FIELD_OPTIONALLY_ENCLOSED_BY = NONE
            NULL_IF = ('')
            EMPTY_FIELD_AS_NULL = FALSE
            FIELD_DELIMITER = ',')
        HEADER = TRUE
        MAX_FILE_SIZE = 256000000;
        '''

        # Act
        delimiter1 = get_delimiter_from_sql_query(query1)
        delimiter2 = get_delimiter_from_sql_query(query2)

        # Assert
        self.assertEqual('\t', delimiter1)
        self.assertEqual(',', delimiter2)

    def test_get_delimiter_from_sql_query_no_match(self):
        with self.assertRaises(ValueError):
            get_delimiter_from_sql_query('random query string')


if __name__ == '__main__':
    unittest.main()
