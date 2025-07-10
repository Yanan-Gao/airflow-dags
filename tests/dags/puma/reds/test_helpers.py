from dags.puma.reds.helpers.redsfeed import RedsFeed
from dags.puma.reds.helpers.exposurefeed import ExposureFeed
from dags.puma.reds.helpers.trie import FilePathTrie
import unittest
import os
import csv


class TestHelpers(unittest.TestCase):
    feed_id = 123
    status = 'Active'
    destination_type = 'S3'
    destination_location = '<bucket>/<partnerid>/redf5/date=<date>/hour=<hour>/<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz'
    partner_id = '123456ab'
    advertiser_id = '123456cd'
    feed_type_name = 'Impressions'
    version = 1
    concatenation_interval = 60

    row = [
        feed_id, status, destination_type, destination_location, partner_id, advertiser_id, feed_type_name, version, concatenation_interval
    ]
    feed = RedsFeed(*row)

    def test_base_getters_reds(self):
        self.assertEqual(self.feed.feed_id, self.feed_id)
        self.assertEqual(self.feed.status, self.status)
        self.assertEqual(self.feed.destination_type, self.destination_type)
        self.assertEqual(self.feed.destination_location, self.destination_location)
        self.assertEqual(self.feed.partner_id, self.partner_id)
        self.assertEqual(self.feed.advertiser_id, self.advertiser_id)
        self.assertEqual(self.feed.feed_type_name, self.feed_type_name)
        self.assertEqual(self.feed.version, self.version)
        self.assertEqual(self.feed.concatenation_interval, self.concatenation_interval)

    def test_normal_destination_path_regex_reds(self):
        destination_location = '<bucket>/<partnerid>/redf5/date=<date>/hour=<hour>/<feedtype>/<partnerid>/<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz'
        row = [
            self.feed_id, self.status, self.destination_type, destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, None
        ]
        feed = RedsFeed(*row)

        self.assertEqual(
            feed.destination_path_regex,
            r'thetradedesk-useast-partner-datafeed/123456ab/redf5/date=\d{4}-\d{2}-\d{2}/hour=\d{1,2}/impressions/123456ab/impressions_123456ab_V5_1_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_[a-f0-9]{32}.log.gz'
        )

    def test_concatenated_destination_path_regex_reds(self):
        destination_location = '<bucket>/<partnerid>/redf5/date=<date>/hour=<hour>/<partnerid>/<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_<processedtime>_<hash>.log.gz'
        row = [
            self.feed_id, self.status, self.destination_type, destination_location, self.partner_id, self.advertiser_id,
            self.feed_type_name, self.version, self.concatenation_interval
        ]
        feed = RedsFeed(*row)
        self.assertEqual(
            feed.destination_path_regex,
            r'thetradedesk-useast-partner-datafeed/123456ab/redf5aggregated/date=\d{4}-\d{2}-\d{2}/hour=\d{1,2}/123456ab/\d{4}-\d{2}-\d{2}\d{1,2}impressions\d{1}.gz'
        )

    def test_trie(self):
        trie = FilePathTrie()

        # DestinationLocation: <bucket>/<partnerid>/reds/<feedtype>/<version>/date=<date>/hour=<hour>/<feedtype>_<partnerid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz
        trie.insert(
            r'thetradedesk-useast-partner-datafeed/x6rg3gv/reds/videoevents/1/date=\d{4}-\d{2}-\d{2}/hour=\d{1,2}/videoevents_x6rg3gv_ver_1_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_12131_[a-f0-9]{32}.log.gz',
            12131
        )

        search_result = trie.search(
            r'thetradedesk-useast-partner-datafeed/x6rg3gv/reds/videoevents/1/date=2024-02-20/hour=19/videoevents_x6rg3gv_ver_1_2024-02-20T192101_2024-02-20T192238_2024-02-20T192814_12131_c5c779f7a5508a38f1b3492297640f50.log.gz'
        )

        self.assertEqual(12131, search_result)
        self.assertEqual(
            12131,
            trie.search(
                'thetradedesk-useast-partner-datafeed/x6rg3gv/reds/videoevents/1/date=2024-02-20/hour=17/videoevents_x6rg3gv_ver_1_2024-02-20T175356_2024-02-20T175446_2024-02-20T180032_12131_e7ae2a3c6592ad29c9e18dafeefbf128.log.gz'
            )
        )

        self.assertEqual(
            None,
            trie.search(
                'thetradedesk-useast-partner-datafeed/Light Reaction (Xaxis) DE/redf5aggregated/date=2024-05-30/hour=18/2024-05-3018impressions0.gz'
            )
        )
        self.assertEqual(None, trie.search('folder1/folder2/folder3/file.gz'))

    def test_destination_path_regex_exposure(self):
        feed_id = 100
        partner_id = "partnerid1"
        advertiser_id = "advertiserid1"
        destination_type = "s3"
        version = 1
        retention_period = 99
        start_date = "2023-01-01"
        enable_date = "2023-01-31"
        campaign_id = "campaignid1"
        recipient = "recipient1/layer1/layer2"
        feed_type = "videoevents"
        test_destination_template_data = [
            (
                "<bucket>/<recipient>/brand/<partnerid>/<version>/date=<date>/hour=<hour>/<feedtype>_<partnerid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz",
                r"thetradedesk-useast-partner-datafeed/recipient1/layer1/layer2/brand/partnerid1/1/date=\d{4}-\d{2}-\d{2}/hour=\d{1,2}/videoevents_partnerid1_ver_1_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_100_[a-z0-9]{32}.log.gz"
            ),
            (
                "thetradedesk-test-bucket/liveramp/exposure/brand/<partnerid>/<version>/date=<date>/hour=<hour>/videoevents_<partnerid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz",
                r"thetradedesk-test-bucket/liveramp/exposure/brand/partnerid1/1/date=\d{4}-\d{2}-\d{2}/hour=\d{1,2}/videoevents_partnerid1_ver_1_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_100_[a-z0-9]{32}.log.gz"
            ),
        ]
        test_data = [[
            feed_id, partner_id, advertiser_id, test_destination[0], destination_type, version, retention_period, start_date, enable_date,
            campaign_id, recipient, feed_type
        ] for test_destination in test_destination_template_data]

        for i in range(len(test_data)):
            feed = ExposureFeed(*test_data[i])
            self.assertEqual(feed.destination_path_regex, test_destination_template_data[i][1])

    def test_all(self):
        csv_file_path_reds = os.path.join('tests/dags/puma/reds/helpers', 'reds.feed.csv')
        csv_file_path_exposure = os.path.join('tests/dags/puma/reds/helpers', 'exposure.feed.csv')
        sumo_file_path_reds = os.path.join('tests/dags/puma/reds/helpers', 'search-results-07-01_with_feed_ids.csv')

        trie = FilePathTrie()

        # Open the reds CSV file and read it
        with open(csv_file_path_reds, mode='r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)

            for row in csv_reader:
                # Replace all instances of NULL with Python's None
                processed_row = {key: (None if value == 'NULL' else value) for key, value in row.items()}
                feed = RedsFeed(*processed_row.values())  # Process each row
                trie.insert(feed.destination_path_regex, feed.feed_id)

        # Open the exposure CSV file and read it
        with open(csv_file_path_exposure, mode='r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)

            for row in csv_reader:
                # Replace all instances of NULL with Python's None
                processed_row = {key: (None if value == 'NULL' else value) for key, value in row.items()}
                feed = ExposureFeed(*processed_row.values())  # Process each row
                trie.insert(feed.destination_path_regex, feed.feed_id)

        # Open the sumo CSV file and read it
        with open(sumo_file_path_reds, mode='r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)

            # Iterate through the rows and compare solution feed_id with trie search result feed_id
            for row in csv_reader:
                parsed_arn = row['parsed_arn']
                feed_id = trie.search(parsed_arn)
                self.assertEqual(feed_id, row['feed_id'])
