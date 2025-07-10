import unittest
from dags.datasrvc.reds.exposurefeed import ExposureFeed

# Dummy test data
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
schema_version = "sv=1-1-0"
date = "2023-12-31"


class TestExposureFeed(unittest.TestCase):

    def test_destination_resolution(self):
        # Test destination data - this is a nested array of destination template, expected prefix, and expected resolved destination
        test_destination_template_data = [
            # Recipient level
            [
                "<bucket>/<recipient>/exposure/brand/<feedtype>/<schemaversion>-<version>/date=<date>/hour=<hour>/<feedtype>_<partnerid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz",
                "recipient1/layer1/layer2/exposure/brand/videoevents/sv=1-1-0-1/date=2023-12-31",
                "recipient1/layer1/layer2/exposure/brand/videoevents/sv=1-1-0-1/date=[0-9]{4}-[0-9]{2}-[0-9]{2}/hour=[0-9]{2}/videoevents_partnerid1_ver_1_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_100_[a-z0-9]{32}.log.gz"
            ],
            # Partner level
            [
                "<bucket>/liveramp/exposure/brand/<partnerid>/<version>/date=<date>/hour=<hour>/videoevents_<partnerid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz",
                "liveramp/exposure/brand/partnerid1/1/date=2023-12-31",
                "liveramp/exposure/brand/partnerid1/1/date=[0-9]{4}-[0-9]{2}-[0-9]{2}/hour=[0-9]{2}/videoevents_partnerid1_ver_1_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_100_[a-z0-9]{32}.log.gz"
            ],
            # Advertiser level
            [
                "<bucket>/liveramp/exposure/crossix/<partnerid>/advertisers/<advertiserid>/<version>/date=<date>/hour=<hour>/<partitionkey>_clicks_<advertiserid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz",
                "liveramp/exposure/crossix/partnerid1/advertisers/advertiserid1/1/date=2023-12-31",
                "liveramp/exposure/crossix/partnerid1/advertisers/advertiserid1/1/date=[0-9]{4}-[0-9]{2}-[0-9]{2}/hour=[0-9]{2}/([a-zA-Z]+|{partitionkey})_clicks_advertiserid1_ver_1_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_100_[a-z0-9]{32}.log.gz"
            ],
            # Campaign level
            [
                "<bucket>/liveramp/exposure/ncs/<partnerid>/advertisers/<advertiserid>/campaigns/<campaignid>/<version>/date=<date>/hour=<hour>/<partitionkey>_imps_<campaignid>_v<version>_<starttime>_<processedtime>_<feedid>_<hash>.log.gz",
                "liveramp/exposure/ncs/partnerid1/advertisers/advertiserid1/campaigns/campaignid1/1/date=2023-12-31",
                "liveramp/exposure/ncs/partnerid1/advertisers/advertiserid1/campaigns/campaignid1/1/date=[0-9]{4}-[0-9]{2}-[0-9]{2}/hour=[0-9]{2}/([a-zA-Z]+|{partitionkey})_imps_campaignid1_v1_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_100_[a-z0-9]{32}.log.gz"
            ]
        ]
        test_data = [[
            feed_id, partner_id, advertiser_id, test_destination[0], destination_type, version, retention_period, start_date, enable_date,
            campaign_id, recipient, feed_type, schema_version
        ] for test_destination in test_destination_template_data]

        for i in range(len(test_data)):
            feed = ExposureFeed(*test_data[i])
            self.assertEqual(feed.populate_destination_date_prefix(date=date), test_destination_template_data[i][1])
            self.assertEqual(feed.populate_destination(), test_destination_template_data[i][2])

    def test_destination_resolution_campaign_id_none(self):
        test_data = [
            feed_id, partner_id, advertiser_id,
            "<bucket>/liveramp/exposure/ncs/<partnerid>/advertisers/<advertiserid>/campaigns/<campaignid>/<version>/date=<date>/hour=<hour>/<partitionkey>_imps_<campaignid>_v<version>_<starttime>_<processedtime>_<feedid>_<hash>.log.gz",
            destination_type, version, retention_period, start_date, enable_date, None, None, None, None
        ]

        for i in range(len(test_data)):
            feed = ExposureFeed(*test_data)
            self.assertEqual(
                feed.populate_destination_date_prefix(date=date),
                "liveramp/exposure/ncs/partnerid1/advertisers/advertiserid1/campaigns//1/date=2023-12-31"
            )
            self.assertEqual(
                feed.populate_destination(),
                "liveramp/exposure/ncs/partnerid1/advertisers/advertiserid1/campaigns//1/date=[0-9]{4}-[0-9]{2}-[0-9]{2}/hour=[0-9]{2}/([a-zA-Z]+|{partitionkey})_imps__v1_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_100_[a-z0-9]{32}.log.gz"
            )

    def test_destination_resolution_bucket(self):
        test_destination_template_data = [
            (
                "<bucket>/liveramp/exposure/brand/<partnerid>/<version>/date=<date>/hour=<hour>/videoevents_<partnerid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz",
                "thetradedesk-useast-partner-datafeed"
            ),
            (
                "thetradedesk-test-bucket/liveramp/exposure/brand/<partnerid>/<version>/date=<date>/hour=<hour>/videoevents_<partnerid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz",
                "thetradedesk-test-bucket"
            ),
        ]
        test_data = [[
            feed_id, partner_id, advertiser_id, test_destination[0], destination_type, version, retention_period, start_date, enable_date,
            campaign_id, recipient, feed_type, None
        ] for test_destination in test_destination_template_data]

        for i in range(len(test_data)):
            feed = ExposureFeed(*test_data[i])
            self.assertEqual(feed.bucket, test_destination_template_data[i][1])

    def test_destination_path_regex(self):
        test_destination_template_data = [
            (
                "<bucket>/liveramp/exposure/brand/<partnerid>/<version>/date=<date>/hour=<hour>/videoevents_<partnerid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz",
                r"thetradedesk-useast-partner-datafeed/liveramp/exposure/brand/partnerid1/1/date=\d{4}-\d{2}-\d{2}/hour=\d{1,2}/videoevents_partnerid1_ver_1_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_100_[a-z0-9]{32}.log.gz"
            ),
            (
                "thetradedesk-test-bucket/liveramp/exposure/brand/<partnerid>/<version>/date=<date>/hour=<hour>/videoevents_<partnerid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz",
                r"thetradedesk-test-bucket/liveramp/exposure/brand/partnerid1/1/date=\d{4}-\d{2}-\d{2}/hour=\d{1,2}/videoevents_partnerid1_ver_1_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_\d{4}-\d{2}-\d{2}T\d{2}\d{2}\d{2}_100_[a-z0-9]{32}.log.gz"
            ),
        ]
        test_data = [[
            feed_id, partner_id, advertiser_id, test_destination[0], destination_type, version, retention_period, start_date, enable_date,
            campaign_id, recipient, feed_type, None
        ] for test_destination in test_destination_template_data]

        for i in range(len(test_data)):
            feed = ExposureFeed(*test_data[i])
            self.assertEqual(feed.destination_path_regex, test_destination_template_data[i][1])

    def test_to_dict_and_from_dict(self):
        test_data = [
            feed_id, partner_id, advertiser_id,
            "<bucket>/<recipient>/exposure/ncs/<partnerid>/advertisers/<advertiserid>/campaigns/<campaignid>/<version>/<feed_type>/date=<date>/hour=<hour>/<feed_type>_<partitionkey>_imps_<campaignid>_v<version>_<starttime>_<processedtime>_<feedid>_<hash>.log.gz",
            destination_type, version, retention_period, start_date, enable_date, None, recipient, feed_type, None
        ]

        feed = ExposureFeed(*test_data)

        feed_dict = feed.to_dict()
        new_feed = ExposureFeed.from_dict(feed_dict)

        assert new_feed == feed
