import unittest
from typing import List, Any

from dags.datasrvc.reds.exposurefeed_dsdr import ExposureFeedDsdr, Delimiter

# Dummy test data
feed_id = 100
feed_status = 'Active'
partner_id = 'partnerid1'
advertiser_id = 'advertiserid1'
campaign_id = 'campaignid1'
feed_destination = '<bucket>/liveramp/exposure/brand/<partnerid>/<version>/date=<date>/hour=<hour>/videoevents_<partnerid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz'
destination_type = 's3'
version = 1
retention_period = 99
start_date = '2023-01-01'
enable_date = '2023-01-31'
last_change_date = '2024-01-31'
export_sql = '''
COPY
INTO @ EXPOSURE_FEEDS_STAGE
FROM([SQLQUERY])
PARTITION
BY([DATEPARTITION])
FILE_FORMAT = (TYPE = CSV COMPRESSION = GZIP FIELD_OPTIONALLY_ENCLOSED_BY = NONE NULL_IF = ('')
EMPTY_FIELD_AS_NULL = FALSE
FIELD_DELIMITER = '\t'        ) HEADER = TRUE
MAX_FILE_SIZE = 256000000;
'''
recipient = "recipient1/layer1/layer2"
feed_type = "videoevents"
schema_version = "sv=1-1-0"

user_id_columns = ['tdid', 'combinedidentifier', 'originalid', 'deviceadvertisingid']


class TestExposureFeed(unittest.TestCase):

    def test_destination_resolution(self):
        # Test destination data - nested array of destination template, expected bucket, expected prefix,
        # and expected include
        test_destination_template_data = [
            # Recipient level
            [
                '<bucket>/<recipient>/exposure/brand/<feedtype>/<partnerid>/<version>/date=<date>/hour=<hour>/<feedtype>_<partnerid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz',
                'thetradedesk-useast-partner-datafeed', 'recipient1/layer1/layer2/exposure/brand/videoevents/partnerid1/1/',
                '.*/hour=[0-9]{2}/videoevents_partnerid1_ver_1_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_100_[a-z0-9]{32}.log.gz'
            ],
            # Partner level
            [
                '<bucket>/liveramp/exposure/brand/<partnerid>/<version>/date=<date>/hour=<hour>/videoevents_<partnerid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz',
                'thetradedesk-useast-partner-datafeed', 'liveramp/exposure/brand/partnerid1/1/',
                '.*/hour=[0-9]{2}/videoevents_partnerid1_ver_1_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_100_[a-z0-9]{32}.log.gz'
            ],
            # Advertiser level
            [
                '<bucket>/liveramp/exposure/crossix/<partnerid>/advertisers/<advertiserid>/<version>/date=<date>/hour=<hour>/<partitionkey>_clicks_<advertiserid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz',
                'thetradedesk-useast-partner-datafeed', 'liveramp/exposure/crossix/partnerid1/advertisers/advertiserid1/1/',
                r'.*/hour=[0-9]{2}/([a-zA-Z]+|\{partitionkey\})_clicks_advertiserid1_ver_1_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_100_[a-z0-9]{32}.log.gz'
            ],
            # Campaign level
            [
                '<bucket>/liveramp/exposure/ncs/<partnerid>/advertisers/<advertiserid>/campaigns/<campaignid>/<version>/date=<date>/hour=<hour>/<partitionkey>_<date>_imps_<campaignid>_v<version>_<starttime>_<processedtime>_<feedid>_<hash>.log.gz',
                'thetradedesk-useast-partner-datafeed',
                'liveramp/exposure/ncs/partnerid1/advertisers/advertiserid1/campaigns/campaignid1/1/',
                r'.*/hour=[0-9]{2}/([a-zA-Z]+|\{partitionkey\})_[0-9]{4}-[0-9]{2}-[0-9]{2}_imps_campaignid1_v1_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_100_[a-z0-9]{32}.log.gz'
            ],
            [
                'thetradedesk-useast-data-import/target/impressions_open/<date>/<feedid>_<processedtime>.csv.gz',
                'thetradedesk-useast-data-import', 'target/impressions_open/', '.*/100_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}.csv.gz'
            ],
            [
                'thetradedesk-useast-data-import/target/impressions_open/<date>/<date>_<feedid>_<processedtime>.csv.gz',
                'thetradedesk-useast-data-import', 'target/impressions_open/',
                '.*/[0-9]{4}-[0-9]{2}-[0-9]{2}_100_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}.csv.gz'
            ]
        ]
        test_data = [[
            feed_id, feed_status, partner_id, advertiser_id, campaign_id, test_destination[0], destination_type, version, retention_period,
            start_date, enable_date, last_change_date, export_sql, recipient, feed_type, schema_version
        ] for test_destination in test_destination_template_data]

        for i in range(len(test_data)):
            feed = ExposureFeedDsdr(*test_data[i])
            self.assertEqual(feed.delimiter, Delimiter.TAB)
            self.assertEqual(feed.bucket, test_destination_template_data[i][1])
            self.assertEqual(feed.prefix_before_date, test_destination_template_data[i][2])
            self.assertEqual(feed.include, test_destination_template_data[i][3])

    def test_feed_scrubbable(self):
        # Test scrubbable data - partner id, advertiser id, feed columns, id graph, expected scrubbable

        test_scrubbable_template_data: List[List[Any]] = [
            # case 1: partner id is None
            [
                None, None, ['LogEntryTime', 'TDID', 'Longtitude'], {
                    'partnerid1': {
                        'advertiserid1': {
                            'tdid': ['tdid1', 'tdid2']
                        },
                        'advertiserid2': {
                            'tdid': ['tdid3', 'tdid4']
                        }
                    }
                }, True
            ],
            # Case 2: partner id is provided, advertiser id is None
            [
                'partnerid1', None, ['LogEntryTime', 'TDID', 'Longtitude'], {
                    'partnerid1': {
                        'advertiserid1': {
                            'tdid': ['tdid1', 'tdid2']
                        },
                        'advertiserid2': {
                            'tdid': ['tdid3', 'tdid4']
                        }
                    }
                }, True
            ],
            # Case 3: partner id and single advertiser id is provided
            [
                'partnerId1', 'advertiserId2', ['LogEntryTime', 'TDID', 'Longtitude'], {
                    'partnerid1': {
                        'advertiserid1': {
                            'tdid': ['tdid1', 'tdid2']
                        },
                        'advertiserid2': {
                            'tdid': ['tdid3', 'tdid4']
                        }
                    }
                }, True
            ],
            # Case 4: partner id and multiple advertiser ids are provided
            [
                'partnerId1', 'advertiserId2,advertiserId3,advertiserId4', ['LogEntryTime', 'TDID', 'Longtitude'], {
                    'partnerid1': {
                        'advertiserid1': {
                            'tdid': ['tdid1', 'tdid2']
                        },
                        'advertiserid2': {
                            'tdid': ['tdid3', 'tdid4']
                        }
                    }
                }, True
            ],
            # Case 5: partner id not in id graph
            [
                'partnerId2', 'advertiserId1', ['LogEntryTime', 'TDID', 'Longtitude'], {
                    'partnerid1': {
                        'advertiserid1': {
                            'tdid': ['tdid1', 'tdid2']
                        },
                        'advertiserid2': {
                            'tdid': ['tdid3', 'tdid4']
                        }
                    }
                }, False
            ],
            # Case 6: advertiser id not in id graph
            [
                'partnerId1', 'advertiserId3', ['LogEntryTime', 'TDID', 'Longtitude'], {
                    'partnerid1': {
                        'advertiserid1': {
                            'tdid': ['tdid1', 'tdid2']
                        },
                        'advertiserid2': {
                            'tdid': ['tdid3', 'tdid4']
                        }
                    }
                }, False
            ],
            # Case 7: advertiser ids not in id graph
            [
                'partnerId1', 'advertiserId3,advertiserId4,advertiserId5', ['LogEntryTime', 'TDID', 'Longtitude'], {
                    'partnerid1': {
                        'advertiserid1': {
                            'tdid': ['tdid1', 'tdid2']
                        },
                        'advertiserid2': {
                            'tdid': ['tdid3', 'tdid4']
                        }
                    }
                }, False
            ],
            # Case 8: empty columns
            [
                'partnerId1', 'advertiserId1', [], {
                    'partnerid1': {
                        'advertiserid1': {
                            'tdid': ['tdid1', 'tdid2']
                        },
                        'advertiserid2': {
                            'tdid': ['tdid3', 'tdid4']
                        }
                    }
                }, False
            ],
            # Case 9: no userid in columns
            [
                'partnerId1', 'advertiserId1,advertiserId3', ['LogEntryTime', 'Longtitude'], {
                    'partnerid1': {
                        'advertiserid1': {
                            'tdid': ['tdid1', 'tdid2']
                        },
                        'advertiserid2': {
                            'tdid': ['tdid3', 'tdid4']
                        }
                    }
                }, False
            ],
            # Case 10: no userid in columns
            [
                None, None, ['LogEntryTime', 'Longtitude'], {
                    'partnerid1': {
                        'advertiserid1': {
                            'tdid': ['tdid1', 'tdid2']
                        },
                        'advertiserid2': {
                            'tdid': ['tdid3', 'tdid4']
                        }
                    }
                }, False
            ]
        ]

        test_data = [[
            feed_id, feed_status, test_scrubbable[0], test_scrubbable[1], campaign_id, feed_destination, destination_type, version,
            retention_period, start_date, enable_date, last_change_date, export_sql, recipient, feed_type, schema_version
        ] for test_scrubbable in test_scrubbable_template_data]

        for i in range(len(test_data)):
            feed = ExposureFeedDsdr(*test_data[i])
            feed.set_columns(test_scrubbable_template_data[i][2])
            is_scrubbable = feed.dsdr_scrubbable(id_graph=test_scrubbable_template_data[i][3], user_id_columns=user_id_columns)
            self.assertEqual(test_scrubbable_template_data[i][4], is_scrubbable)

    def test_to_dsdr_work_item(self):
        # Test to dsdr work item data - export_sql, override bucket, expected json string
        test_to_dsdr_work_item_data: List[List[Any]] = [
            # without override bucket
            [
                export_sql, None,
                '{"FeedId": 100, "FeedVersion": 1, "FeedType": "exfeed", "Prefix": "liveramp/exposure/brand/partnerid1/1/", "PartnerId": "partnerid1", "AdvertiserIds": ["advertiserid1"], "Include": ".*/hour=[0-9]{2}/videoevents_partnerid1_ver_1_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_100_[a-z0-9]{32}.log.gz", "ParentBucket": "thetradedesk-useast-partner-datafeed", "DestinationBucket": "thetradedesk-useast-partner-datafeed", "Delimiter": "TAB", "Schema": ["LogEntryTime", "TDID", "Longtitude"]}'
            ],
            [
                export_sql.replace('\t', ','), 'override_bucket_name',
                '{"FeedId": 100, "FeedVersion": 1, "FeedType": "exfeed", "Prefix": "liveramp/exposure/brand/partnerid1/1/", "PartnerId": "partnerid1", "AdvertiserIds": ["advertiserid1"], "Include": ".*/hour=[0-9]{2}/videoevents_partnerid1_ver_1_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_100_[a-z0-9]{32}.log.gz", "ParentBucket": "override_bucket_name", "DestinationBucket": "override_bucket_name", "Delimiter": "COMMA", "Schema": ["LogEntryTime", "TDID", "Longtitude"]}'
            ],
            [
                export_sql.replace('\t', '\\t'), 'override_bucket_name',
                '{"FeedId": 100, "FeedVersion": 1, "FeedType": "exfeed", "Prefix": "liveramp/exposure/brand/partnerid1/1/", "PartnerId": "partnerid1", "AdvertiserIds": ["advertiserid1"], "Include": ".*/hour=[0-9]{2}/videoevents_partnerid1_ver_1_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}_100_[a-z0-9]{32}.log.gz", "ParentBucket": "override_bucket_name", "DestinationBucket": "override_bucket_name", "Delimiter": "TAB", "Schema": ["LogEntryTime", "TDID", "Longtitude"]}'
            ]
        ]
        test_data = [[
            feed_id, feed_status, partner_id, advertiser_id, campaign_id, feed_destination, destination_type, version, retention_period,
            start_date, enable_date, last_change_date, template[0], recipient, feed_type, schema_version
        ] for template in test_to_dsdr_work_item_data]

        for i in range(len(test_data)):
            feed = ExposureFeedDsdr(*test_data[i])
            feed.set_columns(['LogEntryTime', 'TDID', 'Longtitude'])
            work_item_json = feed.to_dsdr_work_item(test_to_dsdr_work_item_data[i][1])
            self.assertEqual(test_to_dsdr_work_item_data[i][2], work_item_json)

    def test_to_dsdr_work_item_throw_exception(self):
        # Test to dsdr work item data - export_sql, override bucket
        test_to_dsdr_work_item_data = [[export_sql.replace('\t', ';'), 'override_bucket_name']]
        test_data = [[
            feed_id, feed_status, partner_id, advertiser_id, campaign_id, feed_destination, destination_type, version, retention_period,
            start_date, enable_date, last_change_date, template[0], recipient, feed_type, schema_version
        ] for template in test_to_dsdr_work_item_data]

        for i in range(len(test_data)):
            self.assertRaises(ValueError, ExposureFeedDsdr, *test_data[i])
