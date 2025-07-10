import copy
import unittest
from datetime import datetime
from typing import Dict, List
from unittest.mock import patch

from dags.datasrvc.reds import reds_list_iam_users
from dags.datasrvc.reds.exposurefeed import ExposureFeed
from dags.datasrvc.reds.reds_list_iam_users import dag
from dags.datasrvc.reds.redsfeed import RedsFeed

_TEST_EXPOSURE_FEED = ExposureFeed(
    1,  # feed_id
    'partner1',  # partner_id
    'advertiser1',  # advertiser_id
    '<bucket>/liveramp/exposure/brand/<partnerid>/advertisers/<advertiserid>/<version>/date=<date>/hour=<hour'
    '>/<partitionkey>_clicks_ver_<version>_<starttime>_<endtime>_<processedtime>_<hash>.log.gz',  # feed_destination
    'S3',  # feed_destination_type_name
    1,  # version
    99,  # retention_period
    '2022-04-25',  # start_date
    '2022-04-26',  # enable_date
    'campaign1',  # campaign_id
    'recipient1',  # recipient
    'videoevents',  # feed_type
    'sv=1-1-0'  # schema_version
)

_TEST_REDS_FEED = RedsFeed(
    1,  # feed_id
    'Active',  # status
    'S3',  # destination_type
    '<bucket>/<partnerid>/redf5/date=<date>/hour=<hour>/<feedtype>_<partnerid>_V5_1_<starttime>_<endtime>_'
    '<processedtime>_<hash>.log.gz',  # destination_location
    'partner1',  # partner_id
    'advertiser1',  # advertiser_id
    'Impressions',  # feed_type_name
    1,  # version
    60,  # concatenation_interval
    1,  # has_header
    0,  # legacy_column_ordering
    'abc,def,ghi,',  # schema
    99,  # unscrubbed_pii_days
    464,  # retention_period_in_days
    datetime(year=2019, month=1, day=1),  # min_enable_date
    datetime(year=2020, month=1, day=1),  # max_disable_date
)


class TestRedsListIamUsers(unittest.TestCase):

    def test_dag_structure(self):
        expected: Dict[str, List[str]] = {
            'list_exposure_prefixes': ['find_users_in_003576902480', 'find_users_in_949199315127'],
            'list_reds_prefixes': ['find_users_in_003576902480', 'find_users_in_949199315127'],
            'persist_users': [],
            'find_users_in_003576902480': ['persist_users'],
            'find_users_in_949199315127': ['persist_users'],
        }
        actual = dag.airflow_dag.task_dict
        self.assertEqual(expected.keys(), actual.keys())
        for task_id, downstream in expected.items():
            task = dag.airflow_dag.get_task(task_id)
            self.assertEqual(set(downstream), task.downstream_task_ids)

    def test_list_exposure_prefixes(self):

        def _exposure_feeds_for_test():
            feed = copy.deepcopy(_TEST_EXPOSURE_FEED)
            feeds = [feed]

            feed = copy.deepcopy(_TEST_EXPOSURE_FEED)
            feed.feed_id = 2
            feed.partner_id = 'partner2'
            feed.advertiser_id = 'advertiser2'
            feeds.append(feed)

            feed = copy.deepcopy(_TEST_EXPOSURE_FEED)
            feed.feed_id = 3
            feed.partner_id = 'partner3'
            feed.advertiser_id = 'advertiser3'
            feed.feed_destination_type_name = 'NOT-S3'
            feeds.append(feed)

            feed = copy.deepcopy(_TEST_EXPOSURE_FEED)
            feed.feed_id = 4
            feed.feed_destination = None
            return feeds

        with patch.object(reds_list_iam_users, '_get_exposure_feeds', new=_exposure_feeds_for_test):
            actual = reds_list_iam_users.list_exposure_prefixes()
            self.assertSetEqual(
                set(actual), {
                    'liveramp/exposure/brand/partner1/advertisers/advertiser1/1/date=<date>',
                    'liveramp/exposure/brand/partner2/advertisers/advertiser2/1/date=<date>'
                }
            )

    def test_list_reds_prefixes(self):

        def _reds_feeds_for_test():
            return [
                _TEST_REDS_FEED._replace(feed_id=1),
                _TEST_REDS_FEED._replace(feed_id=2, partner_id='partner2'),
                _TEST_REDS_FEED._replace(feed_id=3, partner_id='partner3', destination_type='NOT-S3'),
                _TEST_REDS_FEED._replace(feed_id=4, partner_id='partner4', destination_location=None),
            ]

        with patch.object(reds_list_iam_users, '_get_reds_feeds', new=_reds_feeds_for_test):
            actual = reds_list_iam_users.list_reds_prefixes()

            # Feed 3 is filtered out because it is not delivered to S3
            # Feed 4 is filtered out because it does not have destination location set
            expected = {
                'partner1/redf5',
                'partner1/redf5aggregated',
                'partner2/redf5',
                'partner2/redf5aggregated',
            }
            self.assertTrue(expected.issubset(set(actual)))

            # Check we backfilled
            self.assertIn('qyuwxwr/reds-backfill/clicks/1', actual)

    def test_is_internal_user(self):
        self.assertTrue(reds_list_iam_users._is_internal_user('airflow', []))
        self.assertFalse(reds_list_iam_users._is_internal_user('potentially-reds-user', []))
        self.assertTrue(reds_list_iam_users._is_internal_user('potentially-reds-user', ['Oncall']))

    def test_is_get_action(self):
        self.assertTrue(reds_list_iam_users._is_get_action('*'))
        self.assertTrue(reds_list_iam_users._is_get_action('**'))
        self.assertTrue(reds_list_iam_users._is_get_action('s3:*'))
        self.assertTrue(reds_list_iam_users._is_get_action('s3:Get*'))
        self.assertTrue(reds_list_iam_users._is_get_action('s3:GetObject'))
        self.assertTrue(reds_list_iam_users._is_get_action('s3:GetObjectAcl'))
        self.assertTrue(reds_list_iam_users._is_get_action('s3:GetObject*'))
        self.assertTrue(reds_list_iam_users._is_get_action, 's3:Get?*')
        self.assertTrue(reds_list_iam_users._is_get_action, 's3:*Object')
        self.assertTrue(reds_list_iam_users._is_get_action, 's3:*Object*')

        self.assertFalse(reds_list_iam_users._is_get_action(None))
        self.assertFalse(reds_list_iam_users._is_get_action(''))
        self.assertFalse(reds_list_iam_users._is_get_action('s3:List*'))
        self.assertFalse(reds_list_iam_users._is_get_action('s3:GetObject?'))
        self.assertFalse(reds_list_iam_users._is_get_action('ec2:*'))
        self.assertFalse(reds_list_iam_users._is_get_action('ec2:Get*'))
        self.assertFalse(reds_list_iam_users._is_get_action('ec2:GetObject'))
        self.assertFalse(reds_list_iam_users._is_get_action('ec2:GetObject*'))
        self.assertFalse(reds_list_iam_users._is_get_action('ec2:GetObjectFoo'))

    def test_is_allow_get_action(self):
        self.assertFalse(reds_list_iam_users._is_allow_get_action(None, None))
        statement: Dict[str, str] = {}
        self.assertFalse(reds_list_iam_users._is_allow_get_action(None, statement))
        statement['Effect'] = 'Deny'
        self.assertFalse(reds_list_iam_users._is_allow_get_action(None, statement))
        statement['Effect'] = 'Allow'
        self.assertFalse(reds_list_iam_users._is_allow_get_action(None, statement))
        statement['Action'] = 's3:List*'
        self.assertFalse(reds_list_iam_users._is_allow_get_action(None, statement))
        statement['Action'] = 's3:Get*'
        self.assertTrue(reds_list_iam_users._is_allow_get_action(None, statement))

    def test_is_resource_in_prefixes(self):
        bucket = 'arn:aws:s3:::thetradedesk-useast-partner-datafeed'
        prefixes = [f'{bucket}/partner/redf5/', f'{bucket}/partner/1/date=<date>/']
        self.assertFalse(reds_list_iam_users._is_resource_in_prefixes(None, prefixes))
        self.assertFalse(reds_list_iam_users._is_resource_in_prefixes('', prefixes))
        self.assertTrue(reds_list_iam_users._is_resource_in_prefixes('*', prefixes))
        self.assertTrue(reds_list_iam_users._is_resource_in_prefixes('*/', prefixes))
        self.assertTrue(reds_list_iam_users._is_resource_in_prefixes('arn:aws:s3:::*', prefixes))
        self.assertTrue(reds_list_iam_users._is_resource_in_prefixes('arn:aws:s3:::*/*', prefixes))
        self.assertTrue(reds_list_iam_users._is_resource_in_prefixes('arn:aws:s3:::*/partner*/*', prefixes))

        self.assertFalse(reds_list_iam_users._is_resource_in_prefixes(bucket, prefixes))
        self.assertTrue(reds_list_iam_users._is_resource_in_prefixes(f'{bucket}*', prefixes))
        self.assertTrue(reds_list_iam_users._is_resource_in_prefixes(f'{bucket}/*', prefixes))
        self.assertFalse(reds_list_iam_users._is_resource_in_prefixes(f'wrong{bucket}/*', prefixes))

        self.assertFalse(reds_list_iam_users._is_resource_in_prefixes(f'{bucket}/foo/reds/*', prefixes))
        self.assertFalse(reds_list_iam_users._is_resource_in_prefixes(f'{bucket}/foo/redf5aggregated/', prefixes))

        self.assertFalse(reds_list_iam_users._is_resource_in_prefixes(f'{bucket}/partner/1', prefixes))
        self.assertFalse(reds_list_iam_users._is_resource_in_prefixes(f'{bucket}/partner/1/', prefixes))
        self.assertTrue(reds_list_iam_users._is_resource_in_prefixes(f'{bucket}/partner/1/*', prefixes))
        self.assertTrue(reds_list_iam_users._is_resource_in_prefixes(f'{bucket}/partner/?/*', prefixes))
        self.assertFalse(reds_list_iam_users._is_resource_in_prefixes(f'{bucket}/partner/2/*', prefixes))


if __name__ == '__main__':
    unittest.main()
