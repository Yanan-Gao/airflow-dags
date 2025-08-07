import json
from unittest import TestCase

from datetime import datetime, timezone
from dags.datasrvc.reds.feed_backfill import add_back_fill_prefix


class Test(TestCase):

    def test_add_back_fill_prefix(self):
        configs = set()
        # add one
        options = {
            'Prefix': 'publicis_datalake/emea/edhetts/reds/1/date=2021-12-15',
            'SourceBucket': 'thetradedesk-useast-partner-datafeed'
        }
        configs.add(json.dumps(options))

        dt = datetime(2023, 5, 22, 2, 0, 0, tzinfo=timezone.utc)
        add_back_fill_prefix(configs, dt)
        self.assertEqual(1, 1)

        sample_config = '{"Prefix": "qyuwxwr/reds-backfill/impressions/1/date=2022-02-12"'
        filtered_strings = [pre for pre in configs if sample_config in pre]

        self.assertTrue(len(filtered_strings) == 1)
