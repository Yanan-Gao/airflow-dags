"""
There are some S3 back-filled reds which are not configured in reds.Feed table. And their prefixes are not patterned.
Thus, we use this util method to add those back-fill prefix in hard code before we find a better way.
Below back-fill path was search result on 2023-05-22, by tests/dags/jobs/airflow_jobs/reds/utils/search_backfill_reds.py
Ideally it should be static and won't increase frequently

thetradedesk-useast-partner-datafeed/6a3f5c9/backfill/  -- skip, it's not dated, can be deleted manually after confirm with product

thetradedesk-useast-partner-datafeedmondelez/0afm8ze/reds-backfill/{reds_type}/1/date=2023-03-08 -- videoevents only
thetradedesk-useast-partner-datafeedmondelez/2skita8/reds-backfill/{reds_type}/1/date=2022-01-30 -- clicks,conversions,impressions,videoevents
thetradedesk-useast-partner-datafeedmondelez/7wsorfs/reds-backfill/{reds_type}/1/date=2022-04-08 -- clicks,impressions,videoevents
thetradedesk-useast-partner-datafeedmondelez/hizgw4x/reds-backfill/{reds_type}/1/date=2022-07-11 -- clicks,conversions,impressions,videoevents
thetradedesk-useast-partner-datafeedmondelez/kepgwv2/advertisers/114tm8e/reds-backfill/{reds_type}/1/date=2022-01-26 -- clicks,conversions,impressions,videoevents

thetradedesk-useast-partner-datafeed/octane11/backfill/tdpartnerid/advertisers/ru6qq3d/1/date=2022-07-17
thetradedesk-useast-partner-datafeed/qyuwxwr/reds-backfill/{reds_type}/1/date=2023-04-09 -- clicks,conversions,impressions,videoevents

"""
import json
from datetime import timedelta
from dags.datasrvc.utils.common import is_prod

testing = not is_prod()


def get_bucket():
    return 'thetradedesk-useast-partner-datafeed-test2' if testing else 'thetradedesk-useast-partner-datafeed'


default_expire = 464

# hierarchy bucket -> prefix pattern -> version -> reds types
back_fill_patterns = {
    get_bucket(): {
        'mondelez/0afm8ze/reds-backfill/{reds_type}/{version}': {
            '1': ['videoevents']
        },
        'mondelez/2skita8/reds-backfill/{reds_type}/{version}': {
            '1': ['clicks', 'conversions', 'impressions', 'videoevents']
        },
        'mondelez/7wsorfs/reds-backfill/{reds_type}/{version}': {
            '1': ['clicks', 'impressions', 'videoevents']
        },
        'mondelez/hizgw4x/reds-backfill/{reds_type}/{version}': {
            '1': ['clicks', 'conversions', 'impressions', 'videoevents']
        },
        'mondelez/kepgwv2/advertisers/114tm8e/reds-backfill/{reds_type}/{version}': {
            '1': ['clicks', 'conversions', 'impressions', 'videoevents']
        },
        'octane11/backfill/tdpartnerid/advertisers/ru6qq3d/{version}': {
            '1': ['']
        },
        'qyuwxwr/reds-backfill/{reds_type}/{version}': {
            '1': ['clicks', 'conversions', 'impressions', 'videoevents']
        },
    }
}


def get_back_fill_prefixes():
    for bucket in back_fill_patterns:
        for prefix_pattern in back_fill_patterns[bucket]:
            for version in back_fill_patterns[bucket][prefix_pattern]:
                for reds_type in back_fill_patterns[bucket][prefix_pattern][version]:
                    yield prefix_pattern.replace('{reds_type}', reds_type).replace('{version}', version)


def add_back_fill_prefix(configs: set, dt_slot):
    for prefix in get_back_fill_prefixes():
        expired_date_string = (dt_slot - timedelta(days=default_expire)).strftime('%Y-%m-%d')
        expired_prefix = f'{prefix}/date={expired_date_string}'
        # add the feed bucket
        options = {'Prefix': expired_prefix, 'SourceBucket': get_bucket()}
        configs.add(json.dumps(options))
