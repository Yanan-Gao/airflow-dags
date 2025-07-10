from dataclasses import dataclass


@dataclass
class Config:
    regions: dict[str, list[str]]
    domain_to_dc: dict[str, str]
    push_metrics: bool = True
    dry_run: bool = False  # set to True to not update NS1
    create_new_domain_if_missing: bool = False
    time_buffer_in_seconds: float = 60.0
    lookback_interval_in_seconds: float = 300.0
    lookback_distance_in_seconds: float = 3000.0
    minimum_lookback: int = 5
    minimum_weight: int = 1
    maximum_datacenter_weight_ratio: float = 0.75
    throttle: float = 0.25  # do not allow weights to move more than this away from some moving average
    prometheus_endpoint: str = "https://metric-query.gen.adsrvr.org/prometheus"
    mqp_user: str = "query_global_weight"
    mqp_password_name: str = "mqp_query_global_weight"
    ns1_endpoint: str = 'https://api.nsone.net'
    ns1_api_key_name: str = 'ns1-api-key-global-adsrvr-org'


production_config = Config(
    regions={
        'usw': ['ca2', 'wa2'],
        'use': ['ny1', 'va6', 'vad', 'vae', 'vam'],
        'eu': ['de2', 'ie1'],
        'asiapac': ['jp1', 'sg2'],  # TODO: split jp1 and sg2 into different regions
    },
    domain_to_dc={
        'usw-ca2.adsrvr.org.': 'ca2',
        'wa2-bid.adsrvr.org.': 'wa2',
        'ny1-bid.adsrvr.org.': 'ny1',
        'va6-bid.adsrvr.org.': 'va6',
        'vad-bid.adsrvr.org.': 'vad',
        'vae-bid.adsrvr.org.': 'vae',
        'vam-bid.adsrvr.org.': 'vam',
        'de2-bid.adsrvr.org.': 'de2',
        'ie1-bid.adsrvr.org.': 'ie1',
        'sg2-bid.adsrvr.org.': 'sg2',
        'jp1-bid.adsrvr.org.': 'jp1',
    },
)
