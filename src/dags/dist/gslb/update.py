import time
import logging

from ttd.metrics.opentelemetry.ttdopentelemetry import get_or_register_gauge, push_all, TtdGauge
from airflow.models import Variable

from dags.dist.gslb import ns1, prometheus
from dags.dist.gslb.config import Config, production_config

default_record_json = """
{
    "answers":[
        {"answer":["de2-bid.adsrvr.org."],"id":"67e5d6df3fbf8900019df265","meta":{"weight":100},"region":"eur"},
        {"answer":["ie1-bid.adsrvr.org."],"id":"67e5d6df3fbf8900019df266","meta":{"weight":100},"region":"eur"},
        {"answer":["ny1-bid.adsrvr.org."],"id":"67e5d6df3fbf8900019df267","meta":{"weight":100},"region":"use"},
        {"answer":["va6-bid.adsrvr.org."],"id":"67e5d6df3fbf8900019df268","meta":{"weight":100},"region":"use"},
        {"answer":["vad-bid.adsrvr.org."],"id":"67e5d6df3fbf8900019df269","meta":{"weight":100},"region":"use"},
        {"answer":["vae-bid.adsrvr.org."],"id":"67e5d6df3fbf8900019df26a","meta":{"weight":100},"region":"use"},
        {"answer":["vam-bid.adsrvr.org."],"id":"67e5d6df3fbf8900019df26b","meta":{"weight":100},"region":"use"},
        {"answer":["usw-ca2.adsrvr.org."],"id":"67e5d6df3fbf8900019df26c","meta":{"weight":100},"region":"usw"},
        {"answer":["wa2-bid.adsrvr.org."],"id":"67e5d6df3fbf8900019df26d","meta":{"weight":100},"region":"usw"},
        {"answer":["sg2-bid.adsrvr.org."],"id":"67e5d6df3fbf8900019df26e","meta":{"weight":100},"region":"asiapac"},
        {"answer":["jp1-bid.adsrvr.org."],"id":"67e5d6df3fbf8900019df26f","meta":{"weight":100},"region":"asiapac"}
    ],
    "domain":"global.adsrvr.org",
    "filters":[
        {"config":{},"filter":"geotarget_regional"},
        {"config":{},"filter":"select_first_region"},
        {"config":{},"filter":"weighted_shuffle"},
        {"config":{"N":1},"filter":"select_first_n"}
    ],
    "meta":{},
    "networks":[0,32],
    "regions":{
        "asiapac":{"meta":{"georegion":["ASIAPAC"]}},
        "eur":{"meta":{"georegion":["EUROPE"]}},
        "use":{"meta":{"georegion":["US-EAST"]}},
        "usw":{"meta":{"georegion":["US-WEST"]}}
    },
    "tier":3,
    "ttl":60,
    "type":"CNAME",
    "use_client_subnet":true,
    "zone":"adsrvr.org",
    "zone_name":"adsrvr.org",
    "id":"67cb50b52feb2500016eb695",
    "feeds":[]
}
"""


def default_record(domain_name: str) -> ns1.Record:
    record: ns1.Record = ns1.Record.model_validate_json(default_record_json)
    record.domain = domain_name
    return record


def update_weights(domain_name: str, prometheus_job_name: str, env: str) -> None:
    config = production_config

    mqp_password = Variable.get(config.mqp_password_name)
    prometheus_client = prometheus.MqpPrometheusClient(config.prometheus_endpoint, config.mqp_user, mqp_password)
    query_repository = prometheus.PrometheusQueryRepository(prometheus_client, config)

    ns1_api_key = Variable.get(config.ns1_api_key_name)
    ns1_read_only_api_key = Variable.get(config.ns1_read_only_api_key_name)
    ns1_client = ns1.NS1HttpClient(
        endpoint=config.ns1_endpoint, api_key=ns1_api_key, read_only_api_key=ns1_read_only_api_key, dry_run=config.dry_run
    )

    task = UpdateTask(config, query_repository, ns1_client)
    task.update_weights(domain_name=domain_name, prometheus_job_name=prometheus_job_name, env=env)


class UpdateTask:

    def __init__(self, config: Config, query_repository: prometheus.QueryRepository, ns1_client: ns1.NS1Client):
        self._config = config
        self._query_repository = query_repository
        self._ns1_client = ns1_client

    def get_current_weights(self, record: ns1.Record) -> dict[str, float]:
        weights = {}
        for answer in record.answers:
            answer_domain = str(answer.answer[0])
            if answer_domain not in self._config.domain_to_dc:
                logging.info(f'Skipping {answer_domain}, not found in config')
                continue
            dc = self._config.domain_to_dc[answer_domain]
            weight = float(answer.meta['weight'])
            weights[dc] = weight

        return weights

    def set_weights(self, record: ns1.Record, weights: dict[str, float]) -> None:
        new_answers = []
        existing_dcs = set()
        for answer in record.answers:
            answer_domain = str(answer.answer[0])
            if answer_domain not in self._config.domain_to_dc:
                logging.info(f'Skipping {answer_domain}, not found in config')
                continue
            answer_dc = self._config.domain_to_dc[answer_domain]

            existing_dcs.add(answer_dc)
            if answer_dc in weights:
                new_weight = weights[answer_dc]
                answer.meta['weight'] = new_weight
            else:
                logging.warning(f"Failed to find weight for {answer_domain} ({answer_dc}), keeping weight at {answer.meta['weight']}")

            region = None
            for r in self._config.regions:
                if answer_dc in self._config.regions[r]:
                    region = r
                    break
            if region is None:
                raise Exception(f'Could not find matching entry for {answer_dc} in regions map')

            answer.region = region

            new_answers.append(answer)

        for datacenter in weights:
            if datacenter in existing_dcs:
                continue
            logging.info(f'Found new datacenter {datacenter} in the weights')

            domain = None
            for d in self._config.domain_to_dc:
                if self._config.domain_to_dc[d] == datacenter:
                    domain = d
                    break
            if domain is None:
                raise Exception(f'Could not find matching entry for {datacenter} in domain_to_dc map')

            region = None
            for r in self._config.regions:
                if datacenter in self._config.regions[r]:
                    region = r
                    break
            if region is None:
                raise Exception(f'Could not find matching entry for {datacenter} in regions map')

            answer = ns1.Answer(answer=[domain], meta={'weight': weights[datacenter]}, region=region)
            logging.info(f'New answer: {answer.model_dump_json(exclude_none=True, serialize_as_any=True)}')
            new_answers.append(answer)

        record.answers = new_answers

    def apply_weight_throttle(self, weights: dict[str, float], smoothed_weights: dict[str, float]) -> dict[str, float]:
        total_weight: float = 0
        for weight in weights.values():
            total_weight += weight

        total_smoothed_weight: float = 0
        for weight in smoothed_weights.values():
            total_smoothed_weight += weight

        # avoid division by zero, just zero weights for both
        if total_smoothed_weight == 0:
            total_smoothed_weight = 1

        new_weights: dict[str, float] = {}
        for datacenter in weights:
            weight = weights[datacenter]
            smoothed_weight = smoothed_weights[datacenter]
            smoothed_weight_ratio = smoothed_weight / total_smoothed_weight
            min_weight = total_weight * smoothed_weight_ratio * (1 - self._config.throttle)
            max_weight = total_weight * smoothed_weight_ratio * (1 + self._config.throttle)
            new_weights[datacenter] = max(min_weight, min(max_weight, weight))

        return new_weights

    def apply_minimum_weight(self, weights: dict[str, float]) -> dict[str, float]:
        new_weights: dict[str, float] = {}
        for datacenter in weights:
            weight = weights[datacenter]
            new_weights[datacenter] = max(self._config.minimum_weight, weight)

        return new_weights

    def apply_maximum_weight_ratio(self, weights: dict[str, float]) -> dict[str, float]:
        total_weight: float = 0
        for weight in weights.values():
            total_weight += weight

        new_weights: dict[str, float] = {}
        for datacenter in weights:
            weight = weights[datacenter]
            new_weights[datacenter] = min(self._config.maximum_datacenter_weight_ratio * total_weight, weight)

        return new_weights

    def get_weights_for_region(
        self, region: str, unix_timestamp: float, opted_in_ssps: list[str], opted_out_ssps: list[str], default_opted_in: bool
    ) -> dict[str, float]:
        logging.info(f"Getting weights for region {region} at time {unix_timestamp}")

        dc_list = self._config.regions[region]
        if dc_list is None:
            return {}

        try:
            response = self._query_repository.get_raw_weights_for_region(
                unix_timestamp, dc_list, opted_in_ssps, opted_out_ssps, default_opted_in
            )

            weights = self.apply_weight_throttle(response.raw, response.smoothed)
            weights = self.apply_minimum_weight(weights)
            weights = self.apply_maximum_weight_ratio(weights)

            return weights
        except Exception as err:
            raise Exception(f"Error while getting weights for region {region} at time {unix_timestamp}") from err

    def update_weights(self, domain_name: str, prometheus_job_name: str, env: str) -> None:
        logging.info(f"Updating weights for {domain_name}")

        if env != 'prod' and self._config.create_new_domain_if_missing:
            try:
                self._ns1_client.get_record(domain_name)
            except ns1.NotFoundException:
                logging.info(f"Domain {domain_name} not found, creating record")
                self._ns1_client.create_record(domain_name=domain_name, record=default_record(domain_name))

        zone = self._ns1_client.get_zone()
        logging.info(f"Got zone with {len(zone.records)} records")

        logging.info("Getting record")
        record = self._ns1_client.get_record(domain_name)
        current_weights = self.get_current_weights(record)
        logging.info(current_weights)

        gslb_datacenter_weight: TtdGauge = get_or_register_gauge(
            job=prometheus_job_name,
            name="gslb_datacenter_weight",
            description="Suggested weights for the global server load balancer global.adsrvr.org",
        )

        now = time.time() - self._config.time_buffer_in_seconds
        weights: dict[str, float] = {}

        all_ssps = self._query_repository.get_all_ssps(now)
        opted_in_ssps = self._query_repository.get_opted_in_ssps(domain_name, zone, all_ssps)
        # TODO: support getting opted out SSPs for when direct.adsrvr.org is migrated to global.adsrvr.org
        opted_out_ssps: list[str] = []
        default_opted_in = False

        logging.info(f"Opted in SSPs: {', '.join(opted_in_ssps)}")

        for region in self._config.regions:
            region_weights = self.get_weights_for_region(
                region,
                now - self._config.lookback_interval_in_seconds,
                opted_out_ssps=opted_out_ssps,
                opted_in_ssps=opted_in_ssps,
                default_opted_in=default_opted_in
            )

            # TODO: error if we're missing too many samples?
            contains_all = True
            for dc in self._config.regions[region]:
                if dc not in region_weights:
                    contains_all = False
                    break

            if contains_all:
                weights |= region_weights
                pass
            else:
                raise Exception(f"Failed to get weights for all DCs in region {region}, prometheus data is probably too old")

        for datacenter in weights:
            weight = weights[datacenter]
            logging.info("%s: %d", datacenter, weight)
            if self._config.push_metrics:
                gslb_datacenter_weight.labels({'datacenter': datacenter}).set(weight)

        # TODO: update regions also?
        self.set_weights(record, weights)
        self._ns1_client.update_record(domain_name, record)

        if self._config.push_metrics:
            push_all(prometheus_job_name)
