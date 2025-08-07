import logging
from time import time
import unittest
from dags.dist.gslb.ns1 import Answer, NS1Client, Record, NotFoundException, Zone
from dags.dist.gslb.update import UpdateTask
from dags.dist.gslb.prometheus import MatrixQueryResponse, MatrixResult, PrometheusClient, PrometheusQueryRepository, VectorQueryResponse, VectorData, VectorResult
from dags.dist.gslb.config import Config, production_config
import re

test_config = Config(
    regions={'usw': ['ca2', 'ca4']},
    domain_to_dc={
        'ca2-bid.adsrvr.org': 'ca2',
        'ca4-bid.adsrvr.org': 'ca4'
    },
    push_metrics=False,
    maximum_datacenter_weight_ratio=0.9,
    region_ssp_configs={},
)
domain_name = "test.gslb"


def default_record() -> Record:
    return Record(
        answers=[
            Answer(answer=["ca2-bid.adsrvr.org"], meta={"weight": 1}, region="usw"),
            Answer(answer=["ca4-bid.adsrvr.org"], meta={"weight": 1}, region="usw"),
        ],
        type="CNAME",
        domain=domain_name,
        zone=domain_name
    )


class PrometheusTimeline:

    def __init__(self, responses: list[tuple[float, VectorQueryResponse]]):
        self.responses = responses
        self.responses.sort()

    def get_value(self, time) -> VectorQueryResponse | None:
        previous_time = 0.0
        for (t, r) in self.responses:
            if previous_time < time < t:
                return r
            previous_time = t
        return None


class PrometheusClientFake(PrometheusClient):
    # TODO: a better way to do this would probably be to pull out the weight calculations
    # in a separate class and hide the prometheus calls

    def __init__(self, query_responses: list[tuple[re.Pattern, PrometheusTimeline]]):
        super().__init__()
        self._query_responses = query_responses

    def _format_query(self, query: str) -> str:
        return " ".join(query.strip().split())  # remove all unnecessary whitespace

    def query_instant(self, query, t) -> VectorQueryResponse:
        query = self._format_query(query)
        logging.info("querying at time %f (%f seconds ago): %s", t, time() - t, query)
        for (pattern, timeline) in self._query_responses:
            if pattern.search(query) is not None:
                logging.info("matched pattern %s", pattern)
                response = timeline.get_value(t)
                if response is None:
                    return VectorQueryResponse()
                return response
        return VectorQueryResponse()

    def query_range(self, query, start, end, step_in_seconds) -> MatrixQueryResponse:
        if step_in_seconds <= 0:
            raise Exception("step_in_seconds must be positive")

        query = self._format_query(query)
        response = MatrixQueryResponse()
        # this is messy and very slow, but it's good enough for test code
        for (pattern, timeline) in self._query_responses:
            if pattern.search(query) is not None:
                t = start
                while t <= end:
                    vector_response = timeline.get_value(t)
                    if vector_response is not None:
                        for result in vector_response.data.result:
                            found = False
                            value = [t, result.value[1]]
                            for matrix_result in response.data.result:
                                if matrix_result.metric == result.metric:
                                    matrix_result.values.append(value)
                                    found = True
                            if not found:
                                response.data.result.append(MatrixResult(metric=result.metric, values=[value]))
                    t += step_in_seconds
        return response


class NS1ClientFake(NS1Client):

    def __init__(self):
        super().__init__()
        self._store: dict[str, Record] = dict()

    def get_zone(self):
        return Zone(name=".", records=[])

    def get_record(self, domain_name):
        if domain_name in self._store:
            return self._store[domain_name]
        else:
            raise NotFoundException()

    def update_record(self, domain_name, record):
        if domain_name in self._store:
            self._store[domain_name] = record
        else:
            raise NotFoundException()

    def create_record(self, domain_name, record):
        self._store[domain_name] = record


def weights_response(time: float, weights: dict[str, int]) -> tuple[float, VectorQueryResponse]:
    return (
        time,
        VectorQueryResponse(
            status="success",
            data=
            VectorData(resultType="vector", result=[VectorResult(metric={"datacenter": dc}, value=[time, weights[dc]]) for dc in weights])
        )
    )


class UpdateTestCase(unittest.TestCase):

    def test_normal_case(self):
        now = time()
        timeline = PrometheusTimeline([weights_response(now, {'ca2': 1_000_000, 'ca4': 2_000_000})])
        prometheus_client = PrometheusClientFake([(re.compile('bid_request_count'), timeline)])
        query_repository = PrometheusQueryRepository(prometheus_client, test_config)
        ns1_client = NS1ClientFake()
        ns1_client.create_record(domain_name, default_record())
        task = UpdateTask(test_config, query_repository, ns1_client)
        task.update_weights(domain_name, "test", "test")

        result = ns1_client.get_record(domain_name)
        self.assertEqual(result.answers[0].answer[0], "ca2-bid.adsrvr.org")
        self.assertEqual(result.answers[0].meta['weight'], 1_000_000)
        self.assertEqual(result.answers[1].answer[0], "ca4-bid.adsrvr.org")
        self.assertEqual(result.answers[1].meta['weight'], 2_000_000)

    def test_negative_weights(self):
        now = time()
        timeline = PrometheusTimeline([weights_response(now, {'ca2': -1, 'ca4': 2})])
        prometheus_client = PrometheusClientFake([(re.compile('bid_request_count'), timeline)])
        query_repository = PrometheusQueryRepository(prometheus_client, test_config)
        ns1_client = NS1ClientFake()
        ns1_client.create_record(domain_name, default_record())
        task = UpdateTask(test_config, query_repository, ns1_client)
        task.update_weights(domain_name, "test", "test")

        result = ns1_client.get_record(domain_name)
        self.assertEqual(result.answers[0].answer[0], "ca2-bid.adsrvr.org")
        self.assertEqual(result.answers[0].meta['weight'], test_config.minimum_weight)
        self.assertEqual(result.answers[1].answer[0], "ca4-bid.adsrvr.org")
        self.assertEqual(result.answers[1].meta['weight'], 2)

    def test_lookback(self):
        now = time()
        delta = test_config.lookback_interval_in_seconds * 2
        timeline = PrometheusTimeline([weights_response(now - delta, {'ca2': 1_000_000, 'ca4': 2_000_000})])
        prometheus_client = PrometheusClientFake([(re.compile('bid_request_count'), timeline)])
        query_repository = PrometheusQueryRepository(prometheus_client, test_config)
        ns1_client = NS1ClientFake()
        ns1_client.create_record(domain_name, default_record())
        task = UpdateTask(test_config, query_repository, ns1_client)
        task.update_weights(domain_name, "test", "test")

        result = ns1_client.get_record(domain_name)
        self.assertEqual(result.answers[0].answer[0], "ca2-bid.adsrvr.org")
        self.assertEqual(result.answers[0].meta['weight'], 1_000_000)
        self.assertEqual(result.answers[1].answer[0], "ca4-bid.adsrvr.org")
        self.assertEqual(result.answers[1].meta['weight'], 2_000_000)

    def test_quick_change(self):
        # Goal is to limit fast changes within some reasonable window
        # Exponential moving average of proportion of weight for each DC in region: h_DC
        # New (proposed) weight: x_DC
        # New (proposed) weight proportion: p_DC
        # Clamp it so p_DC > h_DC * R and p_DC < h_DC / R for some 0 < R < 1
        # x_DC / T > R * h_DC / T

        delta = test_config.lookback_interval_in_seconds * 2
        now = time()
        timeline = PrometheusTimeline([
            weights_response(now, {
                'ca2': 1_000,
                'ca4': 2_000_000
            }),
            weights_response(now - delta, {
                'ca2': 1_000_000,
                'ca4': 2_000_000
            }),
        ])
        prometheus_client = PrometheusClientFake([(re.compile('bid_request_count'), timeline)])
        query_repository = PrometheusQueryRepository(prometheus_client, test_config)
        ns1_client = NS1ClientFake()
        ns1_client.create_record(domain_name, default_record())
        task = UpdateTask(test_config, query_repository, ns1_client)
        task.update_weights(domain_name, "test", "test")

        result = ns1_client.get_record(domain_name)
        self.assertEqual(result.answers[0].answer[0], "ca2-bid.adsrvr.org")
        self.assertGreater(result.answers[0].meta['weight'], 100_000)
        self.assertLess(result.answers[0].meta['weight'], 1_000_000)
        self.assertEqual(result.answers[1].answer[0], "ca4-bid.adsrvr.org")
        self.assertGreater(result.answers[1].meta['weight'], 1_500_000)
        self.assertLessEqual(result.answers[1].meta['weight'], 2_000_000)

    def test_no_huge_datacenter(self):
        now = time()
        timeline = PrometheusTimeline([weights_response(now, {'ca2': 1, 'ca4': 2_000_000})])
        prometheus_client = PrometheusClientFake([(re.compile('bid_request_count'), timeline)])
        query_repository = PrometheusQueryRepository(prometheus_client, test_config)
        ns1_client = NS1ClientFake()
        ns1_client.create_record(domain_name, default_record())
        task = UpdateTask(test_config, query_repository, ns1_client)
        task.update_weights(domain_name, "test", "test")

        result = ns1_client.get_record(domain_name)
        self.assertEqual(result.answers[0].answer[0], "ca2-bid.adsrvr.org")
        self.assertEqual(result.answers[0].meta['weight'], 1)
        self.assertEqual(result.answers[1].answer[0], "ca4-bid.adsrvr.org")
        self.assertEqual(result.answers[1].meta['weight'], 2_000_001 * test_config.maximum_datacenter_weight_ratio)


class ConfigTestCase(unittest.TestCase):

    def test_can_get_the_right_ssp_domain(self):
        self.assertEqual('myssp', production_config.get_ssp_from_domain('my-region', 'de2-bid-myssp.adsrvr.org'))
        self.assertIsNone(production_config.get_ssp_from_domain('my-region', 'baddomain-myssp.adsrvr.org'))
        self.assertEqual('viooh', production_config.get_ssp_from_domain('eur', 'de1-bid-viooh.adsrvr.org'))
