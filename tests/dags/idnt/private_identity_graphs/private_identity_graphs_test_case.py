import unittest

from ttd.ttdenv import TtdEnvFactory
from dags.idnt.private_identity_graphs.private_identity_graphs import PrivateIdentityGraphs


class PrivateIdentityGraphsTestCase(unittest.TestCase):

    private_identity_graphs = PrivateIdentityGraphs()

    def test_aws_locations(self):
        singleton_graph_client = self.private_identity_graphs.singleton_graph.v1.standard_input.raw
        self.assertEqual(
            "s3://ttd-insights/graph/prod/singletongraph",
            singleton_graph_client.dataset.with_env(TtdEnvFactory.prod).get_dataset_path()
        )

    def test_public_graphs_also_available(self):
        self.assertIsNotNone(self.private_identity_graphs.ttd_graph)
