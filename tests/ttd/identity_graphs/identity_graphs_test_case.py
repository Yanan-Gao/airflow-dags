import unittest
from datetime import date

from ttd.cloud_provider import CloudProviders
from ttd.identity_graphs.identity_graphs import IdentityGraphs
from ttd.identity_graphs.identity_graph_client import IdentityGraphClient


class IdentityGraphsTestCase(unittest.TestCase):

    def setUp(self):
        self.identity_graphs = IdentityGraphs()

    def test_aws_locations(self):

        def _get_location(identity_graph_client: IdentityGraphClient) -> str:
            return identity_graph_client.dataset.get_dataset_path()

        self.assertEqual(
            "s3://thetradedesk-useast-data-import/sxd-etl/universal/iav2graph", _get_location(self.identity_graphs.default_client)
        )

        identity_alliance = self.identity_graphs.identity_alliance
        self.assertEqual(
            _get_location(self.identity_graphs.default_client),
            _get_location(identity_alliance.v2.based_on_ttd_graph_v2.persons_capped_for_hot_cache_and_with_dats)
        )
        self.assertEqual(
            "s3://thetradedesk-useast-data-import/sxd-etl/universal/iav2graph_household",
            _get_location(identity_alliance.v2.based_on_ttd_graph_v2.households_capped_for_hot_cache_and_with_dats)
        )

        ttd_graph = self.identity_graphs.ttd_graph
        self.assertEqual(
            "s3://thetradedesk-useast-data-import/sxd-etl/universal/nextgen",
            _get_location(ttd_graph.v2.standard_input.persons_capped_for_hot_cache)
        )
        self.assertEqual(
            "s3://thetradedesk-useast-data-import/sxd-etl/universal/nextgen_household",
            _get_location(ttd_graph.v2.standard_input.households_capped_for_hot_cache)
        )
        self.assertEqual(
            "s3://thetradedesk-useast-data-import/sxd-etl/universal/nextgen_singletons",
            _get_location(ttd_graph.v2.standard_input.singleton_persons)
        )

        live_ramp_graph = self.identity_graphs.live_ramp_graph
        self.assertEqual(
            "s3://thetradedesk-useast-data-import/sxd-etl/universal/identitylink",
            _get_location(live_ramp_graph.v1.merged.persons_capped_for_hot_cache)
        )

    def test_release_aws_location(self):
        release_date = date(2025, 1, 1)
        self.assertEqual(
            "s3://thetradedesk-useast-data-import/sxd-etl/universal/iav2graph/2025-01-01/success",
            self.identity_graphs.identity_alliance.default_client.dataset.get_read_path(ds_date=release_date)
        )

    def test_azure_locations(self):
        identity_alliance = self.identity_graphs.identity_alliance.default_version.default_input
        self.assertEqual(
            "wasbs://ttd-identity@ttdexportdata/sxd-etl/universal/iav2graph",
            identity_alliance.persons_capped_for_hot_cache_and_with_dats.dataset.with_cloud(CloudProviders.azure).get_dataset_path()
        )
        self.assertEqual(
            "wasbs://ttd-identity@ttdexportdata/sxd-etl/universal/iav2graph_household",
            identity_alliance.households_capped_for_hot_cache_and_with_dats.dataset.with_cloud(CloudProviders.azure).get_dataset_path()
        )

    def test_get_spark_conf(self):
        # Test data - client and the property key.
        client_key_table = [
            (
                self.identity_graphs.identity_alliance.v2.based_on_ttd_graph_v2.persons_capped_for_hot_cache_and_with_dats,
                "spark.identityGraphs.identityAlliance.v2.basedOnTtdGraphV2.personsCappedForHotCacheAndWithDats.location"
            ),
            (
                self.identity_graphs.identity_alliance.v2.based_on_ttd_graph_v2.households_capped_for_hot_cache_and_with_dats,
                "spark.identityGraphs.identityAlliance.v2.basedOnTtdGraphV2.householdsCappedForHotCacheAndWithDats.location"
            ),
            (
                self.identity_graphs.identity_alliance.v2.based_on_ttd_graph_v1.persons_capped_for_hot_cache_and_with_dats,
                "spark.identityGraphs.identityAlliance.v2.basedOnTtdGraphV1.personsCappedForHotCacheAndWithDats.location"
            ),
            (
                self.identity_graphs.identity_alliance.v2.based_on_ttd_graph_v1.households_capped_for_hot_cache_and_with_dats,
                "spark.identityGraphs.identityAlliance.v2.basedOnTtdGraphV1.householdsCappedForHotCacheAndWithDats.location"
            ),
            (
                self.identity_graphs.ttd_graph.v2.standard_input.persons_capped_for_hot_cache,
                "spark.identityGraphs.ttdGraph.v2.standardInput.personsCappedForHotCache.location"
            ),
            (
                self.identity_graphs.ttd_graph.v2.standard_input.households_capped_for_hot_cache,
                "spark.identityGraphs.ttdGraph.v2.standardInput.householdsCappedForHotCache.location"
            ),
            (
                self.identity_graphs.ttd_graph.v1.standard_input.persons_capped_for_hot_cache,
                "spark.identityGraphs.ttdGraph.v1.standardInput.personsCappedForHotCache.location"
            ),
            (
                self.identity_graphs.ttd_graph.v1.standard_input.households_capped_for_hot_cache,
                "spark.identityGraphs.ttdGraph.v1.standardInput.householdsCappedForHotCache.location"
            ),
        ]

        spark_conf = self.identity_graphs.get_spark_conf()

        for client, key in client_key_table:
            value = client.dataset.get_dataset_path()
            self.assertTrue(key in spark_conf, f"Key {key} is not in spark_conf")
            self.assertEqual(value, spark_conf[key])


if __name__ == '__main__':
    unittest.main()
