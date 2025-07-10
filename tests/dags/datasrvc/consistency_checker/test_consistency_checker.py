from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Set
from unittest import TestCase
from dags.datasrvc.consistency_checker.vertica_cluster import VERTICA_CLUSTERS_TO_CHECK, ALL_EXPORTABLE_DATA_DOMAINS
import dags.datasrvc.consistency_checker.validator as validator

time = datetime(2024, 1, 1, 1, 0, 0)


class TestConsistencyChecker(TestCase):

    def build_row(self, time, domain, source, count, cost):
        return {'Data': time, 'DataDomainName': domain, 'Source': source, 'ImpressionCount': count, 'MediaCostInUSD': cost}

    def build_cluster_sample_data(self, cluster):
        rows = []
        if cluster == 'USEast01' or cluster == 'USWest01':
            rows = [
                self.build_row(time, 'TTD_RestOfWorld', 'raw', 23412678, Decimal('78989.25191180476667')),
                self.build_row(time, 'TTD_RestOfWorld', 'perf_report', 23412678, Decimal('78989.25191180476667')),
                self.build_row(time, 'TTD_RestOfWorld', 'super_report', 23412678, Decimal('78989.25191180476667')),
                self.build_row(time, 'TTD_China', 'perf_report', 701069, Decimal('89.25191180423445')),
                self.build_row(time, 'TTD_China', 'super_report', 701069, Decimal('89.25191180423445')),
                self.build_row(time, 'Walmart_US', 'perf_report', 2701063, Decimal('289.25191180423777')),
                self.build_row(time, 'Walmart_US', 'super_report', 2701063, Decimal('289.25191180423777'))
            ]
        elif cluster == 'USEast03' or cluster == 'USWest03':
            rows = [
                self.build_row(time, 'Walmart_US', 'raw', 2701063, Decimal('289.25191180423777')),
                self.build_row(time, 'Walmart_US', 'perf_report', 2701063, Decimal('289.25191180423777')),
                self.build_row(time, 'Walmart_US', 'super_report', 2701063, Decimal('289.25191180423777'))
            ]
        else:
            rows = [
                self.build_row(time, 'TTD_China', 'raw', 701069, Decimal('89.25191180423445')),
                self.build_row(time, 'TTD_China', 'perf_report', 701069, Decimal('89.25191180423445')),
                self.build_row(time, 'TTD_China', 'super_report', 701069, Decimal('89.25191180423445')),
            ]
        return rows

    def test_single_cluster_default_validate_ok(self):
        # arrage
        cluster_info = VERTICA_CLUSTERS_TO_CHECK['CNEast01']
        query_result = self.build_cluster_sample_data(cluster_info.name)
        sources = {'raw', 'perf_report', 'super_report'}

        err_msg = validator.default_validate(cluster_info=cluster_info, sources=sources, query_result=query_result)
        self.assertEqual(err_msg, '')

    def test_single_cluster_default_validate_inconsistent_no_data(self):
        # arrage
        cluster_info = VERTICA_CLUSTERS_TO_CHECK['CNEast01']
        query_result: List[Dict] = []
        sources = {'raw', 'perf_report', 'super_report'}

        err_msg = validator.default_validate(cluster_info=cluster_info, sources=sources, query_result=query_result)
        self.assertEqual(err_msg, 'Inconsistency in cluster CNEast01: domain TTD_China is missing')

    def test_single_cluster_default_validate_inconsistent_row(self):
        # arrange
        cluster_info = VERTICA_CLUSTERS_TO_CHECK['CNEast01']
        query_result = self.build_cluster_sample_data(cluster_info.name)
        query_result[1]['ImpressionCount'] = 701079
        sources = {'raw', 'perf_report', 'super_report'}

        # act
        err_msg = validator.default_validate(cluster_info=cluster_info, sources=sources, query_result=query_result)

        # assert
        self.assertEqual(
            err_msg,
            'Inconsistency in cluster CNEast01: domain TTD_China, column ImpressionCount, source raw has value 701069, but source perf_report has value 701079'
        )

    def test_single_cluster_default_validate_wrong_tenant(self):
        # arrange
        cluster_info = VERTICA_CLUSTERS_TO_CHECK['CNEast01']
        query_result = self.build_cluster_sample_data(cluster_info.name)
        query_result.append(self.build_row(datetime(2024, 1, 1, 1, 0, 0), 'Walmart_US', 'raw', 45094568, Decimal('7138.788810237023')))
        sources = {'raw', 'perf_report', 'super_report'}

        # act
        err_msg = validator.default_validate(cluster_info=cluster_info, sources=sources, query_result=query_result)

        # assert
        self.assertEqual(err_msg, 'Inconsistency in cluster CNEast01: domain Walmart_US shouldn\'t present')

    def test_single_cluster_default_validate_wrong_source(self):
        # arrange
        cluster_info = VERTICA_CLUSTERS_TO_CHECK['CNEast01']
        query_result = self.build_cluster_sample_data(cluster_info.name)
        query_result.pop()  # remove super_report source data
        sources = {'raw', 'perf_report', 'super_report'}

        # act
        err_msg = validator.default_validate(cluster_info=cluster_info, sources=sources, query_result=query_result)

        # assert
        self.assertEqual(
            err_msg,
            'Inconsistency in cluster CNEast01: domain TTD_China should contain sources (perf_report, raw, super_report), but actually found sources (perf_report, raw)'
        )

    def build_cross_cluster_sample_data(self):
        data = {}
        for cluster in VERTICA_CLUSTERS_TO_CHECK:
            data[cluster] = self.build_cluster_sample_data(cluster)
        return data

    def test_cross_cluster_default_validate_ok(self):
        # arrange
        data = self.build_cross_cluster_sample_data()
        sources = {'perf_report', 'super_report'}

        # act
        err_msg = validator.default_cross_cluster_validate(
            cross_cluster_data=data, cross_cluster_exported_data_domains=ALL_EXPORTABLE_DATA_DOMAINS, cross_cluster_sources=sources
        )

        # assert
        self.assertEqual(err_msg, '')

    def test_cross_cluster_default_validate_inconsistent_row(self):
        # arrange
        data = self.build_cross_cluster_sample_data()
        data['CNWest01'][1]['ImpressionCount'] = 1
        sources = {'perf_report', 'super_report'}

        # act
        err_msg = validator.default_cross_cluster_validate(
            cross_cluster_data=data, cross_cluster_exported_data_domains=ALL_EXPORTABLE_DATA_DOMAINS, cross_cluster_sources=sources
        )

        # assert
        expected = (
            'Inconsistency between cluster USEast01 and cluster CNWest01: domain TTD_China, source perf_report, column ImpressionCount, value (701069, 1)\n'
            +
            'Inconsistency between cluster USWest01 and cluster CNWest01: domain TTD_China, source perf_report, column ImpressionCount, value (701069, 1)\n'
            +
            'Inconsistency between cluster CNEast01 and cluster CNWest01: domain TTD_China, source perf_report, column ImpressionCount, value (701069, 1)'
        )
        self.assertEqual(err_msg, expected)

    def test_cross_cluster_default_validate_missing_domain(self):
        # arrange
        data = self.build_cross_cluster_sample_data()
        data['USEast01'] = [x for x in data['USEast01'] if x["DataDomainName"] != 'TTD_China']  # remove TTD_China domain data from USEast01
        sources = {'perf_report', 'super_report'}

        # act
        err_msg = validator.default_cross_cluster_validate(
            cross_cluster_data=data, cross_cluster_exported_data_domains=ALL_EXPORTABLE_DATA_DOMAINS, cross_cluster_sources=sources
        )

        # assert
        expected = (
            'Inconsistency between cluster USEast01 and cluster USWest01: missing domain TTD_China in cluster USEast01\n' +
            'Inconsistency between cluster USEast01 and cluster CNEast01: missing domain TTD_China in cluster USEast01\n' +
            'Inconsistency between cluster USEast01 and cluster CNWest01: missing domain TTD_China in cluster USEast01'
        )
        self.assertEqual(err_msg, expected)

    def test_cross_cluster_default_validate_missing_source(self):
        # arrange
        data = self.build_cross_cluster_sample_data()
        data['USEast01'].pop(3)  # remove perf_report source from TTD_China domain in USEast01 cluster
        sources = {'perf_report', 'super_report'}

        # act
        err_msg = validator.default_cross_cluster_validate(
            cross_cluster_data=data, cross_cluster_exported_data_domains=ALL_EXPORTABLE_DATA_DOMAINS, cross_cluster_sources=sources
        )

        # assert
        expected = (
            'Inconsistency between cluster USEast01 and cluster USWest01: missing source perf_report for domain TTD_China in cluster USEast01\n'
            +
            'Inconsistency between cluster USEast01 and cluster CNEast01: missing source perf_report for domain TTD_China in cluster USEast01\n'
            +
            'Inconsistency between cluster USEast01 and cluster CNWest01: missing source perf_report for domain TTD_China in cluster USEast01'
        )
        self.assertEqual(err_msg, expected)

    def build_raw_data_cloud_tenant_sample_row(self, source, date, tenant, row_count):
        return {'Source': source, 'Date': date, 'TenantName': tenant, 'RowCount': row_count}

    def build_raw_data_cloud_tenant_sample_data(self, tenant):
        return [
            self.build_raw_data_cloud_tenant_sample_row('bidrequest', time, tenant, 112),
            self.build_raw_data_cloud_tenant_sample_row('bidfeedback', time, tenant, 56),
            self.build_raw_data_cloud_tenant_sample_row('clicktracker', time, tenant, 89),
            self.build_raw_data_cloud_tenant_sample_row('conversiontracker', time, tenant, 39),
            self.build_raw_data_cloud_tenant_sample_row('videoevent', time, tenant, 81)
        ]

    def test_raw_data_cloud_tenant_validate_ok(self):
        # arrange
        cluster_info = VERTICA_CLUSTERS_TO_CHECK['CNEast01']
        query_result = self.build_raw_data_cloud_tenant_sample_data('TTD')

        # act
        err_msg = validator.raw_data_cloud_tenant_validate(cluster_info=cluster_info, query_result=query_result)

        # assert
        self.assertEqual(err_msg, '')

    def test_raw_data_cloud_tenant_validate_wrong_tenant(self):
        # arrange
        cluster_info = VERTICA_CLUSTERS_TO_CHECK['CNEast01']
        query_result = self.build_raw_data_cloud_tenant_sample_data('TTD')
        query_result.append(self.build_raw_data_cloud_tenant_sample_row('conversiontracker', time, 'Walmart', 123))

        # act
        err_msg = validator.raw_data_cloud_tenant_validate(cluster_info=cluster_info, query_result=query_result)

        # assert
        self.assertEqual(err_msg, 'Inconsistency in cluster CNEast01: wrong tenant Walmart, source conversiontracker, rowCount: 123')

    def test_get_data_domains_to_compare_default_exported(self):
        cross_cluster_exported_data_domains: Set[str] = ALL_EXPORTABLE_DATA_DOMAINS

        self.assertEqual(validator.get_data_domains_to_compare('USEast01', 'USWest03', cross_cluster_exported_data_domains), {'Walmart_US'})
        self.assertEqual(validator.get_data_domains_to_compare('USEast01', 'CNWest01', cross_cluster_exported_data_domains), {'TTD_China'})
        self.assertEqual(validator.get_data_domains_to_compare('USWest03', 'CNWest01', cross_cluster_exported_data_domains), set())
        self.assertEqual(
            validator.get_data_domains_to_compare('USEast01', 'USWest01', cross_cluster_exported_data_domains),
            {'TTD_RestOfWorld', 'Walmart_US', 'TTD_China'}
        )
        self.assertEqual(validator.get_data_domains_to_compare('USWest03', 'USEast03', cross_cluster_exported_data_domains), {'Walmart_US'})
        self.assertEqual(validator.get_data_domains_to_compare('CNEast01', 'CNWest01', cross_cluster_exported_data_domains), {'TTD_China'})

    def test_get_data_domains_to_compare_no_domain_exported(self):
        cross_cluster_exported_data_domains: Set[str] = set()

        self.assertEqual(validator.get_data_domains_to_compare('USEast01', 'USWest03', cross_cluster_exported_data_domains), set())
        self.assertEqual(validator.get_data_domains_to_compare('USEast01', 'CNWest01', cross_cluster_exported_data_domains), set())
        self.assertEqual(validator.get_data_domains_to_compare('USWest03', 'CNWest01', cross_cluster_exported_data_domains), set())
        self.assertEqual(
            validator.get_data_domains_to_compare('USEast01', 'USWest01', cross_cluster_exported_data_domains), {'TTD_RestOfWorld'}
        )
        self.assertEqual(validator.get_data_domains_to_compare('USWest03', 'USEast03', cross_cluster_exported_data_domains), {'Walmart_US'})
        self.assertEqual(validator.get_data_domains_to_compare('CNEast01', 'CNWest01', cross_cluster_exported_data_domains), {'TTD_China'})

    def test_get_data_domains_to_compare_one_domain_exported(self):
        cross_cluster_exported_data_domains: Set[str] = {'Walmart_US'}

        self.assertEqual(validator.get_data_domains_to_compare('USEast01', 'USWest03', cross_cluster_exported_data_domains), {'Walmart_US'})
        self.assertEqual(validator.get_data_domains_to_compare('USEast01', 'CNWest01', cross_cluster_exported_data_domains), set())
        self.assertEqual(validator.get_data_domains_to_compare('USWest03', 'CNWest01', cross_cluster_exported_data_domains), set())
        self.assertEqual(
            validator.get_data_domains_to_compare('USEast01', 'USWest01', cross_cluster_exported_data_domains),
            {'TTD_RestOfWorld', 'Walmart_US'}
        )
        self.assertEqual(validator.get_data_domains_to_compare('USWest03', 'USEast03', cross_cluster_exported_data_domains), {'Walmart_US'})
        self.assertEqual(validator.get_data_domains_to_compare('CNEast01', 'CNWest01', cross_cluster_exported_data_domains), {'TTD_China'})
