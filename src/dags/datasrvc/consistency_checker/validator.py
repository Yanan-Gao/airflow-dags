from decimal import Decimal
from typing import Dict, List

from dags.datasrvc.consistency_checker.vertica_cluster import VERTICA_CLUSTERS_TO_CHECK

DECIMAL_ROUNDING = 4


class SingleClusterBaseDiff:

    def __init__(self, cluster):
        self.cluster = cluster

    def __str__(self):
        return f"Inconsistency in cluster {self.cluster}:"


class SingleClusterMissingDomainDiff(SingleClusterBaseDiff):

    def __init__(self, cluster, domain):
        super().__init__(cluster)
        self.missing_domain = domain

    def __str__(self):
        return f"Inconsistency in cluster {self.cluster}: domain {self.missing_domain} is missing"


class SingleClusterWrongDomainDiff(SingleClusterBaseDiff):

    def __init__(self, cluster, domain):
        super().__init__(cluster)
        self.domain = domain

    def __str__(self):
        return f"Inconsistency in cluster {self.cluster}: domain {self.domain} shouldn\'t present"


class SingleClusterWrongSourceDiff(SingleClusterBaseDiff):

    def __init__(self, cluster, domain, sources, expected_sources):
        super().__init__(cluster)
        self.domain = domain
        self.sources = sources
        self.expected_sources = expected_sources

    def __str__(self):
        sources1 = ', '.join(sorted(self.sources))
        sources2 = ', '.join(sorted(self.expected_sources))
        return f"Inconsistency in cluster {self.cluster}: domain {self.domain} should contain sources ({sources2}), but actually found sources ({sources1})"


class SingleClusterColumnValueDiff(SingleClusterBaseDiff):

    def __init__(self, cluster, domain, column, source1, source2, value1, value2):
        super().__init__(cluster)
        self.domain = domain
        self.column = column
        self.source1 = source1
        self.source2 = source2
        self.value1 = value1
        self.value2 = value2

    def __str__(self):
        return f"Inconsistency in cluster {self.cluster}: domain {self.domain}, column {self.column}, source {self.source1} has value {self.value1}, but source {self.source2} has value {self.value2}"


def get_diff(cluster_info, sources, data_by_domain):
    diff: List[SingleClusterBaseDiff] = []

    if cluster_info.data_domain not in data_by_domain:
        diff.append(SingleClusterMissingDomainDiff(cluster_info.name, cluster_info.data_domain))

    for domain in data_by_domain:
        if domain not in cluster_info.possible_data_domains:
            diff.append(SingleClusterWrongDomainDiff(cluster_info.name, domain))
            continue

        # for single cluster check, we only validate the corresponding main domain data
        if domain != cluster_info.data_domain:
            continue

        domain_data = data_by_domain[domain]
        domain_sources = {x["Source"] for x in domain_data}

        if domain_sources != sources:
            diff.append(SingleClusterWrongSourceDiff(cluster_info.name, domain, domain_sources, sources))

        for i in range(1, len(domain_data)):
            s1 = domain_data[0]
            s2 = domain_data[i]
            s1_name = s1['Source']
            s2_name = s2['Source']
            for column in s1:
                if column != 'Source':
                    v1, v2 = s1[column], s2[column]

                    if isinstance(v1, Decimal):
                        v1 = round(v1, DECIMAL_ROUNDING)
                    if isinstance(v2, Decimal):
                        v2 = round(v2, DECIMAL_ROUNDING)

                    if v1 != v2:
                        diff.append(SingleClusterColumnValueDiff(cluster_info.name, domain, column, s1_name, s2_name, v1, v2))
    return diff


def default_validate(**op_kwargs):
    cluster_info = op_kwargs['cluster_info']
    sources = op_kwargs['sources']
    query_result = op_kwargs['query_result']

    print("Running validation")
    data_by_domain: Dict[str, List] = {}

    for row in query_result:
        domain = row['DataDomainName']
        if domain not in data_by_domain:
            data_by_domain[domain] = []
        data_by_domain[domain].append(row)

    diff = get_diff(cluster_info, sources, data_by_domain)
    return '\n'.join([str(x) for x in diff])


class CrossClusterBaseDiff:

    def __init__(self, cluster1, cluster2):
        self.cluster1 = cluster1
        self.cluster2 = cluster2

    def __str__(self):
        return f"Inconsistency between cluster {self.cluster1} and cluster {self.cluster2}:"


class CrossClusterMissingDomainDiff(CrossClusterBaseDiff):

    def __init__(self, cluster1, cluster2, domain, missing_domain_in_cluster):
        super().__init__(cluster1, cluster2)
        self.domain = domain
        self.missing_domain_in_cluster = missing_domain_in_cluster

    def __str__(self):
        return f"Inconsistency between cluster {self.cluster1} and cluster {self.cluster2}: missing domain {self.domain} in cluster {self.missing_domain_in_cluster}"


class CrossClusterMissingSourceDiff(CrossClusterBaseDiff):

    def __init__(self, cluster1, cluster2, domain, source, missing_source_in_cluster):
        super().__init__(cluster1, cluster2)
        self.domain = domain
        self.source = source
        self.missing_source_in_cluster = missing_source_in_cluster

    def __str__(self):
        return f"Inconsistency between cluster {self.cluster1} and cluster {self.cluster2}: missing source {self.source} for domain {self.domain} in cluster {self.missing_source_in_cluster}"


class CrossClusterColumnValueDiff(CrossClusterBaseDiff):

    def __init__(self, cluster1, cluster2, domain, source, column, value1, value2):
        super().__init__(cluster1, cluster2)
        self.domain = domain
        self.source = source
        self.column = column
        self.value1 = value1
        self.value2 = value2

    def __str__(self):
        return f"Inconsistency between cluster {self.cluster1} and cluster {self.cluster2}: domain {self.domain}, source {self.source}, column {self.column}, value ({self.value1}, {self.value2})"


def get_data_domains_to_compare(cluster1, cluster2, cross_cluster_exported_data_domains):
    d1, d2 = VERTICA_CLUSTERS_TO_CHECK[cluster1].data_domain, VERTICA_CLUSTERS_TO_CHECK[cluster2].data_domain
    domain_to_compare = {d1} if d1 == d2 else set()

    exported_domain_to_compare = VERTICA_CLUSTERS_TO_CHECK[cluster1].possible_data_domains.intersection(
        VERTICA_CLUSTERS_TO_CHECK[cluster2].possible_data_domains.intersection(cross_cluster_exported_data_domains)
    )

    domain_to_compare.update(exported_domain_to_compare)
    return domain_to_compare


def get_cross_cluster_diff(cross_cluster_sources, cross_cluster_exported_data_domains, data_by_cluster_domain):
    diff: List[CrossClusterBaseDiff] = []

    clusters = list(data_by_cluster_domain.keys())
    for i in range(len(clusters)):
        for j in range(i + 1, len(clusters)):
            c1, c2 = clusters[i], clusters[j]

            domain_to_compare = get_data_domains_to_compare(c1, c2, cross_cluster_exported_data_domains)
            for domain in domain_to_compare:
                print(f"Comparing domain {domain} for cluster {c1} and cluster {c2}")

                d1, d2 = data_by_cluster_domain[c1].get(domain), data_by_cluster_domain[c2].get(domain)
                match (d1, d2):
                    case (None, None):
                        continue
                    case (None, _):
                        diff.append(CrossClusterMissingDomainDiff(c1, c2, domain, c1))
                        continue
                    case (_, None):
                        diff.append(CrossClusterMissingDomainDiff(c1, c2, domain, c2))
                        continue

                for source in cross_cluster_sources:
                    print(f"Comparing source {source} in domain {domain} for cluster {c1} and cluster {c2}")

                    s1, s2 = next((x for x in d1 if x["Source"] == source), None), next((x for x in d2 if x["Source"] == source), None)
                    match (s1, s2):
                        case (None, None):
                            continue
                        case (None, _):
                            diff.append(CrossClusterMissingSourceDiff(c1, c2, domain, source, c1))
                            continue
                        case (_, None):
                            diff.append(CrossClusterMissingSourceDiff(c1, c2, domain, source, c2))
                            continue
                        case _ if s1 is not None and s2 is not None:
                            for column in s1:
                                if column != 'Source':
                                    v1, v2 = s1[column], s2[column]

                                    if isinstance(v1, Decimal):
                                        v1 = round(v1, DECIMAL_ROUNDING)
                                    if isinstance(v2, Decimal):
                                        v2 = round(v2, DECIMAL_ROUNDING)

                                    if v1 != v2:
                                        diff.append(CrossClusterColumnValueDiff(c1, c2, domain, source, column, v1, v2))

    return diff


def default_cross_cluster_validate(**op_kwargs):
    cross_cluster_data = op_kwargs['cross_cluster_data']
    cross_cluster_sources = op_kwargs['cross_cluster_sources']
    cross_cluster_exported_data_domains = op_kwargs['cross_cluster_exported_data_domains']

    data_by_cluster_domain = {}
    for c in cross_cluster_data:
        data_by_domain: Dict[str, List] = {}
        for row in cross_cluster_data[c]:
            domain = row['DataDomainName']
            if domain not in data_by_domain:
                data_by_domain[domain] = []
            data_by_domain[domain].append(row)
        data_by_cluster_domain[c] = data_by_domain

    diff = get_cross_cluster_diff(cross_cluster_sources, cross_cluster_exported_data_domains, data_by_cluster_domain)
    return '\n'.join([str(x) for x in diff])


class RawDataWrongTenant:

    def __init__(self, cluster, tenant, source, row_count):
        self.cluster = cluster
        self.tenant = tenant
        self.source = source
        self.row_count = row_count

    def __str__(self):
        return f'Inconsistency in cluster {self.cluster}: wrong tenant {self.tenant}, source {self.source}, rowCount: {self.row_count}'


def raw_data_cloud_tenant_validate(**op_kwargs):
    cluster_info = op_kwargs['cluster_info']
    query_result = op_kwargs['query_result']

    print("Running validation")

    wrong_data = []
    for row in query_result:
        tenant = row['TenantName']
        source = row['Source']
        row_count = row['RowCount']

        if tenant != cluster_info.tenant:
            wrong_data.append(RawDataWrongTenant(cluster_info.name, tenant, source, row_count))

    return '\n'.join([str(x) for x in wrong_data])


class SimpleRowDiff:

    def __init__(self, field, source_values):
        self.field = field
        self.source_values = source_values

    def __str__(self):
        mismatch = ', '.join([f'(source: {sv[0]}, value: {sv[1]})' for sv in self.source_values])
        return f"Inconsistency for field {self.field}: {mismatch}"


def default_simple_row_validate(**op_kwargs):
    query_result = op_kwargs['query_result']

    diff: List[SimpleRowDiff] = []
    common_columns = set.intersection(*[set(column for column in query_result[source][0]) for source in query_result])
    print("Common columns to compare: ", common_columns)
    for column in common_columns:
        if len(set(query_result[source][0][column] for source in query_result)) != 1:
            diff.append(SimpleRowDiff(column, [(source, query_result[source][0][column]) for source in query_result]))
    return '\n'.join([str(x) for x in diff])
