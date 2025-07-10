from typing import Dict, List

from dags.datasrvc.consistency_checker.validator import get_diff

CHINA_DATA_DOMAIN = 'TTD_China'


def category_element_validate(**op_kwargs):
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

    print("Pre-Validation check")
    if cluster_info.data_domain not in data_by_domain:
        if cluster_info.data_domain == CHINA_DATA_DOMAIN:
            # TTD_China it is known that there may be no CategoryElement data, so we will take the route of ignoring this hour
            print(
                f'No data found for {CHINA_DATA_DOMAIN} - will assume it is OK and ignore any further checks as this is a known scenario!'
            )
            return ''

    print("Passed Pre-Validation now performing actual validation")
    diff = get_diff(cluster_info, sources, data_by_domain)
    return '\n'.join([str(x) for x in diff])
