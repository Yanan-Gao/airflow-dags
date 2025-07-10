ALL_DATA_DOMAINS = {'TTD_RestOfWorld', 'Walmart_US', 'TTD_China'}
ALL_EXPORTABLE_DATA_DOMAINS = {'Walmart_US', 'TTD_China'}


class VerticaClusterInfo:

    def __init__(self, name, task_variant, main_data_domain, tenant, conn_id, possible_data_domains=None):
        self.name = name
        self.task_variant = task_variant
        self.tenant = tenant
        self.data_domain = main_data_domain
        self.conn_id = conn_id
        self.possible_data_domains = {main_data_domain} if not possible_data_domains else possible_data_domains


VERTICA_CLUSTERS_TO_CHECK = {
    'USEast01': VerticaClusterInfo('USEast01', '5', 'TTD_RestOfWorld', 'TTD', 'shared-vertica-datasrvc-useast01', ALL_DATA_DOMAINS),
    'USWest01': VerticaClusterInfo('USWest01', '9', 'TTD_RestOfWorld', 'TTD', 'shared-vertica-datasrvc-uswest01', ALL_DATA_DOMAINS),
    'USEast03': VerticaClusterInfo('USEast03', '24', 'Walmart_US', 'Walmart', 'shared-vertica-datasrvc-useast03'),
    'USWest03': VerticaClusterInfo('USWest03', '25', 'Walmart_US', 'Walmart', 'shared-vertica-datasrvc-uswest03'),
    'CNEast01': VerticaClusterInfo('CNEast01', '27', 'TTD_China', 'TTD', 'shared-vertica-datasrvc-cneast01'),
    'CNWest01': VerticaClusterInfo('CNWest01', '28', 'TTD_China', 'TTD', 'shared-vertica-datasrvc-cnwest01'),
}

US_AWS_VERTICA_CLUSTERS = ['USEast01', 'USWest01']
