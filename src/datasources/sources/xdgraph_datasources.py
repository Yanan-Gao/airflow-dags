from datasources.datasource import Datasource
from ttd.datasets.date_generated_dataset import DateGeneratedDataset


class XdGraphVendorDataName:
    ADBRAIN = "nextgen"
    ADBRAIN_HH = "nextgen_household"
    LIVERAMP = "identitylink"
    TAPAD_APAC = "tapad/apac"
    TAPAD_EMEA = "tapad/eur"
    TAPAD_NA = "tapad/na"
    IAv2_Person = "iav2graph"
    IAv2_HH = "iav2graph_household"


class XdGraphDatasources(Datasource):

    @staticmethod
    def xdGraph(xdGraphVendorName=XdGraphVendorDataName.IAv2_Person):
        """
        Deprecated. Please use `ttd.identity_graphs.identity_graphs.IdentityGraphs` instead.
        """
        return DateGeneratedDataset(
            bucket="thetradedesk-useast-data-import",
            azure_bucket="ttd-identity@ttdexportdata",
            path_prefix="sxd-etl/universal",
            data_name=xdGraphVendorName,
            date_format="%Y-%m-%d/success",
            version=None,
            env_aware=False,
            buckets_for_other_regions={
                "us-west-2": "thetradedesk-uswest-2-avails",
                "ap-northeast-1": "thetradedesk-jp1-avails",
                "ap-southeast-1": "thetradedesk-sg2-avails",
            },
        )
