from ttd.datasets.date_generated_dataset import DateGeneratedDataset


# Temporary to unblock the adbrain graph location being deprecated. This will be added to identity datasets
class GraphDatasources:
    """
    Deprecated. Please use `ttd.identity_graphs.identity_graphs.IdentityGraphs` instead.
    """
    ttd_graph_adbrain_legacy: DateGeneratedDataset = DateGeneratedDataset(
        bucket="ttd-insights",
        path_prefix="graph",
        data_name="adbrain_legacy",
        date_format="%Y-%m-%d",
        version=None,
    )
