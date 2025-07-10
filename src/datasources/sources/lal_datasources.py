from datasources.datasource import Datasource
from ttd.datasets.date_generated_dataset import DateGeneratedDataset


class LalDatasources(Datasource):

    @staticmethod
    def get_date_format(is_xd, xd_vendor_id, version=2):
        if version == 2:
            if is_xd:
                return f"isxd=true/xdvendorid={xd_vendor_id}/date=%Y%m%d"
            else:
                return "isxd=false/date=%Y%m%d"
        if version == 3:
            return f"xdvendorid={xd_vendor_id}/date=%Y%m%d"
        return "date=%Y%m%d"

    @staticmethod
    def selected_pixels(is_xd=False, xd_vendor_id="-1", version=2):
        return DateGeneratedDataset(
            bucket="ttd-identity",
            path_prefix="datapipeline",
            data_name="models/lal/selected_pixels",
            date_format=LalDatasources.get_date_format(is_xd, xd_vendor_id, version),
            version=version,
            success_file=None,
        )

    @staticmethod
    def model_results(is_xd=False, xd_vendor_id="-1", version=2):
        return DateGeneratedDataset(
            bucket="ttd-identity",
            path_prefix="datapipeline",
            data_name="models/lal/model_results",
            date_format=LalDatasources.get_date_format(is_xd, xd_vendor_id, version),
            version=version,
            success_file=None,
        )

    @staticmethod
    def daily_model_results_for_aerospike(version=1):
        return DateGeneratedDataset(
            bucket="ttd-identity",
            path_prefix="datapipeline",
            data_name="models/lal/daily_model_results_for_aerospike",
            date_format="date=%Y%m%d",
            version=version,
            success_file=None
        )

    rsm_lal_3p_validation_dataset = DateGeneratedDataset(
        bucket="ttd-identity",
        path_prefix="datapipeline",
        env_aware=True,
        data_name="models/rsm_lal/metrics/jaccardSimilarity",
        version=1,
        date_format="date=%Y%m%d",
        success_file="_VALIDATED"
    )
