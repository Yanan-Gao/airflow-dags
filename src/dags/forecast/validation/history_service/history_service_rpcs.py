from google.protobuf.timestamp_pb2 import Timestamp
from dags.forecast.validation.history_service.core import forecasting_hist_pb2
from dags.forecast.validation.history_service.history_service_client_setup import \
    get_authenticated_history_service_client_channel


def retrieve_forecasts_with_audience(
    ttd_env: str, start: Timestamp, end: Timestamp, should_upload_to_cloud_storage: bool, should_return_results: bool
):
    stub = get_authenticated_history_service_client_channel(ttd_env)
    return stub.RetrieveForecastsWithAudience(
        forecasting_hist_pb2.DBRetrievalRequestWithTimeFrame(
            timeframe=forecasting_hist_pb2.Timeframe(start=start, end=end),
            should_upload_to_cloud_storage=should_upload_to_cloud_storage,
            should_return_results=should_return_results,
            number_of_forecast_limit=3000
        ),
        timeout=600
    )


def retrieve_active_experiments(ttd_env: str, should_upload_to_cloud_storage: bool, should_return_results: bool):
    stub = get_authenticated_history_service_client_channel(ttd_env)
    return stub.RetrieveAllActiveExperiments(
        forecasting_hist_pb2
        .ReturnAndStoreOptions(should_upload_to_cloud_storage=should_upload_to_cloud_storage, should_return_results=should_return_results)
    )


def retrieve_production_deployments(ttd_env: str, should_upload_to_cloud_storage: bool, should_return_results: bool):
    stub = get_authenticated_history_service_client_channel(ttd_env)
    return stub.RetrieveProductionDeployments(
        forecasting_hist_pb2
        .ReturnAndStoreOptions(should_upload_to_cloud_storage=should_upload_to_cloud_storage, should_return_results=should_return_results)
    )


def retrieve_active_deployments_with_addresses(ttd_env: str, should_upload_to_cloud_storage: bool, should_return_results: bool):
    stub = get_authenticated_history_service_client_channel(ttd_env)
    return stub.RetrieveActiveDeploymentsWithRamWebAddresses(
        forecasting_hist_pb2
        .ReturnAndStoreOptions(should_upload_to_cloud_storage=should_upload_to_cloud_storage, should_return_results=should_return_results)
    )
