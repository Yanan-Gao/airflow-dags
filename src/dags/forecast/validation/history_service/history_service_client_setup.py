import grpc
from airflow.hooks.base_hook import BaseHook
from keycloak.realm import KeycloakRealm

from dags.forecast.validation.history_service.core import forecasting_hist_pb2_grpc

root_certificate = """
-----BEGIN CERTIFICATE-----
MIIDxzCCAq+gAwIBAgIUDoF84uUPc77argmuKVzzv3LpNFEwDQYJKoZIhvcNAQEL
BQAwLjEsMCoGA1UEAxMjVGhlIFRyYWRlIERlc2sgLSBJbnRlcm5hbCAtIFJvb3Qg
Q0EwHhcNMjEwMzMwMDM0MDMzWhcNMjYwMzI5MDM0MTAzWjA2MTQwMgYDVQQDEytU
aGUgVHJhZGUgRGVzayAtIEludGVybmFsIC0gSW50ZXJtZWRpYXRlIENBMIIBIjAN
BgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtEx2WyJ1xZWNMkIOZixcv6keSup9
MmZ4xOMCj2m1xUuoa31vw9x2mC7p7m3v3xDNBNm/iKXMrFClwH5gnpH5tjCU6cAR
M3q+BvHGMfYQRxpKYsndkLpVlapgWwFNDH5C8DhoKHxYwTeb3n7CCZhhb0JA94Yd
+Yi0QJjemUbbXIqfi4FRx0w1ZbJAkxWw+mLaD/RzVtJlBJH6ock3OlJTcIgIjQaw
9icdAH8zcwwRp3WV3D35AzwpLbDO0F4kF0lAYPK11N+tcAMDLZHUAFII1wxuKX05
dfk+7f6eD2CyzHc9B6SxTf3oUSsIlUGQ6o/ptJVbjh1BpqSL8/s8K4BtWwIDAQAB
o4HUMIHRMA4GA1UdDwEB/wQEAwIBBjAPBgNVHRMBAf8EBTADAQH/MB0GA1UdDgQW
BBQjbaEG67lePpVozHJ8FWboqu3VUjAfBgNVHSMEGDAWgBQ6c6DS599p/M5KZiCo
BP/TRcEa0zA7BggrBgEFBQcBAQQvMC0wKwYIKwYBBQUHMAKGH2h0dHA6Ly8xMjcu
MC4wLjE6ODIwMC92MS9wa2kvY2EwMQYDVR0fBCowKDAmoCSgIoYgaHR0cDovLzEy
Ny4wLjAuMTo4MjAwL3YxL3BraS9jcmwwDQYJKoZIhvcNAQELBQADggEBAAzhy8rK
E3S7gKYSgQin4sdAU7OZeT7m94vFOkX9+aYcCtDWrfpFc/hAeC20Fmfw5jLu9rwd
DFTVEIKJslP0AENTAL7U1DTiYLgZ/+Pcm9J0KhB6c1+V/QoTYcTLztie332M/zoF
RSEEVgkG6RBQaWsN2ucU0QfVdBNb9+BJMa7DYv/+dtwHRcl9725Zh5IuYKiIRxO7
b2VHARBqd/E4iS1+AjMnwXPR3JwJpPKA1RZlhYjSYUfpAA/9cilsYs1u/fgzWNG3
+267VKPf3i6eYABMVb5pQUgUWgOsNahDnsKQvYdudqCBzALnoNVdWMRXtQuHYgHQ
TOoK0iSyZrUrhTc=
-----END CERTIFICATE-----
"""


def get_ttd_infra_service_token():
    keycloak_credentials = BaseHook.get_connection('ttd-infra-service-client-keycloak-UF')
    realm = KeycloakRealm(
        server_url=f"{keycloak_credentials.conn_type}://{keycloak_credentials.host}", realm_name=keycloak_credentials.schema
    )
    oidc_client = realm.open_id_connect(client_id='ttd-infra-service-client', client_secret='')
    token_response = oidc_client.password_credentials(username=keycloak_credentials.login, password=keycloak_credentials.password)
    return token_response['access_token']


def get_history_service_url(ttd_env: str) -> str:
    if ttd_env == 'prod':
        return 'forecastinghistoryservice.ttd-forecast-history.svc.cluster.local'
    elif ttd_env == 'test' or ttd_env == 'prodTest':
        return 'forecastinghistoryservice.ttd-forecast-history-sb.svc.cluster.local'
    else:
        raise Exception(f"Unrecognized environment: ${ttd_env}")


def get_authenticated_history_service_client_channel(ttd_env: str):
    forecast_service_url = get_history_service_url(ttd_env)
    access_token = get_ttd_infra_service_token()
    credentials = grpc.composite_channel_credentials(
        grpc.ssl_channel_credentials(root_certificates=bytes(root_certificate, 'utf-8')), grpc.access_token_call_credentials(access_token)
    )
    channel = grpc.secure_channel(forecast_service_url, credentials)
    return forecasting_hist_pb2_grpc.ForecastingHistoryStub(channel)
