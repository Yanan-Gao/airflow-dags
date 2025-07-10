import shutil
import tarfile
from pathlib import Path
from tempfile import TemporaryFile

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry


def get_request_session(headers=None) -> Session:
    retries = Retry(
        total=5,
        backoff_factor=1,
    )
    request_session = Session()
    if headers is not None:
        request_session.headers.update(headers)
    request_session.mount("https://", HTTPAdapter(max_retries=retries))
    return request_session


def _clear_and_create_directory(path: str) -> None:
    shutil.rmtree(path, ignore_errors=True)
    Path(path).mkdir(parents=True, exist_ok=True)


def _download_ttd_certificates(save_path: str) -> None:
    # Need to create and and clear if already exists, otherwise extract certs will fail
    _clear_and_create_directory(save_path)
    cert_url = "https://nexus.adsrvr.org/repository/ttd-raw/certs/ttd-certs.tgz"
    with TemporaryFile() as tmpfile, get_request_session() as session:
        content = session.get(cert_url, stream=True).content
        tmpfile.write(content)
        tmpfile.flush()
        tmpfile.seek(0)
        with tarfile.open(fileobj=tmpfile) as tar:
            tar.extractall(save_path)


def install_certificates() -> str:
    certificate_save_path = "/tmp/ttd-certs"
    _download_ttd_certificates(certificate_save_path)
    return f"{certificate_save_path}/ttd-root-ca.crt"
