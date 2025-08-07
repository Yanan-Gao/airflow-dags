from abc import abstractmethod
import typing
import requests
import logging
from typing import Protocol
from pydantic import BaseModel


class Answer(BaseModel):
    answer: list[str | int]
    id: str | None = None
    meta: dict[str, typing.Any] = dict()
    region: str | None = None
    feeds: list[typing.Any] | None = None


class Record(BaseModel):
    answers: list[Answer] = []
    domain: str
    filters: list[typing.Any] | None = None
    link: str | None = None
    meta: dict[str, typing.Any] = dict()
    networks: list[int] | None = None
    regions: typing.Any | None = None
    tier: int | None = None
    ttl: int | None = None
    override_ttl: int | None = None
    type: str
    use_client_subnet: bool | None = None
    zone: str
    zone_name: str | None = None
    customer: int | None = None
    blocked_tags: list[str] | None = None
    local_tags: list[str] | None = None
    tags: dict[str, typing.Any] | None = None
    override_access_records: bool | None = None
    created_at: int | None = None
    updated_at: int | None = None
    id: str | None = None
    feeds: typing.Any | None = None


class Zone(BaseModel):
    name: str
    records: list[Record]


class NotFoundException(Exception):
    pass


class NS1Client(Protocol):

    @abstractmethod
    def get_zone(self) -> Zone:
        ...

    @abstractmethod
    def get_record(self, domain_name: str) -> Record:
        ...

    @abstractmethod
    def update_record(self, domain_name: str, record: Record) -> None:
        ...

    @abstractmethod
    def create_record(self, domain_name: str, record: Record) -> None:
        ...


class NS1HttpClient(NS1Client):

    def __init__(self, endpoint: str, api_key: str, read_only_api_key: str, dry_run: bool = False):
        super().__init__()
        self._endpoint = endpoint
        self._api_key = api_key
        self._read_only_api_key = read_only_api_key
        self._dry_run = dry_run

    def send_request(self, path: str, method: str, data: str | None, use_read_only_key=False) -> requests.request:
        key = self._api_key
        if use_read_only_key:
            key = self._read_only_api_key
        headers = {'X-NSONE-Key': key, 'accept': 'application/json'}
        return requests.request(method, self._endpoint + path, headers=headers, data=data)

    def get_zone(self) -> Zone:
        r = self.send_request(path="/v1/zones/adsrvr.org", method="GET", data=None, use_read_only_key=True)
        if r.status_code != 200:
            if r.status_code == 404:
                raise NotFoundException()
            else:
                raise Exception(f"Non-200 status: {r.status_code}")
        return Zone.model_validate_json(r.text)

    def get_record(self, domain_name: str) -> Record:
        r = self.send_request(path=f"/v1/zones/adsrvr.org/{domain_name}/CNAME", method="GET", data=None, use_read_only_key=True)
        if r.status_code != 200:
            if r.status_code == 404:
                raise NotFoundException()
            else:
                raise Exception(f"Non-200 status: {r.status_code}")
        logging.info(f"Got record: {r.text}")
        return Record.model_validate_json(r.text)

    def update_record(self, domain_name: str, record: Record) -> None:
        data = record.model_dump_json(exclude_none=True, serialize_as_any=True)
        logging.info(f"New record: {data}")
        if self._dry_run:
            logging.info("Dry run, skipping update")
        else:
            r = self.send_request(path=f"/v1/zones/adsrvr.org/{domain_name}/CNAME", method="POST", data=data)
            r.raise_for_status()
            logging.info(f"Response: {r.text}")

    def create_record(self, domain_name: str, record: Record) -> None:
        data = record.model_dump_json(exclude_none=True, serialize_as_any=True)
        logging.info(f"New record: {data}")
        if self._dry_run:
            logging.info("Dry run, skipping create")
        else:
            r = self.send_request(path=f"/v1/zones/adsrvr.org/{domain_name}/CNAME", method="PUT", data=data)
            r.raise_for_status()
            logging.info(f"Response: {r.text}")
