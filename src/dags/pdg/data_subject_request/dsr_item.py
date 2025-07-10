from typing import List, Dict
from dags.pdg.data_subject_request.identity import DsrIdentity, DsrIdentityType, DsrIdentitySource


class DsrItem:

    REGION_EU = 'EU'
    REGION_US = 'US'

    def __init__(self, jira_ticket_number: str, email: str, phone: str, region: str, request_type: str):
        self._jira_ticket_number = jira_ticket_number
        self._email = email
        self._phone = phone
        self._region = region
        self._request_type = request_type

        self._identities: List[DsrIdentity] = []

        self._has_third_party_data = False
        self._has_vertica_data = False

        self._email_sort_key: str | None = None
        self._phone_sort_key: str | None = None

    def __repr__(self):
        return str(self.__dict__)

    def add_identity(self, identity: DsrIdentity):
        self._identities.append(identity)

    @property
    def jira_ticket_number(self) -> str:
        return self._jira_ticket_number

    def _get_identity(self, identity_type: DsrIdentityType, identity_source: DsrIdentitySource) -> str | None:
        if not self._identities:
            return None

        identity = [
            identity.identifier for identity in self._identities
            if identity.identity_type == identity_type and identity.identity_source == identity_source
        ]

        # We should only ever have one combination of each identity type and source. If
        # that assumption changes, we'll have to update this as well as the calling code.
        return identity[0] if identity else None

    @property
    def guids(self) -> List[str]:
        return [identity.tdid for identity in self._identities]

    @property
    def identities(self) -> List[DsrIdentity]:
        return self._identities

    @property
    def uid2_identities(self) -> List[DsrIdentity]:
        return [identity for identity in self._identities if identity.identity_type == DsrIdentityType.UID2]

    @property
    def euid_identities(self) -> List[Dict]:
        return [identity for identity in self._identities if identity.identity_type == DsrIdentityType.EUID]

    @property
    def email(self) -> str:
        return self._email

    @property
    def request_is_us(self) -> bool:
        return self._region == DsrItem.REGION_US

    @property
    def request_is_eu(self) -> bool:
        return self._region == DsrItem.REGION_EU

    # Phone
    def _get_phone(self) -> str:
        return self._phone

    def _set_phone(self, phone: str):
        self._phone = phone

    phone = property(fget=_get_phone, fset=_set_phone, doc="Phone Number")

    # UID2 - email
    @property
    def email_uid2(self) -> str | None:
        return self._get_identity(DsrIdentityType.UID2, DsrIdentitySource.EMAIL)

    # EUID - email
    @property
    def email_euid(self) -> str | None:
        return self._get_identity(DsrIdentityType.EUID, DsrIdentitySource.EMAIL)

    # UID2 - phone
    @property
    def phone_uid2(self) -> str | None:
        return self._get_identity(DsrIdentityType.UID2, DsrIdentitySource.PHONE)

    # EUID - phone
    @property
    def phone_euid(self) -> str | None:
        return self._get_identity(DsrIdentityType.EUID, DsrIdentitySource.PHONE)

    # Has ThirdParty Data
    def _get_has_third_party_data(self) -> bool:
        return self._has_third_party_data

    def _set_has_third_party_data(self, has_third_party_data: bool):
        self._has_third_party_data = has_third_party_data

    has_third_party_data = property(
        fget=_get_has_third_party_data, fset=_set_has_third_party_data, doc="Whether the request has ThirdParty data"
    )

    # Email Sort Key
    def _get_email_sort_key(self) -> str | None:
        return self._email_sort_key

    def _set_email_sort_key(self, email_sort_key: str):
        self._email_sort_key = email_sort_key

    email_sort_key = property(fget=_get_email_sort_key, fset=_set_email_sort_key, doc="Dynamo sort key associated with email")

    # Phone Sort Key
    def _get_phone_sort_key(self) -> str | None:
        return self._phone_sort_key

    def _set_phone_sort_key(self, phone_sort_key: str):
        self._phone_sort_key = phone_sort_key

    phone_sort_key = property(fget=_get_phone_sort_key, fset=_set_phone_sort_key, doc="Dynamo sort key associated with phone")

    # Has Vertica Data
    def _get_has_vertica_data(self) -> bool:
        return self._has_vertica_data

    def _set_has_vertica_data(self, has_vertica_data: bool):
        self._has_vertica_data = has_vertica_data

    has_vertica_data = property(fget=_get_has_vertica_data, fset=_set_has_vertica_data, doc="Whether the request has Vertica data")

    # Serialization
    @classmethod
    def from_dict(cls, data):
        obj = cls.__new__(cls)
        for key, value in data.items():
            if isinstance(value, list):
                # Identities is currently the only property that we need to deserialize into a list of
                # a certain type. This could be more generic, but for now it's simple.
                obj.__dict__[key] = [DsrIdentity.from_dict(id) if key == '_identities' else id for id in value]
            else:
                obj.__dict__[key] = value
        return obj

    def to_dict(self):
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, list):
                result[key] = [item.to_dict() if isinstance(item, DsrIdentity) else item for item in value]
            else:
                result[key] = value
        return result
