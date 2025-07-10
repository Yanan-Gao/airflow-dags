from dataclasses import dataclass, asdict
from enum import IntEnum, StrEnum, auto
from typing import Dict


class DsrIdentitySource(IntEnum):
    EMAIL = 1
    PHONE = 2


class DsrIdentityType(StrEnum):
    UID2 = auto()
    EUID = auto()


@dataclass
class DsrIdentity:
    identity_type: DsrIdentityType
    identity_source: DsrIdentitySource
    identifier: str
    tdid: str
    sort_key: str | None = None
    salt_bucket: str | None = None

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    def to_dict(self) -> Dict:
        return asdict(self)
