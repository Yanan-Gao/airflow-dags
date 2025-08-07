import re

from typing import NamedTuple, List, Dict
from enum import Enum
from datetime import timedelta


class ScrubType(Enum):
    # (scrub_mode, scrub_operation) -- do NOT change values as these are serialized into the database for downstream
    DELETE_ROW = (0, "Delete")
    SCRUB_PII_COLUMNS = (1, "Update")


class PiiColumn(NamedTuple):
    col: str
    scrubbed_value: str


_GUID_ZERO_VALUE = "'00000000-0000-0000-0000-000000000000'"

VERTICA_SCRUB_IDS_PER_TASK = 50


class Pii(Enum):
    EUID = PiiColumn("EUID", scrubbed_value="null")
    TDID = PiiColumn("TDID", scrubbed_value=_GUID_ZERO_VALUE)
    UnifiedId2 = PiiColumn("UnifiedId2", scrubbed_value="null")

    # AttributedEventTDID = PiiColumn("AttributedEventTDID", scrubbed_value=_GUID_ZERO_VALUE)
    BrowserTDID = PiiColumn("BrowserTDID", scrubbed_value="null")
    DATId = PiiColumn("DATId", scrubbed_value="null")
    DeviceAdvertisingId = PiiColumn("DeviceAdvertisingId", scrubbed_value=_GUID_ZERO_VALUE)
    DeviceId = PiiColumn("DeviceId", scrubbed_value=_GUID_ZERO_VALUE)
    HashedIpAsUiid = PiiColumn("HashedIpAsUiid", scrubbed_value="null")
    HouseholdId = PiiColumn("HouseholdId", scrubbed_value="null")
    IPAddress = PiiColumn("IPAddress", scrubbed_value="null")
    IdentityLinkId = PiiColumn("IdentityLinkId", scrubbed_value="null")
    Latitude = PiiColumn("Latitude", scrubbed_value="null")
    Longitude = PiiColumn("Longitude", scrubbed_value="null")
    OriginalId = PiiColumn("OriginalId", scrubbed_value="null")
    OriginatingId = PiiColumn("OriginatingId", scrubbed_value="null")
    PersonId = PiiColumn("PersonId", scrubbed_value="null")
    Zip = PiiColumn("Zip", scrubbed_value="null")
    BidRequestIPAddress = PiiColumn("BidRequestIPAddress", scrubbed_value="null")
    # Not technically PII, but overloading the design to account for updating this Column as well
    PiiScrubReasons = PiiColumn("PiiScrubReasons", scrubbed_value="'DSR'")
    CookieId = PiiColumn("CookieId", scrubbed_value="null")
    ConsolidatedUserIds = PiiColumn("ConsolidatedUserIds", scrubbed_value="null")


class VerticaTableConfiguration(NamedTuple):
    raw_table_name: str
    retention_period: timedelta
    filter_columns: List[Pii]
    pii_columns: List[Pii]
    tdid_special_transforms: List[str]
    scrub_type: ScrubType
    start_processing_offset: timedelta
    stop_processing_offset: timedelta


_LOG_KEY_CLEANSE_REGEX = re.compile(r'[^A-Za-z0-9]')


def cleanse_table_name(table_config: VerticaTableConfiguration):
    return cleanse_key(table_config.raw_table_name)


def cleanse_key(log_key):
    return _LOG_KEY_CLEANSE_REGEX.sub('_', log_key)


DSR_VERTICA_TABLE_CONFIGURATIONS: Dict[str, VerticaTableConfiguration] = {
    c.raw_table_name: c
    for c in [
        VerticaTableConfiguration(
            raw_table_name="ttd.ClickTracker",
            retention_period=timedelta(days=90),
            filter_columns=[Pii.TDID, Pii.UnifiedId2, Pii.EUID],
            pii_columns=[Pii.TDID, Pii.DeviceAdvertisingId, Pii.IPAddress, Pii.Zip, Pii.UnifiedId2, Pii.EUID, Pii.IdentityLinkId],
            tdid_special_transforms=[],
            scrub_type=ScrubType.SCRUB_PII_COLUMNS,
            start_processing_offset=timedelta(days=10),
            stop_processing_offset=timedelta(days=1)
        ),
        VerticaTableConfiguration(
            raw_table_name="ttd.ConversionTracker",
            retention_period=timedelta(days=90),
            filter_columns=[Pii.TDID, Pii.UnifiedId2, Pii.EUID],
            pii_columns=[
                Pii.TDID, Pii.IPAddress, Pii.UnifiedId2, Pii.EUID, Pii.IdentityLinkId, Pii.DATId, Pii.DeviceId, Pii.PiiScrubReasons,
                Pii.CookieId, Pii.ConsolidatedUserIds
            ],
            tdid_special_transforms=[],
            scrub_type=ScrubType.SCRUB_PII_COLUMNS,
            start_processing_offset=timedelta(days=10),
            stop_processing_offset=timedelta(days=1)
        ),
        # Defined as a AttributionDataTables which requires special processing to not impact late attribution tasks
        # https://gitlab.adsrvr.org/thetradedesk/adplatform/blob/5e6a60b8eaf3a66c026b5855ec83c12ddc26ee3f/TTD/Domain/LogProcessing/LogExtractor/TTD.Domain.LogProcessing.LogExtractor/Extractions/VerticaScrub/VerticaScrubExtraction.cs#L96
        # Keep in sync with
        # https://gitlab.adsrvr.org/thetradedesk/adplatform/blob/7bc1d778dcf6d5e774a9c5a031ab260a85aefbea/Applications/LogExtractorService/LogExtractorService/Configuration/ConfigurationOverrides.yml#L4135
        # VerticaTableConfiguration(
        #     raw_table_name="ttd.AttributedEvent",
        #     retention_period=timedelta(days=100),
        #     filter_columns=[
        #         Pii.TDID,
        #     ],
        #     pii_columns=[
        #         Pii.TDID,
        #         Pii.AttributedEventTDID,
        #         Pii.IPAddress,
        #         Pii.Zip],
        #     tdid_special_transforms=["AttributedEventTDID"],
        #     scrub_type=ScrubType.SCRUB_PII_COLUMNS,
        #     start_processing_offset=timedelta(days=10),
        #     stop_processing_offset=timedelta(days=1)
        # ),
        VerticaTableConfiguration(
            raw_table_name="ttd.EventTracker",
            retention_period=timedelta(days=90),
            filter_columns=[
                Pii.TDID,
            ],
            pii_columns=[Pii.TDID, Pii.IPAddress, Pii.PiiScrubReasons],
            tdid_special_transforms=[],
            scrub_type=ScrubType.SCRUB_PII_COLUMNS,
            start_processing_offset=timedelta(days=10),
            stop_processing_offset=timedelta(days=1)
        ),
        VerticaTableConfiguration(
            raw_table_name="ttd.VideoEvent",
            retention_period=timedelta(days=90),
            filter_columns=[Pii.TDID, Pii.UnifiedId2, Pii.EUID],
            pii_columns=[Pii.TDID, Pii.DeviceAdvertisingId, Pii.IPAddress, Pii.Zip, Pii.UnifiedId2, Pii.EUID, Pii.IdentityLinkId],
            tdid_special_transforms=[],
            scrub_type=ScrubType.SCRUB_PII_COLUMNS,
            start_processing_offset=timedelta(days=10),
            stop_processing_offset=timedelta(days=1)
        ),
        VerticaTableConfiguration(
            raw_table_name="ttd.BidFeedback",
            retention_period=timedelta(days=90),
            filter_columns=[Pii.TDID, Pii.UnifiedId2, Pii.EUID],
            pii_columns=[
                Pii.TDID, Pii.DeviceAdvertisingId, Pii.BrowserTDID, Pii.HashedIpAsUiid, Pii.IPAddress, Pii.Zip, Pii.Latitude, Pii.Longitude,
                Pii.UnifiedId2, Pii.EUID, Pii.IdentityLinkId, Pii.OriginalId, Pii.OriginatingId, Pii.HouseholdId, Pii.PersonId,
                Pii.PiiScrubReasons, Pii.BidRequestIPAddress
            ],
            tdid_special_transforms=[],
            scrub_type=ScrubType.SCRUB_PII_COLUMNS,
            start_processing_offset=timedelta(days=10),
            stop_processing_offset=timedelta(days=1)
        ),
        VerticaTableConfiguration(
            raw_table_name="ttd.InvalidBidFeedback",
            retention_period=timedelta(days=90),
            filter_columns=[Pii.TDID, Pii.UnifiedId2, Pii.EUID],
            pii_columns=[
                Pii.TDID, Pii.DeviceAdvertisingId, Pii.BrowserTDID, Pii.HashedIpAsUiid, Pii.IPAddress, Pii.Zip, Pii.Latitude, Pii.Longitude,
                Pii.UnifiedId2, Pii.EUID, Pii.IdentityLinkId, Pii.OriginatingId, Pii.PiiScrubReasons, Pii.BidRequestIPAddress
            ],
            tdid_special_transforms=[],
            scrub_type=ScrubType.SCRUB_PII_COLUMNS,
            start_processing_offset=timedelta(days=10),
            stop_processing_offset=timedelta(days=1)
        ),
        VerticaTableConfiguration(
            raw_table_name="ttd.TargetingDataUser_data",
            retention_period=timedelta(days=90),
            filter_columns=[
                Pii.TDID,
            ],
            pii_columns=[Pii.TDID],
            tdid_special_transforms=[],
            scrub_type=ScrubType.DELETE_ROW,
            start_processing_offset=timedelta(days=10),
            stop_processing_offset=timedelta(days=1)
        ),
        VerticaTableConfiguration(
            raw_table_name="reports.FrequencyReport",
            retention_period=timedelta(days=95),
            filter_columns=[  # handled by tdid special transforms
            ],
            pii_columns=[Pii.DeviceId],
            tdid_special_transforms=["DeviceId::Varchar"],
            scrub_type=ScrubType.SCRUB_PII_COLUMNS,
            start_processing_offset=timedelta(days=10),
            stop_processing_offset=timedelta(days=1)
        ),
        VerticaTableConfiguration(
            raw_table_name="ttd.ImpressionTracker",
            retention_period=timedelta(days=90),
            filter_columns=[Pii.TDID],
            pii_columns=[Pii.TDID, Pii.IPAddress, Pii.DATId, Pii.PiiScrubReasons],
            tdid_special_transforms=[],
            scrub_type=ScrubType.SCRUB_PII_COLUMNS,
            start_processing_offset=timedelta(days=10),
            stop_processing_offset=timedelta(days=1)
        ),
    ]
}

PARTNER_DSR_VERTICA_TABLE_CONFIGURATIONS_ADVERTISER: Dict[str, VerticaTableConfiguration] = {
    c.raw_table_name: c
    for c in [
        VerticaTableConfiguration(
            raw_table_name="ttd.ClickTracker",
            retention_period=timedelta(days=90),
            filter_columns=[
                Pii.EUID,
                Pii.TDID,
                Pii.UnifiedId2,
            ],
            pii_columns=[
                Pii.EUID,
                Pii.TDID,
                Pii.UnifiedId2,
                Pii.DATId,
                Pii.DeviceAdvertisingId,
                Pii.IPAddress,
                Pii.IdentityLinkId,
                Pii.Zip,
            ],
            tdid_special_transforms=[],
            scrub_type=ScrubType.SCRUB_PII_COLUMNS,
            start_processing_offset=timedelta(days=10),
            stop_processing_offset=timedelta(days=1)
        ),
        VerticaTableConfiguration(
            raw_table_name="ttd.VideoEvent",
            retention_period=timedelta(days=90),
            filter_columns=[
                Pii.EUID,
                Pii.TDID,
                Pii.UnifiedId2,
            ],
            pii_columns=[
                Pii.EUID,
                Pii.TDID,
                Pii.UnifiedId2,
                Pii.DATId,
                Pii.DeviceAdvertisingId,
                Pii.IPAddress,
                Pii.IdentityLinkId,
                Pii.Zip,
            ],
            tdid_special_transforms=[],
            scrub_type=ScrubType.SCRUB_PII_COLUMNS,
            start_processing_offset=timedelta(days=10),
            stop_processing_offset=timedelta(days=1)
        ),
        VerticaTableConfiguration(
            raw_table_name="ttd.ConversionTracker",
            retention_period=timedelta(days=90),
            filter_columns=[
                Pii.EUID,
                Pii.TDID,
                Pii.UnifiedId2,
            ],
            pii_columns=[
                Pii.EUID, Pii.TDID, Pii.UnifiedId2, Pii.DATId, Pii.DeviceId, Pii.IPAddress, Pii.IdentityLinkId, Pii.PiiScrubReasons,
                Pii.CookieId
            ],
            tdid_special_transforms=[],
            scrub_type=ScrubType.SCRUB_PII_COLUMNS,
            start_processing_offset=timedelta(days=10),
            stop_processing_offset=timedelta(days=1)
        ),
        VerticaTableConfiguration(
            raw_table_name="ttd.ImpressionTracker",
            retention_period=timedelta(days=90),
            filter_columns=[Pii.TDID],
            pii_columns=[Pii.TDID, Pii.IPAddress, Pii.DATId, Pii.PiiScrubReasons],
            tdid_special_transforms=[],
            scrub_type=ScrubType.SCRUB_PII_COLUMNS,
            start_processing_offset=timedelta(days=10),
            stop_processing_offset=timedelta(days=1)
        ),
    ]
}

PARTNER_DSR_VERTICA_TABLE_CONFIGURATIONS_MERCHANT: Dict[str, VerticaTableConfiguration] = {
    c.raw_table_name: c
    for c in [
        VerticaTableConfiguration(
            raw_table_name="ttd.RetailUserConversions",
            retention_period=timedelta(days=90),
            filter_columns=[Pii.TDID],
            pii_columns=[Pii.TDID],
            tdid_special_transforms=[],
            scrub_type=ScrubType.SCRUB_PII_COLUMNS,
            start_processing_offset=timedelta(days=10),
            stop_processing_offset=timedelta(days=1)
        ),
    ]
}
