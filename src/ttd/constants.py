from enum import Enum


class Constants:

    class Country:
        US = 'US'
        AU = 'AU'
        CA = 'CA'

    class ACRProviders:
        Gracenote = 'gracenote'
        Samba = 'samba'
        Inscape = 'inscape'
        Tivo = 'tivo'
        Fwm = 'fwm'
        FwmAudiences = 'fwm-audiences'
        ISpot = 'ispot'

    CrosswalkAcrProviders = [ACRProviders.Fwm, ACRProviders.FwmAudiences]


# Based on datetime.weekday()
class Day(Enum):
    MONDAY = 0
    TUESDAY = 1
    WEDNESDAY = 2
    THURSDAY = 3
    FRIDAY = 4
    SATURDAY = 5
    SUNDAY = 6


# These constants should be in sync with Provisioning.dbo.DataType
class DataTypeId(Enum):
    PaidSearchClicks = 1
    TrackingTag = 2
    FixedPriceUser = 3
    EmailOpen = 4
    ThirdPartyImpression = 5
    IPAddressRange = 6
    ThirdPartyData = 7
    ImportedAdvertiserData = 8
    ImportedAdvertiserDataWithBaseBid = 9
    HouseholdExtension = 11
    ClickRetargeting = 12
    Start = 13
    MidPoint = 14
    Complete = 15
    DirectIPTargeting = 16
    DynamicParameterRetargeting = 17
    BulkUserList = 18
    EcommerceCatalogList = 19
    CrmData = 20
    CampaignSeedData = 21


class ClusterDurations:
    ONE_DAY_IN_SECONDS = 60 * 60 * 24
    DEFAULT_MAX_DURATION = 0.75 * ONE_DAY_IN_SECONDS

    DURATION_VALUES = {
        "extremely_long": 10 * ONE_DAY_IN_SECONDS,
        "very_long": 5 * ONE_DAY_IN_SECONDS,
        "long": 1.5 * ONE_DAY_IN_SECONDS,
    }

    LEGACY_DURATION_TAGS = {
        "very_long_running": 5 * ONE_DAY_IN_SECONDS,
        "long_running": 1.5 * ONE_DAY_IN_SECONDS,
    }
