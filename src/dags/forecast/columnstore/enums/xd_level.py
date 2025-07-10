from enum import Enum


class XdLevel(Enum):
    # We've shut down the DAGs using these values, so they're not being used at the moment.
    # At some point, we may transition the opengraph DAGs to use these instead of the values below.
    no_xd_level = 0
    adbrain_person_level = 10
    adbrain_household_level = 11
    # These values below may be temporary, and I've written some fragile code relating to them,
    # so do not add more XD levels without checking each usage of this enum, as it will break things.
    v2_no_xd_level = 1000
    opengraph_person_level = 1010
    opengraph_household_level = 1011


def get_tables(xd_level: XdLevel, sample_rate) -> list[str]:
    xd_level_name = get_xd_level_name(xd_level)
    return [
        f"{xd_level_name}AvailsWithAttributionId_SampleRate_{sample_rate}",
        f"{xd_level_name}AudienceWithAttributionId_SampleRate_{sample_rate}"
    ]


def get_xd_level_name(xd_level: XdLevel) -> str:
    match xd_level:
        case XdLevel.no_xd_level:
            return "NoXd"
        case XdLevel.adbrain_person_level:
            return "Person"
        case XdLevel.adbrain_household_level:
            return "Household"
        case XdLevel.v2_no_xd_level:
            return "V2NoXd"
        case XdLevel.opengraph_person_level:
            return "V2Person"
        case XdLevel.opengraph_household_level:
            return "V2Household"
        case _:
            raise NotImplementedError


def get_xd_level_class_name(xd_level: XdLevel) -> str:
    match xd_level:
        case XdLevel.no_xd_level:
            return "NoXd"
        case XdLevel.adbrain_person_level:
            return "Person"
        case XdLevel.adbrain_household_level:
            return "Household"
        case XdLevel.v2_no_xd_level:
            return "OpenGraphNoXd"
        case XdLevel.opengraph_person_level:
            return "OpenGraphPerson"
        case XdLevel.opengraph_household_level:
            return "OpenGraphHousehold"
        case _:
            raise NotImplementedError


def get_mapped_pinning_function(xd_level: XdLevel) -> str:
    xd_level_name = get_xd_level_name(xd_level)
    return f"PinMapped{xd_level_name}Tables"


def is_v2_xd_level(xd_level: XdLevel) -> bool:
    return xd_level in {XdLevel.v2_no_xd_level, XdLevel.opengraph_person_level, XdLevel.opengraph_household_level}


def is_s3_migrated(xd_level: XdLevel) -> bool:
    return xd_level in {XdLevel.opengraph_person_level, XdLevel.opengraph_household_level}
