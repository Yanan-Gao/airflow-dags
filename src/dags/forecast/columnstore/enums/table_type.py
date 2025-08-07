from enum import Enum


class TableType(Enum):
    # Unused, retained for historical purposes
    Avails = 1
    # Unused, retained for historical purposes
    Audience = 2
    VectorValueMappings = 3
    IdMappedAvails = 4
    IdMappedAudience = 5
    V2IdMappedAvails = 6
    V2IdMappedAudience = 7
    RelevanceV2IdMappedAvails = 8
    RelevanceV2IdMappedAudience = 9
    RelevanceV2IdMappedClickHouseAvails = 10
    RelevanceV2IdMappedClickHouseAudience = 11
    ClickHouseVectorValueMappings = 12
    RelevanceV2DealOptimisedClickHouseAvails = 13
