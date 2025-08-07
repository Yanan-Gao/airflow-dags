from enum import StrEnum


class MappingType(StrEnum):
    INTERSECT_COMBINATION_TYPE = "intersect"
    UNION_COMBINATION_TYPE = "union"

    AUDIENCE_COMBINATION_MAPPING_TYPE = "audienceCombination"
    INTERSECT_COMBINATION_MAPPING_TYPE = "intersectCombination"
    UNION_COMBINATION_MAPPING_TYPE = "unionCombination"

    AUDIENCE_COMBINATION_LIST_NAME = "audienceOnly"
    INTERSECT_COMBINATION_LIST_NAME = "availSourcedRandomIntersect"
    UNION_COMBINATION_LIST_NAME = "availSourcedRandomUnion"
    AUDIENCE_SUFFIX = "PlusAudience"
    PARTIAL_SUFFIX = "Partial"
