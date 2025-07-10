from enum import IntEnum
"""
This class defines constants for task bash brain types.
They have same values as dbo.fn_Enum_TaskBatchGrain_XXX() in LogWorkflow database
"""


class LogWorkflowTaskBatchGrain(IntEnum):
    hourly = 100001
    daily = 100002
    weekly = 100003


"""
This class defines constants for gating types.
They have same values as dbo.fn_Enum_GatingType_XXX() in LogWorkflow database
"""


class LogWorkflowGatingType(IntEnum):
    """
    dbo.fn_Enum_GatingType_FrequencyReportGuidance()
    """
    frequency_report_guidance = 2000128
    """
    dbo.fn_enum_GatingType_FrequencyReportGuidanceWalmart()
    """
    frequency_report_guidance_walmart = 2000471
