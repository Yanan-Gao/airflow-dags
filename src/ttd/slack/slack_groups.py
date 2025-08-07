from typing import Optional


class SlackTeam:

    def __init__(self, name: str, jira_team: str, alarm_channel: str):
        self.name = name
        self.jira_team = jira_team
        self.alarm_channel = alarm_channel
        self.sub_team: Optional[str] = None
        self.ops_genie_team: Optional[str] = None

    def with_sub_team(self, sub_team: str):
        self.sub_team = sub_team
        return self

    def with_ops_genie_team(self, ops_genie_team: str):
        self.ops_genie_team = ops_genie_team
        return self


class AIFUN:
    team = SlackTeam(name="AI Fundamentals", jira_team="AIFUN", alarm_channel="#dev-aifun-alerts")


class AUDAUTO:
    team = SlackTeam(
        name="Audience Automation",
        jira_team="AUDAUTO",
        alarm_channel="#scrum-perf-automation-alerts",
    ).with_sub_team("AUDAUTO")


class CTV:
    team = SlackTeam(name="CTV", jira_team="TV", alarm_channel="#scrum-tv-alarms")

    @classmethod
    def segments(cls) -> SlackTeam:
        return cls.team.with_sub_team("<!subteam^S01DWQ5N5NJ>")

    @classmethod
    def avails_stream(cls) -> SlackTeam:
        return cls.team.with_sub_team("<!subteam^S01ETRC4STA>")

    @classmethod
    def upstream_insights(cls) -> SlackTeam:
        return cls.team.with_sub_team("<!subteam^S03C3P5CCPJ>")

    @classmethod
    def nielsen(cls) -> SlackTeam:
        return cls.team.with_sub_team("<!subteam^S01E0DS3JMT>")


class CHGROW:
    team = SlackTeam(name="Channels Growth", jira_team="CHGROW", alarm_channel="#scrum-chgrow-alarms")

    @classmethod
    def channels_growth(cls) -> SlackTeam:
        # @dev-channels-growth
        return cls.team.with_sub_team("<!subteam^S05BE6ASLBV>")


class CMO:
    team = SlackTeam(name="CMO", jira_team="CMO", alarm_channel="#scrum-cmo-alarms")
    team.with_sub_team("<!subteam^S01DWQ5N5NJ>")  # old DEV_CTV_ACR_SEGMENTS until we rename on slack


class PFX:
    team = SlackTeam(name="PFX", jira_team="PFX", alarm_channel="#scrum-pfx-alerts")

    @classmethod
    def dev_ctv_forecasting_tool(cls) -> SlackTeam:
        return cls.team.with_sub_team("<!subteam^S05DAR9P8TW>")


class CTXMP:
    team = SlackTeam(name="Contextual Marketplace", jira_team="CTXMP", alarm_channel="#scrum-contextual-marketplace-alarms")


adsrv = SlackTeam(name="Ad Serving", jira_team="ADSRV", alarm_channel="#adserving-alerts")
bid = SlackTeam(name="Bidder", jira_team="BID", alarm_channel="#scrum-bidder-alarms")
trex = SlackTeam(name="Trading Experience Excellence ", jira_team="TREX", alarm_channel="#scrum-trex-alerts")
china = SlackTeam(
    name="China Integrations",
    jira_team="CHINA",
    alarm_channel="#scrum-china-integration-alarms",
)
chnl = SlackTeam(name="Channels", jira_team="CHNL", alarm_channel="#scrum-edge-alerts")
cmkt = SlackTeam(name="Consumer markets", jira_team="CMKT", alarm_channel="#scrum-test-alarms")
cre = SlackTeam(name="Creatives", jira_team="CRE", alarm_channel="#dev-creatives-alerts")
cx = SlackTeam(name="Core Experiences", jira_team="CX", alarm_channel="#scrum-core-exp")
csx = SlackTeam(
    name="Customer Success Excellence",
    jira_team="CSX",
    alarm_channel="#scrum-csx-alerts",
)
dataproc = SlackTeam(name="DataProc", jira_team="DATAPROC", alarm_channel="#scrum-dataproc-alerts").with_sub_team("<!subteam^SSE3N7MC2>")
deskui = SlackTeam(name="Desk UI", jira_team="DESKUI", alarm_channel="#scrum-dna-alerts")
dist = SlackTeam(name="Distributed Computing", jira_team="DIST", alarm_channel="#scrum-dist-alarms")
dprpts = SlackTeam(name="Data Platform Reports", jira_team="DPRPTS", alarm_channel="#scrum-dp-rpts").with_sub_team("<!subteam^S04C8FETFJQ>")
fineng = SlackTeam(name="Business Engineering", jira_team="FINENG", alarm_channel="#scrum-bizeng")
hpc = SlackTeam(
    name="High Performance Computing",
    jira_team="HPC",
    alarm_channel="#scrum-hpc-alerts",
)
mqe = SlackTeam(name="Marketplace Quality", jira_team="MQE", alarm_channel="#scrum-mqe-alarms")

partportal = SlackTeam(
    name="Partner Portal",
    jira_team="PARTPORTAL",
    alarm_channel="#scrum-partportal-alarms",
)

pdg = SlackTeam(
    name="Privacy and Data Governance",
    jira_team="PDG",
    alarm_channel="#scrum-pdg-alerts",
)

puma = SlackTeam(
    name="Platform User Management and Auth",
    jira_team="PUMA",
    alarm_channel="#scrum-puma-alerts",
)
sav = SlackTeam(
    name="Sensitive Advertising Verticals",
    jira_team="SAV",
    alarm_channel="#scrum-sav-alerts",
)
targeting = SlackTeam(name="Targeting", jira_team="TRGT", alarm_channel="#scrum-targeting-alarms")


class DATASRVC:
    team = SlackTeam(
        name="Data Services",
        jira_team="DATASRVC",
        alarm_channel="#scrum-data-services-alarms",
    )


class DATMKT:
    team = SlackTeam(
        name="Data Marketplace",
        jira_team="DATMKT",
        alarm_channel="#scrum-datamarketplace-alerts",
    ).with_sub_team("DATMKT")


class DATPRD:
    team = SlackTeam(
        name="Data Products",
        jira_team="DATPRD",
        alarm_channel="#scrum-data-products-alarms",
    )


class ADPB:
    team = SlackTeam(
        name="Audience, Data Pricing & Bidding",
        jira_team="ADPB",
        alarm_channel="#scrum-adpb-alerts",
    ).with_sub_team("<!subteam^C04MREBCQ3W>")


class DATPERF:
    team = SlackTeam(
        name="Performance Automation",
        jira_team="DATPERF",
        alarm_channel="#scrum-perf-automation-alerts",
    ).with_sub_team("DATPERF")


class DOOH:
    team = SlackTeam(name="Digital Out Of Home", jira_team="EDGE", alarm_channel="#scrum-edge-alerts")


class FORECAST:
    team = SlackTeam(
        name="Forecasting Services",
        jira_team="FORECAST",
        alarm_channel="#dev-forecasting-alerts",
    ).with_sub_team("FORECAST")

    @classmethod
    def data_charter(cls) -> SlackTeam:
        return cls.team.with_sub_team("<!subteam^S062XKTFVU1>")


class FORECAST_TEST:
    team = SlackTeam(
        name="Forecasting Services",
        jira_team="FORECAST",
        alarm_channel="#dev-forecasting-alerts-test",
    )


class FORECAST_MODELS_CHARTER:
    team = SlackTeam(
        name="Forecasting Services Models Charter", jira_team="FORECAST", alarm_channel="#dev-forecast-models-charter-alerts"
    ).with_sub_team("<!subteam^C038K8BP7UJ>")


class IDENTITY:
    team = SlackTeam(name="Identity", jira_team="IDNT", alarm_channel="#scrum-identity-alarms")


class INVENTORY_MARKETPLACE:
    team = SlackTeam(name="Inventory Marketplace", jira_team="INVMKT", alarm_channel="#scrum-invmkt-alarms")


class DEAL_MANAGEMENT_EXPERIENCE:
    team = SlackTeam(name="Deal Management Experience", jira_team="DME", alarm_channel="#scrum-dme-alarms")


class DEAL_MANAGEMENT:
    team = SlackTeam(name="Deal Management", jira_team="DMG", alarm_channel="#scrum-dmg-alarms")


class MARKETS:
    team = SlackTeam(name="Markets", jira_team="MKTS", alarm_channel="#scrum-markets-alarms")


class FORWARD_MARKET:
    team = SlackTeam(name="Forward Market", jira_team="FWMKT", alarm_channel="#scrum-fwmkt-alarms")


class MEASUREMENT:
    team = SlackTeam(
        name="Measurement",
        jira_team="MEASURE",
        alarm_channel="#scrum-measurement-alarms",
    )


class MEASURE_TASKFORCE_CAT:
    team = SlackTeam(name="Measurement Taskforce - CAT", jira_team="MEASURE", alarm_channel="#taskforce-cat-alarms")


class MEASURE_TASKFORCE_LIFT:
    team = SlackTeam(name="Taskforce Lift", jira_team="MEASURE", alarm_channel="#taskforce-lift-alarms")


class MEW:
    team = SlackTeam(name="Measurement Engagement Workflows", jira_team="MEW", alarm_channel="#scrum-mew-alarms")


class MEASUREMENT_UPPER:
    team = SlackTeam(
        name="Measurement Upper Funnel",
        jira_team="MEASURE",
        alarm_channel="#scrum-measurement-up-alarms",
    )


class OPATH:
    team = SlackTeam(name="Open Path", jira_team="OPATH", alarm_channel="#scrum-openpath-alerts")


class PSR:
    team = SlackTeam(name="PSR", jira_team="PSR", alarm_channel="#scrum-psr-alerts")


class OMNIUX:
    team = SlackTeam(name="OMNIUX", jira_team="OMNIUX", alarm_channel="#scrum-omniux-alarms")

    @classmethod
    def omniux(cls) -> SlackTeam:
        return cls.team.with_sub_team("<!subteam^S03C3P5CCPJ>")


class UID2:
    team = SlackTeam(name="UID2", jira_team="UID2", alarm_channel="#dev-uid2-alarms")


class JAN:
    team = SlackTeam(name="Janus", jira_team="JAN", alarm_channel="#dev-janus-alerts")


#####################################################################################
# BELOW IS DEPRECATED
#####################################################################################

# TEAMS/Slack Channels
TEAM_CTV = "#scrum-tv-alarms"  # Replaced Above

# Subgroups
DEV_CTV_ACR_GRACENOTE = "<!subteam^S01DWPX8VC6>"
DEV_CTV_ACR_INSCAPE = "<!subteam^S01E3G31GF4>"
DEV_CTV_ACR_INSCAPE_IRR = "<!subteam^S01E0DJEHGV>"
DEV_CTV_ACR_INSIGHTS = "<!subteam^S01ETBJAS1E>"
DEV_CTV_ACR_SAMBA = "<!subteam^S01DWQ4ED1C>"
DEV_CTV_FREQ_REPORTING = "<!subteam^S01EG5DCNU9>"
DEV_CTV_MONITORING_TOOLS = "<!subteam^S01E3NDQJH1>"
DEV_CTV_NIELSEN = "<!subteam^S01E0DS3JMT>"  # Replaced Above
DEV_CTV_REACH_REPORTING = "<!subteam^S01EG5HMHT3>"

DEV_CTV_ACR_SEGMENTS = "<!subteam^S01DWQ5N5NJ>"  # Replaced Above
DEV_CTV_AVAILS_STREAM = "<!subteam^S01ETRC4STA>"  # Replaced Above

DEV_SCRUM_DATAPROC = "<!subteam^SSE3N7MC2>"
