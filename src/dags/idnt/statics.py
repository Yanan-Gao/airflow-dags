"""Store common configurations and statics here
"""

from ttd.slack.slack_groups import IDENTITY
from ttd.ttdenv import TtdEnvFactory


class Applications:
    """Simple enum for application categories."""
    adbrain_graph = "adbrain"
    iav2_graph = "iav2"
    open_graph = "openGraph"


class VendorTypes:
    """Simple enum for vendor types we input from."""
    uid2_type = "UID2"
    ia_type = "IA"
    deterministic_type = "Deterministic"


class Tags:
    """Tags for clusters and DAGs"""

    cluster = {"Team": IDENTITY.team.jira_team}
    dag = ["IDNT"]
    slack_channel_alarms_testing: str = "#scrum-identity-alarms-testing"

    @staticmethod
    def environment() -> str:
        return TtdEnvFactory.get_from_system().execution_env

    @staticmethod
    def slack_channel() -> str:
        return IDENTITY.team.alarm_channel if TtdEnvFactory.get_from_system().execution_env == "prod" else Tags.slack_channel_alarms_testing


class RunTimes:
    """Useful JINJA templates for runtime variables

    Note that these are all strings but formatted the expected way: `"%Y-%m-%d %H:%M:%S"`
    or `"%Y-%m-%d"`
    """
    current_interval_start = """{{  data_interval_start.to_datetime_string() }}"""
    current_interval_end = """{{  data_interval_end.to_datetime_string() }}"""
    current_full_day_raw = "data_interval_end.start_of('day').to_date_string()"
    current_full_day = f"""{{{{ {current_full_day_raw} }}}}"""
    previous_full_day_raw = "(data_interval_end.start_of('day') - macros.timedelta(days=1)).to_date_string()"
    previous_full_day = f"""{{{{ {previous_full_day_raw} }}}}"""
    days_02_ago_full_day = """{{  (data_interval_end.start_of('day') - macros.timedelta(days=2)).to_date_string() }}"""

    previous_day_last_hour = """{{ (data_interval_end.start_of('day') - macros.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') }}"""
    days_02_ago_last_hour = """{{ (data_interval_end.start_of('day') - macros.timedelta(hours=25)).strftime('%Y-%m-%d %H:%M:%S') }}"""

    @classmethod
    def date_x_days_ago(cls, x: int) -> str:
        return f"""{{{{ (data_interval_end - macros.timedelta(days={x})).start_of('day').to_date_string() }}}}"""

    @classmethod
    def current_interval_x_hour_ago(cls, x: int, format: str) -> str:
        return f"""{{{{ (data_interval_end - macros.timedelta(hours={x})).strftime(\'{format}\') }}}}"""


class Executables:
    """Paths to executables and EMR versions"""

    emr_version_6 = "emr-6.11.1"

    emr_version = emr_version_6

    identity_repo_executable = "s3://ttd-identity/ops/jars/identity/release/com/thetradedesk/idnt/identity_2.12/latest/identity-assembly.jar"
    # this is set as such due to spark3.3 rollback. once ready to upgrade, upgrade jobs one by one
    identity_repo_executable_spark_3_3 = identity_repo_executable
    identity_repo_executable_azure = "abfs://ttd-build-artefacts@ttdartefacts.dfs.core.windows.net/identity/release/com/thetradedesk/idnt/identity_2.12/latest/identity-assembly.jar"  # temporarily reverted to unblock crediting
    identity_repo_executable_aliyun = "oss://ttd-identity/ops/jars/identity/release/com/thetradedesk/idnt/identity_2.12/latest/identity-assembly.jar"
    identity_repo_class = "com.thetradedesk.idnt.identity"
    etl_repo_executable = "s3://ttd-identity/ops/jars/etl/etl-assembly-spark3.jar"
    etl_repo_executable_china = "oss://ttd-identity/ops/jars/etl/etl-assembly-spark3.jar"
    etl_driver_class = "com.thetradedesk.etl.logformats.ETLDriver"
    # dat executable needs EMR 6.9.0 or higher
    dat_executable = "s3://thetradedesk-mlplatform-us-east-1/mlops/feast/nexus/snapshot/uberjars/latest/com/thetradedesk/ds/libs/graphextension_2.12-assembly.jar"
    avails_etl_v2_executable = "s3://ttd-identity/ops/jars/avails-etl/avails-etl_2.12-assembly.jar"
    set_ganglia_memory_limit_script = "s3://ttd-identity/ops/scripts/infrastructure/set_ganglia_memory_limit.sh"

    @classmethod
    def class_name(cls, sub_name: str) -> str:
        """Return the full class name of an identity repo job

        Args:
            sub_name (str): Name of the object you want to run inside `com.thetradedesk.idnt.identity`.

        Returns:
            str: Full class name with `com.thetradedesk.idnt.identity` pre-prended.
        """
        return f"{cls.identity_repo_class}.{sub_name}"


class Directories:
    """Used for backwards compatibility to edit input and output paths for testing purposes.
    After full migration to DataSteps, we shouldn't need these.
    """

    @staticmethod
    def test_dir_no_prod() -> str:
        """If in prod environment, then return nothing. Else, return /test to write to a test path.
        """
        return '' if str(Tags.environment()) == 'prod' else '/test'


class Geos:
    # as defined here: https://gitlab.adsrvr.org/thetradedesk/teams/idnt/identity/-/blob/main/src/main/scala/com/thetradedesk/idnt/identity/utils/CountryUtils.scala#L6
    ttdgraph_regions = {"NAMER": ["NAMER"], "EMEALATAM": ["LATAM", "EUROPE", "MEA"], "ASIA": ["ANZ", "APAC"]}
