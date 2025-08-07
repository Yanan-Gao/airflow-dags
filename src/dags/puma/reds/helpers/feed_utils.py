"""
Helper methods for feeds related operations
"""
from datetime import timedelta
import re


def get_expired_date(current_date, enable_date, start_date, retention_period):
    '''
    Get the date that a feed is expired
    '''
    earliest_retention_date = current_date - timedelta(days=retention_period)

    feed_production_start_date = max(enable_date, start_date)

    if (earliest_retention_date > feed_production_start_date):
        # For backfill feeds, the feeds across multiple days might be generated in a short period (hours), technically these feeds
        # should be deleted together in a single DAG run but this is too complicated to implement.
        #
        # To simiplify the handling of backfill feeds we will delete one day's worth of backfill feeds in a single DAG run.
        # The retention period of backfill feeds will thus increase by the number of days of backfill.
        return start_date + timedelta(days=(earliest_retention_date - feed_production_start_date).days - 1)


def replace_placeholders(template, variables, allowed_placeholders=None, raise_on_unresolved=False):
    """
    Replace destination template placeholders that are wrapped in <> with values, case-insensitive
    """

    variables_lower = {k.lower(): v for k, v in variables.items()}

    def replace(match):
        placeholder_key = match.group(1).lower()
        if placeholder_key in variables_lower:
            if variables_lower[placeholder_key] is None:
                # Return empty string if variable value is none, this is to match the behavior of AdPlatform
                # See https://gitlab.adsrvr.org/thetradedesk/adplatform/-/blob/master/TTD/Domain/Shared/TTD.Domain.Shared.PlatformControllers/ReportScheduling/Exposure/ExposureFeedDestinationLocationHelper.cs#L33
                return ''
            return str(variables_lower[placeholder_key])
        else:
            # If no match, return the placeholder
            return f'<{match.group(1)}>'

    regex = re.compile(r'<([^>]*)>')  # Match values inside angle braces
    result = regex.sub(replace, template)
    check_unresolved_placeholders(result, allowed_placeholders=allowed_placeholders, raise_on_unresolved=raise_on_unresolved)
    return result


def check_unresolved_placeholders(path, allowed_placeholders=None, raise_on_unresolved=False):
    """
    Check for any unresolved placeholders in the template and raise an exception if found
    """
    if allowed_placeholders is None:
        allowed_placeholders = []

    regex = re.compile(r'<([^>]*)>')
    matches = regex.findall(path)

    unresolved_placeholders = [f'<{placeholder}>' for placeholder in matches if f'<{placeholder}>' not in allowed_placeholders]

    if unresolved_placeholders:
        message = f"Unresolved placeholders {', '.join(unresolved_placeholders)} found in: {path}"
        if raise_on_unresolved:
            raise ValueError(message)
        else:
            print(f'Warning: {message}')


def split_location_by_date(template):
    '''
    Split a destination location template into two parts - everything before date (inclusive of date) and after date (exclusive of date)
    For example
        - liveramp/exposure/brand/<partnerid>/<version>/date=<date>/hour=<hour>/<partitionkey>_impressions_<partnerid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz

        would be split into

        - liveramp/exposure/brand/<partnerid>/<version>/date=<date> and
        - /hour=<hour>/<partitionkey>_impressions_<partnerid>_ver_<version>_<starttime>_<endtime>_<processedtime>_<feedid>_<hash>.log.gz
    '''
    parts = template.partition('<date>')

    return parts[0] + parts[1], parts[2]


def get_delimiter_from_sql_query(export_sql: str):
    """
    Parse the delimiter from feed export SQL query
    e.g.
    -------------------------------------------------------------------------------------------------------------------------------
    COPY INTO @EXPOSURE_FEEDS_STAGE
    FROM
    (
        [SQLQUERY]
    )
    PARTITION BY ('/date=' || to_varchar(LogEntryTime, 'YYYY-MM-DD') || '/hour=' || to_varchar(date_part(hour, LogEntryTime)) ||
        CASE
            WHEN DeviceType IN (5, 6) AND DeviceAdvertisingId IS NOT NULL THEN '/partitionkey=ctv'
            WHEN DeviceType NOT IN (5, 6) AND DeviceAdvertisingId IS NOT NULL THEN '/partitionkey=mobile'
            WHEN DeviceType NOT IN (5, 6) AND DeviceAdvertisingId IS NULL THEN '/partitionkey=cookie'
            ELSE '/partitionkey=others'
        END)
    FILE_FORMAT =
    (
        TYPE = CSV
        COMPRESSION = GZIP
        FIELD_OPTIONALLY_ENCLOSED_BY = NONE
        NULL_IF = ('')
        EMPTY_FIELD_AS_NULL = FALSE
        FIELD_DELIMITER = '\t')
    HEADER = TRUE
    MAX_FILE_SIZE = 256000000;
    -------------------------------------------------------------------------------------------------------------------------------
    Get delimiter = '\t'
    """
    match = re.search(r'FIELD_DELIMITER\s*=\s*\'(.*?)\'', export_sql)
    if match:
        return match.group(1)  # Extract the matched substring from the first group
    else:
        raise ValueError("No delimiter to match from the query")
