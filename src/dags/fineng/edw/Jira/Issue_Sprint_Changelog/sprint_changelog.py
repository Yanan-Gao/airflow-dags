import logging
import requests
import pytz
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from requests.auth import HTTPBasicAuth

# ─── Logging Configuration ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Constants ───────────────────────────────────────────────────────────────
CHANGELOG_ENDPOINT = "https://thetradedesk.atlassian.net/rest/api/3/changelog/bulkfetch"
TABLE_NAME = "SPRINT_HISTORY"
SCHEMA = "JIRA"
DATABASE = "EDWSTAGING"

calc_diff = lambda f, t: ",".join([
    new.strip() for new in (t or "").split(",") if (nv := new.strip()) and nv not in {old.strip()
                                                                                      for old in (f or "").split(",")}
])


def to_epoch_ms(dt_str: str | None) -> int | None:
    """
    Parse an ISO-8601 timestamp string into milliseconds since epoch (UTC),
    or return None if the input is falsy.
    """
    if not dt_str:
        return None
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return int(dt.astimezone(pytz.UTC).timestamp() * 1000)


def get_ticket_list(sf_conn: dict, ticket_query: str) -> list[str]:
    """
    Fetch ticket keys/IDs from Snowflake using the provided SQL query.
    Returns a list of strings.
    """
    print(f"Fetching tickets from Snowflake with query: {ticket_query}")
    print(f"Snowflake connection: {sf_conn}")
    print(f"password: {sf_conn['password']}")
    with snowflake.connector.connect(**sf_conn, insecure_mode=True) as conn:
        cursor = conn.cursor()
        cursor.execute(ticket_query)
        rows = cursor.fetchall()
    tickets = [str(r[0]) for r in rows]
    logger.info(f"Fetched {len(tickets)} tickets from Snowflake")
    return tickets


def _flush_buffer(buffer: list[tuple], sf_conn: dict) -> int:
    """
    Write the buffered rows to Snowflake via write_pandas,
    then return the number of rows written.
    """
    import pandas as pd

    df = pd.DataFrame(buffer, columns=["CREATED_AT", "FROM_VALUE", "TO_VALUE", "DIFF", "TICKET_ID"])
    with snowflake.connector.connect(**sf_conn, insecure_mode=True) as conn:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn, df=df, table_name=TABLE_NAME, schema=SCHEMA, database=DATABASE, use_logical_type=True, auto_create_table=False
        )
    logger.info(f"Flushed {nrows} rows (chunks={nchunks}, success={success})")
    return nrows


def fetch_and_load_sprint_history(
    sf_conn: dict,
    jira_user: str,
    jira_pwd: str,
    batch_size: int = 500,
    flush_size: int = 20_000,
    field_ids: list[str] = ["customfield_10020"],
) -> None:
    """
    1) Fetch ticket list from Snowflake.
    2) Fetch sprint-change-history for those tickets via Jira's bulkfetch endpoint.
    3) Load into Snowflake in streaming batches.
    """
    # Truncate the target table once
    with snowflake.connector.connect(**sf_conn) as conn:
        conn.cursor().execute(f"TRUNCATE TABLE {DATABASE}.{SCHEMA}.{TABLE_NAME}")
    logger.info(f"Truncated {DATABASE}.{SCHEMA}.{TABLE_NAME}")

    ticket_query = """
    select distinct(key) from edw.report.vw_jira_master_tickets
    """

    # 1) Fetch tickets
    ticket_list = get_ticket_list(sf_conn, ticket_query)

    buffer = []
    total_loaded = 0

    # 2) Process tickets in batches
    for i in range(0, len(ticket_list), batch_size):
        batch = ticket_list[i:i + batch_size]
        logger.info(f"Processing tickets {i+1}-{i+len(batch)}")

        next_page_token = None
        while True:
            payload = {"fieldIds": field_ids, "issueIdsOrKeys": batch}
            if next_page_token:
                payload["nextPageToken"] = next_page_token

            resp = requests.post(
                CHANGELOG_ENDPOINT,
                json=payload,
                auth=HTTPBasicAuth(jira_user, jira_pwd),
            )
            resp.raise_for_status()
            data = resp.json()

            for issue_log in data.get("issueChangeLogs", []):
                issue_id = issue_log.get("issueId")
                for history in issue_log.get("changeHistories", []):
                    created_ms = history.get("created")
                    for item in history.get("items", []):
                        buffer.append((
                            created_ms,
                            item.get("fromString"),
                            item.get("toString"),
                            # calculate diff, by subtracting fromString from toString
                            calc_diff(item.get("fromString"), item.get("toString")),
                            int(issue_id) if issue_id else None,
                        ))

            next_page_token = data.get("nextPageToken")
            logger.info(f"Next page token: {next_page_token!r}")
            if not next_page_token:
                break

        # 3) Flush if buffer exceeds flush_size
        if len(buffer) >= flush_size:
            n = _flush_buffer(buffer, sf_conn)
            total_loaded += n
            buffer.clear()

    # 4) Final flush of any remaining rows
    if buffer:
        n = _flush_buffer(buffer, sf_conn)
        total_loaded += n
        buffer.clear()

    logger.info(f"Done. Total rows loaded: {total_loaded}")
