import logging
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from requests.auth import HTTPBasicAuth

# ─── Logging Configuration ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Constants ──────────────────────────────────────────────────────────────
SPRINT_HISTORY_TABLE = "EDWSTAGING.JIRA.SPRINT_HISTORY"
MAPPING_TABLE = "EDWSTAGING.JIRA.ID_KEY_MAPPING"
SEARCH_ENDPOINT = "https://thetradedesk.atlassian.net/rest/api/3/search/jql"


def fetch_and_load_id_key_mapping(
    sf_conn: dict,
    jira_user: str,
    jira_pwd: str,
    batch_size: int = 500,
    flush_size: int = 20000,
) -> None:
    """
    1) Read distinct ticket_ids from the SPRINT_HISTORY table in Snowflake.
    2) For each batch of ticket_ids, call the Jira Search API to retrieve their keys,
       handling pagination per batch if total > maxResults.
    3) Stream the (ticket_id, ticket_key) mapping into Snowflake.
    """
    # Truncate target mapping table
    with snowflake.connector.connect(**sf_conn, insecure_mode=True) as conn:
        conn.cursor().execute(f"TRUNCATE TABLE {MAPPING_TABLE}")
    logger.info(f"Truncated {MAPPING_TABLE}")

    # 1) Fetch ticket list
    with snowflake.connector.connect(**sf_conn, insecure_mode=True) as conn:
        df = conn.cursor().execute(f"SELECT DISTINCT ticket_id FROM {SPRINT_HISTORY_TABLE}").fetch_pandas_all()
    ticket_list = df['TICKET_ID'].dropna().astype(int).tolist()
    logger.info(f"Fetched {len(ticket_list)} ticket IDs to map")

    buffer = []
    total_loaded = 0

    # 2) Process in batches via Search API with pagination
    for i in range(0, len(ticket_list), batch_size):
        batch = ticket_list[i:i + batch_size]
        ids_str = ",".join(str(t) for t in batch)
        jql = f"id in ({ids_str})"
        next_page_token = None

        while True:
            params = {
                "jql": jql,
                "fields": "key",
                "maxResults": batch_size,
            }

            # Add nextPageToken if available
            if next_page_token:
                params["nextPageToken"] = next_page_token

            logger.info(f"Fetching keys for tickets {i+1}-{i+len(batch)}, nextPageToken={next_page_token}")

            resp = requests.get(
                SEARCH_ENDPOINT,
                params=params,
                auth=HTTPBasicAuth(jira_user, jira_pwd),
            )
            resp.raise_for_status()
            data = resp.json()

            # Collect issues
            for issue in data.get("issues", []):
                issue_id = int(issue.get("id", 0))
                key = issue.get("key")
                buffer.append((issue_id, key))

                # Check if this is the last page
            if data.get("isLast", False):
                logger.info("Reached last page of results for this batch")
                break

            # Get next page token for next iteration
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                logger.info("No nextPageToken found, ending pagination for this batch")
                break

            # Flush mid-pagination if needed
            if len(buffer) >= flush_size:
                total_loaded += _flush_mapping_buffer(buffer, sf_conn)
                buffer.clear()

        # Flush after each batch
        if len(buffer) >= flush_size:
            total_loaded += _flush_mapping_buffer(buffer, sf_conn)
            buffer.clear()

    # 3) Final flush
    if buffer:
        total_loaded += _flush_mapping_buffer(buffer, sf_conn)
        buffer.clear()

    logger.info(f"Done mapping. Total rows loaded: {total_loaded}")


def _flush_mapping_buffer(buffer: list[tuple], sf_conn: dict) -> int:
    """
    Write ID-to-key rows into Snowflake and return row count.
    """
    import pandas as pd

    df = pd.DataFrame(buffer, columns=["TICKET_ID", "TICKET_KEY"])
    with snowflake.connector.connect(**sf_conn, insecure_mode=True) as conn:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name="ID_KEY_MAPPING",
            schema="JIRA",
            database="EDWSTAGING",
            use_logical_type=True,
            auto_create_table=False
        )
    logger.info(f"Flushed {nrows} mappings (chunks={nchunks}, success={success})")
    return nrows
