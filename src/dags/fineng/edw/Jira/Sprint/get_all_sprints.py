import requests
import pytz
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging

# ─── Logging Configuration ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

base_url = "thetradedesk.atlassian.net"


def get_all_boards(jira_user: str, jira_pwd: str) -> list[int]:
    """
    Fetch all Jira board IDs via the Agile API, handling pagination.
    """
    board_ids: list[int] = []
    url = f"https://{base_url}/rest/agile/1.0/board"
    auth = (jira_user, jira_pwd)
    headers = {"Accept": "application/json"}

    while True:
        resp = requests.get(url, auth=auth, headers=headers)
        if resp.status_code != 200:
            logger.error(f"Error fetching boards: HTTP {resp.status_code}")
            resp.raise_for_status()
        data = resp.json()
        board_ids.extend(b.get("id") for b in data.get("values", []))
        if data.get("isLast"):
            break
        start_at = data.get("startAt", 0) + data.get("maxResults", 0)
        url = f"https://{base_url}/rest/agile/1.0/board?startAt={start_at}"

    logger.info(f"Retrieved {len(board_ids)} boards")
    return board_ids


def get_sprints_and_stream_load(sf_conn: dict, jira_user: str, jira_pwd: str, flush_size: int = 20_000) -> None:
    """
    Stream-fetch all sprints for all boards, flushing to Snowflake every
    `flush_size` rows to avoid high memory usage.
    """
    # 1) Truncate target table once
    with snowflake.connector.connect(**sf_conn, insecure_mode=True) as conn:
        conn.cursor().execute("TRUNCATE TABLE EDWSTAGING.JIRA.SPRINT_DATA")
    logger.info("Truncated EDWSTAGING.JIRA.SPRINT_DATA table")

    buffer: list[dict] = []
    total_loaded = 0

    def flush(buf: list[dict]):
        """
        Convert buffer to DataFrame and bulk-load into Snowflake.
        """
        import pandas as pd

        nonlocal total_loaded
        if not buf:
            return

        df = pd.DataFrame(buf, columns=["BOARD_ID", "SPRINT_ID", "SPRINT_NAME", "START_DATE", "END_DATE"])

        with snowflake.connector.connect(**sf_conn, insecure_mode=True) as conn:
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name="SPRINT_DATA",
                schema="JIRA",
                database="EDWSTAGING",
                use_logical_type=True,
                auto_create_table=False
            )

        total_loaded += nrows
        logger.info(f"Flushed batch of {nrows} rows (chunks={nchunks}); "
                    f"total loaded so far: {total_loaded}")

    # 2) Stream-fetch & buffer sprints
    boards = get_all_boards(jira_user, jira_pwd)
    for board in boards:
        url = f"https://{base_url}/rest/agile/1.0/board/{board}/sprint"
        auth = (jira_user, jira_pwd)
        headers = {"Accept": "application/json"}

        while True:
            resp = requests.get(url, auth=auth, headers=headers)
            if resp.status_code != 200:
                logger.error(f"Error fetching sprints for board {board}: HTTP {resp.status_code}")
                # break and continue to the next board
                break
            data = resp.json()

            for sprint in data.get("values", []):
                # Helper to parse ISO‐8601 dates into UTC datetime or None
                def parse_iso(dt_str: str | None) -> datetime | None:
                    if not dt_str:
                        return None
                    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                    return dt.astimezone(pytz.UTC)

                buffer.append({
                    "BOARD_ID": board,
                    "SPRINT_ID": sprint.get("id", None),
                    "SPRINT_NAME": sprint.get("name", None),
                    "START_DATE": parse_iso(sprint.get("startDate", None)),
                    "END_DATE": parse_iso(sprint.get("endDate", None)),
                })

                # Flush when buffer exceeds threshold
                if len(buffer) >= flush_size:
                    flush(buffer)
                    buffer.clear()

            if data.get("isLast"):
                break

            start_at = data.get("startAt", 0) + data.get("maxResults", 0)
            url = (f"https://{base_url}/rest/agile/1.0/board/{board}/sprint"
                   f"?startAt={start_at}")

    # 3) Final flush of any remaining rows
    flush(buffer)
    logger.info(f"Done streaming load. Total rows loaded: {total_loaded}")


# force