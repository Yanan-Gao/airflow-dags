import logging
from contextlib import contextmanager
from typing import Optional, Iterator, List

from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from psycopg2._psycopg import cursor
from psycopg2.pool import SimpleConnectionPool

from ttd.ttdenv import TtdEnvFactory, ProdEnv

METRICS_DB_CONN_ID = 'metrics_db_rw'
TEST_METRICS_DB_CONN_ID = 'test_metrics_db_rw'


class MetricDBClient(LoggingMixin):

    def __init__(self, push_to_test_db: bool = False, log_level: int = logging.INFO):
        """
        Allows execution of sprocs in the metrics DB.

        :param push_to_test_db: Affects prodtest only. If true will push to the test instance.
        """
        super().__init__()
        self.pool: Optional[SimpleConnectionPool] = None
        self.push_to_test_db = push_to_test_db
        self.log.setLevel(log_level)

    def _create_pool(self) -> SimpleConnectionPool:
        conn_id = TEST_METRICS_DB_CONN_ID if self.push_to_test_db else METRICS_DB_CONN_ID

        conn = BaseHook.get_connection(conn_id)

        pool = SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            host=conn.host,
            user=conn.login,
            password=conn.password,
            dbname=conn.schema,
            port=conn.port,
        )

        return pool

    @contextmanager
    def _get_cursor(self) -> Iterator[cursor]:
        if self.pool is None:
            self.pool = self._create_pool()

        try:
            with self.pool.getconn() as conn:
                with conn.cursor() as cursor:
                    yield cursor
        except Exception as e:
            self.log.error(f"Error: {e}", exc_info=True)
        finally:
            if conn is not None:
                self.pool.putconn(conn)

    def execute_sproc(self, sproc_name: str, parameters: List):
        """
        Executes a sproc against the Metrics DB.

        :param sproc_name: Name of the sproc in the DB (needs to be qualified with the schema)
        :param parameters: List of the parameters to be passed to the sproc as arguments
        """
        if self.push_to_test_db and TtdEnvFactory.get_from_system() == ProdEnv:
            self.log.error(
                f"You're attempting to push metrics to the test metrics DB from production. This isn't "
                f"possible. Sproc {sproc_name} will not be run."
            )
            return

        from psycopg2 import OperationalError, DatabaseError

        try:
            self.log.info(f"Attempting to run sproc: {sproc_name}. Params: {parameters}")

            with self._get_cursor() as cur:
                cur.callproc(sproc_name, parameters)

        except (OperationalError, DatabaseError) as e:
            self.log.error(f"DB error occurred: {e}")
        except Exception as e:
            self.log.error(f"An unexpected error occurred while running sproc: {e}", exc_info=True)
        else:
            self.log.debug(f"Sproc {sproc_name} executed successfully")
