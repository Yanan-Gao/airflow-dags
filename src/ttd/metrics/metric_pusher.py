from datetime import datetime
from typing import Dict, Union, Tuple, Optional

from psycopg2.extras import Json

from ttd.metrics.metric_db_client import MetricDBClient
from ttd.ttdenv import TtdEnvFactory

Numeric = Union[float, int]
MetricValue = Union[Numeric, str]


class MetricPusher(MetricDBClient):

    @staticmethod
    def _handle_value_types(value: MetricValue) -> Tuple[Optional[Numeric], Optional[str]]:
        match value:
            case float() | int():
                return value, None
            case str():
                return None, value

    def push(
        self,
        name: str,
        value: MetricValue,
        labels: Dict[str, str],
        timestamp: datetime = None,
    ) -> None:
        """
        Pushes a metric into the DATAPROC Metrics DB.

        :param name: The name of the metric
        :param value: The value of the metric. Can be either string or numeric type. These get placed in different DB columns
        :param labels: Key value pairs representing metric labels
        :param timestamp: Timestamp associated with the metric. Defaults to the present time.
        :param push_to_test_db: Affects prodtest only. If true will push to the test instance.
        """
        if timestamp is None:
            timestamp = datetime.now()

        labels['env'] = TtdEnvFactory.get_from_system().execution_env
        numeric_value, text_value = self._handle_value_types(value)

        self.execute_sproc('metrics.insert_metric', [name, timestamp, Json(labels), numeric_value, text_value])
