from abc import ABC, abstractmethod

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import vertica_python


class BaseQueryExecutor(ABC):

    def __init__(self, conn_id, name):
        self.conn_id = conn_id
        self.name = name

    @abstractmethod
    def run(self, query):
        pass

    def execute(self, conn, query):
        cursor = conn.cursor()
        cursor.execute(query)

        rows = cursor.fetchall()
        columns = [x[0].lower() for x in cursor.description]
        res = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        return res


class MsSQLQueryExecutor(BaseQueryExecutor):

    def __init__(self, conn_id, name='mssql'):
        super().__init__(conn_id, name)

    def run(self, query):
        sql_hook = MsSqlHook(mssql_conn_id=self.conn_id)
        conn = sql_hook.get_conn()
        res = super().execute(conn, query)
        conn.close()
        return res


class VerticaQueryExecutor(BaseQueryExecutor):

    def __init__(self, conn_id, name='vertica'):
        super().__init__(conn_id, name)

    def run(self, query):
        conn = BaseHook.get_connection(self.conn_id)
        conn_info = {'host': conn.host, 'port': conn.port, 'user': conn.login, 'password': conn.password, 'database': conn.schema}
        conn = vertica_python.connect(**conn_info)
        res = super().execute(conn, query)
        conn.close()
        return res


class SnowflakeQueryExecutor(BaseQueryExecutor):

    def __init__(self, conn_id, warehouse, database, schema, name='snowflake'):
        super().__init__(conn_id, name)
        self.warehouse = warehouse
        self.database = database
        self.schema = schema

    def run(self, query):
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.conn_id, warehouse=self.warehouse, database=self.database, schema=self.schema)
        conn = snowflake_hook.get_conn()
        res = super().execute(conn, query)
        conn.close()
        return res
