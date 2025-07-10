import logging
import pymssql
import vertica_python
from airflow.hooks.base import BaseHook


def get_vertica_db_connection(conn_name):
    try:
        logging.info(f"Establishing connection for vertica user {conn_name}.")
        conn = BaseHook.get_connection(conn_name)
        conn_info = {'host': conn.host, 'port': conn.port, 'user': conn.login, 'password': conn.password, 'database': conn.schema}
        return vertica_python.connect(**conn_info)
    except Exception as e:
        logging.error(f"Failed establish vertica connection for user {conn_name} with exception: {e}")
        raise


def get_mssql_db_connection(conn_name):
    try:
        logging.info(f"Establishing connection for mssql user {conn_name}.")
        conn_info = BaseHook.get_connection(conn_name)
        server = conn_info.host
        user = conn_info.login
        password = conn_info.password
        database = conn_info.schema
        return pymssql.connect(server=server, user=user, password=password, database=database)
    except Exception as e:
        logging.error(f"Failed establish mssql connection for user {conn_name} with exception: {e}")
        raise
