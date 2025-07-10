import logging
from dags.forecast.utils.get_db_connection import get_mssql_db_connection, get_vertica_db_connection

vertica_forecasting_validation_conn_id = "vertica_forecasting_validation"


def run_query(conn, query, cursor=None):
    try:
        if cursor is None:
            logging.info("Getting cursor for connection.")
            cursor = conn.cursor()

        logging.info("Executing query.")
        cursor.execute(query)

        result_set = None
        result_description = None

        logging.info("Process initial query result set.")
        if cursor.description is not None:
            result_set = cursor.fetchall()
            result_description = cursor.description

        logging.info("Running all query result sets.")
        while cursor.nextset():
            if cursor.description is not None:
                result_set = cursor.fetchall()
                result_description = cursor.description

        if result_set is None or result_description is None:
            raise Exception("No result set returned from query.")

        logging.info("Get rows and columns for the last result set.")
        rows = result_set
        columns = [col[0] for col in result_description]

        logging.info("Closing connection.")
        cursor.close()
        conn.close()

        return rows, columns
    except Exception as e:
        logging.error(f"Failed running query with exception: {e}")
        raise


def run_insertions_and_query(create_temp_table_query: str, insert_query: str, insert_strings, query: str):
    try:
        logging.info("Getting vertica connection.")
        conn = get_vertica_db_connection(vertica_forecasting_validation_conn_id)
        cursor = conn.cursor()

        logging.info("Creating temp table:")
        cursor.execute(create_temp_table_query)

        logging.info("Inserting into temp table:")
        for insert_string in insert_strings:
            cursor.execute(insert_query + insert_string + ';')

        logging.info("Running query.")
        return run_query(conn, query, cursor)

    except Exception as e:
        logging.error(f"Failed running vertica query with exception: {e}")
        raise


def run_vertica_query(query: str):
    try:
        logging.info("Getting vertica connection.")
        conn = get_vertica_db_connection(vertica_forecasting_validation_conn_id)
        logging.info("Running query on vertica.")
        return run_query(conn, query)
    except Exception as e:
        logging.error(f"Failed running vertica query with exception: {e}")
        raise


def run_mssql_query(query: str, conn_id: str):
    try:
        logging.info("Getting mssql connection.")
        conn = get_mssql_db_connection(conn_id)
        logging.info("Running query on mssql.")
        return run_query(conn, query)
    except Exception as e:
        logging.error(f"Failed running mssql query with exception: {e}")
        raise
