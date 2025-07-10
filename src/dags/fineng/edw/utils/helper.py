import logging
from snowflake.connector.pandas_tools import write_pandas


def bulk_insert(conn, df, table_name, database, schema, batch_size=16384):
    try:
        if df.empty:
            logging.warning("DataFrame is empty. No records to insert.")
            return False

        df.columns = df.columns.str.upper()
        cols = ",".join(df.columns)
        batch_size = min(batch_size, len(df))

        for i in range(0, len(df), batch_size):
            try:

                chunk = df.iloc[i:i + batch_size]

                values_placeholder = ', '.join(['(' + ', '.join(['%s'] * len(chunk.columns)) + ')' for _ in range(len(chunk))])
                values = [tuple(row) for row in chunk.itertuples(index=False, name=None)]

                query = f"INSERT INTO {database}.{schema}.{table_name} ({cols}) VALUES {values_placeholder}"

                conn.cursor().execute(query, [item for sublist in values for item in sublist])

            except Exception as e:
                logging.error(f"Error in bulk insert chunk starting at index {i}: {e}")
                raise e

        logging.info(f"Inserted {len(df)} records into {database}.{schema}.{table_name}")
        return True

    except Exception as e:
        logging.error(f"Error in bulk insert: {e}")
        raise e


def merge_data(
    conn, source_table, source_schema, source_db, target_table, target_schema, target_db, merge_condition, update_columns_when_match,
    update_columns_when_not_match, update_columns
):
    try:
        # Join the columns with commas for proper SQL syntax
        update_columns_when_match_str = ", ".join(update_columns_when_match)
        update_columns_when_not_match_str = ", ".join(update_columns_when_not_match)
        update_columns_str = ", ".join(update_columns)

        query = f"""
        MERGE INTO {target_db}.{target_schema}.{target_table} AS target
        USING {source_db}.{source_schema}.{source_table} AS source
        ON {merge_condition}
        WHEN MATCHED THEN
        UPDATE SET {update_columns_when_match_str}
        WHEN NOT MATCHED THEN
        INSERT ({update_columns_str})
        VALUES ({update_columns_when_not_match_str})
        """
        logging.info(f"Executing merge query: {query}")
        conn.cursor().execute(query)
        logging.info(f"Data merged successfully from {source_schema}.{source_table} to {target_schema}.{target_table}")
    except Exception as e:
        logging.error(f"Error in merging data: {e}")
        raise e


def write_df(conn, df, table, schema, db):
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table,
            schema=schema,
            database=db,
            use_logical_type=True,
        )
        logging.info(f"Successfully inserted {nrows} rows in {nchunks} chunks in {db}.{schema}.{table}")
        return True
    except Exception as e:
        logging.error(f"Error in writing to snowflake {e}")
        raise e


def write_to_execution_logs(conn, processname, processid, processstatus, processstarttime, processendtime, message):
    try:
        query = """
        INSERT INTO EDWSTAGING.logging.PROCESSEXECUTIONLOGS 
        (processname, processid, processstatus, processstartdatetime, processenddatetime, ENVIRONMENTNAME, message, CREATEDBY)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (processname, processid, processstatus, processstarttime, processendtime, 'PRODUCTION - AIRFLOW', message, 'AIRFLOW')
        conn.cursor().execute(query, values)
        logging.info(f"Successfully inserted execution logs for {processname} with process_id {processid}")
    except Exception as e:
        logging.error(f"Error in writing to execution logs: {e}")
        raise e


#print("Helper functions loaded successfully")