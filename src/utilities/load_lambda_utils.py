import logging

import pandas as pd
from psycopg import Connection, sql
from psycopg.rows import DictRow

logger = logging.getLogger(__name__)

def create_db_entries_from_df(conn:Connection[DictRow], table_name, df:pd.DataFrame):
    with conn.cursor() as cursor:
        if df.empty:
            logger.info(f"No entries available for {table_name}.")
            return
        
        columns = list(df.columns)

        insert_query = sql.SQL("""
            INSERT INTO {table} ({fields})
            VALUES ({placeholders})
        """).format(
            table=sql.Identifier(table_name),  
            fields=sql.SQL(', ').join(map(sql.Identifier, columns)),  
            placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns)) 
        )

        values_to_insert = df.to_records(index=False).tolist()
        
        try:
            cursor.excutemany(insert_query, values_to_insert)

            conn.commit() 

            logger.info(f"Inserted {len(values_to_insert)} rows into {table_name}.")
    
        except Exception as e:
            logger.error(f"Failed to insert records into {table_name}: {str(e)}")
            conn.rollback()
        