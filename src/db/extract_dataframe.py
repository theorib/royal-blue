from pprint import pprint

import pandas as pd

from connection.connection import close_db, connect_db


def get_table_names():
    try: 
        con = connect_db()
        cursor = con.cursor()

        get_tables_name_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
            """
        cursor.execute(get_tables_name_query)
        tables = cursor.fetchall()
        unnested_tables = [table[0] for table in tables if not table[0].startswith('_')]
        
        dataframes = {}
        
        for table in unnested_tables:
            sql_query = f"SELECT * FROM {table}"
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=colnames)
            
            last_updated_query = f"SELECT MAX(last_updated) FROM {table}"
            cursor.execute(last_updated_query)
            recent_query = cursor.fetchone()[0]
            
        
            dataframes[table] = {
                "data": df,
                "last_updated": recent_query
            }
            
        return {
                "success": {
                    "message": "Dataframes created successfully",
                    "data": dataframes
                }
            }
    except Exception as e:
        return {
                "error": {
                    "message": f"ERROR: {e}",
                }
            }
    finally:
        close_db(con)
            
pprint(get_table_names())
