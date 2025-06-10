import pandas as pd
import psycopg2
import pytest

from src.utilities.load_lambda_utils import create_db_entries_from_df


@pytest.mark.describe("Test create_db_entries_from_db utility behaviour")
class TestCreateDbEntriesFromDf:
    def test_insert_data_into_table(self):
        df = pd.DataFrame(
            {
                "enrollment_no": [12, 13],
                "name": ["sarah", "ray"],
                "science_marks": [90, 81],
            }
        )

        conn = psycopg2.connect(
            database="Classroom",
            user="postgres",
            password="pass",
            host="127.0.0.1",
            port="5432",
        )

        create_db_entries_from_df(conn, "classroom", df)

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM classroom")
        rows = cursor.fetchall()

        expected_rows = [(12, "sarah", 90), (13, "ray", 81)]

        assert rows == expected_rows

        cursor.close()
        conn.close()
