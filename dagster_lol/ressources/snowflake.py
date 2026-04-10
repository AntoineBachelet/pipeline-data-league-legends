from uuid import uuid4
from dagster import ConfigurableResource
import snowflake.connector
import pandas as pd


class SnowflakeResource(ConfigurableResource):
    account: str
    user: str
    password: str
    database: str
    schema_name: str
    warehouse: str
    role: str = ""

    def get_connection(self):
        params = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "schema": self.schema_name,
            "warehouse": self.warehouse,
        }
        if self.role:
            params["role"] = self.role
        return snowflake.connector.connect(**params)

    def fetch(self, sql: str, params=None) -> pd.DataFrame:
        """Executes a SELECT query and returns the results as a DataFrame."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [desc[0].lower() for desc in cur.description]
                return pd.DataFrame(cur.fetchall(), columns=columns)

    def execute(self, sql: str, params=None):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)

    def merge(self, table: str, rows: list[dict], columns: list[str], key_columns: list[str], batch_size: int = 10_000) -> dict:
        """Inserts new rows and updates modified rows using Snowflake MERGE.

        Returns a dict with keys: new_count, updated_count.
        """
        staging = f"tmp_merge_{uuid4().hex[:12]}"
        placeholders = ", ".join(["%s"] * len(columns))
        col_list = ", ".join(columns)
        on_clause = " AND ".join([f"target.{k} = source.{k}" for k in key_columns])
        update_cols = [c for c in columns if c not in key_columns]
        set_clause = ", ".join([f"target.{c} = source.{c}" for c in update_cols])
        insert_values = ", ".join([f"source.{c}" for c in columns])
        values = [tuple(row.get(c) for c in columns) for row in rows]

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE TEMPORARY TABLE {staging} LIKE {table}")
                insert_sql = f"INSERT INTO {staging} ({col_list}) VALUES ({placeholders})"
                for i in range(0, len(values), batch_size):
                    cur.executemany(insert_sql, values[i : i + batch_size])
                cur.execute(f"""
                    MERGE INTO {table} AS target
                    USING {staging} AS source
                    ON {on_clause}
                    WHEN MATCHED THEN UPDATE SET {set_clause}
                    WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({insert_values})
                """)
                result = cur.fetchone()
                return {"new_count": result[0], "updated_count": result[1]}

    def truncate_and_insert(self, table: str, rows: list[dict], columns: list[str], batch_size: int = 10_000):
        """Truncates the target table and bulk-inserts rows in batches."""
        placeholders = ", ".join(["%s"] * len(columns))
        cols = ", ".join(columns)
        insert_sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        values = [[row.get(col) for col in columns] for row in rows]

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {table}")
                for i in range(0, len(values), batch_size):
                    cur.executemany(insert_sql, values[i : i + batch_size])
