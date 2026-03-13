from dagster import ConfigurableResource
import snowflake.connector


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

    def execute(self, sql: str, params=None):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)

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
