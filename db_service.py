import os
import sys
import warnings
from threading import Lock
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Add project root to sys.path for imports
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import SNOWFLAKE_CONFIG


class SnowflakeConnectionPool:
    """Simple connection pool for Snowflake to reduce connection overhead."""

    def __init__(self, max_connections=5):
        self.max_connections = max_connections
        self.connections = []
        self.lock = Lock()

    def get_connection(self):
        """Get a connection from the pool or create a new one."""
        with self.lock:
            return (
                self.connections.pop()
                if self.connections
                else snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            )

    def return_connection(self, conn):
        """Return a connection to the pool."""
        with self.lock:
            if len(self.connections) < self.max_connections:
                self.connections.append(conn)
            else:
                conn.close()

    def close_all(self):
        """Close all connections in the pool."""
        with self.lock:
            for conn in self.connections:
                conn.close()
            self.connections.clear()


# Global connection pool instance
_connection_pool = SnowflakeConnectionPool(max_connections=5)


def get_snowflake_connection():
    """Get a Snowflake connection from the pool."""
    return _connection_pool.get_connection()


def return_snowflake_connection(conn):
    """Return a Snowflake connection to the pool."""
    _connection_pool.return_connection(conn)


def fetch_data(query):
    """Fetch data from Snowflake using the connection pool."""
    conn = get_snowflake_connection()
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)

            # Attempt fast Arrow-based fetch
            try:
                cur = conn.cursor(snowflake.connector.DictCursor)
                cur.execute(query)
                table = cur.fetch_arrow_all()
                return table.to_pandas() if table else pd.DataFrame()
            except Exception as arrow_exc:
                print(
                    f"→ Debug: Arrow fetch failed ({type(arrow_exc).__name__}: {arrow_exc}), falling back to fetchall()"
                )

            # Fallback to fetchall()
            cur = conn.cursor()
            cur.execute(query)
            rows = cur.fetchall()
            if not rows:
                return pd.DataFrame()
            col_names = [col[0] for col in cur.description]
            return pd.DataFrame(rows, columns=col_names)
    except Exception as e:
        print(f"→ Debug: Error in fetch_data: {type(e).__name__}: {e}")
        raise
    finally:
        return_snowflake_connection(conn)


def execute_query(query):
    """Execute a query in Snowflake (for DDL/DML operations)."""
    conn = get_snowflake_connection()
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            cur = conn.cursor()
            cur.execute(query)
            return cur.fetchone() if cur.rowcount > 0 else None
    finally:
        return_snowflake_connection(conn)


def upload_to_snowflake(df, table_name, if_exists="replace", chunk_size=16000):
    """
    Upload a pandas DataFrame to a Snowflake table.

    Args:
        df: pandas DataFrame to upload
        table_name: Fully qualified table name (e.g., 'database.schema.table_name')
        if_exists: 'fail', 'replace', or 'append'
        chunk_size: Number of rows to upload per batch (default: 16000)
    """
    conn = get_snowflake_connection()
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)

            # Parse table name
            parts = table_name.split(".")
            database = schema = table = None
            if len(parts) == 3:
                database, schema, table = parts
            elif len(parts) == 2:
                schema, table = parts
                database = SNOWFLAKE_CONFIG.get("database")
            else:
                table = parts[0]
                database = SNOWFLAKE_CONFIG.get("database")
                schema = SNOWFLAKE_CONFIG.get("schema")

            # Handle if_exists
            if if_exists == "replace":
                conn.cursor().execute(
                    f"DROP TABLE IF EXISTS {database}.{schema}.{table}"
                )
                print(
                    f"  Dropped existing table: {database}.{schema}.{table}", flush=True
                )

            # Upload data using write_pandas
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=table.upper(),
                database=database.upper(),
                schema=schema.upper(),
                chunk_size=chunk_size,
                auto_create_table=True,
                overwrite=(if_exists == "replace"),
            )

            if success:
                print(
                    f"  Successfully uploaded {nrows:,} rows in {nchunks} chunks",
                    flush=True,
                )
            else:
                print(f"  Upload completed with warnings", flush=True)

            return success
    except Exception as e:
        print(f"  Error uploading to Snowflake: {e}", flush=True)
        raise
    finally:
        return_snowflake_connection(conn)
