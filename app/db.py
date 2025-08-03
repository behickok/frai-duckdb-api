import os
import duckdb


def get_connection(source: str = "duckdb", path: str | None = None):
    """Return a DuckDB connection based on the requested source."""
    if source == "duckdb":
        db_path = path or os.getenv("DATABASE_PATH", ":memory:")
        return duckdb.connect(db_path)
    if source == "motherduck":
        token = os.getenv("MOTHERDUCK_TOKEN")
        if not token:
            raise ValueError("MOTHERDUCK_TOKEN is not set")
        return duckdb.connect(f"md:?token={token}")
    if source == "parquet":
        conn = duckdb.connect()
        if not path:
            raise ValueError("Parquet source requires a file path")
        conn.execute(f"CREATE VIEW parquet_data AS SELECT * FROM read_parquet('{path}')")
        return conn
    raise ValueError(f"Unsupported source: {source}")
