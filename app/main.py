from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from app.db import get_connection
import os
import tempfile
import shutil
import re
from pathlib import Path

app = FastAPI()


class QueryRequest(BaseModel):
    sql: str
    source: str = "duckdb"
    path: str | None = None


@app.post("/query")
def run_query(request: QueryRequest):
    try:
        conn = get_connection(request.source, request.path)
        cursor = conn.execute(request.sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()



@app.post("/upload")
async def upload_table(
    file: UploadFile = File(...),
    table_name: str = Form(...),

    primary_key: list[str] | None = Form(None),

) -> dict:
    """Upload a CSV or Parquet file and merge it into a DuckDB table."""

    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table_name):
        raise HTTPException(status_code=400, detail="Invalid table name")

    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in {".csv", ".parquet"}:
        raise HTTPException(status_code=400, detail="Only .csv and .parquet files are supported")

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name

    read_func = "read_csv_auto" if suffix == ".csv" else "read_parquet"

    conn = get_connection()
    try:
        table_exists = (
            conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?",
                [table_name],
            ).fetchone()[0]
            > 0
        )

        filter_clause = ""
        if primary_key:
            conditions = [
                f"{col.strip()} IS NOT NULL AND CAST({col.strip()} AS TEXT) <> ''"
                for col in primary_key
            ]
            filter_clause = " WHERE " + " AND ".join(conditions)

        select_query = f"SELECT * FROM {read_func}('{tmp_path}')" + filter_clause

        if not table_exists:
            conn.execute(
                f"CREATE TABLE {table_name} AS {select_query}"
            )
            if primary_key:
                pk_cols = ", ".join(col.strip() for col in primary_key)

                conn.execute(
                    f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_cols})"
                )
        else:
            if not primary_key:
                raise HTTPException(
                    status_code=400, detail="primary_key is required for upsert"
                )
            # Ensure the provided primary key matches the existing table definition
            existing_pk = [
                row[1]
                for row in conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
                if row[5]
            ]
            if set(existing_pk) != set(primary_key):
                raise HTTPException(
                    status_code=400,
                    detail="primary_key must match existing table primary key",
                )

            conn.execute(
                f"INSERT OR REPLACE INTO {table_name} {select_query}"
            )

        row_count = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]
        return {"table": table_name, "rows": row_count}
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        conn.close()
        os.unlink(tmp_path)
