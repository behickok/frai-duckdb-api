from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from app.db import get_connection

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
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        conn.close()
        return {"columns": columns, "rows": rows}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
