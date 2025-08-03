import os
import duckdb
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_query_duckdb():
    response = client.post("/query", json={"sql": "SELECT 42 AS answer"})
    assert response.status_code == 200
    data = response.json()
    assert data["rows"] == [[42]]
    assert data["columns"] == ["answer"]


def test_query_parquet(tmp_path):
    parquet_file = tmp_path / "data.parquet"
    duckdb.sql("SELECT 1 AS id").write_parquet(str(parquet_file))
    response = client.post(
        "/query",
        json={
            "sql": "SELECT * FROM parquet_data",
            "source": "parquet",
            "path": str(parquet_file),
        },
    )
    assert response.status_code == 200
    assert response.json()["rows"] == [[1]]
