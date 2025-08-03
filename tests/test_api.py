import os
import duckdb
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_query_duckdb():
    response = client.post("/query", json={"sql": "SELECT 42 AS answer"})
    assert response.status_code == 200
    assert response.json() == [{"answer": 42}]


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
    assert response.json() == [{"id": 1}]


def test_upload_and_merge(tmp_path):
    os.environ["DATABASE_PATH"] = str(tmp_path / "db.duckdb")

    csv1 = b"id,name\n1,Alice\n2,Bob\n"
    files = {"file": ("people.csv", csv1, "text/csv")}
    data = {"table_name": "people", "primary_key": "id"}

    resp1 = client.post("/upload", files=files, data=data)
    assert resp1.status_code == 200
    assert resp1.json()["rows"] == 2

    csv2 = b"id,name\n2,Bobby\n3,Charlie\n"
    files = {"file": ("people.csv", csv2, "text/csv")}
    data = {"table_name": "people", "primary_key": "id"}

    resp2 = client.post("/upload", files=files, data=data)
    assert resp2.status_code == 200

    query_resp = client.post(
        "/query", json={"sql": "SELECT * FROM people ORDER BY id"}
    )
    assert query_resp.status_code == 200
    assert query_resp.json() == [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bobby"},
        {"id": 3, "name": "Charlie"},
    ]


def test_upload_skips_blank_primary_key(tmp_path):
    os.environ["DATABASE_PATH"] = str(tmp_path / "db.duckdb")

    csv1 = b"id,name\n1,Alice\n,NoID\n2,Bob\n"
    files = {"file": ("people.csv", csv1, "text/csv")}
    data = {"table_name": "people", "primary_key": "id"}

    resp1 = client.post("/upload", files=files, data=data)
    assert resp1.status_code == 200
    assert resp1.json()["rows"] == 2

    csv2 = b"id,name\n2,Bobby\n,Missing\n3,Charlie\n"
    files = {"file": ("people.csv", csv2, "text/csv")}
    data = {"table_name": "people", "primary_key": "id"}

    resp2 = client.post("/upload", files=files, data=data)
    assert resp2.status_code == 200

    query_resp = client.post(
        "/query", json={"sql": "SELECT * FROM people ORDER BY id"}
    )
    assert query_resp.status_code == 200
    assert query_resp.json() == [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bobby"},
        {"id": 3, "name": "Charlie"},
    ]


def test_upload_requires_primary_key(tmp_path):
    os.environ["DATABASE_PATH"] = str(tmp_path / "db.duckdb")

    csv = b"id,name\n1,Alice\n"
    files = {"file": ("people.csv", csv, "text/csv")}
    data = {"table_name": "people", "primary_key": "id"}
    resp1 = client.post("/upload", files=files, data=data)
    assert resp1.status_code == 200

    files = {"file": ("people.csv", csv, "text/csv")}
    data = {"table_name": "people"}
    resp2 = client.post("/upload", files=files, data=data)
    assert resp2.status_code == 400


def test_upload_and_merge_composite_key(tmp_path):
    os.environ["DATABASE_PATH"] = str(tmp_path / "db.duckdb")

    csv1 = b"id,subid,name\n1,1,Alice\n2,1,Bob\n"
    files = {"file": ("people.csv", csv1, "text/csv")}
    data = {"table_name": "people", "primary_key": ["id", "subid"]}
    resp1 = client.post("/upload", files=files, data=data)
    assert resp1.status_code == 200

    csv2 = b"id,subid,name\n2,1,Bobby\n3,1,Charlie\n"
    files = {"file": ("people.csv", csv2, "text/csv")}
    data = {"table_name": "people", "primary_key": ["id", "subid"]}
    resp2 = client.post("/upload", files=files, data=data)
    assert resp2.status_code == 200

    query_resp = client.post(
        "/query", json={"sql": "SELECT * FROM people ORDER BY id, subid"}
    )
    assert query_resp.status_code == 200
    assert query_resp.json() == [
        {"id": 1, "subid": 1, "name": "Alice"},
        {"id": 2, "subid": 1, "name": "Bobby"},
        {"id": 3, "subid": 1, "name": "Charlie"},
    ]

