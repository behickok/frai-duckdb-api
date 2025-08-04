# FRAI DuckDB API

This is a minimal FastAPI application that exposes a RESTful interface for executing DuckDB queries. Data can come from a local DuckDB database file, Parquet files stored on a volume, or a remote MotherDuck instance.

## Development

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```
2. **Run the API**
   ```bash
   uvicorn app.main:app --reload
   ```
3. **Query the API**
   ```bash
   curl -X POST http://localhost:8000/query \\
        -H 'Content-Type: application/json' \\
        -d '{"sql": "SELECT 1 AS one"}'
   ```
   The response will be an array of objects:
   ```json
   [{"one": 1}]
   ```

   Longer queries can be provided as a `.sql` file:
   ```bash
   curl -X POST http://localhost:8000/query-file \\
        -F "sql_file=@path/to/query.sql"
   ```

4. **Upload data**
   ```bash
   curl -X POST http://localhost:8000/upload \\

       -F "file=@path/to/data.csv" \\
       -F "table_name=your_table" \\
       -F "primary_key=id" \\
       -F "primary_key=other_id"
   ```
   The endpoint accepts CSV or Parquet files. If the table exists, the data
   is merged using the supplied primary key(s); otherwise a new table is created.
   For composite primary keys, repeat the `primary_key` field for each column.


## Environment Variables

- `DATABASE_PATH` – Path to a DuckDB file on a persistent Railway volume.
- `MOTHERDUCK_TOKEN` – Authentication token for MotherDuck connections.

## Deployment on Railway

1. Create a new Railway project and add a Python service.
2. Attach a persistent volume and note the mount path.
3. Set environment variables:
   - `DATABASE_PATH` to the path on the volume for the DuckDB file.
   - `MOTHERDUCK_TOKEN` if remote queries are needed.
4. Deploy using the default start command:
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port $PORT
   ```

## Testing

Run the test suite:
```bash
pytest
```
