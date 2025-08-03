# Development Guide for AI Agents

This repository contains a FastAPI service that executes SQL queries using DuckDB. Data sources include:

- A local DuckDB file located at `DATABASE_PATH`
- Parquet files (use `source="parquet"` and provide `path`)
- A remote MotherDuck instance using `MOTHERDUCK_TOKEN`

## Conventions

- Use Python 3.10+
- Keep code in the `app/` directory
- Run `pytest` before committing

## Testing & Running

```bash
pip install -r requirements.txt
pytest
uvicorn app.main:app --reload
```

## Future Work

- Authentication for API endpoints
- Caching and prepared statements
- CI/CD integration with Railway
