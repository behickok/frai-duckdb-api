import os
from dataclasses import dataclass
from typing import Optional, Dict

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer


@dataclass
class AuthContext:
    """Holds authentication details for a request.

    Additional attributes like database routing information or row-level
    security rules can be added here in the future.
    """

    token: str
    db_path: Optional[str] = None

    def rewrite_query(self, sql: str) -> str:  # pragma: no cover - placeholder
        """Rewrite a SQL query for row-level security.

        Currently a no-op but designed for future enhancements where the
        query might be amended based on the authentication context.
        """

        return sql


bearer_scheme = HTTPBearer(auto_error=False)


def _load_token_map() -> Dict[str, Dict[str, Optional[str]]]:
    """Return a mapping of allowed tokens to metadata.

    The `API_TOKENS` environment variable is a comma-separated list of tokens
    with optional database paths using the format `token:db_path`.
    Example::

        API_TOKENS="alpha:/data/a.duckdb,beta"

    yields::

        {
            "alpha": {"db_path": "/data/a.duckdb"},
            "beta": {"db_path": None},
        }
    """

    raw = os.getenv("API_TOKENS", "")
    mapping: Dict[str, Dict[str, Optional[str]]] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            token, db_path = item.split(":", 1)
            mapping[token] = {"db_path": db_path}
        else:
            mapping[item] = {"db_path": None}
    return mapping


def get_auth_context(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> AuthContext:
    """Validate the bearer token and return an authentication context."""

    token_map = _load_token_map()
    if credentials is None or credentials.credentials not in token_map:
        raise HTTPException(status_code=401, detail="Invalid or missing token")
    info = token_map[credentials.credentials]
    return AuthContext(token=credentials.credentials, db_path=info.get("db_path"))

