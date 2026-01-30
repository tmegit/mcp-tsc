# server.py
# MCP server – clean baseline with Postgres read-only access

import os
from typing import Any, Dict

import psycopg
from psycopg.rows import dict_row

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings


# -----------------------------------------------------------------------------
# MCP initialization
# -----------------------------------------------------------------------------

mcp = FastMCP(
    name="icio-mcp",
    stateless_http=True,
    host="127.0.0.1",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[
            "localhost",
            "localhost:*",
            "127.0.0.1",
            "127.0.0.1:*",
            "[::1]",
            "[::1]:*",
        ],
        allowed_origins=[
            "http://localhost",
            "http://localhost:*",
            "http://127.0.0.1",
            "http://127.0.0.1:*",
            "http://[::1]",
            "http://[::1]:*",
        ],
    ),
)

# ASGI app exposed to uvicorn
streamable_http_app = mcp.streamable_http_app


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _get_dsn() -> str:
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise RuntimeError("Missing env var PG_DSN")
    return dsn


def _require_select(sql: str) -> str:
    s = (sql or "").strip()
    if not s:
        raise ValueError("sql is required")

    if not s.lower().startswith("select"):
        raise ValueError("Only SELECT statements are allowed")

    # Prevent multi-statement queries
    if ";" in s.rstrip(";"):
        raise ValueError("Only one SQL statement is allowed")

    return s


# -----------------------------------------------------------------------------
# Tools
# -----------------------------------------------------------------------------

@mcp.tool()
def health() -> Dict[str, str]:
    """
    Simple healthcheck to confirm MCP server is alive.
    """
    return {"result": "ok"}


@mcp.tool()
def health_db() -> Dict[str, str]:
    """
    Healthcheck Postgres connection.
    """
    dsn = _get_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select 1")
            value = cur.fetchone()[0]

    return {"result": f"db_ok={value}"}


@mcp.tool()
def sql_select(sql: str, limit: int = 100) -> Dict[str, Any]:
    """
    Execute a READ-ONLY SELECT query against Postgres.

    Rules:
    - SELECT only
    - single statement
    - LIMIT enforced (default 100, max 1000)
    """
    sql = _require_select(sql)

    limit = int(limit)
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if limit > 1000:
        limit = 1000

    sql_l = sql.lower()
    if " limit " not in sql_l:
        sql = f"{sql} LIMIT {limit}"

    dsn = _get_dsn()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    return {
        "row_count": len(rows),
        "rows": rows,
    }
