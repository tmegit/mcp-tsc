import os
import json
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row

from mcp.server.fastmcp import FastMCP


# -----------------------------
# MCP init
# -----------------------------
mcp = FastMCP(name="icio-mcp", stateless_http=True)


# -----------------------------
# Postgres helpers
# -----------------------------
def _pg_dsn() -> str:
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise RuntimeError("Missing env var PG_DSN")
    return dsn


def _fetch_all(sql: str, params: Optional[dict] = None) -> List[Dict[str, Any]]:
    """
    Execute a read-only query and return rows as dicts.
    """
    with psycopg.connect(_pg_dsn(), row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            return cur.fetchall()


def _fetch_one(sql: str, params: Optional[dict] = None) -> Dict[str, Any]:
    rows = _fetch_all(sql, params)
    return rows[0] if rows else {}


# -----------------------------
# Health tools
# -----------------------------
@mcp.tool()
def health() -> dict:
    """Simple healthcheck to confirm MCP server is alive."""
    return {"result": "ok"}


@mcp.tool()
def health_db() -> dict:
    """Healthcheck Postgres."""
    row = _fetch_one("SELECT 1 AS db_ok;")
    return {"result": f"db_ok={row.get('db_ok', '0')}"}


@mcp.tool()
def db_info() -> dict:
    """Return current_user, current_database and server timestamp."""
    row = _fetch_one(
        """
        SELECT
          current_user AS "user",
          current_database() AS "database",
          now() AS "server_time";
        """
    )
    # server_time is datetime -> stringify for stable output
    if "server_time" in row and row["server_time"] is not None:
        row["server_time"] = row["server_time"].isoformat()
    return row


# -----------------------------
# Dependency tools (via views)
# -----------------------------
@mcp.tool()
def product_dependency_top_suppliers(
    buyer_country: str,
    buyer_sector: str,
    year: int = 2022,
    limit: int = 10,
) -> dict:
    """
    Top supplier countries for a given buyer country + sector + year.
    Reads from: mcp.v_dep_sector_r0_mcp
    """
    rows = _fetch_all(
        """
        SELECT supplier_country, dependency_pct_str
        FROM mcp.v_dep_sector_r0_mcp
        WHERE year = %(year)s
          AND buyer_country = %(buyer_country)s
          AND buyer_sector = %(buyer_sector)s
        ORDER BY dependency_pct_2dp DESC
        LIMIT %(limit)s;
        """,
        {
            "year": year,
            "buyer_country": buyer_country,
            "buyer_sector": buyer_sector,
            "limit": limit,
        },
    )

    return {
        "year": year,
        "buyer_country": buyer_country,
        "buyer_sector": buyer_sector,
        "top_suppliers": [
            {
                "supplier_country": r["supplier_country"],
                "dependency_pct": r["dependency_pct_str"],
            }
            for r in rows
        ],
    }


@mcp.tool()
def product_dependency_top_sectors(
    buyer_country: str,
    supplier_country: str,
    year: int = 2022,
    limit: int = 10,
) -> dict:
    """
    Top buyer sectors for a given buyer country importing from a supplier.
    Reads from: mcp.v_dep_top_sectors_by_supplier_mcp
    """
    rows = _fetch_all(
        """
        SELECT buyer_sector, buyer_sector_label, dependency_pct_str
        FROM mcp.v_dep_top_sectors_by_supplier_mcp
        WHERE year = %(year)s
          AND buyer_country = %(buyer_country)s
          AND supplier_country = %(supplier_country)s
        ORDER BY dependency_pct_2dp DESC
        LIMIT %(limit)s;
        """,
        {
            "year": year,
            "buyer_country": buyer_country,
            "supplier_country": supplier_country,
            "limit": limit,
        },
    )

    return {
        "year": year,
        "buyer_country": buyer_country,
        "supplier_country": supplier_country,
        "top_sectors": [
            {
                "buyer_sector": r["buyer_sector"],
                "buyer_sector_label": r["buyer_sector_label"],
                "dependency_pct": r["dependency_pct_str"],
            }
            for r in rows
        ],
    }


@mcp.tool()
def product_dependency_compare_countries(
    buyer_countries: list[str],
    buyer_sector: str,
    supplier_country: str,
    year: int = 2022,
) -> dict:
    """
    Compare dependency across multiple buyer countries for same sector + supplier.
    Reads from: mcp.v_dep_compare_countries_mcp
    """
    rows = _fetch_all(
        """
        SELECT buyer_country, buyer_country_name, dependency_pct_str
        FROM mcp.v_dep_compare_countries_mcp
        WHERE year = %(year)s
          AND buyer_sector = %(buyer_sector)s
          AND supplier_country = %(supplier_country)s
          AND buyer_country = ANY(%(buyer_countries)s::text[])
        ORDER BY buyer_country;
        """,
        {
            "year": year,
            "buyer_sector": buyer_sector,
            "supplier_country": supplier_country,
            "buyer_countries": buyer_countries,
        },
    )

    return {
        "year": year,
        "buyer_sector": buyer_sector,
        "supplier_country": supplier_country,
        "countries": [
            {
                "buyer_country": r["buyer_country"],
                "buyer_country_name": r["buyer_country_name"],
                "dependency_pct": r["dependency_pct_str"],
            }
            for r in rows
        ],
    }


@mcp.tool()
def product_dependency_time_series(
    buyer_country: str,
    buyer_sector: str,
    supplier_country: str,
    year_from: int = 2016,
    year_to: int = 2022,
) -> dict:
    """
    Time series of dependency for a given buyer country + sector + supplier.
    Reads from: mcp.v_dep_time_series_mcp
    """
    rows = _fetch_all(
        """
        SELECT year, dependency_pct_str
        FROM mcp.v_dep_time_series_mcp
        WHERE buyer_country = %(buyer_country)s
          AND buyer_sector = %(buyer_sector)s
          AND supplier_country = %(supplier_country)s
          AND year BETWEEN %(year_from)s AND %(year_to)s
        ORDER BY year;
        """,
        {
            "buyer_country": buyer_country,
            "buyer_sector": buyer_sector,
            "supplier_country": supplier_country,
            "year_from": year_from,
            "year_to": year_to,
        },
    )

    return {
        "buyer_country": buyer_country,
        "buyer_sector": buyer_sector,
        "supplier_country": supplier_country,
        "series": [{"year": r["year"], "dependency_pct": r["dependency_pct_str"]} for r in rows],
    }


# -----------------------------
# Streamable HTTP app
# -----------------------------
# Important: FastMCP.streamable_http_app() is a factory with your installed version.
streamable_http_app = mcp.streamable_http_app()
