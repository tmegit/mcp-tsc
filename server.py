import os
import re
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
    """Execute a read-only query and return rows as dicts."""
    with psycopg.connect(_pg_dsn(), row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            return cur.fetchall()


def _fetch_one(sql: str, params: Optional[dict] = None) -> Dict[str, Any]:
    rows = _fetch_all(sql, params)
    return rows[0] if rows else {}


def _norm_code(x: str) -> str:
    return (x or "").strip().upper()


def _norm_country(x: str) -> str:
    return _norm_code(x)


def _norm_sector(x: str) -> str:
    return _norm_code(x)


def _validate_country(code: str, *, allow_out: bool = True) -> None:
    """
    Validate that a country code exists in ocde_icio.countries.
    allow_out: allow OUT aggregate.
    """
    c = _norm_country(code)
    if allow_out and c == "OUT":
        return

    row = _fetch_one(
        """
        SELECT 1 AS ok
        FROM ocde_icio.countries
        WHERE country_code = %(c)s
        LIMIT 1;
        """,
        {"c": c},
    )
    if not row:
        raise ValueError(f"Invalid country code: {code!r}. Expected ISO3 like 'FRA', 'DEU', 'CHN' (or 'OUT').")


def _validate_sector(code: str) -> None:
    s = _norm_sector(code)
    row = _fetch_one(
        """
        SELECT 1 AS ok
        FROM ocde_icio.activities
        WHERE activity_code = %(s)s
        LIMIT 1;
        """,
        {"s": s},
    )
    if not row:
        raise ValueError(f"Invalid sector code: {code!r}. Expected OCDE ICIO activity_code like 'C26', 'C20', etc.")


def _validate_year(y: int) -> None:
    if not isinstance(y, int):
        raise ValueError("year must be an integer")
    if y < 1995 or y > 2100:
        raise ValueError(f"year out of range: {y}")


def _validate_limit(n: int) -> int:
    if not isinstance(n, int):
        raise ValueError("limit must be an integer")
    if n < 1:
        raise ValueError("limit must be >= 1")
    return min(n, 200)


def _validate_country_list(codes: List[str]) -> List[str]:
    if not isinstance(codes, list) or not codes:
        raise ValueError("buyer_countries must be a non-empty list of ISO3 codes")
    out: List[str] = []
    for c in codes:
        cc = _norm_country(c)
        _validate_country(cc, allow_out=False)
        out.append(cc)
    # de-dup while preserving order
    seen = set()
    dedup = []
    for c in out:
        if c not in seen:
            seen.add(c)
            dedup.append(c)
    return dedup


def _dsn_safe_summary(dsn: str) -> str:
    """
    Best-effort: log host/db/user without password.
    Handles typical DSN: postgresql://user:pass@host:5432/dbname
    """
    # Do not fail if format is unexpected.
    m = re.match(r"^postgres(?:ql)?://([^:]+):[^@]+@([^:/]+)(?::(\d+))?/([^?]+)", dsn)
    if not m:
        return "PG_DSN set (details hidden)"
    user, host, port, db = m.group(1), m.group(2), (m.group(3) or "5432"), m.group(4)
    return f"PG_DSN set (user={user} host={host} port={port} db={db})"


# Fail fast at import time: if PG_DSN is missing, crash immediately.
# (You can comment this block for dev if you want the server to start without DB.)
_dsn = os.getenv("PG_DSN")
if not _dsn:
    # We still allow tools/list etc. without DB? No: better fail fast for your use-case.
    raise RuntimeError("Missing env var PG_DSN (export PG_DSN=postgresql://...)")
else:
    print(_dsn_safe_summary(_dsn))


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
    if "server_time" in row and row["server_time"] is not None:
        row["server_time"] = row["server_time"].isoformat()
    return row


# -----------------------------
# Core implementations
# -----------------------------
def _top_suppliers_impl(buyer_country: str, buyer_sector: str, year: int, limit: int) -> dict:
    buyer_country = _norm_country(buyer_country)
    buyer_sector = _norm_sector(buyer_sector)
    _validate_year(year)
    limit = _validate_limit(limit)
    _validate_country(buyer_country, allow_out=False)
    _validate_sector(buyer_sector)

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
            {"supplier_country": r["supplier_country"], "dependency_pct": r["dependency_pct_str"]}
            for r in rows
        ],
    }


def _top_sectors_impl(buyer_country: str, supplier_country: str, year: int, limit: int) -> dict:
    buyer_country = _norm_country(buyer_country)
    supplier_country = _norm_country(supplier_country)
    _validate_year(year)
    limit = _validate_limit(limit)
    _validate_country(buyer_country, allow_out=False)
    _validate_country(supplier_country, allow_out=True)

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


def _compare_countries_impl(buyer_countries: list[str], buyer_sector: str, supplier_country: str, year: int) -> dict:
    buyer_sector = _norm_sector(buyer_sector)
    supplier_country = _norm_country(supplier_country)
    _validate_year(year)
    _validate_sector(buyer_sector)
    _validate_country(supplier_country, allow_out=True)
    buyer_countries_norm = _validate_country_list(buyer_countries)

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
            "buyer_countries": buyer_countries_norm,
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


def _time_series_impl(
    buyer_country: str,
    buyer_sector: str,
    supplier_country: str,
    year_from: int,
    year_to: int,
) -> dict:
    buyer_country = _norm_country(buyer_country)
    buyer_sector = _norm_sector(buyer_sector)
    supplier_country = _norm_country(supplier_country)

    _validate_year(year_from)
    _validate_year(year_to)
    if year_from > year_to:
        raise ValueError("year_from must be <= year_to")

    _validate_country(buyer_country, allow_out=False)
    _validate_sector(buyer_sector)
    _validate_country(supplier_country, allow_out=True)

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
# Dependency tools (product_dependency_*)
# -----------------------------
@mcp.tool()
def product_dependency_top_suppliers(
    buyer_country: str,
    buyer_sector: str,
    year: int = 2022,
    limit: int = 10,
) -> dict:
    """Top supplier countries for a given buyer country + sector + year."""
    return _top_suppliers_impl(buyer_country, buyer_sector, year, limit)


@mcp.tool()
def product_dependency_top_sectors(
    buyer_country: str,
    supplier_country: str,
    year: int = 2022,
    limit: int = 10,
) -> dict:
    """Top buyer sectors for a buyer country importing from a supplier."""
    return _top_sectors_impl(buyer_country, supplier_country, year, limit)


@mcp.tool()
def product_dependency_compare_countries(
    buyer_countries: list[str],
    buyer_sector: str,
    supplier_country: str,
    year: int = 2022,
) -> dict:
    """Compare dependency across multiple buyer countries for same sector + supplier."""
    return _compare_countries_impl(buyer_countries, buyer_sector, supplier_country, year)


@mcp.tool()
def product_dependency_time_series(
    buyer_country: str,
    buyer_sector: str,
    supplier_country: str,
    year_from: int = 2016,
    year_to: int = 2022,
) -> dict:
    """Time series of dependency for buyer country + sector + supplier."""
    return _time_series_impl(buyer_country, buyer_sector, supplier_country, year_from, year_to)


# -----------------------------
# Dependency tools (sector_dependency_* aliases)
# -----------------------------
@mcp.tool()
def sector_dependency_top_suppliers(
    buyer_country: str,
    buyer_sector: str,
    year: int = 2022,
    limit: int = 10,
) -> dict:
    """Alias of product_dependency_top_suppliers."""
    return _top_suppliers_impl(buyer_country, buyer_sector, year, limit)


@mcp.tool()
def sector_dependency_top_sectors(
    buyer_country: str,
    supplier_country: str,
    year: int = 2022,
    limit: int = 10,
) -> dict:
    """Alias of product_dependency_top_sectors."""
    return _top_sectors_impl(buyer_country, supplier_country, year, limit)


@mcp.tool()
def sector_dependency_compare_countries(
    buyer_countries: list[str],
    buyer_sector: str,
    supplier_country: str,
    year: int = 2022,
) -> dict:
    """Alias of product_dependency_compare_countries."""
    return _compare_countries_impl(buyer_countries, buyer_sector, supplier_country, year)


@mcp.tool()
def sector_dependency_time_series(
    buyer_country: str,
    buyer_sector: str,
    supplier_country: str,
    year_from: int = 2016,
    year_to: int = 2022,
) -> dict:
    """Alias of product_dependency_time_series."""
    return _time_series_impl(buyer_country, buyer_sector, supplier_country, year_from, year_to)


# -----------------------------
# Streamable HTTP app
# -----------------------------
streamable_http_app = mcp.streamable_http_app()
