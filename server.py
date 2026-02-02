import os
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

import psycopg
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

# -----------------------------------------------------------------------------
# Transport security (DNS rebinding protection) allowlist
# -----------------------------------------------------------------------------
transport_security = TransportSecuritySettings(
    enable_dns_rebinding_protection=True,
    allowed_hosts=[
        "localhost",
        "127.0.0.1",
        "[::1]",
        "localhost:*",
        "127.0.0.1:*",
        "[::1]:*",
        # prod later:
        # "mcp.thesovcie.com",
        # "mcp.thesovcie.com:*",
    ],
    allowed_origins=[
        "http://localhost",
        "http://127.0.0.1",
        "http://[::1]",
        "http://localhost:*",
        "http://127.0.0.1:*",
        "http://[::1]:*",
        # prod later:
        # "https://mcp.thesovcie.com",
    ],
)

# -----------------------------------------------------------------------------
# MCP server
# IMPORTANT: pass transport_security HERE (not to streamable_http_app)
# -----------------------------------------------------------------------------
mcp = FastMCP(
    name="icio-mcp",
    stateless_http=True,
    transport_security=transport_security,
)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _get_dsn() -> str:
    dsn = os.getenv("PG_DSN", "").strip()
    if not dsn:
        raise RuntimeError("Missing env var PG_DSN")
    return dsn

def fmt_pct_fr(x: float | Decimal, decimals: int = 2) -> str:
    d = x if isinstance(x, Decimal) else Decimal(str(x))
    q = Decimal("1." + "0" * decimals)
    d = d.quantize(q, rounding=ROUND_HALF_UP)
    # virgule FR
    s = f"{d:.{decimals}f}".replace(".", ",")
    return f"{s}%"

def _pct_str(x: Any, decimals: int = 2) -> str:
    """0..1 -> '50,12%' (French formatting)"""
    if x is None:
        return ""
    d = x if isinstance(x, Decimal) else Decimal(str(x))
    pct = (d * Decimal("100")).quantize(Decimal("1." + "0" * decimals), rounding=ROUND_HALF_UP)
    return f"{format(pct, 'f').replace('.', ',')}%"


def _connect():
    return psycopg.connect(_get_dsn())


# -----------------------------------------------------------------------------
# Tools
# -----------------------------------------------------------------------------
@mcp.tool(description="Simple healthcheck to confirm MCP server is alive.")
def health() -> dict:
    return {"result": "ok"}


@mcp.tool(description="Healthcheck Postgres.")
def health_db() -> dict:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("select 1")
            one = cur.fetchone()[0]
    return {"result": f"db_ok={one}"}


@mcp.tool(description="Return DB identity and server time (useful for debugging).")
def db_info() -> dict:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("select current_user, current_database(), now()")
            user, db, ts = cur.fetchone()
    return {"user": user, "database": db, "server_time": ts.isoformat()}


@mcp.tool(description="Top suppliers for a given buyer country + sector + year (R0 dependency).")
def product_dependency_top_suppliers(
    buyer_country: str,
    buyer_sector: str,
    year: int = 2022,
    limit: int = 10,
) -> dict:
    sql = """
    SELECT supplier_country, dependency
    FROM mcp.v_dep_sector_r0
    WHERE year = %s
      AND buyer_country = %s
      AND buyer_sector = %s
    ORDER BY dependency DESC
    LIMIT %s;
    """
    rows = []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (year, buyer_country, buyer_sector, limit))
            for supplier_country, dependency in cur.fetchall():
                rows.append(
                    {
                        "supplier_country": supplier_country,
                        "dependency_pct": _pct_str(dependency),
                    }
                )
    return {"year": year, "buyer_country": buyer_country, "buyer_sector": buyer_sector, "top_suppliers": rows}


@mcp.tool(description="Top buyer sectors depending on a supplier country for a given buyer country + year.")
def product_dependency_top_sectors(
    buyer_country: str,
    supplier_country: str,
    year: int = 2022,
    limit: int = 10,
) -> dict:
    sql = """
    SELECT buyer_sector, dependency
    FROM mcp.v_dep_sector_r0
    WHERE year = %s
      AND buyer_country = %s
      AND supplier_country = %s
    ORDER BY dependency DESC
    LIMIT %s;
    """
    rows = []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (year, buyer_country, supplier_country, limit))
            for buyer_sector, dependency in cur.fetchall():
                rows.append(
                    {
                        "buyer_sector": buyer_sector,
                        "dependency_pct": _pct_str(dependency),
                    }
                )
    return {"year": year, "buyer_country": buyer_country, "supplier_country": supplier_country, "top_sectors": rows}


@mcp.tool(description="Matrix (long format) for buyer_country + year (sector x supplier).")
def product_dependency_matrix(buyer_country: str, year: int = 2022, limit: int = 2000) -> dict:
    sql = """
    SELECT buyer_sector, supplier_country, dependency
    FROM mcp.v_dep_sector_r0
    WHERE year = %s
      AND buyer_country = %s
    ORDER BY buyer_sector, dependency DESC
    LIMIT %s;
    """
    rows = []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (year, buyer_country, limit))
            for buyer_sector, supplier_country, dependency in cur.fetchall():
                rows.append(
                    {
                        "buyer_sector": buyer_sector,
                        "supplier_country": supplier_country,
                        "dependency_pct": _pct_str(dependency),
                    }
                )
    return {"year": year, "buyer_country": buyer_country, "matrix": rows, "limit": limit}


@mcp.tool(description="Compare multiple buyer countries on a (buyer_sector -> supplier_country) dependency for a given year.")
def product_dependency_compare_countries(
    buyer_countries: list[str],
    buyer_sector: str,
    supplier_country: str,
    year: int = 2022,
) -> dict:
    sql = """
    SELECT buyer_country, dependency
    FROM mcp.v_dep_sector_r0
    WHERE year = %s
      AND buyer_sector = %s
      AND supplier_country = %s
      AND buyer_country = ANY(%s)
    ORDER BY buyer_country;
    """
    rows = []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (year, buyer_sector, supplier_country, buyer_countries))
            for buyer_country, dependency in cur.fetchall():
                rows.append({"buyer_country": buyer_country, "dependency_pct": _pct_str(dependency)})
    return {"year": year, "buyer_sector": buyer_sector, "supplier_country": supplier_country, "countries": rows}


@mcp.tool(description="Time series for (buyer_country, buyer_sector) dependency on supplier_country, between years.")
def product_dependency_time_series(
    buyer_country: str,
    buyer_sector: str,
    supplier_country: str,
    year_from: int = 2016,
    year_to: int = 2022,
) -> dict:
    sql = """
    SELECT year, dependency
    FROM mcp.v_dep_sector_r0
    WHERE buyer_country = %s
      AND buyer_sector = %s
      AND supplier_country = %s
      AND year BETWEEN %s AND %s
    ORDER BY year;
    """
    rows = []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (buyer_country, buyer_sector, supplier_country, year_from, year_to))
            for y, dependency in cur.fetchall():
                rows.append({"year": int(y), "dependency_pct": _pct_str(dependency)})
    return {
        "buyer_country": buyer_country,
        "buyer_sector": buyer_sector,
        "supplier_country": supplier_country,
        "series": rows,
    }


# -----------------------------------------------------------------------------
# ASGI app
# -----------------------------------------------------------------------------
streamable_http_app = mcp.streamable_http_app()
