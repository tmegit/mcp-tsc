import os
import re
import functools
from typing import Any, Dict, List, Optional, Callable, Iterable, Tuple

import psycopg
from psycopg.rows import dict_row

from mcp.server.fastmcp import FastMCP

# TODO: add @require_valid_inputs decorator when exposing MCP to external agents / public traffic

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


# -----------------------------
# Normalization helpers
# -----------------------------
def _norm_code(x: str) -> str:
    return (x or "").strip().upper()


def _norm_country(x: str) -> str:
    return _norm_code(x)


def _norm_sector(x: str) -> str:
    return _norm_code(x)


# -----------------------------
# Validators
# -----------------------------
ISO3_RE = re.compile(r"^[A-Z]{3}$")


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
        raise ValueError(
            f"Invalid country code: {code!r}. Expected ISO3 like 'FRA', 'DEU', 'CHN' (or 'OUT')."
        )


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
        raise ValueError(
            f"Invalid sector code: {code!r}. Expected OCDE ICIO activity_code like 'C26', 'C20', etc."
        )


def _validate_year(y: int) -> None:
    if not isinstance(y, int):
        raise ValueError("year must be an integer")
    # ICIO is typically 1995+ ; keep wide enough not to break future
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
        if not ISO3_RE.match(cc):
            raise ValueError(f"buyer_countries: invalid ISO3 code {c!r}")
        _validate_country(cc, allow_out=False)
        out.append(cc)

    # de-dup while preserving order
    seen = set()
    dedup: List[str] = []
    for c in out:
        if c not in seen:
            seen.add(c)
            dedup.append(c)

    return dedup


# -----------------------------
# "require_*" helpers used by decorator
# -----------------------------
def _require_iso3(code: str, field: str, *, allow_out: bool) -> str:
    c = _norm_country(code)

    if allow_out and c == "OUT":
        return c

    if not ISO3_RE.match(c):
        raise ValueError(f"{field}: invalid ISO3 code {code!r}. Expected 'FRA', 'DEU', 'CHN' (or 'OUT').")

    _validate_country(c, allow_out=allow_out)
    return c


def _require_sector(code: str, field: str) -> str:
    s = _norm_sector(code)
    _validate_sector(s)
    return s


def _require_year(y: int, field: str) -> int:
    _validate_year(y)
    return y


def _require_limit(n: int, field: str) -> int:
    return _validate_limit(n)


def _require_year_range(year_from: int, year_to: int) -> Tuple[int, int]:
    y1 = _require_year(year_from, "year_from")
    y2 = _require_year(year_to, "year_to")
    if y1 > y2:
        raise ValueError("year_from must be <= year_to")
    return y1, y2


def _require_iso3_list(codes: List[str], field: str) -> List[str]:
    # reuse your existing list validator (norm + dedup + checks)
    return _validate_country_list(codes)


# -----------------------------
# Decorator: central input validation
# -----------------------------
def require_valid_inputs(
    *,
    iso3_fields: Iterable[str] = (),
    iso3_fields_allow_out: Iterable[str] = (),
    sector_fields: Iterable[str] = (),
    year_fields: Iterable[str] = (),
    limit_fields: Iterable[str] = (),
    iso3_list_fields: Iterable[str] = (),
    year_range: Optional[Tuple[str, str]] = None,
) -> Callable:
    """
    Decorator to normalize & validate MCP tool arguments.

    Important: Use as:
        @mcp.tool()
        @require_valid_inputs(...)
        def tool(...):
            ...
    """
    iso3_fields = tuple(iso3_fields)
    iso3_fields_allow_out = tuple(iso3_fields_allow_out)
    sector_fields = tuple(sector_fields)
    year_fields = tuple(year_fields)
    limit_fields = tuple(limit_fields)
    iso3_list_fields = tuple(iso3_list_fields)

    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            # ISO3 strict (no OUT)
            for f in iso3_fields:
                if f in kwargs and kwargs[f] is not None:
                    kwargs[f] = _require_iso3(kwargs[f], f, allow_out=False)

            # ISO3 allow OUT
            for f in iso3_fields_allow_out:
                if f in kwargs and kwargs[f] is not None:
                    kwargs[f] = _require_iso3(kwargs[f], f, allow_out=True)

            # Sectors
            for f in sector_fields:
                if f in kwargs and kwargs[f] is not None:
                    kwargs[f] = _require_sector(kwargs[f], f)

            # Years
            for f in year_fields:
                if f in kwargs and kwargs[f] is not None:
                    kwargs[f] = _require_year(kwargs[f], f)

            # Limits
            for f in limit_fields:
                if f in kwargs and kwargs[f] is not None:
                    kwargs[f] = _require_limit(kwargs[f], f)

            # ISO3 lists
            for f in iso3_list_fields:
                if f in kwargs and kwargs[f] is not None:
                    kwargs[f] = _require_iso3_list(kwargs[f], f)

            # Year range
            if year_range:
                f_from, f_to = year_range
                if f_from in kwargs and f_to in kwargs:
                    kwargs[f_from], kwargs[f_to] = _require_year_range(kwargs[f_from], kwargs[f_to])

            return fn(*args, **kwargs)

        return _wrapped

    return _decorator


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
# Dependency tools (product_dependency_*)
# -----------------------------
@mcp.tool()
@require_valid_inputs(
    iso3_fields=("buyer_country",),
    sector_fields=("buyer_sector",),
    year_fields=("year",),
    limit_fields=("limit",),
)
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
            {"supplier_country": r["supplier_country"], "dependency_pct": r["dependency_pct_str"]}
            for r in rows
        ],
    }


@mcp.tool()
@require_valid_inputs(
    iso3_fields=("buyer_country",),
    iso3_fields_allow_out=("supplier_country",),
    year_fields=("year",),
    limit_fields=("limit",),
)
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
@require_valid_inputs(
    iso3_list_fields=("buyer_countries",),
    sector_fields=("buyer_sector",),
    iso3_fields_allow_out=("supplier_country",),
    year_fields=("year",),
)
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
@require_valid_inputs(
    iso3_fields=("buyer_country",),
    sector_fields=("buyer_sector",),
    iso3_fields_allow_out=("supplier_country",),
    year_range=("year_from", "year_to"),
)
def product_dependency_time_series(
    buyer_country: str,
    buyer_sector: str,
    supplier_country: str,
    year_from: int = 2016,
    year_to: int = 2022,
) -> dict:
    """
    Time series of dependency for buyer country + sector + supplier.
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
# Aliases (sector_dependency_*)
# -----------------------------
@mcp.tool()
@require_valid_inputs(
    iso3_fields=("buyer_country",),
    sector_fields=("buyer_sector",),
    year_fields=("year",),
    limit_fields=("limit",),
)
def sector_dependency_top_suppliers(
    buyer_country: str,
    buyer_sector: str,
    year: int = 2022,
    limit: int = 10,
) -> dict:
    """Alias of product_dependency_top_suppliers."""
    return product_dependency_top_suppliers(buyer_country, buyer_sector, year, limit)


@mcp.tool()
@require_valid_inputs(
    iso3_fields=("buyer_country",),
    iso3_fields_allow_out=("supplier_country",),
    year_fields=("year",),
    limit_fields=("limit",),
)
def sector_dependency_top_sectors(
    buyer_country: str,
    supplier_country: str,
    year: int = 2022,
    limit: int = 10,
) -> dict:
    """Alias of product_dependency_top_sectors."""
    return product_dependency_top_sectors(buyer_country, supplier_country, year, limit)


@mcp.tool()
@require_valid_inputs(
    iso3_list_fields=("buyer_countries",),
    sector_fields=("buyer_sector",),
    iso3_fields_allow_out=("supplier_country",),
    year_fields=("year",),
)
def sector_dependency_compare_countries(
    buyer_countries: list[str],
    buyer_sector: str,
    supplier_country: str,
    year: int = 2022,
) -> dict:
    """Alias of product_dependency_compare_countries."""
    return product_dependency_compare_countries(buyer_countries, buyer_sector, supplier_country, year)


@mcp.tool()
@require_valid_inputs(
    iso3_fields=("buyer_country",),
    sector_fields=("buyer_sector",),
    iso3_fields_allow_out=("supplier_country",),
    year_range=("year_from", "year_to"),
)
def sector_dependency_time_series(
    buyer_country: str,
    buyer_sector: str,
    supplier_country: str,
    year_from: int = 2016,
    year_to: int = 2022,
) -> dict:
    """Alias of product_dependency_time_series."""
    return product_dependency_time_series(buyer_country, buyer_sector, supplier_country, year_from, year_to)


# -----------------------------
# Streamable HTTP app
# -----------------------------
streamable_http_app = mcp.streamable_http_app()
