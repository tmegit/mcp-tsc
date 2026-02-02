# MCP TSC – ICIO Dependency Server

MCP (Model Context Protocol) server exposing economic dependency indicators
computed from OCDE ICIO data.

## Features

- Sector / product dependency indicators
- Top suppliers by country & sector
- Top dependent sectors by supplier
- Cross-country comparison
- Time series (R0 dependency)
- Percentages formatted and rounded (e.g. `3,20%`)

## Requirements

- Python 3.12
- PostgreSQL (read-only access)
- `PG_DSN` environment variable

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
