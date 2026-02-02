#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
source .venv/bin/activate

: "${PG_DSN:?Missing PG_DSN. Example: export PG_DSN='postgresql://mcp_reader:***@127.0.0.1:5432/sov_main'}"

exec python -m uvicorn server:streamable_http_app --host 127.0.0.1 --port 8088 --log-level info
