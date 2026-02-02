#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

source .venv/bin/activate

# Optional .env loading (local only)
if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

: "${PG_DSN:?Missing PG_DSN. Put it in .env or export it in your shell.}"

exec python -m uvicorn server:streamable_http_app --host 127.0.0.1 --port 8088 --log-level info
