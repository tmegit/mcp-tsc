#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
git pull
sudo systemctl restart mcp-tsc
sudo systemctl status mcp-tsc --no-pager -l
