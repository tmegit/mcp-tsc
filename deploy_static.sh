#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Sync static assets to nginx directory
sudo mkdir -p /var/www/mcp-static
sudo rsync -a --delete static/ /var/www/mcp-static/

# (Optional) ensure correct perms
sudo chown -R root:root /var/www/mcp-static
sudo find /var/www/mcp-static -type d -exec chmod 755 {} \;
sudo find /var/www/mcp-static -type f -exec chmod 644 {} \;

sudo nginx -t
sudo systemctl reload nginx

echo "Static deployed to /var/www/mcp-static"
