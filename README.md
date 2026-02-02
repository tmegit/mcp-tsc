# The Sov Company – ICIO Dependency MCP (HTTPS)

Public MCP endpoint (Streamable HTTP):
- **https://mcp.thesov.company/mcp**

## What this server provides
This MCP exposes read-only analytics tools over OECD ICIO-derived dependency views:
- Top suppliers for a buyer country + sector + year
- Top dependent sectors for a buyer country importing from a supplier
- Compare dependency across multiple buyer countries
- Dependency time series

Notes:
- `buyer_country` and `supplier_country` are **ISO3** (e.g., `FRA`, `DEU`, `CHN`)
- `OUT` is an aggregate bucket (not a country)
- `buyer_sector` uses OECD ICIO `activity_code` (e.g., `C26`)

## Quick test (curl)
List tools:
```bash
curl --http1.1 -i -N https://mcp.thesov.company/mcp \
  -H "content-type: application/json" \
  -H "accept: application/json, text/event-stream" \
  --data-binary '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'

## Call a tool 
curl --http1.1 -i -N https://mcp.thesov.company/mcp \
  -H "content-type: application/json" \
  -H "accept: application/json, text/event-stream" \
  --data-binary '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"product_dependency_top_suppliers","arguments":{"buyer_country":"FRA","buyer_sector":"C26","year":2022,"limit":10}}}'

## Dataset metadata
Call:
	•	meta

## Example prompt for LLM agents
Use the tool product_dependency_top_suppliers to find France’s top supplier countries for sector C26 (Manufacture of computer, electronic, and optical products) in 2022.
Then summarize the top 5 suppliers and explicitly explain what “OUT” means in the output.
