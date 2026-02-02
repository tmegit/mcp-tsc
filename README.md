# MCP – Global Production Dependency Observatory

This repository exposes a **Model Context Protocol (MCP)** server providing
structured access to **country–sector production dependency data**
(OECD ICIO-based).

The MCP is publicly accessible over HTTPS and designed to be consumed by:
- LLMs (via MCP)
- analysts
- research tools
- internal sovereign / industrial intelligence systems

---

## 🌍 Public endpoint
https://mcp.thesov.company/mcp

Protocol:
- HTTP/1.1
- Server-Sent Events (SSE)
- JSON-RPC 2.0
- Read-only

---

## 🧠 What data is exposed?

The MCP provides **country × sector dependency indicators**, answering questions like:

- *Which countries does France depend on for electronics?*
- *Which French sectors are most exposed to China?*
- *How does dependency evolve over time?*
- *How do multiple countries compare for the same supply chain risk?*

All results are **pre-aggregated**, **non-sensitive**, and **policy-safe**.

---

## 🧰 Available tools

### `product_dependency_top_suppliers`

Top supplier countries for a given buyer country + sector.

**Arguments**
```json
{
  "buyer_country": "FRA",
  "buyer_sector": "C26",
  "year": 2022,
  "limit": 10
}

⸻

product_dependency_top_sectors

Top buyer sectors importing from a given supplier country.

⸻

product_dependency_compare_countries

Compare multiple buyer countries for the same sector and supplier.

⸻

product_dependency_time_series

Time series of dependency for a buyer country × sector × supplier.

---

## Example (curl)

curl --http1.1 -N https://mcp.thesov.company/mcp \
  -H "content-type: application/json" \
  -H "accept: application/json, text/event-stream" \
  --data-binary '{
    "jsonrpc":"2.0",
    "id":1,
    "method":"tools/list"
  }'

🔐 Security & scope
	•	Read-only
	•	No personal data
	•	No firm-level data
	•	Aggregated at country / sector level
	•	Designed for public research and policy exploration

⸻

⚠️ Known limitations
	•	Based on OECD ICIO tables (macro-structure, not firm-level)
	•	Supplier country OUT = Rest of World aggregate
	•	Dependencies are economic exposure, not direct trade flows

⸻

📜 License & usage

Data access is provided for:
	•	research
	•	analysis
	•	policy design
	•	AI reasoning

Commercial reuse requires explicit authorization.

⸻

🧠 About

This MCP is developed by The Sov Company
to support economic sovereignty, supply-chain resilience, and strategic autonomy.
