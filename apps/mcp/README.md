# AxData MCP Server

Exposes the local-first AxData quantitative data platform over the
[Model Context Protocol](https://modelcontextprotocol.io) (stdio transport), so
MCP clients such as **Claude Code** and **Codex CLI** can browse the interface
catalog, inspect locally collected Parquet datasets, query the stable core
tables, and make one-shot live source requests.

## Tools

| Tool | Description | Network |
| --- | --- | --- |
| `list_interfaces` | List/filter the source interface catalog (135+ interfaces). | local |
| `describe_interface` | Parameters, fields and notes for one interface. | local |
| `call_interface` | One-shot **live** request to an upstream source. | upstream |
| `list_tables` | Stable core tables and their schemas. | local |
| `query` | Query a core table (`daily`, `adj_factor`, `trade_cal`, `stock_basic_exchange`). | local |
| `list_datasets` | Locally collected Parquet datasets. | local |
| `preview_dataset` | Sample rows from a collected dataset. | local |
| `list_downloaders` | Collectors that persist source data to local Parquet. | local |
| `download` | Run a collector to **download & persist** data locally. | upstream |

Codes use the AxData id format (`000001.SZ`, `600000.SH`); dates are `YYYYMMDD`
or `YYYY-MM-DD`.

### Downloading A-share history to local

`download` runs an AxData collector (persists Parquet under `data/`). For daily
K-line history via TDX (returns the **full** history back to each stock's
listing, so 2010+ is fully covered):

```jsonc
// tool: download
{
  "target": "stock_kline_daily_tdx",          // or "tdx.stock_kline_daily_tdx.snapshot"
  "params": { "code": ["000001.SZ", "600000.SH"], "adjust": "qfq" }
}
```

Notes:
- Do **not** pass `count` for daily klines — the collector already pulls the
  full history; filter by date when reading.
- Read the data back with either `query` (canonical `ts_code`/`trade_date`/`vol`,
  supports `symbol`/`start`/`end` filters) or `preview_dataset` (raw dataset
  columns `instrument_id`/`trade_time`/`volume`). Both see all collected data.

### Full-market collection

To hold every A-share's full daily history locally (for K-line / trend
analysis), use the resumable batch collector instead of one giant call:

```bash
./.venv/bin/python scripts/collect_full_market_daily.py --batch-size 120 --adjust qfq
```

It fetches the current TDX code list (~5500 stocks) and writes one atomic
Parquet file per batch under `data/core/table=daily/`. Safe to interrupt and
re-run — already-collected codes are skipped. A full run is ~16-17M rows
(history back to listing, ~1990) in a few minutes. `list_datasets` reports the
true on-disk totals via `actual_rows`/`actual_instruments`.

## Backends

- **Local (default)** — reads the machine's AxData data directory and local
  provider plugins through `axdata_core`; no API service required.
- **Remote API** — set `AXDATA_API_BASE` (and optionally `AXDATA_TOKEN`) to route
  `query` and `call_interface` through a running AxData HTTP service.

### Environment variables

| Variable | Purpose |
| --- | --- |
| `AXDATA_DATA_DIR` / `AXDATA_HOME` | Local data root (defaults to the repo `data/`). |
| `AXDATA_API_BASE` | Optional remote AxData API base URL. |
| `AXDATA_TOKEN` | Optional bearer token for the remote API. |

## Install

The server needs the `mcp` package plus the AxData workspace (`axdata` SDK and
`axdata_core`) importable — use the workspace virtualenv created by
`scripts/bootstrap`:

```bash
./.venv/bin/python -m pip install -r apps/mcp/requirements.txt
```

Quick self-check (starts the server and lists tools over stdio):

```bash
./.venv/bin/python apps/mcp/server.py
```

(It waits on stdin for a client — Ctrl-C to exit.)

## Use from Claude Code

A project-scoped [`.mcp.json`](../../.mcp.json) is already committed at the repo
root, so opening Claude Code in this directory auto-discovers the `axdata`
server (approve it once when prompted). To register it globally instead:

```bash
claude mcp add axdata -- /ABS/PATH/AxData/.venv/bin/python /ABS/PATH/AxData/apps/mcp/server.py
```

## Use from Codex CLI

Add to `~/.codex/config.toml` (Codex needs absolute paths):

```toml
[mcp_servers.axdata]
command = "/ABS/PATH/AxData/.venv/bin/python"
args = ["/ABS/PATH/AxData/apps/mcp/server.py"]
# Optional remote API mode:
# env = { AXDATA_API_BASE = "http://127.0.0.1:8666" }
```

Replace `/ABS/PATH/AxData` with this repository's absolute path
(`pwd` from the repo root).
