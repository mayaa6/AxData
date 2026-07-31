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

Codes use the AxData id format (`000001.SZ`, `600000.SH`); dates are `YYYYMMDD`
or `YYYY-MM-DD`.

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
