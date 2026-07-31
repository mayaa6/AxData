"""AxData MCP server.

Exposes the local-first AxData quantitative data platform over the Model
Context Protocol (stdio transport) so MCP clients such as Claude Code and
Codex CLI can browse the interface catalog, inspect local Parquet datasets,
query core tables, and make one-shot live source requests.

Backends
--------
* Local (default): reads the current machine's AxData data directory and local
  provider plugins directly through ``axdata_core`` -- no API service required.
* Remote API: set ``AXDATA_API_BASE`` (and optionally ``AXDATA_TOKEN``) to route
  ``query`` and ``call_interface`` through a running AxData HTTP service instead.

Environment variables
----------------------
* ``AXDATA_DATA_DIR`` / ``AXDATA_HOME`` -- local data root (defaults to the
  repository ``data/`` directory that ships next to this file).
* ``AXDATA_API_BASE`` -- optional remote AxData API base URL.
* ``AXDATA_TOKEN`` -- optional bearer token for the remote API.
"""

from __future__ import annotations

import dataclasses
import json
import os
from pathlib import Path
from typing import Any

try:
    # MCP Python SDK 2.x
    from mcp.server.mcpserver import MCPServer as _McpServer
except ImportError:  # pragma: no cover - MCP Python SDK 1.x fallback
    from mcp.server.fastmcp import FastMCP as _McpServer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _data_root() -> Path:
    """Resolve the local AxData data directory."""

    env_dir = os.getenv("AXDATA_DATA_DIR")
    if env_dir:
        return Path(env_dir).expanduser().resolve()
    home = os.getenv("AXDATA_HOME")
    if home:
        return (Path(home).expanduser().resolve() / "data")
    return (_REPO_ROOT / "data").resolve()


def _api_base() -> str | None:
    base = os.getenv("AXDATA_API_BASE")
    return base.strip() or None if base else None


def _client():
    """Build an AxDataClient honouring AXDATA_API_BASE (remote) or local mode."""

    import axdata as ax

    base = _api_base()
    if base:
        return ax.AxDataClient(api_base=base, token=os.getenv("AXDATA_TOKEN"))
    return ax.AxDataClient(mode="local", data_root=_data_root())


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------


def _records(df: Any) -> list[dict[str, Any]]:
    """Convert a pandas DataFrame (or record list) to JSON-safe records."""

    if df is None:
        return []
    if isinstance(df, list):
        return df
    to_json = getattr(df, "to_json", None)
    if callable(to_json):
        return json.loads(df.to_json(orient="records", date_format="iso"))
    return list(df)


def _asdict(obj: Any) -> Any:
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    return obj


def _dump(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str, indent=2)


def _split_fields(fields: str | list[str] | None) -> list[str] | None:
    if fields is None:
        return None
    if isinstance(fields, str):
        return [f.strip() for f in fields.split(",") if f.strip()] or None
    return [str(f).strip() for f in fields if str(f).strip()] or None


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

mcp = _McpServer(
    "axdata",
    instructions=(
        "AxData is a local-first quantitative (A-share) data platform. "
        "Use `list_interfaces`/`describe_interface` to discover source APIs, "
        "`call_interface` for one-shot live requests to upstream sources, "
        "`list_datasets`/`preview_dataset` to browse locally collected Parquet "
        "assets, and `list_tables`/`query` to read the stable core tables "
        "(daily, adj_factor, trade_cal, stock_basic_exchange). "
        "Codes use the AxData id format, e.g. 000001.SZ / 600000.SH; dates are "
        "YYYYMMDD or YYYY-MM-DD."
    ),
)


@mcp.tool()
def list_interfaces(keyword: str = "", source: str = "", category: str = "") -> str:
    """List available AxData source interfaces (the request/download catalog).

    Args:
        keyword: Optional case-insensitive substring matched against the
            interface name and Chinese/English display text.
        source: Optional source code filter, e.g. ``tdx``, ``exchange``,
            ``cninfo``, ``tencent``.
        category: Optional category filter (matched as a substring).

    Returns a compact JSON list of {name, display_name_zh, source_code,
    category, request_mode, description}.
    """

    from axdata_core import list_request_interface_dicts

    kw = keyword.strip().lower()
    src = source.strip().lower()
    cat = category.strip().lower()
    out: list[dict[str, Any]] = []
    for item in list_request_interface_dicts():
        if src and str(item.get("source_code", "")).lower() != src:
            continue
        if cat and cat not in str(item.get("category", "")).lower():
            continue
        if kw:
            hay = " ".join(
                str(item.get(k, ""))
                for k in ("name", "display_name_zh", "description", "source_name_zh")
            ).lower()
            if kw not in hay:
                continue
        out.append(
            {
                "name": item.get("name"),
                "display_name_zh": item.get("display_name_zh"),
                "source_code": item.get("source_code"),
                "category": item.get("category"),
                "request_mode": item.get("request_mode"),
                "description": item.get("description"),
            }
        )
    return _dump({"count": len(out), "interfaces": out})


@mcp.tool()
def describe_interface(name: str) -> str:
    """Describe one source interface: parameters, fields, and usage notes.

    Args:
        name: Interface name from `list_interfaces`, e.g.
            ``stock_basic_info_exchange`` or ``stock_realtime_snapshot_tdx``.
    """

    from axdata_core import get_request_interface

    try:
        interface = get_request_interface(name)
    except Exception as exc:  # noqa: BLE001 - surface a clean message to the model
        return _dump({"error": f"Unknown interface {name!r}: {exc}"})
    payload = interface.to_dict() if hasattr(interface, "to_dict") else _asdict(interface)
    return _dump(payload)


@mcp.tool()
def call_interface(
    interface: str,
    params: dict[str, Any] | None = None,
    fields: str | list[str] | None = None,
    limit: int = 100,
) -> str:
    """Make a one-shot LIVE request to an upstream source interface.

    This calls the real data source once (no local persistence, no collection
    task). Network access to the upstream source is required. Use
    `describe_interface` first to learn the accepted parameters.

    Args:
        interface: Interface name, e.g. ``stock_basic_info_exchange``.
        params: Request parameters, e.g. {"exchange": "SSE", "code": "000001.SZ"}.
        fields: Optional field subset (list or comma-separated string).
        limit: Maximum number of records to return in the response (client-side
            trim; default 100).
    """

    client = _client()
    try:
        df = client.call(interface, fields=_split_fields(fields), **(params or {}))
    except Exception as exc:  # noqa: BLE001
        return _dump({"error": str(exc), "interface": interface})
    records = _records(df)
    total = len(records)
    if limit and total > limit:
        records = records[:limit]
    return _dump(
        {
            "interface": interface,
            "returned": len(records),
            "total": total,
            "records": records,
        }
    )


@mcp.tool()
def list_tables() -> str:
    """List the stable local core tables and their schemas.

    These are the query-able logical tables backed by local Parquet assets:
    ``daily``, ``adj_factor``, ``trade_cal``, ``stock_basic_exchange``.
    """

    from axdata_core import get_schema
    from axdata_core import list_tables as _list_tables

    tables = []
    for name in _list_tables():
        entry: dict[str, Any] = {"table": name}
        try:
            schema = get_schema(name)
            fields = getattr(schema, "fields", None)
            if fields is not None:
                entry["fields"] = [
                    {"name": getattr(f, "name", None), "dtype": getattr(f, "dtype", None)}
                    for f in fields
                ]
        except Exception:  # noqa: BLE001 - schema is best-effort metadata
            pass
        tables.append(entry)
    return _dump({"count": len(tables), "tables": tables})


@mcp.tool()
def query(
    table: str,
    fields: str | list[str] | None = None,
    symbol: str = "",
    start: str = "",
    end: str = "",
    limit: int = 100,
    filters: dict[str, Any] | None = None,
) -> str:
    """Query a stable local core table and return records.

    Args:
        table: One of the tables from `list_tables` (e.g. ``daily``).
        fields: Optional field subset (list or comma-separated string).
        symbol: Optional instrument id filter, e.g. ``000001.SZ``.
        start: Optional start date (YYYYMMDD or YYYY-MM-DD).
        end: Optional end date (YYYYMMDD or YYYY-MM-DD).
        limit: Maximum rows to return (default 100).
        filters: Optional extra equality filters, e.g. {"exchange": "SSE"}.
    """

    client = _client()
    kwargs: dict[str, Any] = {}
    if symbol.strip():
        kwargs["symbol"] = symbol.strip()
    if start.strip():
        kwargs["start_date"] = start.strip()
    if end.strip():
        kwargs["end_date"] = end.strip()
    if filters:
        kwargs["filters"] = filters
    kwargs["limit"] = int(limit) if limit else None
    try:
        df = client.query(table, fields=_split_fields(fields), **kwargs)
    except Exception as exc:  # noqa: BLE001
        return _dump({"error": str(exc), "table": table})
    records = _records(df)
    return _dump({"table": table, "returned": len(records), "records": records})


@mcp.tool()
def list_datasets() -> str:
    """List locally collected Parquet datasets (the persisted data assets).

    Returns dataset name, backing interface, layer, row count, date range and
    quality status. Empty until collection tasks have written local data.
    """

    from axdata_core import list_datasets as _list_datasets

    root = _data_root()
    try:
        summaries = _list_datasets(data_root=root)
    except Exception as exc:  # noqa: BLE001
        return _dump({"error": str(exc), "data_root": str(root)})
    out = []
    for summary in summaries:
        d = _asdict(summary)
        out.append(
            {
                "dataset": d.get("dataset"),
                "interface_name": d.get("interface_name"),
                "display_name_zh": d.get("display_name_zh"),
                "layer": d.get("layer"),
                "row_count": d.get("row_count"),
                "date_min": d.get("date_min"),
                "date_max": d.get("date_max"),
                "columns": d.get("columns"),
                "quality_status": d.get("quality_status"),
                "updated_at": d.get("updated_at"),
            }
        )
    return _dump({"count": len(out), "data_root": str(root), "datasets": out})


@mcp.tool()
def preview_dataset(
    dataset: str,
    symbol: str = "",
    start: str = "",
    end: str = "",
    fields: str | list[str] | None = None,
    limit: int = 20,
) -> str:
    """Preview sample rows from a locally collected dataset.

    Args:
        dataset: Dataset name from `list_datasets`.
        symbol: Optional instrument id filter, e.g. ``000001.SZ``.
        start: Optional start date (YYYYMMDD or YYYY-MM-DD).
        end: Optional end date (YYYYMMDD or YYYY-MM-DD).
        fields: Optional field subset (list or comma-separated string).
        limit: Maximum rows to return (default 20).
    """

    from axdata_core import preview_dataset as _preview_dataset

    root = _data_root()
    try:
        preview = _preview_dataset(
            dataset,
            data_root=root,
            fields=_split_fields(fields),
            symbol=symbol.strip() or None,
            start=start.strip() or None,
            end=end.strip() or None,
            limit=int(limit) if limit else None,
        )
    except Exception as exc:  # noqa: BLE001
        return _dump({"error": str(exc), "dataset": dataset})
    d = _asdict(preview)
    return _dump(
        {
            "dataset": d.get("dataset"),
            "columns": d.get("columns"),
            "returned": len(d.get("rows") or []),
            "rows": d.get("rows"),
        }
    )


def main() -> None:
    """Run the MCP server over stdio."""

    mcp.run()


if __name__ == "__main__":
    main()
