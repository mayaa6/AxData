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
    extra_filters: dict[str, Any] = dict(filters or {})
    if symbol.strip():
        # Map the friendly ``symbol`` to the table's actual identifier column
        # (daily/adj_factor use ``ts_code``; stock_basic_exchange uses
        # ``instrument_id``), so callers don't need to know the schema.
        id_field = "ts_code"
        try:
            from axdata_core import get_schema

            names = {f.name for f in get_schema(table).fields}
            id_field = next(
                (c for c in ("ts_code", "instrument_id", "symbol") if c in names),
                "ts_code",
            )
        except Exception:  # noqa: BLE001 - fall back to ts_code
            pass
        extra_filters[id_field] = symbol.strip()
    if extra_filters:
        kwargs["filters"] = extra_filters
    if start.strip():
        kwargs["start_date"] = start.strip()
    if end.strip():
        kwargs["end_date"] = end.strip()
    kwargs["limit"] = int(limit) if limit else None
    try:
        df = client.query(table, fields=_split_fields(fields), **kwargs)
    except Exception as exc:  # noqa: BLE001
        return _dump({"error": str(exc), "table": table})
    records = _records(df)
    return _dump({"table": table, "returned": len(records), "records": records})


def _on_disk_stats(root: Path, dataset: str) -> dict[str, Any]:
    """Actual row/instrument counts from all Parquet files backing a dataset.

    The metadata-derived summary reflects the last collector run only; querying
    the files directly gives the true totals when data was written in batches.
    """

    parquet_root = root / "core" / f"table={dataset}"
    files = [str(p) for p in parquet_root.rglob("*.parquet")]
    if not files:
        return {}
    try:
        import duckdb

        rel = "read_parquet(" + repr(files) + ", union_by_name = true)"
        has_id = "instrument_id" in duckdb.sql(f"SELECT * FROM {rel} LIMIT 0").columns
        id_expr = "COUNT(DISTINCT instrument_id)" if has_id else "NULL"
        rows, instruments = duckdb.sql(f"SELECT COUNT(*), {id_expr} FROM {rel}").fetchone()
        return {"actual_rows": int(rows), "actual_instruments": instruments, "files": len(files)}
    except Exception:  # noqa: BLE001 - best-effort augmentation
        return {"files": len(files)}


@mcp.tool()
def list_datasets() -> str:
    """List locally collected Parquet datasets (the persisted data assets).

    Returns dataset name, backing interface, layer, row count, date range and
    quality status. ``row_count`` is the last collector run's metadata; when data
    was written in batches, ``actual_rows``/``actual_instruments`` reflect the
    true totals across all Parquet files. Empty until data has been collected.
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
        entry = {
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
        entry.update(_on_disk_stats(root, str(d.get("dataset"))))
        out.append(entry)
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


def _resolve_collector_id(target: str) -> str | None:
    """Resolve a collector id from a collector id or an interface name."""

    from axdata_core import list_registry_collector_dicts

    def _cid(entry: dict[str, Any]) -> str:
        return str(entry.get("collector_id") or entry.get("collector_name") or entry.get("name"))

    target = target.strip()
    collectors = list_registry_collector_dicts()
    if target in {_cid(c) for c in collectors}:
        return target
    # Match by backing interface name, e.g. "stock_kline_daily_tdx".
    for c in collectors:
        interfaces = c.get("interfaces") or []
        if target in interfaces or target == c.get("target_interface"):
            return _cid(c)
        cid = _cid(c)
        if cid.split(".")[1:2] == [target] or target in cid:
            return cid
    return None


@mcp.tool()
def list_downloaders() -> str:
    """List collectors that download source data and PERSIST it to local Parquet.

    These are the `download` targets. For A-share daily history the key one is
    ``stock_kline_daily_tdx`` (日K线, TDX), collector id
    ``tdx.stock_kline_daily_tdx.snapshot``. Returns each collector's id, backing
    interface, output dataset and default parameters.
    """

    from axdata_core import list_registry_collector_dicts

    out = []
    for c in list_registry_collector_dicts():
        out.append(
            {
                "collector_id": c.get("collector_id") or c.get("collector_name") or c.get("name"),
                "display_name_zh": c.get("display_name_zh"),
                "interfaces": c.get("interfaces"),
                "dataset_id": c.get("dataset_id"),
                "resource_group": c.get("resource_group"),
                "default_params": c.get("default_params"),
                "description": c.get("description"),
            }
        )
    return _dump({"count": len(out), "downloaders": out})


@mcp.tool()
def download(
    target: str,
    params: dict[str, Any] | None = None,
    fields: str | list[str] | None = None,
    formats: str | list[str] | None = None,
    output_dir: str = "",
) -> str:
    """Download source data and PERSIST it to the local Parquet data layer.

    Runs an AxData collector: it connects to the upstream source (network
    required), writes Parquet under the local AxData data directory, and records
    run metadata. Use `list_downloaders` to discover targets. Local mode only
    (does not use ``AXDATA_API_BASE``).

    A-share daily history example (TDX 日K线 — returns full history back to the
    stock's listing, so 2010+ is fully covered):
        target = "stock_kline_daily_tdx"   # or "tdx.stock_kline_daily_tdx.snapshot"
        params = {"code": "000001.SZ", "adjust": "qfq"}
    ``adjust`` is one of none / qfq / hfq / fixed_qfq. ``code`` accepts a single
    code, a list, or a comma-separated string. Do NOT pass ``count`` for daily
    klines — the collector already pulls the full history; filter by date
    afterwards.

    IMPORTANT: the daily-kline collector uses snapshot write mode — each run
    REPLACES the dataset. To collect several stocks, pass them all in ONE call
    (``code`` as a list); sequential single-code calls overwrite each other.

    Read the result back with `preview_dataset` (the TDX ``daily`` dataset uses
    columns instrument_id/trade_time/volume).

    Args:
        target: Collector id or backing interface name.
        params: Collector parameters (code, adjust, ...).
        fields: Optional field subset (list or comma-separated string).
        formats: Optional output formats, e.g. "parquet" or ["parquet", "csv"].
        output_dir: Optional output directory override.
    """

    from axdata_core import run_collector

    collector_id = _resolve_collector_id(target)
    if collector_id is None:
        return _dump(
            {"error": f"No collector found for {target!r}. Call list_downloaders for targets."}
        )
    try:
        result = run_collector(
            collector_id,
            params=dict(params or {}),
            fields=_split_fields(fields),
            data_root=_data_root(),
            formats=_split_fields(formats),
            output_dir=output_dir.strip() or None,
        )
    except Exception as exc:  # noqa: BLE001
        return _dump({"error": str(exc), "collector": collector_id})

    download_result = result.get("download_result") or {}
    summary = {
        "collector_id": collector_id,
        "status": result.get("status"),
        "dataset": download_result.get("dataset_id") or result.get("dataset_id"),
        "row_count": download_result.get("row_count"),
        "rows_written": download_result.get("rows_written"),
        "rows_after": download_result.get("rows_after"),
        "output_paths": download_result.get("output_paths"),
        "quality_status": (result.get("quality") or {}).get("status"),
        "duration_ms": download_result.get("duration_ms"),
    }
    return _dump(summary)


def main() -> None:
    """Run the MCP server over stdio."""

    mcp.run()


if __name__ == "__main__":
    main()
