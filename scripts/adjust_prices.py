"""Restate unadjusted (除权) daily prices as 前复权 / 后复权.

Reads the locally collected 除权除息 events and converts one instrument's
unadjusted daily bars. Two conventions are supported:

    tdx     compose the ex-rights maps the way TDX / 同花顺 do; reproduces
            what those clients display, but old prices can go negative
    factor  Tushare-style cumulative adj_factor; prices stay positive and
            percentage returns stay correct (the default)

Usage (from the workspace root, with the workspace venv):

    ./.venv/bin/python scripts/adjust_prices.py 300894.SZ
    ./.venv/bin/python scripts/adjust_prices.py 600809.SH --mode hfq --convention tdx
    ./.venv/bin/python scripts/adjust_prices.py 000002.SZ --source tdx --out qfq.csv

``--source local`` reads the local ``daily`` core table (which must hold
UNADJUSTED prices); ``--source tdx`` pulls a fresh unadjusted series from
TDX, which is also how the conversion is verified against TDX's own output.

Collect the events first::

    run_downloader("stock_capital_changes_tdx",
                   params={"scope": "all", "category": "xdxr"})
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _load_local_daily(instrument_id: str, data_root: Path):
    import duckdb

    files = sorted((data_root / "core" / "table=daily").rglob("*.parquet"))
    if not files:
        raise SystemExit(f"No local daily parquet under {data_root / 'core' / 'table=daily'}.")
    paths = [str(path) for path in files]
    frame = duckdb.sql(
        "SELECT trade_time, open, high, low, close, volume, amount "
        "FROM read_parquet($paths, union_by_name = true) "
        "WHERE instrument_id = $code ORDER BY trade_time",
        params={"paths": paths, "code": instrument_id},
    ).df()
    if frame.empty:
        raise SystemExit(f"{instrument_id} is not present in the local daily table.")
    frame["trade_date"] = frame["trade_time"].astype(str).str.slice(0, 10).str.replace("-", "")
    # The core table is an append of collector snapshots, so one trade date can
    # appear in several files; keep the most recently written row per date.
    return frame.drop_duplicates(subset="trade_date", keep="last").reset_index(drop=True)


def _load_tdx_daily(instrument_id: str):
    import pandas as pd
    from axdata_core import request_interface

    result = request_interface(
        "stock_kline_daily_tdx", params={"code": instrument_id, "adjust": "none"}
    )
    frame = pd.DataFrame(result.records)
    if frame.empty:
        raise SystemExit(f"TDX returned no rows for {instrument_id}.")
    frame["trade_date"] = frame["trade_time"].astype(str).str.slice(0, 10).str.replace("-", "")
    return frame


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python scripts/adjust_prices.py")
    parser.add_argument("code", help="Instrument id, e.g. 300894.SZ")
    parser.add_argument("--mode", default="qfq", choices=["qfq", "hfq", "none"])
    parser.add_argument("--convention", default="factor", choices=["factor", "tdx"])
    parser.add_argument("--source", default="local", choices=["local", "tdx"])
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--out", help="Optional CSV output path")
    parser.add_argument("--limit", type=int, default=10, help="Rows to print (0 for all)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    data_root = Path(args.data_root)

    from axdata_core.adjust import apply_adjustment, load_xdxr_events

    events = load_xdxr_events(data_root, instrument_id=args.code)
    if events.empty:
        print(f"No 除权除息 events for {args.code}; prices are returned unchanged.")

    prices = (
        _load_tdx_daily(args.code)
        if args.source == "tdx"
        else _load_local_daily(args.code, data_root)
    )
    adjusted = apply_adjustment(prices, events, mode=args.mode, convention=args.convention)

    if args.out:
        adjusted.to_csv(args.out, index=False)
        print(f"Wrote {len(adjusted)} rows -> {args.out}")

    columns = [c for c in ("trade_date", "open", "high", "low", "close") if c in adjusted.columns]
    view = adjusted[columns]
    print(f"{args.code} {args.mode} ({args.convention}) rows={len(view)}")
    print(
        view.to_string(index=False)
        if args.limit == 0
        else view.tail(args.limit).to_string(index=False)
    )
    negatives = int((adjusted["close"] < 0).sum())
    if negatives:
        print(f"\n{negatives} row(s) have a negative close — expected under the 'tdx' convention.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
