"""Collect full-market A-share daily K-line history into the local data layer.

Fetches the current TDX stock code list, then runs the daily-kline collector in
resumable batches so the local ``daily`` core table holds every stock's full
history (back to listing). Safe to interrupt and re-run: codes already present
in the local dataset are skipped.

Usage (from the workspace root, with the workspace venv):

    ./.venv/bin/python scripts/collect_full_market_daily.py               # full market
    ./.venv/bin/python scripts/collect_full_market_daily.py --limit 20    # smoke test
    ./.venv/bin/python scripts/collect_full_market_daily.py --batch-size 150 --adjust qfq

Reads/writes the AxData data root (AXDATA_DATA_DIR / AXDATA_HOME, else ./data).
Each batch appends one Parquet file under ``core/table=daily/``; ``query('daily')``
and ``preview_dataset('daily')`` union all of them.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path


def _data_root() -> Path:
    import os

    env_dir = os.getenv("AXDATA_DATA_DIR")
    if env_dir:
        return Path(env_dir).expanduser().resolve()
    home = os.getenv("AXDATA_HOME")
    if home:
        return (Path(home).expanduser().resolve() / "data")
    return (Path(__file__).resolve().parents[1] / "data").resolve()


def _collected_instrument_ids(data_root: Path) -> set[str]:
    """Instrument ids already present in the local daily dataset."""

    files = sorted((data_root / "core" / "table=daily").rglob("*.parquet"))
    if not files:
        return set()
    import duckdb

    paths = [str(f) for f in files]
    try:
        rows = duckdb.sql(
            "SELECT DISTINCT instrument_id FROM "
            "read_parquet($paths, union_by_name = true)",
            params={"paths": paths},
        ).fetchall()
    except Exception:
        # Fall back to a per-file scan if a single file is unreadable.
        ids: set[str] = set()
        for path in paths:
            try:
                for (value,) in duckdb.sql(
                    f"SELECT DISTINCT instrument_id FROM read_parquet('{path}')"
                ).fetchall():
                    if value:
                        ids.add(str(value))
            except Exception:
                continue
        return ids
    return {str(value) for (value,) in rows if value}


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=120, help="Codes per collector run.")
    parser.add_argument("--adjust", default="qfq", choices=["none", "qfq", "hfq"], help="Adjust.")
    parser.add_argument("--limit", type=int, default=0, help="Only first N codes (0 = all).")
    parser.add_argument("--no-resume", action="store_true", help="Re-collect present codes.")
    parser.add_argument("--exchanges", default="", help="Filter, e.g. SSE,SZSE,BSE.")
    args = parser.parse_args(argv)

    import axdata as ax

    data_root = _data_root()
    client = ax.AxDataClient(mode="local", data_root=data_root)
    out_dir = data_root / "core" / "table=daily" / "parquet"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[axdata] data root: {data_root}", flush=True)
    print("[axdata] fetching TDX stock code list ...", flush=True)
    codes_df = client.call("stock_codes_tdx")
    id_col = "instrument_id" if "instrument_id" in codes_df.columns else codes_df.columns[0]
    if args.exchanges and "exchange" in codes_df.columns:
        wanted = {x.strip().upper() for x in args.exchanges.split(",") if x.strip()}
        codes_df = codes_df[codes_df["exchange"].str.upper().isin(wanted)]
    all_codes = [str(x) for x in codes_df[id_col].tolist() if str(x)]
    all_codes = sorted(set(all_codes))
    if args.limit:
        all_codes = all_codes[: args.limit]
    print(f"[axdata] {len(all_codes)} codes in scope.", flush=True)

    done: set[str] = set() if args.no_resume else _collected_instrument_ids(data_root)
    if done:
        print(f"[axdata] {len(done)} codes already collected; skipping them.", flush=True)
    pending = [c for c in all_codes if c not in done]
    print(f"[axdata] {len(pending)} codes to collect.", flush=True)
    if not pending:
        print("[axdata] nothing to do.", flush=True)
        return 0

    batches = _chunks(pending, max(1, args.batch_size))
    total_rows = 0
    failures: list[tuple[str, str]] = []
    started = time.perf_counter()

    for index, batch in enumerate(batches, start=1):
        label = f"{batch[0]}..{batch[-1]}" if len(batch) > 1 else batch[0]
        stem = f"fullmarket_{batch[0]}_{batch[-1]}".replace("/", "_")
        final_path = out_dir / f"{stem}.parquet"
        tmp_path = out_dir / f".{stem}.tmp.parquet"
        try:
            frame = client.call(
                "stock_kline_daily_tdx", code=batch, adjust=args.adjust
            )
            rows = 0 if frame is None else len(frame)
            # Write atomically to a unique per-batch file; query('daily') unions all.
            frame.to_parquet(tmp_path, index=False)
            tmp_path.replace(final_path)
            total_rows += int(rows)
            elapsed = time.perf_counter() - started
            print(
                f"[axdata] batch {index}/{len(batches)} ({len(batch)} codes {label}): "
                f"+{rows} rows | cumulative {total_rows} rows | {elapsed:.0f}s",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001 - keep going, record the failure
            tmp_path.unlink(missing_ok=True)
            failures.append((label, str(exc)))
            print(f"[axdata] batch {index}/{len(batches)} ({label}) FAILED: {exc}", flush=True)

    elapsed = time.perf_counter() - started
    print(
        f"[axdata] done: {len(batches) - len(failures)}/{len(batches)} batches ok, "
        f"{total_rows} rows written in {elapsed:.0f}s.",
        flush=True,
    )
    if failures:
        print(f"[axdata] {len(failures)} batch(es) failed:", flush=True)
        for label, message in failures:
            print(f"  - {label}: {message}", flush=True)
        print("[axdata] re-run to retry failed/missing codes (resume skips done ones).", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
