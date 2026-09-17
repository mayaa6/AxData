"""Rebuild the local A-share daily dataset after the close, once per trading day.

Collects the current TDX code list, re-pulls every stock's full unadjusted daily
history, refreshes the 除权除息 event snapshot and materializes ``adj_factor``.

Why a full rebuild rather than an incremental append: measured on this machine a
whole-market pull is ~4 minutes (5,572 codes at ~0.04s each), and an incremental
path has to reconcile late corrections, re-adjust prices around every 除权除息
event and de-duplicate by primary key. The existing dataset already carries the
scar of getting that wrong — ``000001.SZ`` sits in it four times over. Rebuilding
costs a few minutes and cannot drift.

The new dataset is written to a staging directory and only swapped in after it
passes ``validate``; a partial or truncated collection never replaces a good one.

Usage (from the workspace root, with the workspace venv):

    ./.venv/bin/python scripts/daily_update.py            # skip non-trading days
    ./.venv/bin/python scripts/daily_update.py --force    # collect regardless
    ./.venv/bin/python scripts/daily_update.py --limit 200

Exit codes: 0 success or deliberately skipped, 1 failure (nothing was swapped).
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

CHINA_TZ = timezone(timedelta(hours=8))

#: Codes per TDX request. Larger batches amortize the round trip: 0.11s/code at
#: 120, 0.040s/code at 480.
BATCH_SIZE = 480
#: A trading session is only considered closed and final after this Beijing time,
#: matching the 15:15 rule the invest-panel cron uses.
CLOSED_AFTER = (15, 15)
#: Reject the rebuild unless this share of requested codes came back with bars.
MIN_CODE_COVERAGE = 0.98
#: Reject the rebuild if it holds less than this share of the previous row count.
MIN_ROW_RATIO = 0.95
#: Reject the rebuild unless this share of instruments carries the newest bar
#: date; suspended names legitimately lag, an upstream truncation does not.
MIN_FRESH_SHARE = 0.85
#: 除权除息 snapshots accumulate under one directory and the newest wins.
XDXR_KEEP = 10
#: Trading days of 前复权 bars to materialize. The round-bottom scanner asks for
#: 1,280 (~5 years: 800-bar body plus handle plus post-breakout tracking); the
#: margin covers a longer body without a rebuild. Measured ~16ms per instrument,
#: so a full-history pass would cost minutes instead of ~90 seconds.
QFQ_WINDOW_BARS = 1600
#: Instruments per 前复权 parquet chunk.
QFQ_CHUNK = 500
#: Run records kept under logs/daily_update.
LOG_KEEP = 120


def data_root() -> Path:
    import os

    env_dir = os.getenv("AXDATA_DATA_DIR")
    if env_dir:
        return Path(env_dir).expanduser().resolve()
    home = os.getenv("AXDATA_HOME")
    if home:
        return Path(home).expanduser().resolve() / "data"
    return (Path(__file__).resolve().parents[1] / "data").resolve()


def log(message: str) -> None:
    stamp = datetime.now(CHINA_TZ).strftime("%H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


def chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def trade_date_column(frame):
    """``trade_time`` is an ISO timestamp; the core table's key is YYYYMMDD."""

    return frame["trade_time"].astype(str).str.slice(0, 10).str.replace("-", "", regex=False)


def previous_stats(daily_dir: Path) -> dict[str, object]:
    """Row/instrument/date totals of the dataset currently in place."""

    # Only the live directory. Recursing would also pick up parquet.previous and
    # parquet.staging, and a doubled row count makes the MIN_ROW_RATIO guard
    # reject every rebuild after the first one.
    files = sorted((daily_dir / "parquet").glob("*.parquet"))
    if not files:
        return {"rows": 0, "instruments": 0, "max_date": None}

    import pyarrow.compute as pc
    import pyarrow.dataset as ds

    table = ds.dataset([str(path) for path in files], format="parquet").to_table(
        columns=["instrument_id", "trade_time"]
    )
    return {
        "rows": table.num_rows,
        "instruments": len(pc.unique(table["instrument_id"])),
        "max_date": (pc.max(table["trade_time"]).as_py() or "")[:10].replace("-", "") or None,
    }


def collect_daily(client, codes: list[str], staging: Path) -> dict[str, object]:
    """Pull every code's full unadjusted history into ``staging``."""

    staging.mkdir(parents=True, exist_ok=True)
    batches = chunks(codes, BATCH_SIZE)
    collected: set[str] = set()
    rows = 0
    failures: list[dict[str, str]] = []
    started = time.perf_counter()

    for index, batch in enumerate(batches, start=1):
        stem = f"fullmarket_{batch[0]}_{batch[-1]}".replace("/", "_")
        final_path = staging / f"{stem}.parquet"
        tmp_path = staging / f".{stem}.tmp.parquet"
        try:
            frame = client.call("stock_kline_daily_tdx", code=batch, adjust="none")
            if frame is None or frame.empty:
                raise ValueError("empty response")
            frame.to_parquet(tmp_path, index=False)
            tmp_path.replace(final_path)
            collected.update(str(value) for value in frame["instrument_id"].unique())
            rows += len(frame)
            log(
                f"batch {index}/{len(batches)}: +{len(frame)} rows, "
                f"{rows} total, {time.perf_counter() - started:.0f}s"
            )
        except Exception as exc:  # noqa: BLE001 - one bad batch must not end the run
            tmp_path.unlink(missing_ok=True)
            failures.append({"batch": f"{batch[0]}..{batch[-1]}", "error": str(exc)})
            log(f"batch {index}/{len(batches)} FAILED: {exc}")

    # One retry for the failed batches: TDX drops a connection often enough that
    # a single flake should not cost the whole rebuild.
    if failures:
        missing = [code for code in codes if code not in collected]
        log(f"retrying {len(missing)} codes from {len(failures)} failed batch(es)")
        failures = []
        for batch in chunks(missing, BATCH_SIZE):
            stem = f"retry_{batch[0]}_{batch[-1]}".replace("/", "_")
            tmp_path = staging / f".{stem}.tmp.parquet"
            try:
                frame = client.call("stock_kline_daily_tdx", code=batch, adjust="none")
                if frame is None or frame.empty:
                    raise ValueError("empty response")
                frame.to_parquet(tmp_path, index=False)
                tmp_path.replace(staging / f"{stem}.parquet")
                collected.update(str(value) for value in frame["instrument_id"].unique())
                rows += len(frame)
            except Exception as exc:  # noqa: BLE001
                tmp_path.unlink(missing_ok=True)
                failures.append({"batch": f"{batch[0]}..{batch[-1]}", "error": str(exc)})

    return {
        "rows": rows,
        "collected": len(collected),
        "requested": len(codes),
        "missing": sorted(set(codes) - collected)[:50],
        "failures": failures,
        "durationMs": int((time.perf_counter() - started) * 1000),
    }


def validate(staging: Path, requested: int, previous: dict[str, object]) -> list[str]:
    """Reasons this staged dataset must not replace the live one."""

    import pandas as pd
    import pyarrow.dataset as ds

    problems: list[str] = []
    files = sorted(staging.glob("*.parquet"))
    if not files:
        return ["staging holds no parquet files"]

    frame = (
        ds.dataset([str(path) for path in files], format="parquet")
        .to_table(columns=["instrument_id", "trade_time"])
        .to_pandas()
    )
    frame["trade_date"] = trade_date_column(frame)

    instruments = frame["instrument_id"].nunique()
    coverage = instruments / requested if requested else 0.0
    if coverage < MIN_CODE_COVERAGE:
        problems.append(
            f"code coverage {coverage:.1%} below {MIN_CODE_COVERAGE:.0%} "
            f"({instruments}/{requested})"
        )

    previous_rows = int(previous.get("rows") or 0)
    if previous_rows and len(frame) < previous_rows * MIN_ROW_RATIO:
        problems.append(
            f"{len(frame)} rows is under {MIN_ROW_RATIO:.0%} of the previous {previous_rows}"
        )

    duplicated = int(frame.duplicated(["instrument_id", "trade_date"]).sum())
    if duplicated:
        problems.append(f"{duplicated} duplicate (instrument_id, trade_date) rows")

    max_date = frame["trade_date"].max()
    previous_max = previous.get("max_date")
    if previous_max and max_date < str(previous_max):
        problems.append(f"newest bar {max_date} is older than the previous {previous_max}")

    # Suspended names lag by design, so require a share rather than all of them.
    last_bar = frame.groupby("instrument_id")["trade_date"].max()
    fresh = float((last_bar == max_date).mean())
    if fresh < MIN_FRESH_SHARE:
        problems.append(
            f"only {fresh:.1%} of instruments carry the newest bar {max_date}, "
            f"below {MIN_FRESH_SHARE:.0%}"
        )

    log(
        f"validate: {len(frame)} rows, {instruments} instruments, newest {max_date}, "
        f"{fresh:.1%} fresh, {duplicated} duplicates"
    )
    _ = pd  # imported for the groupby/duplicated API above
    return problems


def swap_in(staging: Path, daily_dir: Path) -> None:
    """Replace the live parquet directory with the staged one, keeping one backup."""

    live = daily_dir / "parquet"
    previous = daily_dir / "parquet.previous"
    shutil.rmtree(previous, ignore_errors=True)
    if live.exists():
        live.rename(previous)
    staging.rename(live)
    log(f"swapped in {live} (previous kept at {previous})")


def refresh_xdxr(client, root: Path) -> dict[str, object]:
    """Re-pull 除权除息 events; ``load_xdxr_events`` reads the newest snapshot."""

    started = time.perf_counter()
    client.download("stock_capital_changes_tdx", scope="all", category="xdxr")
    directory = root / "通达信" / "股票数据" / "基础数据" / "stock_capital_changes_tdx" / "parquet"
    snapshots = sorted(directory.glob("*.parquet"))
    for stale in snapshots[:-XDXR_KEEP]:
        stale.unlink(missing_ok=True)

    import pandas as pd

    events = pd.read_parquet(snapshots[-1]) if snapshots else None
    return {
        "snapshot": snapshots[-1].name if snapshots else None,
        "events": 0 if events is None else len(events),
        "durationMs": int((time.perf_counter() - started) * 1000),
    }


def build_adj_factors(root: Path, daily_dir: Path) -> dict[str, object]:
    """Materialize the ``adj_factor`` core table from bars plus 除权除息 events.

    Recomputing qfq per scan means re-reading the 58k-row event table and
    replaying every event for every stock; persisting the cumulative factor makes
    a downstream scan a join instead.
    """

    import pandas as pd
    import pyarrow.dataset as ds
    from axdata_core import write_core_table
    from axdata_core.adjust import adj_factors, load_xdxr_events

    started = time.perf_counter()
    events = load_xdxr_events(root)
    by_instrument = dict(tuple(events.groupby("instrument_id")))

    files = sorted((daily_dir / "parquet").glob("*.parquet"))
    prices = (
        ds.dataset([str(path) for path in files], format="parquet")
        .to_table(columns=["instrument_id", "trade_time", "close"])
        .to_pandas()
    )
    prices["trade_date"] = trade_date_column(prices)

    frames: list[pd.DataFrame] = []
    failures = 0
    for instrument_id, group in prices.groupby("instrument_id", sort=False):
        instrument_events = by_instrument.get(instrument_id)
        if instrument_events is None or instrument_events.empty:
            factors = pd.DataFrame(
                {"trade_date": group["trade_date"].to_numpy(), "adj_factor": 1.0}
            )
        else:
            try:
                factors = adj_factors(instrument_events, group)
            except Exception:  # noqa: BLE001 - one bad symbol must not stop the table
                failures += 1
                continue
        factors.insert(0, "ts_code", instrument_id)
        frames.append(factors)

    table = pd.concat(frames, ignore_index=True)
    table = table.drop_duplicates(["ts_code", "trade_date"])
    write_core_table("adj_factor", table, root=root)
    return {
        "rows": len(table),
        "instruments": table["ts_code"].nunique(),
        "failures": failures,
        "durationMs": int((time.perf_counter() - started) * 1000),
    }


def build_qfq(root: Path, daily_dir: Path) -> dict[str, object]:
    """Materialize 前复权 bars under ``core/table=daily_qfq`` in the TDX convention.

    ``tdx`` rather than ``factor`` on purpose: it is what TDX, 同花顺 and Tencent
    display, and it reproduces the Tencent ``newfqkline`` series this dataset is
    replacing bar for bar. ``adj_factor`` carries the multiplicative convention
    alongside it for anything that needs correct returns instead.
    """

    import pandas as pd
    import pyarrow.dataset as ds
    from axdata_core.adjust import apply_adjustment, load_xdxr_events

    started = time.perf_counter()
    events = load_xdxr_events(root)
    by_instrument = dict(tuple(events.groupby("instrument_id")))

    import pyarrow.compute as pc

    files = [str(path) for path in sorted((daily_dir / "parquet").glob("*.parquet"))]
    dataset = ds.dataset(files, format="parquet")

    # One window for the whole market, cut on the calendar rather than per stock,
    # so a suspended name keeps the same date range as everything else. The cutoff
    # is resolved from a timestamps-only scan and then pushed down, because the
    # full table is 16M+ rows and materializing all of it costs well over a GB.
    stamps = pc.unique(dataset.to_table(columns=["trade_time"])["trade_time"]).to_pylist()
    sessions = sorted({str(value)[:10] for value in stamps if value})
    cutoff = sessions[-QFQ_WINDOW_BARS] if len(sessions) > QFQ_WINDOW_BARS else sessions[0]

    prices = dataset.to_table(
        filter=pc.field("trade_time") >= cutoff,
        columns=["instrument_id", "symbol", "exchange", "trade_time",
                 "open", "high", "low", "close", "volume", "amount"],
    ).to_pandas()
    prices["trade_date"] = trade_date_column(prices)

    target = daily_dir.parent / "table=daily_qfq"
    staging = target / "parquet.staging"
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True, exist_ok=True)

    chunk: list[pd.DataFrame] = []
    written = 0
    instruments = 0
    failures = 0
    for instrument_id, group in prices.groupby("instrument_id", sort=True):
        instrument_events = by_instrument.get(instrument_id)
        if instrument_events is None or instrument_events.empty:
            adjusted = group.copy()
        else:
            try:
                adjusted = apply_adjustment(
                    group, instrument_events, mode="qfq",
                    convention="tdx", date_field="trade_date",
                )
            except Exception:  # noqa: BLE001 - one bad symbol must not stop the table
                failures += 1
                continue
        chunk.append(adjusted)
        instruments += 1
        if len(chunk) >= QFQ_CHUNK:
            frame = pd.concat(chunk, ignore_index=True)
            frame.to_parquet(staging / f"qfq_{written:03d}.parquet", index=False)
            written += len(frame)
            chunk = []
    if chunk:
        frame = pd.concat(chunk, ignore_index=True)
        frame.to_parquet(staging / f"qfq_{written:03d}.parquet", index=False)
        written += len(frame)

    live = target / "parquet"
    shutil.rmtree(target / "parquet.previous", ignore_errors=True)
    if live.exists():
        live.rename(target / "parquet.previous")
    staging.rename(live)

    return {
        "rows": written,
        "instruments": instruments,
        "failures": failures,
        "windowBars": QFQ_WINDOW_BARS,
        "from": cutoff.replace("-", ""),
        "durationMs": int((time.perf_counter() - started) * 1000),
    }


def write_run_record(root: Path, record: dict[str, object]) -> Path:
    directory = root.parent / "logs" / "daily_update"
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(CHINA_TZ).strftime("%Y%m%d_%H%M%S")
    path = directory / f"daily_update_{stamp}.json"
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    for stale in sorted(directory.glob("daily_update_*.json"))[:-LOG_KEEP]:
        stale.unlink(missing_ok=True)
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="Ignore the close/trading-day check.")
    parser.add_argument("--limit", type=int, default=0, help="Only first N codes (0 = all).")
    parser.add_argument("--skip-adj", action="store_true", help="Skip the adj_factor rebuild.")
    parser.add_argument("--skip-qfq", action="store_true", help="Skip the 前复权 table rebuild.")
    args = parser.parse_args(argv)

    root = data_root()
    daily_dir = root / "core" / "table=daily"
    staging = daily_dir / "parquet.staging"
    now = datetime.now(CHINA_TZ)
    record: dict[str, object] = {
        "job": "daily_update",
        "ranAt": now.isoformat(),
        "dataRoot": str(root),
        "forced": args.force,
    }

    log(f"data root: {root}")
    if not args.force and (now.hour, now.minute) < CLOSED_AFTER:
        close_at = f"{CLOSED_AFTER[0]}:{CLOSED_AFTER[1]:02d}"
        reason = f"{now:%Y-%m-%d %H:%M} 未到收盘口径（北京 {close_at}）"
        log(f"skipped: {reason}")
        record["skipped"] = reason
        write_run_record(root, record)
        return 0

    import axdata as ax

    client = ax.AxDataClient(mode="local", data_root=root)

    try:
        codes_frame = client.call("stock_codes_tdx")
        codes = sorted({str(value) for value in codes_frame["instrument_id"] if str(value)})
        if args.limit:
            codes = codes[: args.limit]
        log(f"{len(codes)} codes in scope")
        record["codes"] = len(codes)
    except Exception as exc:  # noqa: BLE001
        log(f"code list failed: {exc}")
        record["error"] = f"stock_codes_tdx: {exc}"
        write_run_record(root, record)
        return 1

    previous = previous_stats(daily_dir)
    record["previous"] = previous
    log(f"current dataset: {previous['rows']} rows, newest {previous['max_date']}")

    # The trading-day question is answered by the data, never by a holiday table:
    # one code's newest bar says whether today's session produced one.
    if not args.force:
        try:
            probe = client.call("stock_kline_daily_tdx", code=["000001.SZ"], adjust="none")
            latest = trade_date_column(probe).max()
            record["calendar"] = {"latestBar": latest, "today": f"{now:%Y%m%d}"}
            if latest != f"{now:%Y%m%d}":
                reason = f"{now:%Y-%m-%d} 不是交易日（最新收盘 {latest}）"
                log(f"skipped: {reason}")
                record["skipped"] = reason
                write_run_record(root, record)
                return 0
        except Exception as exc:  # noqa: BLE001 - a rebuild on a holiday is cheap
            log(f"trading-day probe failed, collecting anyway: {exc}")
            record["calendarError"] = str(exc)

    shutil.rmtree(staging, ignore_errors=True)
    try:
        record["collect"] = collect_daily(client, codes, staging)
        problems = validate(staging, len(codes), previous)
        record["problems"] = problems
        if problems:
            for problem in problems:
                log(f"REJECTED: {problem}")
            log("staging kept for inspection; live dataset untouched")
            record["swapped"] = False
            write_run_record(root, record)
            return 1

        swap_in(staging, daily_dir)
        record["swapped"] = True

        record["xdxr"] = refresh_xdxr(client, root)
        log(f"除权除息 events: {record['xdxr']['events']}")

        if not args.skip_adj:
            record["adjFactor"] = build_adj_factors(root, daily_dir)
            log(
                f"adj_factor: {record['adjFactor']['rows']} rows, "
                f"{record['adjFactor']['instruments']} instruments"
            )

        if not args.skip_qfq:
            record["qfq"] = build_qfq(root, daily_dir)
            log(
                f"前复权: {record['qfq']['rows']} rows, "
                f"{record['qfq']['instruments']} instruments from {record['qfq']['from']}"
            )
    except Exception as exc:  # noqa: BLE001 - always leave a run record behind
        log(f"failed: {exc}")
        record["error"] = str(exc)
        record["traceback"] = traceback.format_exc()
        write_run_record(root, record)
        return 1

    record["durationMs"] = int((datetime.now(CHINA_TZ) - now).total_seconds() * 1000)
    path = write_run_record(root, record)
    log(f"done in {record['durationMs'] / 1000:.0f}s; run record {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
