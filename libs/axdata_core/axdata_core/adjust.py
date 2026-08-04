"""Convert unadjusted (除权) A-share prices into 前复权 / 后复权 series.

The local data layer stores unadjusted daily prices plus the TDX 除权除息
event table (``stock_capital_changes_tdx`` with ``category=xdxr``). Both
adjustment conventions are derived from those events on demand.

Every 除权除息 event maps an earlier price onto the post-event basis with
the standard ex-rights reference formula::

    A(p) = (p - dividend + rights * rights_price) / (1 + split + rights)

which is affine, not merely additive: a pure cash dividend only shifts the
series, but 送转股 / 配股 also rescale it.

``tdx``
    Compose those maps directly, exactly as TDX and 同花顺 do for their own
    charts. ``qfq(t)`` applies every event after ``t``; ``hfq`` is the same
    series shifted onto the latest basis. Reproduces what those clients
    display, but long dividend histories can go negative and percentage
    returns are distorted.

``factor``
    Tushare-style cumulative ``adj_factor``: each event contributes the
    ratio ``pre_close / A(pre_close)`` and prices are scaled rather than
    shifted, so returns stay correct and prices stay positive.

Event columns follow the TDX gbbq layout, quoted per 10 shares:
``c1`` 派息, ``c2`` 配股价, ``c3`` 送转股, ``c4`` 配股.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    import pandas as pd

Convention = Literal["tdx", "factor"]
Mode = Literal["qfq", "hfq", "none"]

XDXR_CATEGORY = 1


def _normalize_dates(values: pd.Series) -> pd.Series:
    import pandas as pd

    text = values.astype(str).str.replace("-", "", regex=False).str.slice(0, 8)
    return text.where(text.str.fullmatch(r"\d{8}"), pd.NA)


def xdxr_events(events: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw gbbq rows into per-share 除权除息 terms, sorted by date."""

    import pandas as pd

    normalized_columns = {"event_date", "dividend", "rights_price", "split", "rights"}
    if normalized_columns.issubset(events.columns):
        return events.sort_values("event_date").reset_index(drop=True)

    frame = events.copy()
    if "category_raw" in frame.columns:
        frame = frame.loc[pd.to_numeric(frame["category_raw"], errors="coerce") == XDXR_CATEGORY]
    frame["event_date"] = _normalize_dates(frame["event_date"])
    frame = frame.dropna(subset=["event_date"])
    for column in ("c1", "c2", "c3", "c4"):
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce").fillna(0.0)
    frame["dividend"] = frame["c1"] / 10.0
    frame["rights_price"] = frame["c2"]
    frame["split"] = frame["c3"] / 10.0
    frame["rights"] = frame["c4"] / 10.0
    columns = ["event_date", "dividend", "rights_price", "split", "rights"]
    return frame[columns].sort_values("event_date").reset_index(drop=True)


def load_xdxr_events(
    data_root: str | Path | None = None,
    *,
    instrument_id: str | None = None,
) -> pd.DataFrame:
    """Load the locally collected 除权除息 events, newest snapshot first.

    Collect them with the ``stock_capital_changes_tdx`` downloader using
    ``{"scope": "all", "category": "xdxr"}``.
    """

    import pandas as pd

    root = Path(data_root) if data_root is not None else Path("data")
    directory = root / "通达信" / "股票数据" / "基础数据" / "stock_capital_changes_tdx" / "parquet"
    files = sorted(directory.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No 除权除息 event snapshot under {directory}. Run the "
            "stock_capital_changes_tdx downloader with category=xdxr first."
        )
    frame = pd.read_parquet(files[-1])
    if instrument_id is not None:
        frame = frame.loc[frame["instrument_id"] == instrument_id]
    return frame


def _event_affine(row: pd.Series | object) -> tuple[float, float]:
    """Affine coefficients ``(a, b)`` of one event, mapping ``p -> a * p + b``."""

    denominator = 1.0 + float(row.split) + float(row.rights)
    if denominator <= 0:
        return (1.0, 0.0)
    a = 1.0 / denominator
    b = (-float(row.dividend) + float(row.rights) * float(row.rights_price)) / denominator
    return (a, b)


def _suffix_transforms(events: pd.DataFrame) -> tuple[list[str], list[tuple[float, float]]]:
    """For each event index ``k``, the composition of events ``k..n``.

    Composing ``A_2 ∘ A_1`` gives ``(a2*a1, a2*b1 + b2)``; iterating from the
    last event backwards yields every suffix in one pass.
    """

    dates = list(events["event_date"])
    suffix: list[tuple[float, float]] = [(1.0, 0.0)]
    for row in reversed(list(events.itertuples(index=False))):
        a_event, b_event = _event_affine(row)
        a_next, b_next = suffix[0]
        suffix.insert(0, (a_next * a_event, a_next * b_event + b_next))
    return dates, suffix


def adj_factors(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    date_field: str = "trade_date",
    close_field: str = "close",
) -> pd.DataFrame:
    """Tushare-style cumulative ``adj_factor`` for one instrument.

    ``prices`` must be that instrument's UNADJUSTED daily series; the close
    immediately before each event sets that event's ratio.
    """

    import pandas as pd

    frame = prices.copy()
    frame["_d"] = _normalize_dates(frame[date_field])
    frame = frame.dropna(subset=["_d"]).sort_values("_d").reset_index(drop=True)
    closes = pd.to_numeric(frame[close_field], errors="coerce")

    event_dates: list[str] = []
    cumulative: list[float] = []
    factor = 1.0
    for row in xdxr_events(events).itertuples(index=False):
        prior = closes.loc[frame["_d"] < row.event_date].dropna()
        if prior.empty:
            continue
        pre_close = float(prior.iloc[-1])
        a_event, b_event = _event_affine(row)
        reference = a_event * pre_close + b_event
        if pre_close <= 0 or reference <= 0:
            continue
        factor *= pre_close / reference
        event_dates.append(row.event_date)
        cumulative.append(factor)

    # An event may fall on a non-trading day, so match by position rather than
    # by an exact date hit: each row takes every event dated on or before it.
    running = [1.0, *cumulative]
    index = pd.Series(event_dates, dtype="object")
    positions = [int(index.searchsorted(value, side="right")) for value in frame["_d"]]
    values = [running[position] for position in positions]
    return pd.DataFrame({date_field: frame["_d"], "adj_factor": values})


def apply_adjustment(
    prices: pd.DataFrame,
    events: pd.DataFrame,
    *,
    mode: Mode = "qfq",
    convention: Convention = "factor",
    date_field: str = "trade_date",
    price_fields: tuple[str, ...] = ("open", "high", "low", "close"),
    round_to: int | None = 2,
) -> pd.DataFrame:
    """Return one instrument's UNADJUSTED daily rows restated under ``mode``."""

    import pandas as pd

    if mode == "none":
        return prices.copy()

    frame = prices.copy()
    frame["_d"] = _normalize_dates(frame[date_field])
    frame = frame.dropna(subset=["_d"]).sort_values("_d").reset_index(drop=True)
    fields = [field for field in price_fields if field in frame.columns]
    normalized = xdxr_events(events)

    if convention == "tdx":
        dates, suffix = _suffix_transforms(normalized)
        # Each row takes the composition of every event strictly after it.
        positions = [
            int(pd.Series(dates, dtype="object").searchsorted(value, side="right")) if dates else 0
            for value in frame["_d"]
        ]
        a_values = [suffix[index][0] for index in positions]
        b_values = [suffix[index][1] for index in positions]
        scale = pd.Series(a_values, index=frame.index)
        shift = pd.Series(b_values, index=frame.index)
        for field in fields:
            frame[field] = pd.to_numeric(frame[field], errors="coerce") * scale + shift
        if mode == "hfq":
            # Undo the all-events transform so the series sits on today's basis.
            a_total, b_total = suffix[0]
            for field in fields:
                frame[field] = (frame[field] - b_total) / a_total
    else:
        factors = adj_factors(normalized, frame, date_field="_d", close_field="close")
        frame = frame.merge(
            factors.rename(columns={"_d": "_fd"}), left_on="_d", right_on="_fd", how="left"
        )
        frame["adj_factor"] = frame["adj_factor"].ffill().fillna(1.0)
        latest = frame["adj_factor"].iloc[-1] if len(frame) else 1.0
        scale = frame["adj_factor"] if mode == "hfq" else frame["adj_factor"] / latest
        for field in fields:
            frame[field] = pd.to_numeric(frame[field], errors="coerce") * scale
        frame = frame.drop(columns=["_fd"])

    if round_to is not None:
        for field in fields:
            frame[field] = frame[field].round(round_to)
    return frame.drop(columns=["_d"])
