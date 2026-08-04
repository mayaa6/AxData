"""Adjustment conventions derived from TDX 除权除息 events."""

from __future__ import annotations

import pandas as pd
import pytest
from axdata_core.adjust import adj_factors, apply_adjustment, xdxr_events


def _events(rows: list[dict[str, object]]) -> pd.DataFrame:
    base = {"category_raw": 1, "c1": 0.0, "c2": 0.0, "c3": 0.0, "c4": 0.0}
    return pd.DataFrame([{**base, **row} for row in rows])


def _prices(pairs: list[tuple[str, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"trade_date": date, "open": close, "high": close, "low": close, "close": close}
            for date, close in pairs
        ]
    )


def test_xdxr_events_scales_per_ten_shares() -> None:
    frame = xdxr_events(_events([{"event_date": "20210527", "c1": 6.0, "c3": 2.0, "c4": 1.0}]))
    row = frame.iloc[0]
    assert row["dividend"] == pytest.approx(0.6)
    assert row["split"] == pytest.approx(0.2)
    assert row["rights"] == pytest.approx(0.1)


def test_xdxr_events_ignores_non_dividend_categories() -> None:
    frame = xdxr_events(
        _events(
            [{"event_date": "20210527", "c1": 6.0}, {"event_date": "20220520", "category_raw": 5}]
        )
    )
    assert list(frame["event_date"]) == ["20210527"]


def test_cash_dividend_shifts_series_additively_under_tdx_convention() -> None:
    """A pure cash dividend only offsets earlier prices; this is why 前复权 can go negative."""

    events = _events([{"event_date": "20210527", "c1": 6.0}])
    prices = _prices([("20210101", 10.0), ("20210601", 12.0)])

    qfq = apply_adjustment(prices, events, mode="qfq", convention="tdx")
    hfq = apply_adjustment(prices, events, mode="hfq", convention="tdx")

    assert list(qfq["close"]) == [9.4, 12.0]
    assert list(hfq["close"]) == [10.0, 12.6]
    assert (hfq["close"] - qfq["close"]).round(4).nunique() == 1


def test_split_rescales_rather_than_shifts() -> None:
    """送转股 divides earlier prices, so a purely additive model would be wrong."""

    events = _events([{"event_date": "20210527", "c3": 10.0}])
    prices = _prices([("20210101", 12.0), ("20210601", 6.0)])

    qfq = apply_adjustment(prices, events, mode="qfq", convention="tdx")

    assert list(qfq["close"]) == [6.0, 6.0]


def test_rights_issue_is_included_in_the_reference_price() -> None:
    events = _events([{"event_date": "20210527", "c2": 4.4, "c4": 5.0}])
    prices = _prices([("20210101", 11.29), ("20210601", 8.0)])

    qfq = apply_adjustment(prices, events, mode="qfq", convention="tdx")

    # (11.29 + 0.5 * 4.4) / 1.5
    assert qfq["close"].iloc[0] == pytest.approx(8.99, abs=0.01)


def test_factor_convention_keeps_prices_positive() -> None:
    """The multiplicative convention scales instead of shifting, so it never goes negative."""

    events = _events([{"event_date": "20210527", "c1": 60.0}])
    prices = _prices([("20210101", 5.0), ("20210601", 4.0)])

    tdx = apply_adjustment(prices, events, mode="qfq", convention="tdx")
    factor = apply_adjustment(prices, events, mode="qfq", convention="factor")

    assert tdx["close"].iloc[0] < 0
    assert (factor["close"] > 0).all()


def test_adj_factor_is_cumulative_and_starts_at_one() -> None:
    events = _events([{"event_date": "20210527", "c1": 5.0}, {"event_date": "20220527", "c1": 5.0}])
    prices = _prices([("20210101", 10.0), ("20210601", 10.0), ("20220601", 10.0)])

    factors = adj_factors(events, prices)

    assert factors["adj_factor"].iloc[0] == pytest.approx(1.0)
    assert factors["adj_factor"].is_monotonic_increasing
    assert factors["adj_factor"].iloc[-1] > factors["adj_factor"].iloc[0]


def test_events_off_the_trading_calendar_still_accumulate() -> None:
    """An event dated on a non-trading day must not be silently dropped."""

    events = _events([{"event_date": "20210530", "c1": 10.0}])
    prices = _prices([("20210101", 10.0), ("20210601", 10.0)])

    qfq = apply_adjustment(prices, events, mode="qfq", convention="tdx")

    assert qfq["close"].iloc[0] == pytest.approx(9.0)


def test_mode_none_is_a_passthrough() -> None:
    events = _events([{"event_date": "20210527", "c1": 6.0}])
    prices = _prices([("20210101", 10.0)])

    assert apply_adjustment(prices, events, mode="none")["close"].iloc[0] == 10.0
