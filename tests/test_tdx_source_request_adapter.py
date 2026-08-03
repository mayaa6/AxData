from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
import threading
import time
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo
from zipfile import ZipFile

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TDX_PACKAGE_ROOT = REPO_ROOT / "packages" / "axdata-source-tdx"
TDX_PROVIDER_ID = "axdata.source.tdx_external"
sys.path.insert(0, str(TDX_PACKAGE_ROOT / "src"))

import axdata_core.adapters.tdx.request as tdx_request
from axdata_core._tdx_wire.models.quote import (
    CategoryQuote,
    CategoryQuotePage,
    QuoteLevel,
    QuoteRefreshBatch,
    QuoteRefreshRecord,
)
from axdata_core.adapters.tdx import interface_sets as tdx_interface_sets
from axdata_core.adapters.tdx import price_limit_calendar as tdx_price_limit_calendar
from axdata_core.adapters.tdx import request_client as tdx_request_client
from axdata_core.adapters.tdx import request_dispatch as tdx_request_dispatch
from axdata_core.adapters.tdx import request_limits as tdx_request_limits
from axdata_core.adapters.tdx import (
    TdxRequestAdapter,
    instrument_id_to_tdx_code,
    tdx_code_to_instrument_id,
)

from axdata_source_tdx._tdx_wire._security_classification import classify_board, classify_security
from axdata_core.adapters.tdx.stats_resource import (
    default_tdx_stats_metadata_path,
    default_tdx_stats_resource_path,
    request_tdx_stats_resource,
)
from axdata_core.source_errors import (
    SourceAdapterNotFound,
    SourceInterfaceNotFound,
    SourceRequestValidationError,
)
from axdata_core.source_request import (
    request_interface,
)
from axdata_core.tdx_f10_specs import F10_INTERFACE_SPECS



@pytest.fixture(autouse=True)
def _enable_local_tdx_provider(monkeypatch, tmp_path):
    from axdata_core.plugin_config import enable_provider
    from axdata_core.plugins import manifest_from_provider
    from axdata_core.provider_registry import ProviderRegistry
    from axdata_source_tdx.provider import provider as tdx_provider

    data_root = tmp_path / "axdata"
    monkeypatch.setenv("AXDATA_DATA_DIR", str(data_root))
    monkeypatch.setenv("AXDATA_TDX_STATS_ROOT", str(tmp_path / "default-tdx-stats-cache"))
    for root in (data_root, tmp_path, tmp_path / "data"):
        enable_provider(TDX_PROVIDER_ID, data_root=root)

    def build_registry(**_kwargs):
        registry = ProviderRegistry(enabled_provider_ids={TDX_PROVIDER_ID})
        registry.register_manifest(
            manifest_from_provider(tdx_provider),
            provider=tdx_provider,
            enabled=True,
            built_in=False,
        )
        return registry

    monkeypatch.setattr(
        "axdata_core.provider_catalog.build_builtin_provider_registry",
        build_registry,
    )


def _optional_tdx_provider_module(module_name: str):
    full_name = f"axdata_source_tdx.{module_name}"
    try:
        return importlib.import_module(full_name)
    except ModuleNotFoundError as exc:
        if exc.name in {"axdata_source_tdx", full_name}:
            return None
        raise


def test_tdx_request_interface_sets_are_shared_with_dispatch_module():
    assert tdx_request.SUPPORTED_INTERFACES is tdx_interface_sets.SUPPORTED_INTERFACES
    assert tdx_request.FINANCE_INTERFACE_FIELDS is tdx_interface_sets.FINANCE_INTERFACE_FIELDS
    assert tdx_request.INTRADAY_SUBCHART_INTERFACE_SPECS is tdx_interface_sets.INTRADAY_SUBCHART_INTERFACE_SPECS
    assert "stock_finance_profile_tdx" in tdx_request.SUPPORTED_INTERFACES
    assert "stock_intraday_buy_sell_strength_tdx" in tdx_request.SUPPORTED_INTERFACES
    assert "stock_kline_daily_tdx" in tdx_request.SUPPORTED_INTERFACES
    assert "stock_topic_exposure_tdx" in tdx_request.SUPPORTED_INTERFACES


def test_tdx_request_compat_reuses_loaded_wrapper_exports():
    code = (
        "import axdata_core.adapters.tdx.request as request\n"
        "from axdata_core.adapters.tdx import interface_sets, request_dispatch\n"
        "sentinel_supported = {'sentinel_tdx_interface'}\n"
        "sentinel_exact = {'sentinel_tdx_interface': '_request_sentinel'}\n"
        "interface_sets.SUPPORTED_INTERFACES = sentinel_supported\n"
        "request_dispatch.TDX_EXACT_REQUEST_METHODS = sentinel_exact\n"
        "request.__dict__.pop('SUPPORTED_INTERFACES', None)\n"
        "request.__dict__.pop('TDX_EXACT_REQUEST_METHODS', None)\n"
        "print('supported_same=' + str(request.SUPPORTED_INTERFACES is sentinel_supported))\n"
        "print('exact_same=' + str(request.TDX_EXACT_REQUEST_METHODS is sentinel_exact))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(TDX_PACKAGE_ROOT / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "supported_same=True" in result.stdout
    assert "exact_same=True" in result.stdout


def test_tdx_request_limits_are_reexported_from_dispatch_module():
    assert tdx_request.TDX_CODE_PAGE_SIZE == tdx_request_limits.TDX_CODE_PAGE_SIZE
    assert tdx_request.TDX_SUSPENSION_HOST_COUNT == tdx_request_limits.TDX_SUSPENSION_HOST_COUNT
    assert tdx_request.TDX_SUSPENSION_POOL_SIZE == tdx_request_limits.TDX_SUSPENSION_POOL_SIZE
    assert tdx_request.TDX_KLINE_HOST_COUNT == tdx_request_limits.TDX_KLINE_HOST_COUNT
    assert tdx_request.TDX_KLINE_POOL_SIZE == tdx_request_limits.TDX_KLINE_POOL_SIZE
    assert tdx_request.TDX_TRADE_PAGE_SIZE == tdx_request_limits.TDX_TRADE_PAGE_SIZE
    assert tdx_request.TDX_RANK_DEFAULT_COUNT == tdx_request_limits.TDX_RANK_DEFAULT_COUNT
    assert tdx_request.TDX_LIMIT_LADDER_SCAN_PAGE_SIZE == tdx_request_limits.TDX_LIMIT_LADDER_SCAN_PAGE_SIZE


def test_tdx_price_limit_calendar_source_request_bridge_uses_injected_request():
    calls = []

    def fake_request_interface(interface_name, *, params, fields, persist):
        calls.append(
            {
                "interface_name": interface_name,
                "params": params,
                "fields": fields,
                "persist": persist,
            }
        )
        return SimpleNamespace(
            records=[
                {
                    "cal_date": "20260622",
                    "is_open": True,
                    "pretrade_date": "20260619",
                    "next_trade_date": "20260623",
                }
            ]
        )

    result = tdx_price_limit_calendar.latest_calendar_dates_from_source_request(
        today=date(2026, 6, 22),
        request_interface=fake_request_interface,
        default_trade_date=lambda today_text: today_text,
        before_daily_close_buffer=lambda _now: False,
    )

    assert calls == [
        {
            "interface_name": "stock_trade_calendar_exchange",
            "params": {"start_date": "20260615", "end_date": "20260706"},
            "fields": ["cal_date", "is_open", "pretrade_date", "next_trade_date"],
            "persist": False,
        }
    ]
    assert result == tdx_price_limit_calendar.PriceLimitCalendarDates(
        target_trade_date="20260622",
        pre_close_trade_date="20260619",
        source="exchange_calendar",
        snapshot_base_field="last_price",
    )


def test_tdx_exact_request_methods_cover_only_simple_dispatch_paths():
    exact_methods = tdx_request.TDX_EXACT_REQUEST_METHODS
    assert exact_methods is tdx_request_dispatch.TDX_EXACT_REQUEST_METHODS
    assert callable(tdx_request_dispatch.dispatch_adapter_request)
    assert callable(tdx_request_dispatch.dispatch_request_with_client)
    assert callable(tdx_request_dispatch.supports_tdx_interface)
    assert exact_methods["index_codes_tdx"] == "_request_index_codes"
    assert exact_methods["stock_daily_share_tdx"] == "_request_stock_daily_share"
    assert exact_methods["etf_kline_tdx"] == "_request_etf_kline"

    for interface_name, method_name in exact_methods.items():
        assert interface_name in tdx_request.SUPPORTED_INTERFACES
        assert hasattr(TdxRequestAdapter, method_name)

    assert "stock_codes_tdx" not in exact_methods
    assert "stock_suspensions_tdx" not in exact_methods
    assert "stock_kline_daily_tdx" not in exact_methods
    assert "stock_intraday_buy_sell_strength_tdx" not in exact_methods


def test_tdx_supports_helper_matches_adapter_supported_interfaces():
    adapter = TdxRequestAdapter(client=object())

    assert tdx_request_dispatch.supports_tdx_interface("stock_codes_tdx", tdx_request.SUPPORTED_INTERFACES) is True
    assert tdx_request_dispatch.supports_tdx_interface("stock_topic_exposure_tdx", tdx_request.SUPPORTED_INTERFACES) is True
    assert tdx_request_dispatch.supports_tdx_interface("custom_interface", tdx_request.SUPPORTED_INTERFACES) is False
    assert adapter.supports("stock_codes_tdx") is True
    assert adapter.supports("stock_topic_exposure_tdx") is True
    assert adapter.supports("custom_interface") is False


def test_tdx_dispatch_helper_preserves_lifecycle_and_stock_code_meta():
    class FakeClient:
        def __init__(self):
            self.connected = False
            self.closed = False

        def connect(self):
            self.connected = True

        def close(self):
            self.closed = True

    class FakeAdapter:
        def __init__(self):
            self.last_meta = {}

        def _request_stock_codes(self, client, params):
            assert client.connected is True
            self.last_meta = {
                "tdx_server": "request-server",
                "tdx_code_market_scan_mode": "parallel",
            }
            return [{"instrument_id": "000001.SZ"}]

    client = FakeClient()
    adapter = FakeAdapter()
    rows = tdx_request_dispatch.dispatch_request_with_client(
        adapter,
        "stock_codes_tdx",
        {},
        client,
        should_close=True,
        use_parallel_suspension_quotes=None,
        intraday_subchart_interfaces=(),
        finance_interface_fields=(),
        kline_interface_specs=(),
        exact_request_methods={},
        client_meta=lambda _: {"tdx_server": "client-server", "tdx_pool_size": 3},
        not_found_error=lambda name: AssertionError(name),
    )

    assert rows == [{"instrument_id": "000001.SZ"}]
    assert client.closed is True
    assert adapter.last_meta == {
        "tdx_server": "request-server",
        "tdx_pool_size": 3,
        "tdx_code_market_scan_mode": "parallel",
    }


def test_tdx_request_client_helpers_keep_dispatch_module_monkeypatch_points():
    assert callable(tdx_request_client.create_request_client)
    assert callable(tdx_request_client.request_hosts)
    assert callable(tdx_request_client.configured_hosts)
    assert callable(tdx_request_client.should_parallelize_stock_code_request)
    assert callable(tdx_request.create_tdx_client)
    assert callable(tdx_request.effective_host_strings)


def test_tdx_request_client_helper_respects_pool_size_env(monkeypatch):
    params = {"scope": "all"}

    monkeypatch.delenv("AXDATA_TDX_POOL_SIZE", raising=False)
    assert tdx_request._should_parallelize_stock_code_request(params) is True

    monkeypatch.setenv("AXDATA_TDX_POOL_SIZE", "1")
    assert tdx_request._should_parallelize_stock_code_request(params) is False


def test_tdx_request_client_helper_stock_code_pool_size_matches_market_count():
    assert tdx_request._stock_code_request_pool_size({"scope": "all"}) == 3
    assert tdx_request._stock_code_request_pool_size({"exchange": "SSE"}) == 1


def test_tdx_request_client_helper_uses_explicit_connection_options_first():
    calls = []

    def fake_create_client(**kwargs):
        calls.append(kwargs)
        return {"client": "explicit"}

    client = tdx_request_client.create_request_client(
        "stock_codes_tdx",
        {"scope": "all"},
        {"hosts": ["option-host:7709"], "pool_size": 8},
        has_connection_options=lambda options: True,
        create_client=fake_create_client,
        option_pool_size=lambda options: options["pool_size"],
        option_hosts=lambda options, *, configured_hosts: options["hosts"],
        configured_hosts_from_options=lambda options: ["configured-host:7709"],
        should_parallelize_stock_codes=lambda params: True,
        stock_code_pool_size=lambda params: 3,
    )

    assert client == {"client": "explicit"}
    assert calls == [{"hosts": ["option-host:7709"], "pool_size": 8}]


def test_tdx_request_client_helper_parallelizes_stock_codes_without_explicit_options():
    calls = []

    def fake_create_client(**kwargs):
        calls.append(kwargs)
        return {"client": "stock-codes"}

    client = tdx_request_client.create_request_client(
        "stock_codes_tdx",
        {"scope": "all"},
        {},
        has_connection_options=lambda options: False,
        create_client=fake_create_client,
        option_pool_size=lambda options: None,
        option_hosts=lambda options, *, configured_hosts: ["option-host:7709"],
        configured_hosts_from_options=lambda options: ["configured-a:7709", "configured-b:7709"],
        should_parallelize_stock_codes=lambda params: True,
        stock_code_pool_size=lambda params: 3,
    )

    assert client == {"client": "stock-codes"}
    assert calls == [{"hosts": ["configured-a:7709", "configured-b:7709"], "pool_size": 3}]


def test_tdx_request_client_helper_uses_configured_hosts_for_plain_requests():
    calls = []

    def fake_create_client(**kwargs):
        calls.append(kwargs)
        return {"client": "plain"}

    client = tdx_request_client.create_request_client(
        "stock_realtime_snapshot_tdx",
        {"code": "000001.SZ"},
        {},
        has_connection_options=lambda options: False,
        create_client=fake_create_client,
        option_pool_size=lambda options: None,
        option_hosts=lambda options, *, configured_hosts: ["option-host:7709"],
        configured_hosts_from_options=lambda options: ["configured-host:7709"],
        should_parallelize_stock_codes=lambda params: True,
        stock_code_pool_size=lambda params: 3,
    )

    assert client == {"client": "plain"}
    assert calls == [{"hosts": ["configured-host:7709"]}]


def test_tdx_execution_utils_helpers_cover_client_meta_and_progress():
    from axdata_core.adapters.tdx import execution_utils

    client = SimpleNamespace(
        transport=SimpleNamespace(
            hosts=("host-a:7709", "host-b:7709"),
            connected_hosts=("host-a:7709",),
            connected_host="host-a:7709",
            pool_size=3,
            heartbeat_interval=1.5,
        ),
        pool_size=4,
        heartbeat_interval=2.0,
    )
    rows, meta = execution_utils.finish_request_result(
        client=client,
        result=SimpleNamespace(rows=[{"code": "000001.SZ"}], meta={"source": "tdx"}),
    )

    assert execution_utils.client_pool_size(client) == 3
    assert execution_utils.chunked([1, 2, 3, 4], 2) == [[1, 2], [3, 4]]
    assert rows == [{"code": "000001.SZ"}]
    assert meta["tdx_connected_host"] == "host-a:7709"
    assert meta["tdx_configured_host_count"] == 2
    assert meta["tdx_pool_size"] == 3
    assert meta["source"] == "tdx"


def test_tdx_stats_resource_params_prefer_public_param_over_internal_option():
    from axdata_core.adapters.tdx.options import stats_resource_params

    assert stats_resource_params({"code": "000001.SZ"}, {"stats_cache_root": "internal"}) == {
        "code": "000001.SZ",
        "stats_cache_root": "internal",
    }
    assert stats_resource_params(
        {"code": "000001.SZ", "stats_cache_root": "public"},
        {"stats_cache_root": "internal"},
    ) == {
        "code": "000001.SZ",
        "stats_cache_root": "public",
    }


class FakeAuctionResultTradeClient:
    def __init__(self) -> None:
        self.today_trade_calls = []
        self.historical_trade_calls = []
        self.connected = False

    def connect(self) -> None:
        self.connected = True

    def get_today_trades(self, code, *, start=0, count=115, include_raw=False):
        self.today_trade_calls.append(
            {"code": code, "start": start, "count": count, "include_raw": include_raw}
        )
        if start > 0:
            return SimpleNamespace(
                exchange=code[:2],
                code=code[2:],
                full_code=code,
                start=start,
                request_count=count,
                records=(),
            )
        return SimpleNamespace(
            exchange=code[:2],
            code=code[2:],
            full_code=code,
            start=start,
            request_count=count,
            records=(
                SimpleNamespace(
                    trade_time=datetime(2026, 5, 19, 9, 24).time(),
                    trade_datetime=None,
                    trade_date=None,
                    absolute_index=0,
                    price=10.8,
                    volume=100,
                    order_count=5,
                    side="neutral",
                ),
                SimpleNamespace(
                    trade_time=datetime(2026, 5, 19, 9, 25).time(),
                    trade_datetime=None,
                    trade_date=None,
                    absolute_index=1,
                    price=10.86,
                    volume=1200,
                    order_count=28,
                    side="buy",
                ),
                SimpleNamespace(
                    trade_time=datetime(2026, 5, 19, 9, 26).time(),
                    trade_datetime=None,
                    trade_date=None,
                    absolute_index=2,
                    price=10.9,
                    volume=300,
                    order_count=11,
                    side="sell",
                ),
            ),
        )

    def get_historical_trades(self, code, *, trade_date, start=0, count=900, include_raw=False):
        self.historical_trade_calls.append(
            {
                "code": code,
                "trade_date": trade_date,
                "start": start,
                "count": count,
                "include_raw": include_raw,
            }
        )
        trade_day = date(
            int(str(trade_date)[:4]),
            int(str(trade_date)[4:6]),
            int(str(trade_date)[6:8]),
        )
        if start > 0:
            return SimpleNamespace(
                exchange=code[:2],
                code=code[2:],
                full_code=code,
                trade_date=trade_day,
                start=start,
                request_count=count,
                records=(),
            )
        return SimpleNamespace(
            exchange=code[:2],
            code=code[2:],
            full_code=code,
            trade_date=trade_day,
            start=start,
            request_count=count,
            records=(
                SimpleNamespace(
                    trade_time=datetime(2026, 5, 11, 9, 24).time(),
                    trade_datetime=datetime(2026, 5, 11, 9, 24, tzinfo=timezone(timedelta(hours=8))),
                    trade_date=trade_day,
                    absolute_index=0,
                    price=10.8,
                    volume=100,
                    order_count=5,
                    side="neutral",
                ),
                SimpleNamespace(
                    trade_time=datetime(2026, 5, 11, 9, 25).time(),
                    trade_datetime=datetime(2026, 5, 11, 9, 25, tzinfo=timezone(timedelta(hours=8))),
                    trade_date=trade_day,
                    absolute_index=1,
                    price=10.86,
                    volume=1200,
                    order_count=28,
                    side="buy",
                ),
                SimpleNamespace(
                    trade_time=datetime(2026, 5, 11, 9, 26).time(),
                    trade_datetime=datetime(2026, 5, 11, 9, 26, tzinfo=timezone(timedelta(hours=8))),
                    trade_date=trade_day,
                    absolute_index=2,
                    price=10.9,
                    volume=300,
                    order_count=11,
                    side="sell",
                ),
            ),
        )


KLINE_PERIOD_CASES = [
    ("stock_kline_second_tdx", {"code": "000001.SZ", "seconds": 5, "count": 2}, "5s"),
    ("stock_kline_minute_tdx", {"code": "000001.SZ", "period": "15m", "count": 2}, "15m"),
    ("stock_kline_nminute_tdx", {"code": "000001.SZ", "minutes": 10, "count": 2}, "10m"),
    ("stock_kline_daily_tdx", {"code": "000001.SZ", "count": 2}, "day"),
    ("stock_kline_nday_tdx", {"code": "000001.SZ", "days": 45, "count": 2}, "45d"),
    ("stock_kline_weekly_tdx", {"code": "000001.SZ", "count": 2}, "week"),
    ("stock_kline_monthly_tdx", {"code": "000001.SZ", "count": 2}, "month"),
    ("stock_kline_quarterly_tdx", {"code": "000001.SZ", "count": 2}, "quarter"),
    ("stock_kline_yearly_tdx", {"code": "000001.SZ", "count": 2}, "year"),
]

KLINE_PUBLIC_PARAM_CASES = [
    ("stock_kline_second_tdx", {"code": "000001.SZ", "seconds": 5}),
    ("stock_kline_minute_tdx", {"code": "000001.SZ", "period": "1m"}),
    ("stock_kline_nminute_tdx", {"code": "000001.SZ", "minutes": 10}),
    ("stock_kline_daily_tdx", {"code": "000001.SZ"}),
    ("stock_kline_nday_tdx", {"code": "000001.SZ", "days": 45}),
    ("stock_kline_weekly_tdx", {"code": "000001.SZ"}),
    ("stock_kline_monthly_tdx", {"code": "000001.SZ"}),
    ("stock_kline_quarterly_tdx", {"code": "000001.SZ"}),
    ("stock_kline_yearly_tdx", {"code": "000001.SZ"}),
]


class FakeTdxClient:
    def __init__(self, *, include_extra_stocks: bool = False, include_etfs: bool = False) -> None:
        self.calls = []
        self.quote_calls = []
        self.explicit_quote_calls = []
        self.refresh_quote_calls = []
        self.category_quote_calls = []
        self.category_quote_pages = None
        self.kline_calls = []
        self.capital_change_calls = []
        self.finance_calls = []
        self.price_limit_calls = []
        self.auction_calls = []
        self.subchart_calls = []
        self.intraday_calls = []
        self.recent_intraday_calls = []
        self.today_intraday_calls = []
        self.today_trade_calls = []
        self.historical_trade_calls = []
        self.download_file_calls = []
        self.resource_payloads = {}
        self.connected = False
        self.include_extra_stocks = include_extra_stocks
        self.include_etfs = include_etfs

    def connect(self) -> None:
        self.connected = True

    def download_file_resource(self, path, *, chunk_size=30000, max_bytes=None):
        self.download_file_calls.append({"path": path, "chunk_size": chunk_size, "max_bytes": max_bytes})
        return self.resource_payloads[path]

    def get_codes(self, market, *, start=0, limit=None):
        self.calls.append({"market": market, "start": start, "limit": limit})
        etf_rows_sh = [
            SimpleNamespace(
                full_code="sh510050",
                code="510050",
                exchange="sh",
                name="50ETF",
                category="etf",
                category_reason="test",
                board="none",
                board_reason="test",
                decimal=3,
                multiple=100,
                previous_close_price=2.86,
                volume_ratio_base=1.0,
            ),
            SimpleNamespace(
                full_code="sh588000",
                code="588000",
                exchange="sh",
                name="科创50ETF",
                category="etf",
                category_reason="test",
                board="none",
                board_reason="test",
                decimal=3,
                multiple=100,
                previous_close_price=1.12,
                volume_ratio_base=1.0,
            ),
        ]
        etf_rows_sz = [
            SimpleNamespace(
                full_code="sz159915",
                code="159915",
                exchange="sz",
                name="创业板ETF",
                category="etf",
                category_reason="test",
                board="none",
                board_reason="test",
                decimal=3,
                multiple=100,
                previous_close_price=2.04,
                volume_ratio_base=1.0,
            ),
            SimpleNamespace(
                full_code="sz159919",
                code="159919",
                exchange="sz",
                name="300ETF",
                category="etf",
                category_reason="test",
                board="none",
                board_reason="test",
                decimal=3,
                multiple=100,
                previous_close_price=3.98,
                volume_ratio_base=1.0,
            ),
        ]
        if market == "sh":
            if start >= 1600:
                return []
            index_rows = [
                SimpleNamespace(
                    full_code="sh000001",
                    code="000001",
                    exchange="sh",
                    name="上证指数",
                    category="index",
                    category_reason="test",
                    board="none",
                    board_reason="test",
                    decimal=2,
                    multiple=100,
                    previous_close_price=4108.08,
                    volume_ratio_base=1.0,
                ),
                SimpleNamespace(
                    full_code="sh881001",
                    code="881001",
                    exchange="sh",
                    name="通达信板块指数",
                    category="index",
                    category_reason="test",
                    board="none",
                    board_reason="test",
                    decimal=2,
                    multiple=100,
                    previous_close_price=1000.0,
                    volume_ratio_base=1.0,
                ),
            ]
            if not self.include_extra_stocks:
                return (index_rows + etf_rows_sh) if self.include_etfs else index_rows
            extra = [
                SimpleNamespace(
                    full_code="sh600717",
                    code="600717",
                    exchange="sh",
                    name="天津港",
                    category="a_share",
                    category_reason="test",
                    board="sse_main_board",
                    board_reason="test",
                    decimal=2,
                    multiple=100,
                    previous_close_price=4.1,
                    volume_ratio_base=1.0,
                )
            ] + index_rows
            return (extra + etf_rows_sh) if self.include_etfs else extra
        if market == "bj":
            return []
        if start >= 1600:
            rows = [
                SimpleNamespace(
                    full_code="sz200001",
                    code="200001",
                    exchange="sz",
                    name="深市B股",
                    category="b_share",
                    category_reason="test",
                    board="none",
                    board_reason="test",
                    decimal=2,
                    multiple=100,
                    previous_close_price=3.2,
                    volume_ratio_base=1.0,
                ),
                SimpleNamespace(
                    full_code="sz000001",
                    code="000001",
                    exchange="sz",
                    name="平安银行",
                    category="a_share",
                    category_reason="test",
                    board="szse_main_board",
                    board_reason="test",
                    decimal=2,
                    multiple=100,
                    previous_close_price=10.2,
                    volume_ratio_base=1.0,
                ),
            ]
            if self.include_extra_stocks:
                rows.extend(
                    [
                        SimpleNamespace(
                            full_code="sz000004",
                            code="000004",
                            exchange="sz",
                            name="*ST国华",
                            category="a_share",
                            category_reason="test",
                            board="szse_main_board",
                            board_reason="test",
                            decimal=2,
                            multiple=100,
                            previous_close_price=10.2,
                            volume_ratio_base=1.0,
                        ),
                        SimpleNamespace(
                            full_code="sz001399",
                            code="001399",
                            exchange="sz",
                            name="未上市样本",
                            category="a_share",
                            category_reason="test",
                            board="szse_main_board",
                            board_reason="test",
                            decimal=2,
                            multiple=100,
                            previous_close_price=0,
                            volume_ratio_base=0,
                        ),
                        SimpleNamespace(
                            full_code="sz002102",
                            code="002102",
                            exchange="sz",
                            name="ST能特",
                            category="a_share",
                            category_reason="test",
                            board="szse_main_board",
                            board_reason="test",
                            decimal=2,
                            multiple=100,
                            previous_close_price=2.2,
                            volume_ratio_base=1.0,
                        ),
                    ]
                )
            if self.include_etfs:
                rows.extend(etf_rows_sz)
            return rows
        if limit == 2:
            return self._prefix_rows()
        return self._prefix_rows() + [
            SimpleNamespace(
                full_code=f"sz399{index:03d}",
                code=f"399{index:03d}",
                exchange="sz",
                name=f"指数{index}",
                category="index",
                category_reason="test",
                board="none",
                board_reason="test",
                decimal=2,
                multiple=100,
                previous_close_price=10.2,
                volume_ratio_base=1.0,
            )
            for index in range(2, 1600)
        ]

    def get_legacy_quotes(self, securities, *, batch_size=80):
        self.quote_calls.append({"securities": list(securities), "batch_size": batch_size})
        status_by_security = {
            ("sz", "000001"): 0x0000,
            ("sz", "000004"): 0xC020,
            ("sz", "001399"): 0x8000,
            ("sh", "600717"): 0x8020,
        }
        return [
            SimpleNamespace(
                exchange=market,
                code=code,
                full_code=f"{market}{code}",
                bid_levels=(
                    SimpleNamespace(price=10.13, volume=320),
                    SimpleNamespace(price=10.12, volume=118),
                    SimpleNamespace(price=10.11, volume=94),
                    SimpleNamespace(price=10.10, volume=87),
                    SimpleNamespace(price=10.09, volume=66),
                ),
                ask_levels=(
                    SimpleNamespace(price=10.14, volume=428),
                    SimpleNamespace(price=10.15, volume=260),
                    SimpleNamespace(price=10.16, volume=136),
                    SimpleNamespace(price=10.17, volume=92),
                    SimpleNamespace(price=10.18, volume=71),
                ),
                trading_status_raw=status_by_security.get((market, code), 0x0000),
            )
            for market, code in securities
        ]

    def get_explicit_quotes(self, securities, *, batch_size=80):
        self.explicit_quote_calls.append({"securities": list(securities), "batch_size": batch_size})
        return [
            SimpleNamespace(
                exchange=market,
                code=code,
                full_code=f"{market}{code}",
                last_price=10.14 if code == "000001" else 8.42,
                pre_close=10.28 if code == "000001" else 8.39,
                open=10.13 if code == "000001" else 8.4,
                high=10.2 if code == "000001" else 8.45,
                low=10.08 if code == "000001" else 8.38,
                change=-0.14 if code == "000001" else 0.03,
                change_pct=-1.361868 if code == "000001" else 0.357568,
                amplitude_pct=1.167315 if code == "000001" else 0.834327,
                total_hand=1000 if code == "000001" else 2100,
                current_hand=15 if code == "000001" else 22,
                amount=1014000.0 if code == "000001" else 1768200.0,
                inside_dish=400 if code == "000001" else 920,
                outer_disc=600 if code == "000001" else 1180,
                open_amount=10000.0 if code == "000001" else 182000.0,
                bid1_price=10.13 if code == "000001" else 8.42,
                bid1_volume=320 if code == "000001" else 610,
                ask1_price=10.14 if code == "000001" else 8.43,
                ask1_volume=428 if code == "000001" else 430,
                rise_speed=0.21 if code == "000001" else 0.05,
                short_turnover=0.08 if code == "000001" else 0.03,
                min2_amount=320000.0 if code == "000001" else 510000.0,
                opening_rush=0.12 if code == "000001" else 0.04,
                vol_rise_speed=1.25 if code == "000001" else 0.92,
                entrust_ratio=18.5 if code == "000001" else 9.6,
                activity=11 if code == "000001" else 22,
            )
            for market, code in securities
        ]

    def get_price_limits_all(self):
        self.price_limit_calls.append({"all": True})
        return []

    def get_quote_refresh(self, cursors):
        self.refresh_quote_calls.append({"cursors": list(cursors)})
        records = []
        for market, code, _cursor in cursors:
            records.append(
                QuoteRefreshRecord(
                    exchange=market,
                    market_id=0 if market == "sz" else 1 if market == "sh" else 2,
                    code=code,
                    active=31 if code == "000001" else 42,
                    last_price=10.15 if code == "000001" else 8.43,
                    pre_close=10.28 if code == "000001" else 8.39,
                    open=10.13 if code == "000001" else 8.4,
                    high=10.2 if code == "000001" else 8.45,
                    low=10.08 if code == "000001" else 8.38,
                    update_time_raw=103000 if code == "000001" else 103500,
                    status_or_reserved_raw=0,
                    total_hand=1008 if code == "000001" else 2110,
                    current_hand=8 if code == "000001" else 10,
                    amount_raw=0,
                    amount=1023120.0 if code == "000001" else 1778730.0,
                    inside_dish=405 if code == "000001" else 925,
                    outer_disc=603 if code == "000001" else 1185,
                    open_amount_raw=1000,
                    open_amount=10000.0 if code == "000001" else 182000.0,
                    bid_levels=(
                        QuoteLevel(price=10.14 if code == "000001" else 8.43, volume=300),
                        QuoteLevel(price=10.13 if code == "000001" else 8.42, volume=210),
                    ),
                    ask_levels=(
                        QuoteLevel(price=10.15 if code == "000001" else 8.44, volume=420),
                        QuoteLevel(price=10.16 if code == "000001" else 8.45, volume=180),
                    ),
                )
            )
        return QuoteRefreshBatch(records=tuple(records))

    def get_category_quotes(
        self,
        *,
        category=6,
        sort_type=0,
        start=0,
        count=80,
        ascending=False,
        filter_raw=0,
    ):
        self.category_quote_calls.append(
            {
                "category": category,
                "sort_type": sort_type,
                "start": start,
                "count": count,
                "ascending": ascending,
                "filter_raw": filter_raw,
            }
        )
        if self.category_quote_pages is not None:
            records = self.category_quote_pages.get(start, [])[:count]
            return SimpleNamespace(
                category=category,
                sort_type=sort_type,
                start=start,
                request_count=count,
                sort_reverse=0 if sort_type == 0 else (2 if ascending else 1),
                filter_raw=filter_raw,
                records=records,
            )
        records = self.get_explicit_quotes([("sz", "000001"), ("sh", "600000")])[:count]
        return SimpleNamespace(
            category=category,
            sort_type=sort_type,
            start=start,
            request_count=count,
            sort_reverse=0 if sort_type == 0 else (2 if ascending else 1),
            filter_raw=filter_raw,
            records=records,
        )

    def get_auction_process(self, code, *, mode_or_selector_raw=3, start=0, count=500, include_raw=False):
        self.auction_calls.append(
            {
                "code": code,
                "mode_or_selector_raw": mode_or_selector_raw,
                "start": start,
                "count": count,
                "include_raw": include_raw,
            }
        )
        return SimpleNamespace(
            exchange=code[:2],
            market_id=0 if code[:2] == "sz" else 1 if code[:2] == "sh" else 2,
            code=code[2:],
            full_code=code,
            mode_or_selector_raw=mode_or_selector_raw,
            start=start,
            request_count=count,
            records=(
                SimpleNamespace(
                    index=0,
                    auction_time=datetime(2026, 5, 11, 9, 15, 0).time(),
                    time_seconds=33300,
                    price=162.12,
                    price_milli=162120,
                    matched_volume=2568,
                    matched_amount_estimated=41632416.0,
                    unmatched_signed_raw=2433,
                    unmatched_volume=2433,
                    unmatched_amount_estimated=394442.0,
                    unmatched_direction=1,
                ),
                SimpleNamespace(
                    index=1,
                    auction_time=datetime(2026, 5, 11, 9, 15, 9).time(),
                    time_seconds=33309,
                    price=162.12,
                    price_milli=162120,
                    matched_volume=6630,
                    matched_amount_estimated=107485560.0,
                    unmatched_signed_raw=-1115,
                    unmatched_volume=1115,
                    unmatched_amount_estimated=180763.8,
                    unmatched_direction=-1,
                ),
            ),
        )

    def get_kline(
        self,
        code,
        *,
        period="day",
        start=0,
        count=800,
        adjust=None,
        anchor_date=None,
        kind="stock",
    ):
        self.kline_calls.append(
            {
                "code": code,
                "period": period,
                "start": start,
                "count": count,
                "adjust": adjust,
                "anchor_date": anchor_date,
                "kind": kind,
            }
        )
        bar_time = (
            datetime(2024, 5, 31, 15, 0, tzinfo=timezone(timedelta(hours=8)))
            if period == "day"
            else datetime(2026, 5, 19, 13, 39, 50, tzinfo=timezone(timedelta(hours=8)))
        )
        bars = (
            (
                SimpleNamespace(
                    time=datetime(2024, 5, 31, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                    open=10.0,
                    high=10.0,
                    low=10.0,
                    close=10.0,
                    open_price_milli=10000,
                    high_price_milli=10000,
                    low_price_milli=10000,
                    close_price_milli=10000,
                    last_close_price_milli=9000,
                    volume_lots=100.0,
                    amount=100000.0,
                    up_count=1087 if kind == "index" else None,
                    down_count=1222 if kind == "index" else None,
                ),
                SimpleNamespace(
                    time=datetime(2024, 6, 3, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                    open=8.0,
                    high=8.0,
                    low=8.0,
                    close=8.0,
                    open_price_milli=8000,
                    high_price_milli=8000,
                    low_price_milli=8000,
                    close_price_milli=8000,
                    last_close_price_milli=10000,
                    volume_lots=120.0,
                    amount=96000.0,
                    up_count=785 if kind == "index" else None,
                    down_count=1534 if kind == "index" else None,
                ),
            )
            if period == "day"
            else (
                SimpleNamespace(
                    time=bar_time,
                    open=10.13,
                    high=10.15,
                    low=10.12,
                    close=10.14,
                    volume_lots=120.0,
                    amount=121680.0,
                    up_count=900 if kind == "index" else None,
                    down_count=1200 if kind == "index" else None,
                ),
            )
        )
        return SimpleNamespace(
            exchange=code[:2],
            code=code[2:],
            full_code=code,
            period_raw=13,
            period_param_raw=int(str(period).removesuffix("s")) if str(period).endswith("s") else 1,
            period_name=period,
            start=start,
            request_count=count,
            adjust_mode_raw=0,
            adjust_mode=adjust or "none",
            anchor_date_raw=0,
            bars=bars,
        )

    def get_capital_changes(self, code, *, include_raw=False):
        self.capital_change_calls.append({"code": code, "include_raw": include_raw})
        return SimpleNamespace(
            exchange=code[:2],
            code=code[2:],
            full_code=code,
            records=(
                SimpleNamespace(
                    exchange=code[:2],
                    code=code[2:],
                    full_code=code,
                    date=date(2024, 6, 1),
                    category_raw=1,
                    category_name="除权除息",
                    c1_float=1.0,
                    c2_float=0.0,
                    c3_float=2.0,
                    c4_float=0.0,
                    c1_quantity=1.0,
                    c2_quantity=0.0,
                    c3_quantity=2.0,
                    c4_quantity=0.0,
                    c1_raw=b"\x00\x00\x80?",
                    c2_raw=b"\x00\x00\x00\x00",
                    c3_raw=b"\x00\x00\x00@",
                    c4_raw=b"\x00\x00\x00\x00",
                    record_hex="0000303030303100c054340101000000000000000000004000000000",
                ),
            ),
        )

    def get_finance_info(self, code, *, include_raw=False):
        codes = list(code) if isinstance(code, (list, tuple)) else [code]
        self.finance_calls.append({"code": codes, "include_raw": include_raw})
        records = []
        for value in codes:
            if value == "sz999999":
                records.append(
                    SimpleNamespace(
                        exchange="sz",
                        code="999999",
                        full_code="sz999999",
                        updated_date_raw=0,
                        ipo_date_raw=0,
                        updated_date=None,
                        ipo_date=None,
                        float_share=0.0,
                        total_share=0.0,
                        total_assets=0.0,
                        net_profit=0.0,
                        eps=0.0,
                        bps=0.0,
                        is_empty=True,
                    )
                )
                continue
            records.append(
                SimpleNamespace(
                    exchange=value[:2],
                    code=value[2:],
                    full_code=value,
                    updated_date=date(2026, 4, 25),
                    ipo_date=date(1991, 4, 3),
                    province_raw=18,
                    province_name=None,
                    province_board_name=None,
                    province_board_code=None,
                    industry_raw=101,
                    tdx_industry_code=None,
                    tdx_industry_name=None,
                    tdx_industry_path=None,
                    tdx_research_industry_code=None,
                    tdx_research_industry_name=None,
                    tdx_research_industry_path=None,
                    total_share=19405918750.0,
                    float_share=19405601250.0,
                    state_share=0.0,
                    founder_legal_person_share=0.0,
                    legal_person_share=0.0,
                    b_share=0.0,
                    h_share=0.0,
                    shareholder_count=457610,
                    eps=0.67,
                    bps=23.91,
                    total_assets=35277000000.0,
                    current_assets=1000000.0,
                    fixed_assets=2000000.0,
                    intangible_assets=3000000.0,
                    current_liabilities=4000000.0,
                    long_term_liabilities=5000000.0,
                    capital_reserve=6000000.0,
                    net_assets=7000000.0,
                    revenue=35277000000.0,
                    main_business_profit=8000000.0,
                    accounts_receivable=9000000.0,
                    operating_profit=10000000.0,
                    investment_income=11000000.0,
                    operating_cashflow=12000000.0,
                    total_cashflow=13000000.0,
                    inventory=14000000.0,
                    total_profit=15000000.0,
                    after_tax_profit=16000000.0,
                    net_profit=14523000000.0,
                    undistributed_profit=17000000.0,
                    is_empty=False,
                )
            )
        return SimpleNamespace(records=tuple(records), count=len(records))

    def get_historical_intraday(self, code, *, trade_date, include_raw=False):
        self.intraday_calls.append({"code": code, "trade_date": trade_date, "include_raw": include_raw})
        trade_day = date(
            int(str(trade_date)[:4]),
            int(str(trade_date)[4:6]),
            int(str(trade_date)[6:8]),
        )
        return SimpleNamespace(
            exchange=code[:2],
            code=code[2:],
            full_code=code,
            trade_date=trade_day,
            prev_close=10.08,
            points=(
                SimpleNamespace(
                    time=datetime(2026, 5, 19, 9, 31, tzinfo=timezone(timedelta(hours=8))),
                    trade_date=trade_day,
                    minute_index=0,
                    price=10.13,
                    volume=120,
                ),
                SimpleNamespace(
                    time=datetime(2026, 5, 19, 9, 32, tzinfo=timezone(timedelta(hours=8))),
                    trade_date=trade_day,
                    minute_index=1,
                    price=10.15,
                    volume=80,
                ),
            ),
        )

    def get_recent_historical_intraday(self, code, *, trade_date, include_raw=False):
        self.recent_intraday_calls.append({"code": code, "trade_date": trade_date, "include_raw": include_raw})
        trade_day = date(
            int(str(trade_date)[:4]),
            int(str(trade_date)[4:6]),
            int(str(trade_date)[6:8]),
        )
        return SimpleNamespace(
            exchange=code[:2],
            code=code[2:],
            full_code=code,
            trade_date=trade_day,
            prev_close=10.08,
            open_price=10.12,
            points=(
                SimpleNamespace(
                    time=datetime(2026, 5, 19, 9, 31, tzinfo=timezone(timedelta(hours=8))),
                    trade_date=trade_day,
                    time_label="09:31",
                    minute_index=0,
                    price=10.13,
                    avg_price=10.115,
                    volume=120,
                ),
                SimpleNamespace(
                    time=datetime(2026, 5, 19, 9, 32, tzinfo=timezone(timedelta(hours=8))),
                    trade_date=trade_day,
                    time_label="09:32",
                    minute_index=1,
                    price=10.15,
                    avg_price=10.128,
                    volume=80,
                ),
            ),
        )

    def get_today_intraday(self, code, *, include_raw=False):
        self.today_intraday_calls.append({"code": code, "include_raw": include_raw})
        return SimpleNamespace(
            exchange=code[:2],
            code=code[2:],
            full_code=code,
            reserved_zero=0,
            points=(
                SimpleNamespace(
                    time_label="09:31",
                    minute_index=0,
                    price=10.86,
                    avg_price=10.8417,
                    volume=120,
                ),
                SimpleNamespace(
                    time_label="09:32",
                    minute_index=1,
                    price=10.88,
                    avg_price=10.8427,
                    volume=80,
                ),
            ),
        )

    def get_intraday_subchart(self, code, *, selector=0, include_raw=False):
        self.subchart_calls.append({"code": code, "selector": selector, "include_raw": include_raw})
        if selector == 0x0B:
            points = (
                SimpleNamespace(
                    index=0,
                    previous_day_cumulative_volume=59153.0,
                    current_day_cumulative_volume=105552.0,
                    cumulative_volume=105552.0,
                ),
                SimpleNamespace(
                    index=1,
                    previous_day_cumulative_volume=98511.0,
                    current_day_cumulative_volume=177336.0,
                    cumulative_volume=177336.0,
                ),
            )
            selector_name = "volume_comparison"
        else:
            points = (
                SimpleNamespace(index=0, bid_order=174, ask_order=98),
                SimpleNamespace(index=1, bid_order=792, ask_order=87),
            )
            selector_name = "buy_sell_strength"
        return SimpleNamespace(
            exchange=code[:2],
            code=code[2:],
            full_code=code,
            selector_raw=selector,
            selector_name=selector_name,
            points=points,
        )

    def get_today_trades(self, code, *, start=0, count=115, include_raw=False):
        self.today_trade_calls.append(
            {"code": code, "start": start, "count": count, "include_raw": include_raw}
        )
        if start > 0:
            return SimpleNamespace(
                exchange=code[:2],
                code=code[2:],
                full_code=code,
                start=start,
                request_count=count,
                records=(),
            )
        return SimpleNamespace(
            exchange=code[:2],
            code=code[2:],
            full_code=code,
            start=start,
            request_count=count,
            records=(
                SimpleNamespace(
                    trade_time=datetime(2026, 5, 19, 14, 8).time(),
                    trade_datetime=None,
                    trade_date=None,
                    index=0,
                    absolute_index=0,
                    price=10.86,
                    volume=89,
                    order_count=9,
                    side="buy",
                ),
                SimpleNamespace(
                    trade_time=datetime(2026, 5, 19, 14, 8).time(),
                    trade_datetime=None,
                    trade_date=None,
                    index=1,
                    absolute_index=1,
                    price=10.85,
                    volume=86,
                    order_count=8,
                    side="sell",
                ),
            ),
        )

    def get_historical_trades(self, code, *, trade_date, start=0, count=900, include_raw=False):
        self.historical_trade_calls.append(
            {
                "code": code,
                "trade_date": trade_date,
                "start": start,
                "count": count,
                "include_raw": include_raw,
            }
        )
        trade_day = date(
            int(str(trade_date)[:4]),
            int(str(trade_date)[4:6]),
            int(str(trade_date)[6:8]),
        )
        if start > 0:
            return SimpleNamespace(
                exchange=code[:2],
                code=code[2:],
                full_code=code,
                trade_date=trade_day,
                start=start,
                request_count=count,
                records=(),
            )
        return SimpleNamespace(
            exchange=code[:2],
            code=code[2:],
            full_code=code,
            trade_date=trade_day,
            start=start,
            request_count=count,
            records=(
                SimpleNamespace(
                    trade_time=datetime(2026, 5, 11, 14, 12).time(),
                    trade_datetime=datetime(2026, 5, 11, 14, 12, tzinfo=timezone(timedelta(hours=8))),
                    trade_date=trade_day,
                    index=0,
                    absolute_index=0,
                    price=10.86,
                    volume=89,
                    order_count=9,
                    side="buy",
                ),
                SimpleNamespace(
                    trade_time=datetime(2026, 5, 11, 14, 13).time(),
                    trade_datetime=datetime(2026, 5, 11, 14, 13, tzinfo=timezone(timedelta(hours=8))),
                    trade_date=trade_day,
                    index=1,
                    absolute_index=1,
                    price=10.85,
                    volume=86,
                    order_count=8,
                    side="sell",
                ),
            ),
        )

    def _prefix_rows(self):
        return [
            SimpleNamespace(
                full_code="sz399001",
                code="399001",
                exchange="sz",
                name="深证成指",
                category="index",
                category_reason="test",
                board="none",
                board_reason="test",
                decimal=2,
                multiple=100,
                previous_close_price=10.2,
                volume_ratio_base=1.0,
            ),
            SimpleNamespace(
                full_code="sz159001",
                code="159001",
                exchange="sz",
                name="深证ETF",
                category="etf",
                category_reason="test",
                board="none",
                board_reason="test",
                decimal=3,
                multiple=100,
                previous_close_price=1.2,
                volume_ratio_base=1.0,
            ),
        ]


class FakeTqlexClient:
    def __init__(self, payloads=None):
        self.payloads = payloads or {}
        self.calls = []

    def request(self, entry, body):
        self.calls.append({"entry": entry, "body": body})
        if entry in self.payloads:
            return self.payloads[entry]
        return {"ErrorCode": 0, "ResultSets": []}


class EchoTqlexClient:
    def __init__(self):
        self.calls = []

    def request(self, entry, body):
        self.calls.append({"entry": entry, "body": body})
        if entry == "CWServ.tdxf10_gg_comreq":
            return {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["T002"],
                        "Content": [["20260614"]],
                    }
                ],
            }
        return {
            "ErrorCode": 0,
            "ResultSets": [
                {
                    "ColName": _f10_source_columns_for_entry(entry),
                    "Content": [_f10_source_values_for_entry(entry)],
                },
                {
                    "ColName": _f10_source_columns_for_entry(entry),
                    "Content": [_f10_source_values_for_entry(entry)],
                },
            ],
        }


def _f10_source_columns_for_entry(entry):
    columns = []
    for spec in F10_INTERFACE_SPECS.values():
        if spec.entry != entry:
            continue
        for field in spec.fields:
            sources = field.source if isinstance(field.source, tuple) else (field.source,)
            for source in sources:
                if not source or source.startswith("{") or source in columns:
                    continue
                columns.append(source)
    return columns or ["N001"]


def _f10_source_values_for_entry(entry):
    return [_f10_sample_value(column) for column in _f10_source_columns_for_entry(entry)]


def _f10_sample_value(column):
    upper = str(column).upper()
    if "DATE" in upper or upper in {"RQ", "CJRQ", "ZTRQ", "RXSJ", "JZRQ", "PUBLISH_DATE"}:
        return "20260614"
    if "TIME" in upper:
        return "09:30:00"
    if "URL" in upper:
        return "http://example.test/a.pdf"
    if "TEXT" in upper or "TXT" in upper:
        return "示例正文"
    return "1"


def _minimal_f10_params(spec):
    params = {}
    for parameter in spec.params:
        if parameter.name == "code":
            params[parameter.name] = "000034.SZ"
        elif parameter.default is not None:
            params[parameter.name] = parameter.default
        elif parameter.required:
            params[parameter.name] = {
                "event_date": "20260614",
                "detail_id": "1001",
                "concept_code": "2817",
            }.get(parameter.name, "1")
    return params


def test_tdx_adapter_supports_registered_tdx_interfaces_only():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    assert adapter.supports("stock_codes_tdx") is True
    assert adapter.supports("index_codes_tdx") is True
    assert adapter.supports("index_realtime_snapshot_tdx") is True
    assert adapter.supports("index_realtime_rank_tdx") is True
    assert adapter.supports("index_quote_refresh_tdx") is True
    assert adapter.supports("index_intraday_today_tdx") is True
    assert adapter.supports("index_intraday_history_tdx") is True
    assert adapter.supports("index_kline_tdx") is True
    assert adapter.supports("stock_st_list_tdx") is True
    assert adapter.supports("stock_suspensions_tdx") is True
    assert adapter.supports("stock_adj_factor_tdx") is True
    assert adapter.supports("stock_realtime_snapshot_tdx") is True
    assert adapter.supports("stock_order_book_tdx") is True
    assert adapter.supports("stock_intraday_buy_sell_strength_tdx") is True
    assert adapter.supports("stock_intraday_volume_comparison_tdx") is True
    assert adapter.supports("stock_intraday_today_tdx") is True
    assert adapter.supports("stock_intraday_history_tdx") is True
    assert adapter.supports("stock_intraday_recent_history_tdx") is True
    assert adapter.supports("stock_trades_today_tdx") is True
    assert adapter.supports("stock_trades_history_tdx") is True
    assert adapter.supports("stock_auction_result_tdx") is True
    assert adapter.supports("stock_auction_result_history_tdx") is True
    assert adapter.supports("stock_finance_summary_tdx") is True
    assert adapter.supports("stock_share_capital_tdx") is True
    assert adapter.supports("stock_balance_summary_tdx") is True
    assert adapter.supports("stock_profit_cashflow_summary_tdx") is True
    assert adapter.supports("stock_finance_profile_tdx") is True
    assert adapter.supports("stock_ipo_listing_profile_tdx") is True
    assert adapter.supports("stock_index_constituent_changes_tdx") is True
    assert adapter.supports("stock_company_profile_tdx") is True
    assert adapter.supports("stock_disclosure_feed_tdx") is True
    assert adapter.supports("stock_valuation_metrics_tdx") is True
    assert adapter.supports("stock_kline_second_tdx") is True
    assert adapter.supports("stock_kline_yearly_tdx") is True
    assert adapter.supports("custom_interface") is False
    with pytest.raises(SourceAdapterNotFound):
        adapter.request("custom_interface", {})


def test_tdx_adapter_requests_and_normalizes_stock_codes():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_codes_tdx",
        {"exchange": "SZSE", "start": 3, "limit": 5},
    )

    assert client.connected is True
    assert client.calls == [
        {"market": "sz", "start": 3, "limit": 1600},
        {"market": "sz", "start": 1603, "limit": 1600},
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "name": "平安银行",
            "asset_type": "stock",
            "tdx_category": "a_share",
            "tdx_category_reason": "test",
            "listing_status": "listed",
            "list_date": None,
            "market": "主板",
            "market_code": "szse_main_board",
            "tdx_market_reason": "test",
            "tdx_decimal": 2,
            "tdx_multiple": 100,
            "tdx_previous_close_price": 10.2,
            "tdx_volume_ratio_base": 1.0,
        }
    ]


def test_tdx_adapter_always_filters_to_stock_rows():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_codes_tdx",
        {"exchange": "SZSE", "start": 0, "limit": 2},
    )

    assert client.calls == [
        {"market": "sz", "start": 0, "limit": 1600},
        {"market": "sz", "start": 1600, "limit": 1600},
    ]
    assert [row["tdx_code"] for row in rows] == ["sz000001"]
    assert [row["tdx_category"] for row in rows] == ["a_share"]


def test_tdx_adapter_requests_and_normalizes_index_codes():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("index_codes_tdx", {"exchange": "SSE", "limit": 2})

    assert rows == [
        {
            "instrument_id": "000001.SH",
            "symbol": "000001",
            "tdx_code": "sh000001",
            "exchange": "SSE",
            "name": "上证指数",
            "index_type": "official_index",
            "previous_close": 4108.08,
        }
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x044d"
    assert adapter.last_meta["tdx_include_tdx_block_index"] is False

    rows = adapter.request(
        "index_codes_tdx",
        {"exchange": "SSE", "limit": 2, "include_tdx_block_index": True},
    )

    assert [row["instrument_id"] for row in rows] == ["000001.SH", "881001.SH"]
    assert rows[1]["index_type"] == "tdx_block_index"


def test_tdx_adapter_exchange_all_requests_every_stock_market():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    adapter.request(
        "stock_codes_tdx",
        {"exchange": "all", "start": 0, "limit": 3},
    )

    requested_markets = []
    for call in client.calls:
        if call["market"] not in requested_markets:
            requested_markets.append(call["market"])
    assert requested_markets == ["sh", "sz", "bj"]


def test_tdx_adapter_parallelizes_full_stock_code_scan_with_connection_pool():
    client = FakeTdxClient(include_extra_stocks=True)
    client.pool_size = 3
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_codes_tdx", {"exchange": "all"})

    requested_markets = []
    for call in client.calls:
        if call["market"] not in requested_markets:
            requested_markets.append(call["market"])
    assert set(requested_markets) == {"sh", "sz", "bj"}
    assert [row["instrument_id"] for row in rows][:2] == ["600717.SH", "000001.SZ"]
    assert len(rows) == 5
    assert adapter.last_meta["tdx_code_market_scan_mode"] == "parallel"
    assert adapter.last_meta["tdx_code_market_worker_count"] == 3


def test_tdx_adapter_creates_parallel_stock_code_client_by_default(monkeypatch):
    captured = {}

    def fake_create_tdx_client(*, hosts=None, pool_size=None, heartbeat_interval=None):
        captured["hosts"] = hosts
        captured["pool_size"] = pool_size
        captured["heartbeat_interval"] = heartbeat_interval
        client = FakeTdxClient(include_extra_stocks=True)
        client.pool_size = pool_size
        client.transport = SimpleNamespace(
            hosts=tuple(hosts or ("demo:7709",)),
            connected_host="demo:7709",
            connected_hosts=tuple(f"demo:7709#{index}" for index in range(pool_size or 1)),
            pool_size=pool_size,
            heartbeat_interval=heartbeat_interval,
        )
        return client

    monkeypatch.delenv("AXDATA_TDX_POOL_SIZE", raising=False)
    monkeypatch.setattr(tdx_request, "create_tdx_client", fake_create_tdx_client)

    adapter = TdxRequestAdapter()
    rows = adapter.request("stock_codes_tdx", {"scope": "all"})

    assert captured["pool_size"] == 3
    assert len(rows) == 5
    assert adapter.last_meta["tdx_code_market_scan_mode"] == "parallel"
    assert adapter.last_meta["tdx_code_market_worker_count"] == 3


def test_tdx_adapter_uses_server_cache_root_without_changing_default_pool(monkeypatch, tmp_path):
    captured = {}
    expected_cache_root = str((tmp_path / "cache" / "tdx_servers").resolve())

    def fake_effective_host_strings(kind, *, cache_root=None):
        assert kind == "quote"
        captured["cache_root"] = cache_root
        return ["10.0.0.1:7709", "10.0.0.2:7709"]

    def fake_create_tdx_client(*, hosts=None, pool_size=None, heartbeat_interval=None):
        captured["hosts"] = hosts
        captured["pool_size"] = pool_size
        captured["heartbeat_interval"] = heartbeat_interval
        client = FakeTdxClient(include_extra_stocks=True)
        client.pool_size = pool_size
        client.transport = SimpleNamespace(
            hosts=tuple(hosts or ("demo:7709",)),
            connected_host=hosts[0] if hosts else "demo:7709",
            connected_hosts=tuple(f"{hosts[0]}#{index}" for index in range(pool_size or 1)) if hosts else ("demo:7709",),
            pool_size=pool_size,
            heartbeat_interval=heartbeat_interval,
        )
        return client

    monkeypatch.delenv("AXDATA_TDX_POOL_SIZE", raising=False)
    monkeypatch.delenv("AXDATA_TDX_HOSTS", raising=False)
    monkeypatch.setattr(tdx_request, "effective_host_strings", fake_effective_host_strings)
    monkeypatch.setattr(tdx_request, "create_tdx_client", fake_create_tdx_client)

    adapter = TdxRequestAdapter(options={"server_cache_root": expected_cache_root})
    rows = adapter.request("stock_codes_tdx", {"scope": "all"})

    assert captured["cache_root"] == expected_cache_root
    assert captured["hosts"] == ["10.0.0.1:7709", "10.0.0.2:7709"]
    assert captured["pool_size"] == 3
    assert len(rows) == 5
    assert adapter.last_meta["tdx_code_market_scan_mode"] == "parallel"


def test_tdx_adapter_respects_env_pool_size_for_stock_code_client(monkeypatch):
    captured = {}

    def fake_create_tdx_client(*, hosts=None, pool_size=None, heartbeat_interval=None):
        captured["pool_size"] = pool_size
        client = FakeTdxClient(include_extra_stocks=True)
        client.pool_size = 1
        client.transport = SimpleNamespace(
            hosts=tuple(hosts or ("demo:7709",)),
            connected_host="demo:7709",
            connected_hosts=("demo:7709",),
            pool_size=1,
            heartbeat_interval=heartbeat_interval,
        )
        return client

    monkeypatch.setenv("AXDATA_TDX_POOL_SIZE", "1")
    monkeypatch.setattr(tdx_request, "create_tdx_client", fake_create_tdx_client)

    adapter = TdxRequestAdapter()
    rows = adapter.request("stock_codes_tdx", {"scope": "all"})

    assert captured["pool_size"] is None
    assert len(rows) == 5
    assert adapter.last_meta["tdx_code_market_scan_mode"] == "sequential"


def test_tdx_adapter_scope_filters_board_rows():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_codes_tdx",
        {"scope": "main", "limit": 5},
    )

    requested_markets = []
    for call in client.calls:
        if call["market"] not in requested_markets:
            requested_markets.append(call["market"])
    assert requested_markets == ["sh", "sz"]
    assert rows
    assert {row["market_code"] for row in rows} == {"szse_main_board"}


def test_tdx_adapter_filters_by_multiple_scopes():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_codes_tdx",
        {"scope": ["main", "star"], "limit": 5},
    )

    requested_markets = []
    for call in client.calls:
        if call["market"] not in requested_markets:
            requested_markets.append(call["market"])
    assert requested_markets == ["sh", "sz"]
    assert [row["instrument_id"] for row in rows] == ["000001.SZ"]


def test_tdx_adapter_filters_by_comma_separated_scopes():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_codes_tdx",
        {"scope": "main,star", "limit": 5},
    )

    assert [row["instrument_id"] for row in rows] == ["000001.SZ"]


@pytest.mark.parametrize("code", ["000001", "sz000001", "000001.SZ"])
def test_tdx_adapter_filters_by_code(code):
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_codes_tdx",
        {"exchange": "SZSE", "code": code, "limit": 5},
    )

    assert [row["instrument_id"] for row in rows] == ["000001.SZ"]


def test_tdx_adapter_filters_by_name_keyword():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_codes_tdx",
        {"exchange": "SZSE", "name": "平安", "limit": 5},
    )

    assert [row["name"] for row in rows] == ["平安银行"]


def test_tdx_adapter_filters_by_multiple_names():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_codes_tdx",
        {"exchange": "SZSE", "name": ["平安", "浦发"], "limit": 5},
    )

    assert [row["name"] for row in rows] == ["平安银行"]


def test_tdx_adapter_filters_by_comma_separated_names():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_codes_tdx",
        {"exchange": "SZSE", "name": "平安,浦发", "limit": 5},
    )

    assert [row["name"] for row in rows] == ["平安银行"]


def test_tdx_adapter_filters_by_multiple_codes():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_codes_tdx",
        {"exchange": "SZSE", "code": ["000001", "300001"], "limit": 5},
    )

    assert [row["instrument_id"] for row in rows] == ["000001.SZ"]


def test_tdx_adapter_filters_by_comma_separated_codes():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_codes_tdx",
        {"exchange": "SZSE", "code": "000001,300001", "limit": 5},
    )

    assert [row["instrument_id"] for row in rows] == ["000001.SZ"]



@pytest.mark.parametrize(
    ("full_code", "category", "board"),
    [
        ("sh600000", "a_share", "sse_main_board"),
        ("sh688001", "a_share", "sse_star_market"),
        ("sh689009", "cdr", "sse_cdr"),
        ("sz000001", "a_share", "szse_main_board"),
        ("sz300001", "a_share", "szse_chinext"),
        ("bj920001", "a_share", "bse_listed_stock"),
        ("sh510300", "etf", "none"),
        ("sz159915", "etf", "none"),
        ("sh501018", "fund", "none"),
        ("sz161725", "fund", "none"),
        ("sh000001", "index", "none"),
        ("sz399001", "index", "none"),
    ],
)
def test_tdx_security_classifier_separates_asset_types(full_code, category, board):
    detected_category, category_reason = classify_security(full_code)
    detected_board, board_reason = classify_board(full_code, detected_category)

    assert detected_category == category
    assert detected_board == board
    assert category_reason
    assert board_reason


def test_source_request_gateway_calls_stock_codes_tdx_and_filters_fields():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    result = request_interface(
        "stock_codes_tdx",
        params={"scope": "all", "code": "000001.SZ"},
        fields=["instrument_id", "name", "market"],
        adapter=adapter,
    )

    assert result.records == [
        {"instrument_id": "000001.SZ", "name": "平安银行", "market": "主板"}
    ]
    assert result.meta["interface_name"] == "stock_codes_tdx"
    assert result.meta["source"] == "tdx"
    assert result.meta["persisted"] is False


def test_source_request_gateway_defaults_to_contract_fields():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    result = request_interface(
        "stock_codes_tdx",
        params={"scope": "all", "code": "000001.SZ"},
        adapter=adapter,
    )

    assert result.records == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "name": "平安银行",
            "market": "主板",
        }
    ]
    assert result.meta["requested_fields"] == [
        "instrument_id",
        "symbol",
        "tdx_code",
        "exchange",
        "name",
        "market",
    ]


def test_tdx_adapter_requests_current_st_list_from_stock_names():
    client = FakeTdxClient(include_extra_stocks=True)
    progress_events = []
    adapter = TdxRequestAdapter(
        client=client,
        progress_callback=lambda percent, message, **details: progress_events.append(
            {"percent": percent, "message": message, **details}
        ),
    )

    rows = adapter.request("stock_st_list_tdx", {"scope": "all"})

    assert rows == [
        {
            "instrument_id": "000004.SZ",
            "symbol": "000004",
            "tdx_code": "sz000004",
            "exchange": "SZSE",
            "name": "*ST国华",
            "market": "主板",
            "st_type": "*ST",
        },
        {
            "instrument_id": "002102.SZ",
            "symbol": "002102",
            "tdx_code": "sz002102",
            "exchange": "SZSE",
            "name": "ST能特",
            "market": "主板",
            "st_type": "ST",
        },
    ]
    assert adapter.last_meta["tdx_scanned_count"] == 5
    assert adapter.last_meta["tdx_st_count"] == 2
    assert progress_events[-1]["progress_current"] == 5
    assert progress_events[-1]["progress_total"] == 5
    assert progress_events[-1]["progress_unit"] == "只"


def test_source_request_gateway_defaults_to_st_list_contract_fields():
    adapter = TdxRequestAdapter(client=FakeTdxClient(include_extra_stocks=True))

    result = request_interface(
        "stock_st_list_tdx",
        params={"scope": "all"},
        adapter=adapter,
    )

    assert result.records == [
        {
            "instrument_id": "000004.SZ",
            "symbol": "000004",
            "tdx_code": "sz000004",
            "exchange": "SZSE",
            "name": "*ST国华",
            "market": "主板",
            "st_type": "*ST",
        },
        {
            "instrument_id": "002102.SZ",
            "symbol": "002102",
            "tdx_code": "sz002102",
            "exchange": "SZSE",
            "name": "ST能特",
            "market": "主板",
            "st_type": "ST",
        },
    ]
    assert result.meta["requested_fields"] == [
        "instrument_id",
        "symbol",
        "tdx_code",
        "exchange",
        "name",
        "market",
        "st_type",
    ]


def test_tdx_adapter_requests_current_suspension_list_from_legacy_status_bit():
    client = FakeTdxClient(include_extra_stocks=True)
    progress_events = []
    adapter = TdxRequestAdapter(
        client=client,
        progress_callback=lambda percent, message, **details: progress_events.append(
            {"percent": percent, "message": message, **details}
        ),
    )

    rows = adapter.request("stock_suspensions_tdx", {"scope": "all"})

    assert rows == [
        {
            "instrument_id": "600717.SH",
            "symbol": "600717",
            "tdx_code": "sh600717",
            "exchange": "SSE",
            "name": "天津港",
            "market": "主板",
        },
        {
            "instrument_id": "000004.SZ",
            "symbol": "000004",
            "tdx_code": "sz000004",
            "exchange": "SZSE",
            "name": "*ST国华",
            "market": "主板",
        },
    ]
    assert "001399.SZ" not in [row["instrument_id"] for row in rows]
    quote_progress = [event for event in progress_events if "请求停牌状态" in event["message"]]
    assert quote_progress
    assert quote_progress[-1]["progress_current"] == quote_progress[-1]["progress_total"]
    assert quote_progress[-1]["progress_unit"] == "批"
    assert client.quote_calls


def test_tdx_suspension_scan_uses_recommended_parallel_quote_strategy():
    assert tdx_request.DEFAULT_QUOTE_BATCH_SIZE == 80
    assert tdx_request.TDX_SUSPENSION_HOST_COUNT == 4
    assert tdx_request.TDX_SUSPENSION_POOL_SIZE == 2


def test_tdx_suspension_scan_reuses_existing_connection_pool(monkeypatch):
    created_clients = []

    def fake_create_tdx_client(**kwargs):
        created_clients.append(kwargs)
        return FakeTdxClient(include_extra_stocks=True)

    monkeypatch.setattr(tdx_request, "create_tdx_client", fake_create_tdx_client)
    client = FakeTdxClient(include_extra_stocks=True)
    client.pool_size = 2
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_suspensions_tdx", {"scope": "all"})

    assert rows
    assert client.quote_calls
    assert created_clients == []


def test_tdx_suspension_parallel_quote_progress_uses_batch_emitter(monkeypatch):
    from axdata_source_tdx import request_methods, request_seams

    progress_events = []

    def fake_request_parallel(*args, emit_progress, progress_callback, **kwargs):
        emit_progress(
            progress_callback,
            completed=1,
            total=1,
            unit="批",
            started_at=time.monotonic(),
            percent_start=28,
            percent_span=40,
            label="请求停牌状态",
        )
        return []

    monkeypatch.setattr(request_seams, "request_legacy_quotes_parallel", fake_request_parallel)

    request_methods._request_legacy_quotes_parallel(
        SimpleNamespace(pool_size=1),
        [{"tdx_code": "sz000001"}],
        use_parallel_quote_clients=True,
        hosts=["10.0.0.1:7709", "10.0.0.2:7709"],
        progress_callback=lambda percent, message, **details: progress_events.append(
            {"percent": percent, "message": message, **details}
        ),
    )

    assert progress_events == [
        {
            "percent": 68,
            "message": "请求停牌状态 1/1 批",
            "progress_current": 1,
            "progress_total": 1,
            "progress_unit": "批",
            "eta_ms": None,
        }
    ]


def test_tdx_adapter_requests_order_book_from_legacy_quotes():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_order_book_tdx", {"code": "000001.SZ"})

    assert client.quote_calls == [
        {"securities": [("sz", "000001")], "batch_size": 80}
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "level": 1,
            "bid_price": 10.13,
            "bid_volume": 320,
            "ask_price": 10.14,
            "ask_volume": 428,
        },
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "level": 2,
            "bid_price": 10.12,
            "bid_volume": 118,
            "ask_price": 10.15,
            "ask_volume": 260,
        },
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "level": 3,
            "bid_price": 10.11,
            "bid_volume": 94,
            "ask_price": 10.16,
            "ask_volume": 136,
        },
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "level": 4,
            "bid_price": 10.1,
            "bid_volume": 87,
            "ask_price": 10.17,
            "ask_volume": 92,
        },
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "level": 5,
            "bid_price": 10.09,
            "bid_volume": 66,
            "ask_price": 10.18,
            "ask_volume": 71,
        },
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x053e"
    assert adapter.last_meta["tdx_requested_code_count"] == 1
    assert adapter.last_meta["tdx_quote_count"] == 1


def test_tdx_adapter_requests_realtime_snapshot_from_explicit_quotes():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_realtime_snapshot_tdx", {"code": ["000001.SZ", "600000.SH"]})

    assert client.explicit_quote_calls == [
        {"securities": [("sz", "000001"), ("sh", "600000")], "batch_size": 80}
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "last_price": 10.14,
            "pre_close": 10.28,
            "open": 10.13,
            "high": 10.2,
            "low": 10.08,
            "change": -0.14,
            "change_pct": -1.361868,
            "open_change_pct": -1.459144,
            "high_change_pct": -0.77821,
            "low_change_pct": -1.945525,
            "amplitude_pct": 1.167315,
            "average_price": 10.14,
            "average_change_pct": -1.361868,
            "drawdown_pct": 0.583658,
            "attack_pct": 0.583658,
            "volume": 1000,
            "current_volume": 15,
            "amount": 1014000.0,
            "inside_volume": 400,
            "outside_volume": 600,
            "inside_outside_ratio": 0.666667,
            "open_amount": 10000.0,
            "open_amount_ratio_pct": 0.986193,
            "bid1_price": 10.13,
            "bid1_volume": 320,
            "ask1_price": 10.14,
            "ask1_volume": 428,
            "locked_amount": 324160.0,
            "bid1_ask1_volume_diff": -108,
            "bid1_ask1_balance_pct": -14.438503,
            "rise_speed": 0.21,
            "short_turnover": 0.08,
            "min2_amount": 320000.0,
            "opening_rush": 0.12,
            "vol_rise_speed": 1.25,
            "entrust_ratio": 18.5,
            "activity": 11,
        },
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "tdx_code": "sh600000",
            "exchange": "SSE",
            "last_price": 8.42,
            "pre_close": 8.39,
            "open": 8.4,
            "high": 8.45,
            "low": 8.38,
            "change": 0.03,
            "change_pct": 0.357568,
            "open_change_pct": 0.11919,
            "high_change_pct": 0.715137,
            "low_change_pct": -0.11919,
            "amplitude_pct": 0.834327,
            "average_price": 8.42,
            "average_change_pct": 0.357569,
            "drawdown_pct": 0.357569,
            "attack_pct": 0.476758,
            "volume": 2100,
            "current_volume": 22,
            "amount": 1768200.0,
            "inside_volume": 920,
            "outside_volume": 1180,
            "inside_outside_ratio": 0.779661,
            "open_amount": 182000.0,
            "open_amount_ratio_pct": 10.292953,
            "bid1_price": 8.42,
            "bid1_volume": 610,
            "ask1_price": 8.43,
            "ask1_volume": 430,
            "locked_amount": 513620.0,
            "bid1_ask1_volume_diff": 180,
            "bid1_ask1_balance_pct": 17.307692,
            "rise_speed": 0.05,
            "short_turnover": 0.03,
            "min2_amount": 510000.0,
            "opening_rush": 0.04,
            "vol_rise_speed": 0.92,
            "entrust_ratio": 9.6,
            "activity": 22,
        },
    ]
    assert "level" not in rows[0]
    assert adapter.last_meta["tdx_protocol"] == "0x054c"
    assert adapter.last_meta["tdx_requested_code_count"] == 2
    assert adapter.last_meta["tdx_quote_count"] == 2


def test_tdx_adapter_requests_index_realtime_snapshot_without_stock_only_fields():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("index_realtime_snapshot_tdx", {"code": ["000001.SH", "399001.SZ"]})

    assert client.explicit_quote_calls == [
        {"securities": [("sh", "000001"), ("sz", "399001")], "batch_size": 80}
    ]
    assert [row["instrument_id"] for row in rows] == ["000001.SH", "399001.SZ"]
    assert rows[0]["last_price"] == 10.14
    assert rows[0]["change_pct"] == -1.361868
    assert "bid1_price" not in rows[0]
    assert "locked_amount" not in rows[0]
    assert "inside_volume" not in rows[0]
    assert adapter.last_meta["tdx_protocol"] == "0x054c"


def test_tdx_realtime_refresh_rows_uses_0547_and_snapshot_field_shape():
    client = FakeTdxClient()

    rows = tdx_request.request_realtime_refresh_rows(
        code=["000001.SZ", "600000.SH"],
        fields=["instrument_id", "last_price", "bid1_price", "ask1_price", "activity"],
        cursors={"000001.SZ": 103000},
        client=client,
    )

    assert client.connected is True
    assert client.refresh_quote_calls == [
        {"cursors": [("sz", "000001", 103000), ("sh", "600000", 0)]}
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "last_price": 10.15,
            "bid1_price": 10.14,
            "ask1_price": 10.15,
            "activity": 31,
        },
        {
            "instrument_id": "600000.SH",
            "last_price": 8.43,
            "bid1_price": 8.43,
            "ask1_price": 8.44,
            "activity": 42,
        },
    ]


def test_tdx_realtime_refresh_rows_can_return_internal_refresh_cursors():
    client = FakeTdxClient()

    rows = tdx_request.request_realtime_refresh_rows(
        code="000001.SZ",
        fields=["instrument_id", "last_price"],
        include_internal=True,
        client=client,
    )

    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "last_price": 10.15,
            "_tdx_instrument_id": "000001.SZ",
            "_tdx_update_time_raw": 103000,
        }
    ]


def test_tdx_realtime_refresh_lightweight_module_matches_request_entrypoint():
    from axdata_core.adapters.tdx.realtime_refresh import request_realtime_refresh_rows

    client = FakeTdxClient()

    rows = request_realtime_refresh_rows(
        code=["000001.SZ", "600000.SH"],
        fields=["instrument_id", "last_price", "bid1_price", "ask1_price", "activity"],
        cursors={"000001.SZ": 103000},
        client=client,
    )

    assert client.refresh_quote_calls == [
        {"cursors": [("sz", "000001", 103000), ("sh", "600000", 0)]}
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "last_price": 10.15,
            "bid1_price": 10.14,
            "ask1_price": 10.15,
            "activity": 31,
        },
        {
            "instrument_id": "600000.SH",
            "last_price": 8.43,
            "bid1_price": 8.43,
            "ask1_price": 8.44,
            "activity": 42,
        },
    ]


def test_tdx_request_realtime_refresh_entrypoint_respects_create_client_monkeypatch(monkeypatch):
    created = []

    def fake_create_tdx_client(**kwargs):
        created.append(kwargs)
        return FakeTdxClient()

    monkeypatch.setattr(tdx_request, "create_tdx_client", fake_create_tdx_client)

    rows = tdx_request.request_realtime_refresh_rows(
        code="000001.SZ",
        fields=["instrument_id", "last_price"],
    )

    assert created == [{}]
    assert rows == [{"instrument_id": "000001.SZ", "last_price": 10.15}]


def test_tdx_adapter_requests_realtime_rank_from_category_quotes():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_realtime_rank_tdx",
        {
            "category": "a_share",
            "sort": "amount",
            "start": 80,
            "count": 2,
            "filters": ["exclude_st"],
        },
    )

    assert client.category_quote_calls == [
        {
            "category": 6,
            "sort_type": 0x000A,
            "start": 80,
            "count": 2,
            "ascending": False,
            "filter_raw": 4,
        }
    ]
    assert rows[0]["rank"] == 81
    assert rows[0]["instrument_id"] == "000001.SZ"
    assert rows[0]["last_price"] == 10.14
    assert rows[0]["amount"] == 1014000.0
    assert rows[0]["change_pct"] == -1.361868
    assert rows[0]["locked_amount"] == 324160.0
    assert rows[1]["rank"] == 82
    assert rows[1]["instrument_id"] == "600000.SH"
    assert "level" not in rows[0]
    assert adapter.last_meta["tdx_protocol"] == "0x054b"
    assert adapter.last_meta["tdx_category"] == 6
    assert adapter.last_meta["tdx_sort_type"] == 0x000A
    assert adapter.last_meta["tdx_filter_raw"] == 4
    assert adapter.last_meta["tdx_returned_count"] == 2
    assert adapter.last_meta["tdx_page_count"] == 1
    assert adapter.last_meta["tdx_full_rank"] is False


def test_tdx_adapter_requests_index_realtime_rank_from_index_category():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "index_realtime_rank_tdx",
        {"sort": "change_pct", "start": 0, "count": 2, "ascending": True},
    )

    assert client.category_quote_calls[-1] == {
        "category": 0x2B2C,
        "sort_type": 0x000E,
        "start": 0,
        "count": 2,
        "ascending": True,
        "filter_raw": 0,
    }
    assert [row["rank"] for row in rows] == [1, 2]
    assert rows[0]["instrument_id"] == "000001.SZ"
    assert "locked_amount" not in rows[0]
    assert adapter.last_meta["tdx_category"] == 0x2B2C


def test_tdx_adapter_requests_index_quote_refresh():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("index_quote_refresh_tdx", {"code": ["000001.SH", "000300.SH"]})

    assert client.refresh_quote_calls == [
        {"cursors": [("sh", "000001", 0), ("sh", "000300", 0)]}
    ]
    assert [row["instrument_id"] for row in rows] == ["000001.SH", "000300.SH"]
    assert rows[0]["last_price"] == 10.15
    assert "bid1_price" not in rows[0]
    assert adapter.last_meta["tdx_protocol"] == "0x0547"


def test_tdx_adapter_realtime_rank_keeps_category_quote_derived_fields():
    quote = CategoryQuote(
        exchange="sz",
        market_id=0,
        code="000001",
        active1=7,
        active2=11,
        last_price=10.14,
        pre_close=10.28,
        open=10.13,
        high=10.2,
        low=10.08,
        time_raw=93000,
        total_hand=1000,
        current_hand=15,
        amount_raw=1014000,
        amount=1014000.0,
        inside_dish=400,
        outer_disc=600,
        open_amount_raw=10000,
        open_amount=10000.0,
        bid1_price=10.13,
        bid1_volume=320,
        ask1_price=10.14,
        ask1_volume=428,
        rise_speed=0.21,
        short_turnover=0.08,
        min2_amount=320000.0,
        opening_rush=0.12,
        vol_rise_speed=1.25,
        entrust_ratio=18.5,
    )
    client = FakeTdxClient()
    client.category_quote_pages = {
        0: CategoryQuotePage(
            category=6,
            sort_type=0x000E,
            start=0,
            request_count=1,
            sort_reverse=1,
            filter_raw=0,
            header=0,
            records=(quote,),
        ).records
    }
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_realtime_rank_tdx", {"count": 1})

    assert rows[0]["change"] == -0.14
    assert rows[0]["change_pct"] == -1.361868
    assert rows[0]["amplitude_pct"] == 1.167315
    assert rows[0]["activity"] == 11


def test_tdx_adapter_requests_realtime_rank_all_pages():
    client = FakeTdxClient()
    securities = [("sz", "000001") if index % 2 == 0 else ("sh", "600000") for index in range(1001)]
    page_quotes = client.get_explicit_quotes(securities)
    client.explicit_quote_calls.clear()
    client.category_quote_pages = {
        0: page_quotes[:1000],
        1000: page_quotes[1000:],
    }
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_realtime_rank_tdx",
        {
            "category": "a_share",
            "sort": "change_pct",
            "count": "all",
        },
    )

    assert client.category_quote_calls == [
        {
            "category": 6,
            "sort_type": 0x000E,
            "start": 0,
            "count": 1000,
            "ascending": False,
            "filter_raw": 0,
        },
        {
            "category": 6,
            "sort_type": 0x000E,
            "start": 1000,
            "count": 1000,
            "ascending": False,
            "filter_raw": 0,
        },
    ]
    assert rows[0]["rank"] == 1
    assert rows[-1]["rank"] == 1001
    assert len(rows) == 1001
    assert adapter.last_meta["tdx_protocol"] == "0x054b"
    assert adapter.last_meta["tdx_requested_count"] == 1000
    assert adapter.last_meta["tdx_returned_count"] == 1001
    assert adapter.last_meta["tdx_page_count"] == 2
    assert adapter.last_meta["tdx_full_rank"] is True


def test_tdx_adapter_requests_stock_limit_ladder_without_local_result_cache(monkeypatch, tmp_path):
    monkeypatch.setenv("AXDATA_TDX_STATS_ROOT", str(tmp_path / "stats-cache"))
    stat_000001 = [""] * 35
    stat_000001[0] = "0"
    stat_000001[1] = "000001"
    stat_000001[4] = "20260616"
    stat_000001[11] = "1000"
    stat_000001[26] = "8"
    stat_000001[31] = "7"
    stat_000001[32] = "5"
    stat_000001[33] = "3"
    stat_000002 = [""] * 35
    stat_000002[0] = "0"
    stat_000002[1] = "000002"
    stat_000002[4] = "20260616"
    stat_000002[11] = "2000"
    stat_000002[26] = "2"
    stat_000002[31] = "3"
    stat_000002[32] = "1"
    stat_000002[33] = "0"
    stat_000004 = [""] * 35
    stat_000004[0] = "0"
    stat_000004[1] = "000004"
    stat_000004[4] = "20260616"
    stat_000004[11] = "500"
    stat_000004[26] = "1"
    stat_000004[31] = "1"
    stat_000004[32] = "1"
    stat_000004[33] = "0"
    stat2_000001 = [""] * 21
    stat2_000001[0] = "0"
    stat2_000001[1] = "000001"
    stat2_000001[2] = "20260616"
    stat2_000002 = [""] * 21
    stat2_000002[0] = "0"
    stat2_000002[1] = "000002"
    stat2_000002[2] = "20260616"
    stat2_000004 = [""] * 21
    stat2_000004[0] = "0"
    stat2_000004[1] = "000004"
    stat2_000004[2] = "20260616"

    class LimitLadderClient(FakeTdxClient):
        def get_codes(self, market, *, start=0, limit=None):
            if market == "sz":
                return [
                    SimpleNamespace(full_code="sz000001", code="000001", exchange="sz", name="平安银行"),
                    SimpleNamespace(full_code="sz000002", code="000002", exchange="sz", name="万科A"),
                    SimpleNamespace(full_code="sz000004", code="000004", exchange="sz", name="ST国华"),
                ]
            return []

    client = LimitLadderClient()
    client.resource_payloads["zhb.zip"] = _stats_zip_bytes(
        "\n".join(["|".join(stat_000001), "|".join(stat_000002), "|".join(stat_000004)]),
        "\n".join(["|".join(stat2_000001), "|".join(stat2_000002), "|".join(stat2_000004)]),
    )
    client.category_quote_pages = {
        0: [
            SimpleNamespace(
                exchange="sz",
                code="000001",
                full_code="sz000001",
                last_price=11.0,
                pre_close=10.0,
                open=10.5,
                high=11.0,
                low=10.5,
                change=1.0,
                change_pct=10.0,
                amplitude_pct=5.0,
                total_hand=10000,
                current_hand=100,
                amount=110000000.0,
                inside_dish=4000,
                outer_disc=6000,
                open_amount=20000000.0,
                bid1_price=11.0,
                bid1_volume=5000,
                ask1_price=0,
                ask1_volume=0,
            ),
            SimpleNamespace(
                exchange="sz",
                code="000002",
                full_code="sz000002",
                last_price=10.8,
                pre_close=10.0,
                open=10.2,
                high=11.0,
                low=10.1,
                change=0.8,
                change_pct=8.0,
                amplitude_pct=9.0,
                total_hand=8000,
                current_hand=80,
                amount=86400000.0,
                inside_dish=3000,
                outer_disc=5000,
                open_amount=15000000.0,
                bid1_price=10.8,
                bid1_volume=100,
                ask1_price=10.81,
                ask1_volume=200,
            ),
            SimpleNamespace(
                exchange="sz",
                code="000003",
                full_code="sz000003",
                last_price=10.2,
                pre_close=10.0,
                open=10.0,
                high=10.3,
                low=9.9,
                change=0.2,
                change_pct=2.0,
                amplitude_pct=4.0,
                total_hand=5000,
                current_hand=50,
                amount=51000000.0,
                inside_dish=2000,
                outer_disc=3000,
                open_amount=10000000.0,
                bid1_price=10.19,
                bid1_volume=10,
                ask1_price=10.2,
                ask1_volume=20,
            ),
            SimpleNamespace(
                exchange="sz",
                code="000004",
                full_code="sz000004",
                last_price=10.5,
                pre_close=10.0,
                open=10.2,
                high=10.5,
                low=10.1,
                change=0.5,
                change_pct=5.0,
                amplitude_pct=4.0,
                total_hand=3000,
                current_hand=30,
                amount=31500000.0,
                inside_dish=1000,
                outer_disc=2000,
                open_amount=5000000.0,
                bid1_price=10.5,
                bid1_volume=2000,
                ask1_price=0,
                ask1_volume=0,
            ),
            SimpleNamespace(
                exchange="sz",
                code="001399",
                full_code="sz001399",
                last_price=0.0,
                pre_close=0.0,
                open=0.0,
                high=0.0,
                low=0.0,
                change=0.0,
                change_pct=None,
                amplitude_pct=0.0,
                total_hand=0,
                current_hand=0,
                amount=0.0,
                inside_dish=0,
                outer_disc=0,
                open_amount=0.0,
                bid1_price=0.0,
                bid1_volume=0,
                ask1_price=0.0,
                ask1_volume=0,
            ),
        ]
    }
    adapter = TdxRequestAdapter(client=client)
    monkeypatch.setattr(
        adapter,
        "_topic_rows_by_instrument_id",
        lambda instrument_ids, *, topic_type, **kwargs: {instrument_id: [] for instrument_id in instrument_ids},
    )

    rows = adapter.request("stock_limit_ladder_tdx", {})

    assert client.download_file_calls == [{"path": "zhb.zip", "chunk_size": 30000, "max_bytes": None}]
    assert client.category_quote_calls == [
        {
            "category": 6,
            "sort_type": 0x000E,
            "start": 0,
            "count": 80,
            "ascending": False,
            "filter_raw": 0,
        }
    ]
    assert rows == [
        {
            "trade_date": tdx_request._default_daily_price_limit_trade_date("20260616"),
            "ladder_level": 4,
            "limit_board_text": "8天6板",
            "limit_status": "sealed",
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "exchange": "SZSE",
            "name": "平安银行",
            "last_price": 11.0,
            "pre_close": 10.0,
            "limit_up_price": 11.0,
            "change_pct": 10.0,
            "amount": 110000000.0,
            "seal_amount": 5500000.0,
            "free_float_market_value": 110000000.0,
            "seal_to_amount_ratio": 0.05,
            "year_limit_up_days": 9,
            "primary_theme": None,
            "secondary_themes": None,
        }
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x054b+tdxstat+7615"
    assert adapter.last_meta["tdx_stats_cached"] is False
    assert adapter.last_meta["tdx_stats_refreshed"] is True
    assert adapter.last_meta["tdx_returned_count"] == 1
    assert adapter.last_meta["tdx_count"] == "all"
    assert adapter.last_meta["tdx_scope"] == ("sse_main_board", "szse_main_board")
    assert adapter.last_meta["tdx_f10_topic_workers"] == 1
    assert set(adapter.last_meta["tdx_limit_ladder_timing_ms"]) == {
        "stats",
        "rank_scan",
        "name_lookup",
        "normalize_filter",
        "theme_lookup",
        "theme_attach",
        "sort_and_slice",
        "total",
    }

    rows_with_touched = adapter.request(
        "stock_limit_ladder_tdx",
        {"count": 10, "include_touched": True},
    )

    assert [row["instrument_id"] for row in rows_with_touched] == ["000001.SZ", "000002.SZ"]
    assert rows_with_touched[1]["limit_status"] == "touched"
    assert rows_with_touched[1]["ladder_level"] is None


def test_tdx_adapter_limit_ladder_stops_after_sealed_threshold_page(monkeypatch, tmp_path):
    monkeypatch.setenv("AXDATA_TDX_STATS_ROOT", str(tmp_path / "stats-cache"))
    stat_000081 = [""] * 35
    stat_000081[0] = "0"
    stat_000081[1] = "000081"
    stat_000081[4] = "20260616"
    stat_000081[11] = "1000"
    stat_000081[33] = "1"
    stat2_000081 = [""] * 21
    stat2_000081[0] = "0"
    stat2_000081[1] = "000081"
    stat2_000081[2] = "20260616"

    class LimitLadderClient(FakeTdxClient):
        def get_codes(self, market, *, start=0, limit=None):
            if market == "sz":
                return [SimpleNamespace(full_code="sz000081", code="000081", exchange="sz", name="海二页")]
            return []

    client = LimitLadderClient()
    client.resource_payloads["zhb.zip"] = _stats_zip_bytes(
        "|".join(stat_000081),
        "|".join(stat2_000081),
    )
    first_page = []
    for index in range(80):
        code = f"10{index:04d}"[-6:]
        first_page.append(
            SimpleNamespace(
                exchange="sz",
                code=code,
                full_code=f"sz{code}",
                last_price=10.1,
                pre_close=10.0,
                open=10.0,
                high=10.1,
                low=10.0,
                change=0.1,
                change_pct=1.0,
                amplitude_pct=1.0,
                total_hand=1000,
                current_hand=10,
                amount=1000000.0,
                inside_dish=100,
                outer_disc=200,
                open_amount=100000.0,
                bid1_price=10.1,
                bid1_volume=10,
                ask1_price=10.2,
                ask1_volume=20,
            )
        )
    client.category_quote_pages = {0: first_page}
    adapter = TdxRequestAdapter(client=client)
    monkeypatch.setattr(
        adapter,
        "_topic_rows_by_instrument_id",
        lambda instrument_ids, *, topic_type, **kwargs: {instrument_id: [] for instrument_id in instrument_ids},
    )

    rows = adapter.request("stock_limit_ladder_tdx", {})

    assert [call["start"] for call in client.category_quote_calls] == [0]
    assert [call["count"] for call in client.category_quote_calls] == [80]
    assert adapter.last_meta["tdx_rank_page_count"] == 1
    assert adapter.last_meta["tdx_rank_scanned_count"] == 80
    assert adapter.last_meta["tdx_stats_refreshed"] is True
    assert rows == []

    rows_again = adapter.request("stock_limit_ladder_tdx", {})

    assert client.download_file_calls == [{"path": "zhb.zip", "chunk_size": 30000, "max_bytes": None}]
    assert rows_again == []
    assert adapter.last_meta["tdx_stats_cached"] is True
    assert adapter.last_meta["tdx_stats_refreshed"] is False


def test_tdx_adapter_limit_ladder_include_touched_scans_beyond_first_rank_page(monkeypatch, tmp_path):
    monkeypatch.setenv("AXDATA_TDX_STATS_ROOT", str(tmp_path / "stats-cache"))
    stat_000081 = [""] * 35
    stat_000081[0] = "0"
    stat_000081[1] = "000081"
    stat_000081[4] = "20260616"
    stat_000081[11] = "1000"
    stat_000081[33] = "1"
    stat2_000081 = [""] * 21
    stat2_000081[0] = "0"
    stat2_000081[1] = "000081"
    stat2_000081[2] = "20260616"

    class LimitLadderClient(FakeTdxClient):
        def get_codes(self, market, *, start=0, limit=None):
            if market == "sz":
                return [SimpleNamespace(full_code="sz000081", code="000081", exchange="sz", name="海二页")]
            return []

    client = LimitLadderClient()
    client.resource_payloads["zhb.zip"] = _stats_zip_bytes(
        "|".join(stat_000081),
        "|".join(stat2_000081),
    )
    first_page = []
    for index in range(80):
        code = f"10{index:04d}"[-6:]
        first_page.append(
            SimpleNamespace(
                exchange="sz",
                code=code,
                full_code=f"sz{code}",
                last_price=10.1,
                pre_close=10.0,
                open=10.0,
                high=10.1,
                low=10.0,
                change=0.1,
                change_pct=1.0,
                amplitude_pct=1.0,
                total_hand=1000,
                current_hand=10,
                amount=1000000.0,
                inside_dish=100,
                outer_disc=200,
                open_amount=100000.0,
                bid1_price=10.1,
                bid1_volume=10,
                ask1_price=10.2,
                ask1_volume=20,
            )
        )
    client.category_quote_pages = {
        0: first_page,
        80: [
            SimpleNamespace(
                exchange="sz",
                code="000081",
                full_code="sz000081",
                last_price=10.5,
                pre_close=10.0,
                open=10.5,
                high=11.0,
                low=10.5,
                change=0.5,
                change_pct=5.0,
                amplitude_pct=5.0,
                total_hand=10000,
                current_hand=100,
                amount=110000000.0,
                inside_dish=4000,
                outer_disc=6000,
                open_amount=20000000.0,
                bid1_price=10.5,
                bid1_volume=5000,
                ask1_price=10.6,
                ask1_volume=20,
            )
        ],
    }
    adapter = TdxRequestAdapter(client=client)
    monkeypatch.setattr(
        adapter,
        "_topic_rows_by_instrument_id",
        lambda instrument_ids, *, topic_type, **kwargs: {instrument_id: [] for instrument_id in instrument_ids},
    )

    rows = adapter.request("stock_limit_ladder_tdx", {"include_touched": True})

    assert [call["start"] for call in client.category_quote_calls] == [0, 80]
    assert [call["count"] for call in client.category_quote_calls] == [80, 80]
    assert adapter.last_meta["tdx_rank_page_count"] == 2
    assert adapter.last_meta["tdx_rank_scanned_count"] == 81
    assert [row["instrument_id"] for row in rows] == ["000081.SZ"]
    assert rows[0]["limit_status"] == "touched"


def test_tdx_adapter_limit_ladder_defaults_to_main_board_and_supports_all_scope(monkeypatch):
    stat_000001 = [""] * 35
    stat_000001[0] = "0"
    stat_000001[1] = "000001"
    stat_000001[4] = "20260616"
    stat_000001[33] = "1"
    stat_300001 = [""] * 35
    stat_300001[0] = "0"
    stat_300001[1] = "300001"
    stat_300001[4] = "20260616"
    stat_300001[33] = "1"
    stat2_000001 = [""] * 21
    stat2_000001[0] = "0"
    stat2_000001[1] = "000001"
    stat2_000001[2] = "20260616"
    stat2_300001 = [""] * 21
    stat2_300001[0] = "0"
    stat2_300001[1] = "300001"
    stat2_300001[2] = "20260616"

    class LimitLadderClient(FakeTdxClient):
        def get_codes(self, market, *, start=0, limit=None):
            if market == "sz":
                return [
                    SimpleNamespace(full_code="sz000001", code="000001", exchange="sz", name="平安银行"),
                    SimpleNamespace(full_code="sz300001", code="300001", exchange="sz", name="特锐德"),
                ]
            return []

    client = LimitLadderClient()
    client.resource_payloads["zhb.zip"] = _stats_zip_bytes(
        "\n".join(["|".join(stat_000001), "|".join(stat_300001)]),
        "\n".join(["|".join(stat2_000001), "|".join(stat2_300001)]),
    )
    client.category_quote_pages = {
        0: [
            SimpleNamespace(
                exchange="sz",
                code="000001",
                full_code="sz000001",
                last_price=11.0,
                pre_close=10.0,
                open=10.5,
                high=11.0,
                low=10.5,
                change=1.0,
                change_pct=10.0,
                amplitude_pct=5.0,
                total_hand=10000,
                current_hand=100,
                amount=110000000.0,
                inside_dish=4000,
                outer_disc=6000,
                open_amount=20000000.0,
                bid1_price=11.0,
                bid1_volume=5000,
                ask1_price=0,
                ask1_volume=0,
            ),
            SimpleNamespace(
                exchange="sz",
                code="300001",
                full_code="sz300001",
                last_price=12.0,
                pre_close=10.0,
                open=11.5,
                high=12.0,
                low=11.5,
                change=2.0,
                change_pct=20.0,
                amplitude_pct=5.0,
                total_hand=10000,
                current_hand=100,
                amount=120000000.0,
                inside_dish=4000,
                outer_disc=6000,
                open_amount=20000000.0,
                bid1_price=12.0,
                bid1_volume=5000,
                ask1_price=0,
                ask1_volume=0,
            ),
        ]
    }
    adapter = TdxRequestAdapter(client=client)
    monkeypatch.setattr(
        adapter,
        "_topic_rows_by_instrument_id",
        lambda instrument_ids, *, topic_type, **kwargs: {instrument_id: [] for instrument_id in instrument_ids},
    )

    default_rows = adapter.request("stock_limit_ladder_tdx", {})
    assert [row["instrument_id"] for row in default_rows] == ["000001.SZ"]
    assert adapter.last_meta["tdx_count"] == "all"
    assert adapter.last_meta["tdx_scope"] == ("sse_main_board", "szse_main_board")

    all_rows = adapter.request("stock_limit_ladder_tdx", {"scope": "all"})
    assert [row["instrument_id"] for row in all_rows] == ["300001.SZ", "000001.SZ"]
    assert adapter.last_meta["tdx_scope"] == "all"


def test_tdx_adapter_attaches_limit_ladder_top_themes(monkeypatch, tmp_path):
    monkeypatch.setenv("AXDATA_TDX_STATS_ROOT", str(tmp_path / "stats-cache"))
    stat_000001 = [""] * 35
    stat_000001[0] = "0"
    stat_000001[1] = "000001"
    stat_000001[4] = "20260616"
    stat_000001[11] = "1000"
    stat_000001[26] = "8"
    stat_000001[31] = "7"
    stat_000001[32] = "5"
    stat_000001[33] = "3"
    stat_000002 = [""] * 35
    stat_000002[0] = "0"
    stat_000002[1] = "000002"
    stat_000002[4] = "20260616"
    stat_000002[11] = "2000"
    stat_000002[26] = "2"
    stat_000002[31] = "3"
    stat_000002[32] = "1"
    stat_000002[33] = "0"
    stat_000004 = [""] * 35
    stat_000004[0] = "0"
    stat_000004[1] = "000004"
    stat_000004[4] = "20260616"
    stat_000004[11] = "500"
    stat_000004[26] = "1"
    stat_000004[31] = "1"
    stat_000004[32] = "1"
    stat_000004[33] = "0"
    stat2_000001 = [""] * 21
    stat2_000001[0] = "0"
    stat2_000001[1] = "000001"
    stat2_000001[2] = "20260616"
    stat2_000002 = [""] * 21
    stat2_000002[0] = "0"
    stat2_000002[1] = "000002"
    stat2_000002[2] = "20260616"
    stat2_000004 = [""] * 21
    stat2_000004[0] = "0"
    stat2_000004[1] = "000004"
    stat2_000004[2] = "20260616"

    class LimitLadderClient(FakeTdxClient):
        def get_codes(self, market, *, start=0, limit=None):
            if market == "sz":
                return [
                    SimpleNamespace(full_code="sz000001", code="000001", exchange="sz", name="平安银行"),
                    SimpleNamespace(full_code="sz000002", code="000002", exchange="sz", name="万科A"),
                    SimpleNamespace(full_code="sz000004", code="000004", exchange="sz", name="ST国华"),
                ]
            return []

    client = LimitLadderClient()
    client.resource_payloads["zhb.zip"] = _stats_zip_bytes(
        "\n".join(["|".join(stat_000001), "|".join(stat_000002), "|".join(stat_000004)]),
        "\n".join(["|".join(stat2_000001), "|".join(stat2_000002), "|".join(stat2_000004)]),
    )
    client.category_quote_pages = {
        0: [
            SimpleNamespace(
                exchange="sz",
                code="000001",
                full_code="sz000001",
                last_price=11.0,
                pre_close=10.0,
                open=10.5,
                high=11.0,
                low=10.5,
                change=1.0,
                change_pct=10.0,
                amplitude_pct=5.0,
                total_hand=10000,
                current_hand=100,
                amount=110000000.0,
                inside_dish=4000,
                outer_disc=6000,
                open_amount=20000000.0,
                bid1_price=11.0,
                bid1_volume=5000,
                ask1_price=0,
                ask1_volume=0,
            ),
            SimpleNamespace(
                exchange="sz",
                code="000002",
                full_code="sz000002",
                last_price=11.0,
                pre_close=10.0,
                open=10.2,
                high=11.0,
                low=10.1,
                change=1.0,
                change_pct=10.0,
                amplitude_pct=9.0,
                total_hand=8000,
                current_hand=80,
                amount=86400000.0,
                inside_dish=3000,
                outer_disc=5000,
                open_amount=15000000.0,
                bid1_price=11.0,
                bid1_volume=3000,
                ask1_price=0,
                ask1_volume=0,
            ),
            SimpleNamespace(
                exchange="sz",
                code="000004",
                full_code="sz000004",
                last_price=10.5,
                pre_close=10.0,
                open=10.2,
                high=10.5,
                low=10.1,
                change=0.5,
                change_pct=5.0,
                amplitude_pct=4.0,
                total_hand=3000,
                current_hand=30,
                amount=31500000.0,
                inside_dish=1000,
                outer_disc=2000,
                open_amount=5000000.0,
                bid1_price=10.5,
                bid1_volume=2000,
                ask1_price=0,
                ask1_volume=0,
            ),
        ]
    }
    adapter = TdxRequestAdapter(client=client)

    def fake_topics(instrument_ids, *, topic_type, **kwargs):
        assert topic_type == "theme"
        assert instrument_ids == ["000001.SZ", "000002.SZ"]
        return {
            "000001.SZ": [
                {"topic_name": "不可减持(新规)", "topic_id": "9", "relevance": 99.0},
                {"topic_name": "国企改革", "topic_id": "10", "relevance": 98.0},
                {"topic_name": "央企改革", "topic_id": "12", "relevance": 97.0},
                {"topic_name": "最近闪拉", "topic_id": "14", "relevance": 96.0},
                {"topic_name": "银行", "topic_id": "1", "relevance": 9.0},
                {"topic_name": "机器人", "topic_id": "2", "relevance": 3.0},
            ],
            "000002.SZ": [
                {"topic_name": "罗素大盘", "topic_id": "8", "relevance": 99.0},
                {"topic_name": "参股券商", "topic_id": "11", "relevance": 98.0},
                {"topic_name": "江西国资改革", "topic_id": "13", "relevance": 97.0},
                {"topic_name": "参股新三板", "topic_id": "15", "relevance": 96.0},
                {"topic_name": "机器人", "topic_id": "2", "relevance": 8.0},
                {"topic_name": "地产", "topic_id": "3", "relevance": 7.0},
            ],
        }

    monkeypatch.setattr(adapter, "_topic_rows_by_instrument_id", fake_topics)

    rows = adapter.request("stock_limit_ladder_tdx", {"count": 10})

    assert rows[0]["instrument_id"] == "000001.SZ"
    assert rows[0]["primary_theme"] == "机器人"
    assert rows[0]["secondary_themes"] == "银行"
    assert rows[1]["primary_theme"] == "机器人"
    assert rows[1]["secondary_themes"] == "地产"
    assert adapter.last_meta["tdx_protocol"] == "0x054b+tdxstat+7615"


def test_tdx_adapter_orders_limit_ladder_themes_by_limit_up_count():
    rows = [
        {
            "instrument_id": "000001.SZ",
            "tdx_code": "sz000001",
            "limit_status": "sealed",
            "ladder_level": 2,
        },
        {
            "instrument_id": "000002.SZ",
            "tdx_code": "sz000002",
            "limit_status": "sealed",
            "ladder_level": 2,
        },
        {
            "instrument_id": "000003.SZ",
            "tdx_code": "sz000003",
            "limit_status": "sealed",
            "ladder_level": 6,
        },
        {
            "instrument_id": "000004.SZ",
            "tdx_code": "sz000004",
            "limit_status": "sealed",
            "ladder_level": 1,
        },
    ]
    topic_rows = {
        "000001.SZ": [
            {"topic_name": "broad_theme", "topic_id": "a", "relevance": 1.0},
            {"topic_name": "high_theme", "topic_id": "b", "relevance": 99.0},
        ],
        "000002.SZ": [
            {"topic_name": "broad_theme", "topic_id": "a", "relevance": 1.0},
        ],
        "000003.SZ": [
            {"topic_name": "high_theme", "topic_id": "b", "relevance": 99.0},
        ],
        "000004.SZ": [
            {"topic_name": "broad_theme", "topic_id": "a", "relevance": 1.0},
        ],
    }

    tdx_request._attach_limit_ladder_themes(rows, topic_rows)

    assert rows[0]["primary_theme"] == "broad_theme"
    assert rows[0]["secondary_themes"] == "high_theme"


def test_tdx_adapter_queries_limit_ladder_topics_in_parallel():
    adapter = TdxRequestAdapter(options={"f10_topic_workers": 3})
    active = 0
    max_active = 0
    lock = threading.Lock()

    def fake_request_stock_f10(interface_name, params):
        nonlocal active, max_active
        assert interface_name == "stock_topic_exposure_tdx"
        with lock:
            active += 1
            max_active = max(max_active, active)
        try:
            time.sleep(0.03)
            return [{"topic_name": f"题材{params['code']}", "topic_id": params["code"], "relevance": 1.0}]
        finally:
            with lock:
                active -= 1

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(adapter, "_request_stock_f10", fake_request_stock_f10)
        rows = adapter._topic_rows_by_instrument_id(
            ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"],
            topic_type="theme",
        )
    finally:
        monkeypatch.undo()

    assert set(rows) == {"000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"}
    assert max_active > 1


def test_tdx_adapter_defaults_limit_ladder_topics_to_six_workers_and_refills_only_when_missing():
    adapter = TdxRequestAdapter()

    def fake_request_stock_f10(interface_name, params):
        assert interface_name == "stock_topic_exposure_tdx"
        return [{"topic_name": f"题材{params['code']}", "topic_id": params["code"], "relevance": 1.0}]

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(adapter, "_request_stock_f10", fake_request_stock_f10)
        adapter._topic_rows_by_instrument_id(
            [f"00000{index}.SZ" for index in range(1, 10)],
            topic_type="theme",
        )
    finally:
        monkeypatch.undo()

    assert adapter._last_topic_lookup_meta["tdx_f10_topic_workers"] == 6
    assert adapter._last_topic_lookup_meta["tdx_f10_topic_refill_workers"] == 6
    assert adapter._last_topic_lookup_meta["tdx_f10_topic_refill_configured_rounds"] == 1
    assert adapter._last_topic_lookup_meta["tdx_f10_topic_refill_rounds"] == 0
    assert adapter._last_topic_lookup_meta["tdx_f10_topic_refill_requested_count"] == 0


def test_tdx_adapter_refills_missing_limit_ladder_topics():
    adapter = TdxRequestAdapter(
        options={
            "f10_topic_workers": 8,
            "f10_topic_refill_workers": 8,
            "f10_topic_refill_rounds": 1,
        }
    )
    call_counts: dict[str, int] = {}
    active = 0
    max_active = 0
    lock = threading.Lock()

    def fake_request_stock_f10(interface_name, params):
        nonlocal active, max_active
        assert interface_name == "stock_topic_exposure_tdx"
        code = params["code"]
        with lock:
            active += 1
            max_active = max(max_active, active)
        try:
            time.sleep(0.01)
            call_counts[code] = call_counts.get(code, 0) + 1
            if code == "000003.SZ" and call_counts[code] == 1:
                return []
            return [{"topic_name": f"题材{code}", "topic_id": code, "relevance": 1.0}]
        finally:
            with lock:
                active -= 1

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(adapter, "_request_stock_f10", fake_request_stock_f10)
        rows = adapter._topic_rows_by_instrument_id(
            [
                "000001.SZ",
                "000002.SZ",
                "000003.SZ",
                "000004.SZ",
                "000005.SZ",
                "000006.SZ",
                "000007.SZ",
                "000008.SZ",
                "000009.SZ",
            ],
            topic_type="theme",
        )
    finally:
        monkeypatch.undo()

    assert rows["000003.SZ"] == [{"topic_name": "题材000003.SZ", "topic_id": "000003.SZ", "relevance": 1.0}]
    assert call_counts["000003.SZ"] == 2
    assert max_active > 1
    assert adapter._last_topic_lookup_meta["tdx_f10_topic_workers"] == 8
    assert adapter._last_topic_lookup_meta["tdx_f10_topic_refill_workers"] == 8
    assert adapter._last_topic_lookup_meta["tdx_f10_topic_refill_rounds"] == 1
    assert adapter._last_topic_lookup_meta["tdx_f10_topic_refill_requested_count"] == 1
    assert adapter._last_topic_lookup_meta["tdx_f10_topic_missing_stock_count"] == 0


def test_tdx_adapter_requests_stock_theme_strength_rank(monkeypatch, tmp_path):
    monkeypatch.setenv("AXDATA_TDX_STATS_ROOT", str(tmp_path / "stats-cache"))
    stat_000001 = [""] * 35
    stat_000001[0] = "0"
    stat_000001[1] = "000001"
    stat_000001[4] = "20260616"
    stat_000001[11] = "1000"
    stat_000001[26] = "8"
    stat_000001[31] = "7"
    stat_000001[32] = "5"
    stat_000001[33] = "3"
    stat_000002 = [""] * 35
    stat_000002[0] = "0"
    stat_000002[1] = "000002"
    stat_000002[4] = "20260616"
    stat_000002[11] = "2000"
    stat_000002[26] = "2"
    stat_000002[31] = "3"
    stat_000002[32] = "1"
    stat_000002[33] = "0"
    stat_000004 = [""] * 35
    stat_000004[0] = "0"
    stat_000004[1] = "000004"
    stat_000004[4] = "20260616"
    stat_000004[11] = "500"
    stat_000004[26] = "1"
    stat_000004[31] = "1"
    stat_000004[32] = "1"
    stat_000004[33] = "0"
    stat2_000001 = [""] * 21
    stat2_000001[0] = "0"
    stat2_000001[1] = "000001"
    stat2_000001[2] = "20260616"
    stat2_000002 = [""] * 21
    stat2_000002[0] = "0"
    stat2_000002[1] = "000002"
    stat2_000002[2] = "20260616"
    stat2_000004 = [""] * 21
    stat2_000004[0] = "0"
    stat2_000004[1] = "000004"
    stat2_000004[2] = "20260616"

    class ThemeRankClient(FakeTdxClient):
        def get_codes(self, market, *, start=0, limit=None):
            if market == "sz":
                return [
                    SimpleNamespace(full_code="sz000001", code="000001", exchange="sz", name="平安银行"),
                    SimpleNamespace(full_code="sz000002", code="000002", exchange="sz", name="万科A"),
                    SimpleNamespace(full_code="sz000004", code="000004", exchange="sz", name="ST国华"),
                ]
            return []

    client = ThemeRankClient()
    client.resource_payloads["zhb.zip"] = _stats_zip_bytes(
        "\n".join(["|".join(stat_000001), "|".join(stat_000002), "|".join(stat_000004)]),
        "\n".join(["|".join(stat2_000001), "|".join(stat2_000002), "|".join(stat2_000004)]),
    )
    client.category_quote_pages = {
        0: [
            SimpleNamespace(
                exchange="sz",
                code="000001",
                full_code="sz000001",
                last_price=11.0,
                pre_close=10.0,
                open=10.5,
                high=11.0,
                low=10.5,
                change=1.0,
                change_pct=10.0,
                amplitude_pct=5.0,
                total_hand=10000,
                current_hand=100,
                amount=110000000.0,
                inside_dish=4000,
                outer_disc=6000,
                open_amount=20000000.0,
                bid1_price=11.0,
                bid1_volume=5000,
                ask1_price=0,
                ask1_volume=0,
            ),
            SimpleNamespace(
                exchange="sz",
                code="000002",
                full_code="sz000002",
                last_price=11.0,
                pre_close=10.0,
                open=10.2,
                high=11.0,
                low=10.1,
                change=1.0,
                change_pct=10.0,
                amplitude_pct=9.0,
                total_hand=8000,
                current_hand=80,
                amount=86400000.0,
                inside_dish=3000,
                outer_disc=5000,
                open_amount=15000000.0,
                bid1_price=11.0,
                bid1_volume=3000,
                ask1_price=0,
                ask1_volume=0,
            ),
            SimpleNamespace(
                exchange="sz",
                code="000004",
                full_code="sz000004",
                last_price=10.5,
                pre_close=10.0,
                open=10.2,
                high=10.5,
                low=10.1,
                change=0.5,
                change_pct=5.0,
                amplitude_pct=4.0,
                total_hand=3000,
                current_hand=30,
                amount=31500000.0,
                inside_dish=1000,
                outer_disc=2000,
                open_amount=5000000.0,
                bid1_price=10.5,
                bid1_volume=2000,
                ask1_price=0,
                ask1_volume=0,
            ),
        ]
    }
    adapter = TdxRequestAdapter(client=client)

    def fake_topics(instrument_ids, *, topic_type, **kwargs):
        assert topic_type == "theme"
        assert instrument_ids == ["000001.SZ", "000002.SZ"]
        return {
            "000001.SZ": [
                {"topic_name": "不可减持(新规)", "topic_id": "9", "relevance": 99.0},
                {"topic_name": "国企改革", "topic_id": "10", "relevance": 98.0},
                {"topic_name": "央企改革", "topic_id": "12", "relevance": 97.0},
                {"topic_name": "最近闪拉", "topic_id": "14", "relevance": 96.0},
                {"topic_name": "银行", "topic_id": "1", "relevance": 9.0},
                {"topic_name": "机器人", "topic_id": "2", "relevance": 3.0},
            ],
            "000002.SZ": [
                {"topic_name": "罗素大盘", "topic_id": "8", "relevance": 99.0},
                {"topic_name": "参股券商", "topic_id": "11", "relevance": 98.0},
                {"topic_name": "江西国资改革", "topic_id": "13", "relevance": 97.0},
                {"topic_name": "参股新三板", "topic_id": "15", "relevance": 96.0},
                {"topic_name": "机器人", "topic_id": "2", "relevance": 8.0},
                {"topic_name": "地产", "topic_id": "3", "relevance": 7.0},
            ],
        }

    monkeypatch.setattr(adapter, "_topic_rows_by_instrument_id", fake_topics)

    rows = adapter.request("stock_theme_strength_rank_tdx", {"count": 10})

    assert [row["topic_name"] for row in rows] == ["机器人", "银行", "地产"]
    assert rows[0]["rank"] == 1
    assert rows[0]["topic_id"] == "2"
    assert rows[0]["theme_strength_score"] == 245.0
    assert rows[0]["limit_up_count"] == 2
    assert rows[0]["highest_ladder_level"] == 4
    assert rows[0]["lianban_stock_count"] == 1
    assert rows[0]["first_board_count"] == 1
    assert rows[0]["leader_instrument_id"] == "000001.SZ"
    assert rows[0]["leader_name"] == "平安银行"
    assert rows[0]["leader_ladder_level"] == 4
    assert rows[0]["leader_limit_board_text"] == "8天6板"
    assert rows[0]["leader_seal_amount"] == 5500000.0
    assert rows[0]["seal_amount_sum"] == 8800000.0
    assert rows[0]["amount_sum"] == 196400000.0
    assert rows[0]["top_stock_summary"] == "平安银行（8天6板） 万科A（4天2板）"
    assert rows[1]["theme_strength_score"] == 145.0
    assert rows[2]["theme_strength_score"] == 110.0
    assert set(adapter.last_meta["tdx_theme_strength_timing_ms"]) == {
        "stats",
        "rank_scan",
        "name_lookup",
        "normalize_filter",
        "theme_lookup",
        "rank_build",
        "total",
    }
    assert adapter.last_meta["tdx_protocol"] == "0x054b+tdxstat+7615"
    assert adapter.last_meta["tdx_ladder_count"] == 2
    assert adapter.last_meta["tdx_returned_count"] == 3


def test_tdx_adapter_requests_stock_auction_process_and_normalizes_rows():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_auction_process_tdx", {"code": "000988.SZ"})

    assert client.auction_calls == [
        {
            "code": "sz000988",
            "mode_or_selector_raw": 3,
            "start": 0,
            "count": 500,
            "include_raw": False,
        }
    ]
    assert rows == [
        {
            "instrument_id": "000988.SZ",
            "symbol": "000988",
            "tdx_code": "sz000988",
            "exchange": "SZSE",
            "auction_time": "09:15:00",
            "auction_index": 0,
            "price": 162.12,
            "matched_volume": 2568,
            "matched_amount_estimated": 41632416.0,
            "unmatched_volume": 2433,
            "unmatched_amount_estimated": 394442.0,
            "unmatched_direction": 1,
        },
        {
            "instrument_id": "000988.SZ",
            "symbol": "000988",
            "tdx_code": "sz000988",
            "exchange": "SZSE",
            "auction_time": "09:15:09",
            "auction_index": 1,
            "price": 162.12,
            "matched_volume": 6630,
            "matched_amount_estimated": 107485560.0,
            "unmatched_volume": 1115,
            "unmatched_amount_estimated": 180763.8,
            "unmatched_direction": -1,
        },
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x056a"
    assert adapter.last_meta["tdx_requested_code_count"] == 1
    assert adapter.last_meta["tdx_point_count"] == 2


def test_source_request_gateway_filters_stock_auction_process_fields():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    result = request_interface(
        "stock_auction_process_tdx",
        params={"code": "000988.SZ"},
        fields=["instrument_id", "auction_time", "price", "matched_volume"],
        adapter=adapter,
    )

    assert result.records == [
        {
            "instrument_id": "000988.SZ",
            "auction_time": "09:15:00",
            "price": 162.12,
            "matched_volume": 2568,
        },
        {
            "instrument_id": "000988.SZ",
            "auction_time": "09:15:09",
            "price": 162.12,
            "matched_volume": 6630,
        },
    ]
    assert result.meta["interface_name"] == "stock_auction_process_tdx"
    assert result.meta["tdx_protocol"] == "0x056a"


def test_tdx_adapter_requests_second_kline_and_normalizes_rows():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_kline_second_tdx",
        {"code": "000001.SZ", "seconds": 5, "start": 0, "count": 2},
    )

    assert client.connected is True
    assert client.kline_calls == [
        {
            "code": "sz000001",
            "period": "5s",
            "start": 0,
            "count": 2,
            "adjust": "none",
            "anchor_date": None,
            "kind": "stock",
        }
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "trade_time": "2026-05-19T13:39:50+08:00",
            "period": "5s",
            "open": 10.13,
            "high": 10.15,
            "low": 10.12,
            "close": 10.14,
            "volume": 120.0,
            "amount": 121680.0,
        }
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x052d"
    assert adapter.last_meta["tdx_period_raw"] == 13
    assert adapter.last_meta["tdx_period_param_raw"] == 5
    assert adapter.last_meta["tdx_requested_code_count"] == 1


def test_tdx_adapter_requests_stock_capital_changes_and_normalizes_rows():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_capital_changes_tdx", {"code": "000001.SZ"})

    assert client.capital_change_calls == [{"code": "sz000001", "include_raw": False}]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "ts_code": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "event_date": "20240601",
            "category_raw": 1,
            "category_name": "除权除息",
            "c1": 1.0,
            "c2": 0.0,
            "c3": 2.0,
            "c4": 0.0,
            "c1_raw_hex": "0000803f",
            "c2_raw_hex": "00000000",
            "c3_raw_hex": "00000040",
            "c4_raw_hex": "00000000",
            "record_hex": "0000303030303100c054340101000000000000000000004000000000",
        }
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x000f"
    assert adapter.last_meta["tdx_event_count"] == 1
    assert adapter.last_meta["tdx_returned_event_count"] == 1


def test_tdx_adapter_filters_stock_capital_changes_by_category_alias():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_capital_changes_tdx", {"code": "000001.SZ", "category": "xdxr"})

    assert [row["category_raw"] for row in rows] == [1]
    assert adapter.last_meta["tdx_category_filter"] == (1,)


def test_tdx_adapter_requests_stock_capital_changes_all_expands_scope_and_parallelizes():
    class ParallelCapitalChangeClient(FakeTdxClient):
        pool_size = 3

        def __init__(self) -> None:
            super().__init__()
            self._active_capital_change_calls = 0
            self.max_active_capital_change_calls = 0
            self._capital_change_lock = threading.Lock()

        def get_codes(self, market, *, start=0, limit=None):
            self.calls.append({"market": market, "start": start, "limit": limit})
            if market != "sz" or start > 0:
                return []
            return [
                SimpleNamespace(
                    full_code=f"sz30{index:04d}",
                    code=f"30{index:04d}",
                    exchange="sz",
                    name=f"样本{index}",
                    category="a_share",
                    category_reason="test",
                    board="szse_chinext",
                    board_reason="test",
                    decimal=2,
                    multiple=100,
                    previous_close_price=10.0,
                    volume_ratio_base=1.0,
                )
                for index in range(5)
            ]

        def get_capital_changes(self, code, *, include_raw=False):
            with self._capital_change_lock:
                self._active_capital_change_calls += 1
                self.max_active_capital_change_calls = max(
                    self.max_active_capital_change_calls,
                    self._active_capital_change_calls,
                )
            try:
                time.sleep(0.02)
                self.capital_change_calls.append({"code": code, "include_raw": include_raw})
                return SimpleNamespace(
                    exchange=code[:2],
                    code=code[2:],
                    full_code=code,
                    records=(
                        SimpleNamespace(
                            exchange=code[:2],
                            code=code[2:],
                            full_code=code,
                            date=date(2024, 6, 1),
                            category_raw=1,
                            category_name="除权除息",
                            c1_float=1.0,
                            c2_float=0.0,
                            c3_float=2.0,
                            c4_float=0.0,
                            c1_quantity=1.0,
                            c2_quantity=0.0,
                            c3_quantity=2.0,
                            c4_quantity=0.0,
                            c1_raw=b"\x00\x00\x80?",
                            c2_raw=b"\x00\x00\x00\x00",
                            c3_raw=b"\x00\x00\x00@",
                            c4_raw=b"\x00\x00\x00\x00",
                            record_hex=None,
                        ),
                    ),
                )
            finally:
                with self._capital_change_lock:
                    self._active_capital_change_calls -= 1

    client = ParallelCapitalChangeClient()
    progress_events = []
    adapter = TdxRequestAdapter(
        client=client,
        progress_callback=lambda percent, message, **details: progress_events.append(
            {"percent": percent, "message": message, **details}
        ),
    )

    rows = adapter.request("stock_capital_changes_tdx", {"scope": "chinext", "category": "xdxr"})

    assert len(rows) == 5
    assert [row["tdx_code"] for row in rows] == [f"sz30{index:04d}" for index in range(5)]
    assert all(row["category_raw"] == 1 for row in rows)
    assert rows[0]["record_hex"].startswith("sz300000|20240601|1|")
    assert len(client.capital_change_calls) == 5
    assert client.max_active_capital_change_calls > 1
    assert adapter.last_meta["tdx_protocol"] == "0x000f"
    assert adapter.last_meta["tdx_stock_scope"] == "chinext"
    assert adapter.last_meta["tdx_code_expansion_source"] == "stock_codes_tdx"
    assert adapter.last_meta["tdx_requested_code_count"] == 5
    assert adapter.last_meta["tdx_event_count"] == 5
    assert adapter.last_meta["tdx_returned_event_count"] == 5
    assert adapter.last_meta["tdx_capital_change_concurrency"] == 3
    assert adapter.last_meta["tdx_category_filter"] == (1,)
    request_progress = [event for event in progress_events if "请求股本变迁" in event["message"]]
    assert request_progress
    assert request_progress[-1]["progress_current"] == 5
    assert request_progress[-1]["progress_total"] == 5
    assert request_progress[-1]["progress_unit"] == "只"
    assert request_progress[-1]["percent"] == 68


def test_tdx_adapter_rejects_unknown_stock_capital_change_category():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    with pytest.raises(SourceRequestValidationError, match="category must be"):
        adapter.request("stock_capital_changes_tdx", {"code": "000001.SZ", "category": "bad"})


def test_tdx_adapter_builds_qfq_adj_factor_from_xdxr_and_daily_bars():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_adj_factor_tdx", {"code": "000001.SZ", "adjust": "qfq"})

    assert client.capital_change_calls == [{"code": "sz000001", "include_raw": False}]
    assert client.kline_calls[0]["period"] == "day"
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "ts_code": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "trade_date": "20240531",
            "adj_factor": 0.825,
        },
        {
            "instrument_id": "000001.SZ",
            "ts_code": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "trade_date": "20240603",
            "adj_factor": 1.0,
        },
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x000f+0x052d"
    assert adapter.last_meta["tdx_adjust_method"] == "qfq"
    assert adapter.last_meta["tdx_xdxr_event_count"] == 1


def test_tdx_adapter_builds_hfq_adj_factor_from_xdxr_and_daily_bars():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_adj_factor_tdx", {"code": "000001.SZ", "adjust": "hfq"})

    assert [row["adj_factor"] for row in rows] == [1.0, 1.2121212121]
    assert adapter.last_meta["tdx_adjust_method"] == "hfq"


def test_tdx_adapter_requests_stock_intraday_buy_sell_strength_and_normalizes_rows():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_intraday_buy_sell_strength_tdx", {"code": "000001.SZ"})

    assert client.subchart_calls == [{"code": "sz000001", "selector": 0, "include_raw": False}]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "minute_time": "09:30",
            "minute_index": 0,
            "bid_order": 174,
            "ask_order": 98,
        },
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "minute_time": "09:31",
            "minute_index": 1,
            "bid_order": 792,
            "ask_order": 87,
        },
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x051b"
    assert adapter.last_meta["tdx_selector_raw"] == 0
    assert adapter.last_meta["tdx_selector_name"] == "buy_sell_strength"
    assert adapter.last_meta["tdx_requested_code_count"] == 1
    assert adapter.last_meta["tdx_point_count"] == 2


def test_tdx_adapter_requests_stock_intraday_volume_comparison_and_normalizes_rows():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_intraday_volume_comparison_tdx", {"code": "000001.SZ"})

    assert client.subchart_calls == [{"code": "sz000001", "selector": 0x0B, "include_raw": False}]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "minute_time": "09:30",
            "minute_index": 0,
            "today_volume": 105552.0,
            "yesterday_volume": 59153.0,
            "volume_change": 46399.0,
            "volume_change_pct": 78.438963,
        },
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "minute_time": "09:31",
            "minute_index": 1,
            "today_volume": 177336.0,
            "yesterday_volume": 98511.0,
            "volume_change": 78825.0,
            "volume_change_pct": 80.016445,
        },
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x051b"
    assert adapter.last_meta["tdx_selector_raw"] == 0x0B
    assert adapter.last_meta["tdx_selector_name"] == "volume_comparison"
    assert adapter.last_meta["tdx_point_count"] == 2


def test_intraday_subchart_minute_time_matches_tdx_cursor_time():
    assert tdx_request._intraday_subchart_minute_time(0) == "09:30"
    assert tdx_request._intraday_subchart_minute_time(85) == "10:55"
    assert tdx_request._intraday_subchart_minute_time(120) == "13:00"
    assert tdx_request._intraday_subchart_minute_time(239) == "14:59"


def test_source_request_gateway_filters_stock_intraday_volume_comparison_fields():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    result = request_interface(
        "stock_intraday_volume_comparison_tdx",
        params={"code": "000001.SZ"},
        fields=["instrument_id", "minute_index", "today_volume", "volume_change_pct"],
        adapter=adapter,
    )

    assert result.records == [
        {
            "instrument_id": "000001.SZ",
            "minute_index": 0,
            "today_volume": 105552.0,
            "volume_change_pct": 78.438963,
        },
        {
            "instrument_id": "000001.SZ",
            "minute_index": 1,
            "today_volume": 177336.0,
            "volume_change_pct": 80.016445,
        },
    ]
    assert result.meta["interface_name"] == "stock_intraday_volume_comparison_tdx"
    assert result.meta["tdx_protocol"] == "0x051b"


def test_tdx_adapter_requests_stock_intraday_history_and_normalizes_rows():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_intraday_history_tdx",
        {"code": "000001.SZ", "trade_date": "2026-05-19"},
    )

    assert client.intraday_calls == [
        {"code": "sz000001", "trade_date": "20260519", "include_raw": False}
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "trade_date": "20260519",
            "trade_time": "2026-05-19T09:31:00+08:00",
            "minute_index": 0,
            "price": 10.13,
            "volume": 120,
            "prev_close": 10.08,
        },
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "trade_date": "20260519",
            "trade_time": "2026-05-19T09:32:00+08:00",
            "minute_index": 1,
            "price": 10.15,
            "volume": 80,
            "prev_close": 10.08,
        },
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x0fb4"
    assert adapter.last_meta["tdx_trade_date"] == "20260519"
    assert adapter.last_meta["tdx_requested_code_count"] == 1
    assert adapter.last_meta["tdx_point_count"] == 2


def test_tdx_adapter_requests_stock_intraday_recent_history_and_normalizes_rows():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_intraday_recent_history_tdx",
        {"code": "000001.SZ", "trade_date": "2026-05-19"},
    )

    assert client.recent_intraday_calls == [
        {"code": "sz000001", "trade_date": "20260519", "include_raw": False}
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "trade_date": "20260519",
            "trade_time": "2026-05-19T09:31:00+08:00",
            "time_label": "09:31",
            "minute_index": 0,
            "price": 10.13,
            "avg_price": 10.115,
            "volume": 120,
            "prev_close": 10.08,
            "open_price": 10.12,
        },
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "trade_date": "20260519",
            "trade_time": "2026-05-19T09:32:00+08:00",
            "time_label": "09:32",
            "minute_index": 1,
            "price": 10.15,
            "avg_price": 10.128,
            "volume": 80,
            "prev_close": 10.08,
            "open_price": 10.12,
        },
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x0feb"
    assert adapter.last_meta["tdx_trade_date"] == "20260519"
    assert adapter.last_meta["tdx_requested_code_count"] == 1
    assert adapter.last_meta["tdx_point_count"] == 2


def test_tdx_adapter_requests_stock_intraday_today_and_normalizes_rows():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_intraday_today_tdx", {"code": "000001.SZ"})

    assert client.today_intraday_calls == [{"code": "sz000001", "include_raw": False}]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "time_label": "09:31",
            "minute_index": 0,
            "price": 10.86,
            "avg_price": 10.8417,
            "volume": 120,
        },
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "time_label": "09:32",
            "minute_index": 1,
            "price": 10.88,
            "avg_price": 10.8427,
            "volume": 80,
        },
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x0537"
    assert adapter.last_meta["tdx_requested_code_count"] == 1
    assert adapter.last_meta["tdx_point_count"] == 2


def test_tdx_adapter_requests_index_intraday_today_and_history():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    today_rows = adapter.request("index_intraday_today_tdx", {"code": "000001.SH"})
    history_rows = adapter.request(
        "index_intraday_history_tdx",
        {"code": "000001.SH", "trade_date": "20260519"},
    )

    assert today_rows[0]["instrument_id"] == "000001.SH"
    assert today_rows[0]["time_label"] == "09:31"
    assert today_rows[0]["avg_price"] == 10.8417
    assert history_rows[0]["instrument_id"] == "000001.SH"
    assert history_rows[0]["trade_time"] == "2026-05-19T09:31:00+08:00"
    assert adapter.last_meta["tdx_asset_type"] == "index"


def test_source_request_gateway_filters_stock_intraday_today_fields():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    result = request_interface(
        "stock_intraday_today_tdx",
        params={"code": "000001.SZ"},
        fields=["instrument_id", "time_label", "price", "avg_price", "volume"],
        adapter=adapter,
    )

    assert result.records == [
        {
            "instrument_id": "000001.SZ",
            "time_label": "09:31",
            "price": 10.86,
            "avg_price": 10.8417,
            "volume": 120,
        },
        {
            "instrument_id": "000001.SZ",
            "time_label": "09:32",
            "price": 10.88,
            "avg_price": 10.8427,
            "volume": 80,
        },
    ]
    assert result.meta["interface_name"] == "stock_intraday_today_tdx"
    assert result.meta["tdx_protocol"] == "0x0537"


def test_source_request_gateway_filters_stock_intraday_history_fields():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    result = request_interface(
        "stock_intraday_history_tdx",
        params={"code": "000001.SZ", "trade_date": "20260519"},
        fields=["instrument_id", "trade_time", "price", "volume"],
        adapter=adapter,
    )

    assert result.records == [
        {
            "instrument_id": "000001.SZ",
            "trade_time": "2026-05-19T09:31:00+08:00",
            "price": 10.13,
            "volume": 120,
        },
        {
            "instrument_id": "000001.SZ",
            "trade_time": "2026-05-19T09:32:00+08:00",
            "price": 10.15,
            "volume": 80,
        },
    ]
    assert result.meta["interface_name"] == "stock_intraday_history_tdx"
    assert result.meta["tdx_protocol"] == "0x0fb4"


def test_source_request_gateway_filters_stock_intraday_recent_history_fields():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    result = request_interface(
        "stock_intraday_recent_history_tdx",
        params={"code": "000001.SZ", "trade_date": "20260519"},
        fields=["instrument_id", "time_label", "price", "avg_price", "open_price"],
        adapter=adapter,
    )

    assert result.records == [
        {
            "instrument_id": "000001.SZ",
            "time_label": "09:31",
            "price": 10.13,
            "avg_price": 10.115,
            "open_price": 10.12,
        },
        {
            "instrument_id": "000001.SZ",
            "time_label": "09:32",
            "price": 10.15,
            "avg_price": 10.128,
            "open_price": 10.12,
        },
    ]
    assert result.meta["interface_name"] == "stock_intraday_recent_history_tdx"
    assert result.meta["tdx_protocol"] == "0x0feb"


def test_tdx_adapter_rejects_invalid_stock_intraday_history_trade_date():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    with pytest.raises(SourceRequestValidationError, match="trade_date must be a valid date"):
        adapter.request(
            "stock_intraday_history_tdx",
            {"code": "000001.SZ", "trade_date": "20260231"},
        )


def test_tdx_adapter_requests_stock_trades_today_and_normalizes_rows():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_trades_today_tdx", {"code": "000001.SZ"})

    assert client.today_trade_calls == [
        {"code": "sz000001", "start": 0, "count": 1800, "include_raw": False}
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "trade_date": None,
            "trade_time": "14:08",
            "trade_datetime": None,
            "trade_index": 0,
            "price": 10.86,
            "volume": 89,
            "order_count": 9,
            "side": "buy",
        },
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "trade_date": None,
            "trade_time": "14:08",
            "trade_datetime": None,
            "trade_index": 1,
            "price": 10.85,
            "volume": 86,
            "order_count": 8,
            "side": "sell",
        },
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x0fc5"
    assert adapter.last_meta["tdx_trade_date"] is None
    assert adapter.last_meta["tdx_requested_code_count"] == 1
    assert adapter.last_meta["tdx_trade_count"] == 2


def test_tdx_adapter_requests_stock_trades_history_and_normalizes_rows():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_trades_history_tdx",
        {"code": "000001.SZ", "trade_date": "2026-05-11"},
    )

    assert client.historical_trade_calls == [
        {
            "code": "sz000001",
            "trade_date": "20260511",
            "start": 0,
            "count": 1800,
            "include_raw": False,
        }
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "trade_date": "20260511",
            "trade_time": "14:12",
            "trade_datetime": "2026-05-11T14:12:00+08:00",
            "trade_index": 0,
            "price": 10.86,
            "volume": 89,
            "order_count": 9,
            "side": "buy",
        },
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "trade_date": "20260511",
            "trade_time": "14:13",
            "trade_datetime": "2026-05-11T14:13:00+08:00",
            "trade_index": 1,
            "price": 10.85,
            "volume": 86,
            "order_count": 8,
            "side": "sell",
        },
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x0fc6"
    assert adapter.last_meta["tdx_trade_date"] == "20260511"
    assert adapter.last_meta["tdx_requested_code_count"] == 1
    assert adapter.last_meta["tdx_trade_count"] == 2


def test_source_request_gateway_filters_stock_trades_history_fields():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    result = request_interface(
        "stock_trades_history_tdx",
        params={"code": "000001.SZ", "trade_date": "20260511"},
        fields=["instrument_id", "trade_datetime", "price", "volume", "side"],
        adapter=adapter,
    )

    assert result.records == [
        {
            "instrument_id": "000001.SZ",
            "trade_datetime": "2026-05-11T14:12:00+08:00",
            "price": 10.86,
            "volume": 89,
            "side": "buy",
        },
        {
            "instrument_id": "000001.SZ",
            "trade_datetime": "2026-05-11T14:13:00+08:00",
            "price": 10.85,
            "volume": 86,
            "side": "sell",
        },
    ]
    assert result.meta["interface_name"] == "stock_trades_history_tdx"
    assert result.meta["tdx_protocol"] == "0x0fc6"


def test_tdx_adapter_requests_stock_auction_result_today_from_0925_trade():
    client = FakeAuctionResultTradeClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_auction_result_tdx", {"code": "000001.SZ"})

    assert client.today_trade_calls == [
        {"code": "sz000001", "start": 0, "count": 1800, "include_raw": False}
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "auction_time": "09:25",
            "trade_index": 1,
            "price": 10.86,
            "volume": 1200,
            "amount": 1303200.0,
            "order_count": 28,
        }
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x0fc5"
    assert adapter.last_meta["tdx_result_time"] == "09:25"
    assert adapter.last_meta["tdx_trade_count"] == 3
    assert adapter.last_meta["tdx_result_count"] == 1


def test_tdx_adapter_requests_stock_auction_result_history_from_0925_trade():
    client = FakeAuctionResultTradeClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_auction_result_history_tdx",
        {"code": "000001.SZ", "trade_date": "2026-05-11"},
    )

    assert client.historical_trade_calls == [
        {
            "code": "sz000001",
            "trade_date": "20260511",
            "start": 0,
            "count": 1800,
            "include_raw": False,
        }
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "trade_date": "20260511",
            "auction_time": "09:25",
            "auction_datetime": "2026-05-11T09:25:00+08:00",
            "trade_index": 1,
            "price": 10.86,
            "volume": 1200,
            "amount": 1303200.0,
            "order_count": 28,
        }
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x0fc6"
    assert adapter.last_meta["tdx_trade_date"] == "20260511"
    assert adapter.last_meta["tdx_result_time"] == "09:25"
    assert adapter.last_meta["tdx_trade_count"] == 3
    assert adapter.last_meta["tdx_result_count"] == 1


def test_source_request_gateway_filters_stock_auction_result_history_fields():
    adapter = TdxRequestAdapter(client=FakeAuctionResultTradeClient())

    result = request_interface(
        "stock_auction_result_history_tdx",
        params={"code": "000001.SZ", "trade_date": "20260511"},
        fields=["instrument_id", "auction_datetime", "price", "volume", "amount"],
        adapter=adapter,
    )

    assert result.records == [
        {
            "instrument_id": "000001.SZ",
            "auction_datetime": "2026-05-11T09:25:00+08:00",
            "price": 10.86,
            "volume": 1200,
            "amount": 1303200.0,
        }
    ]
    assert result.meta["interface_name"] == "stock_auction_result_history_tdx"
    assert result.meta["tdx_protocol"] == "0x0fc6"


def test_tdx_stat2_parser_maps_current_and_prior_day_columns():
    from axdata_source_tdx.stats_models import parse_stat2_rows

    line = (
        "0|002841|20260710|242501.84||254664.58|851.80|134819.47|3468.47|"
        "11141|9513|51.90|54.67|880812|6002.06|4291.39|19.060|55.980|31.360|43.54|37.30"
    )

    row = next(iter(parse_stat2_rows([line])))

    assert row.amount_10k == 242501.84
    assert row.seal_amount_10k is None
    assert row.prev_amount_10k == 254664.58
    assert row.prev_seal_amount_10k == 851.80
    assert row.prev2_amount_10k == 134819.47
    assert row.prev2_seal_amount_10k == 3468.47
    assert row.open_volume_hand == 11141
    assert row.prev_open_volume_hand == 9513
    assert row.open_amount_10k == 6002.06
    assert row.prev_open_amount_10k == 4291.39


def test_recent_daily_bars_excludes_base_date_and_accumulates_pages():
    from axdata_source_tdx.series_history import request_recent_daily_bars

    calls = []
    page_dates = {
        0: [13, 12, 11, 10, 9],
        5: [8, 7, 6, 5, 4],
    }

    def fake_tdx_kline(client, code, *, period, start, count, adjust, anchor_date):
        calls.append({"start": start, "count": count})
        bars = tuple(
            SimpleNamespace(time=datetime(2026, 6, day, 15, 0))
            for day in page_dates.get(start, [])
        )
        return SimpleNamespace(bars=bars)

    bars, page_count = request_recent_daily_bars(
        object(),
        "sz000001",
        count=5,
        target_trade_date="20260612",
        require_trading_activity=False,
        max_count=10,
        tdx_kline=fake_tdx_kline,
        kline_bars=lambda series: series.bars,
        get_value=lambda value, name, default=None: getattr(value, name, default),
        bar_trade_date=lambda bar: bar.time.strftime("%Y%m%d"),
    )

    assert [bar.time.strftime("%Y%m%d") for bar in bars] == [
        "20260611",
        "20260610",
        "20260609",
        "20260608",
        "20260607",
    ]
    assert page_count == 2
    assert calls == [{"start": 0, "count": 5}, {"start": 5, "count": 5}]


def test_recent_daily_bars_uses_latest_unique_actual_trading_days():
    from axdata_source_tdx.series_history import request_recent_daily_bars

    bars_by_start = {
        0: [
            (12, 100),
            (11, 500),
            (11, 500),
            (10, 0),
            (9, 400),
        ],
        5: [(8, 300), (7, 200), (6, 100)],
    }

    def fake_tdx_kline(client, code, *, period, start, count, adjust, anchor_date):
        bars = tuple(
            SimpleNamespace(
                time=datetime(2026, 6, day, 15, 0),
                volume_lots=volume,
                amount=float(volume) * 1000,
            )
            for day, volume in bars_by_start.get(start, [])
        )
        return SimpleNamespace(bars=bars)

    bars, page_count = request_recent_daily_bars(
        object(),
        "sz000001",
        count=5,
        target_trade_date="20260612",
        require_trading_activity=True,
        max_count=10,
        tdx_kline=fake_tdx_kline,
        kline_bars=lambda series: series.bars,
        get_value=lambda value, name, default=None: getattr(value, name, default),
        bar_trade_date=lambda bar: bar.time.strftime("%Y%m%d"),
    )

    assert [bar.time.strftime("%Y%m%d") for bar in bars] == [
        "20260611",
        "20260609",
        "20260608",
        "20260607",
        "20260606",
    ]
    assert page_count == 2


def test_auction_indicator_stats_alignment_handles_same_and_previous_trade_date():
    from axdata_source_tdx.derived_rows import auction_indicator_stats_alignment

    row = SimpleNamespace(
        stats_date="20260612",
        amount_10k=310.0,
        seal_amount_10k=31.0,
        prev_amount_10k=220.0,
        prev_seal_amount_10k=22.0,
        prev2_seal_amount_10k=11.0,
        open_volume_hand=130.0,
        prev_open_volume_hand=120.0,
        open_amount_10k=90.0,
        prev_open_amount_10k=80.0,
    )

    same_day = auction_indicator_stats_alignment(
        row,
        target_trade_date="20260612",
        previous_trade_date="20260611",
    )
    assert same_day == {
        "status": "same_day",
        "prev_amount_10k": 220.0,
        "prev_seal_amount_10k": 22.0,
        "prev2_seal_amount_10k": 11.0,
        "prev_open_volume_hand": 120.0,
        "prev_open_amount_10k": 80.0,
    }

    previous_day = auction_indicator_stats_alignment(
        row,
        target_trade_date="20260613",
        previous_trade_date="20260612",
    )
    assert previous_day == {
        "status": "previous_trading_day",
        "prev_amount_10k": 310.0,
        "prev_seal_amount_10k": 31.0,
        "prev2_seal_amount_10k": 22.0,
        "prev_open_volume_hand": 130.0,
        "prev_open_amount_10k": 90.0,
    }


def test_auction_indicator_stats_alignment_rejects_unaligned_date():
    from axdata_source_tdx.derived_rows import auction_indicator_stats_alignment

    alignment = auction_indicator_stats_alignment(
        SimpleNamespace(stats_date="20260610"),
        target_trade_date="20260613",
        previous_trade_date="20260612",
    )

    assert alignment["status"] == "stats_date_unaligned"
    assert all(value is None for key, value in alignment.items() if key != "status")


def test_tdx_stats_resource_uses_dominant_date_instead_of_first_row():
    from axdata_source_tdx.stats_models import stats_resource_from_lines

    stat_lines = [
        _minimal_stat_line("20260610"),
        _minimal_stat_line_for_code("20260612", "000002"),
        _minimal_stat_line_for_code("20260612", "000003"),
    ]
    stat2_lines = [
        _minimal_stat2_line("20260610", prev_amount="100.00"),
        _minimal_stat2_line_for_code("20260612", "000002"),
        _minimal_stat2_line_for_code("20260612", "000003"),
    ]

    resource = stats_resource_from_lines(
        stat_lines,
        stat2_lines,
        source_path="test",
    )

    assert resource.stats_date == "20260612"
    assert resource.stats_date_counts == {"20260610": 2, "20260612": 4}
    assert resource.stats_date_coverage == pytest.approx(4 / 6)


def test_market_date_context_uses_exchange_calendar_and_tdx_handshake():
    from axdata_source_tdx.market_dates import resolve_market_date_context

    handshake = SimpleNamespace(
        server_datetime=datetime(2026, 6, 12, 9, 25, tzinfo=ZoneInfo("Asia/Shanghai")),
        server_date_1=date(2026, 6, 12),
        server_date_2=date(2026, 6, 12),
    )
    client = SimpleNamespace(transport=SimpleNamespace(last_handshake=handshake))

    def fake_request_interface(interface_name, *, params, fields, persist):
        assert interface_name == "stock_trade_calendar_exchange"
        return SimpleNamespace(
            records=[
                {"cal_date": "20260611", "is_open": 1, "pretrade_date": "20260610"},
                {"cal_date": "20260612", "is_open": 1, "pretrade_date": "20260611"},
            ]
        )

    context = resolve_market_date_context(
        client,
        request_interface=fake_request_interface,
    )

    assert context.target_trade_date == "20260612"
    assert context.previous_trade_date == "20260611"
    assert context.phase == "trading"
    assert context.ready is True
    assert context.source == "exchange_calendar+tdx_handshake"


def test_market_date_context_blocks_preauction_and_unconfirmed_handshake():
    from axdata_source_tdx.market_dates import resolve_market_date_context

    def calendar(interface_name, *, params, fields, persist):
        return SimpleNamespace(
            records=[
                {"cal_date": "20260611", "is_open": 1, "pretrade_date": "20260610"},
                {"cal_date": "20260612", "is_open": 1, "pretrade_date": "20260611"},
            ]
        )

    preauction = SimpleNamespace(
        server_datetime=datetime(2026, 6, 12, 9, 24, tzinfo=ZoneInfo("Asia/Shanghai")),
        server_date_1=date(2026, 6, 11),
        server_date_2=date(2026, 6, 11),
    )
    preauction_context = resolve_market_date_context(
        SimpleNamespace(transport=SimpleNamespace(last_handshake=preauction)),
        request_interface=calendar,
    )
    assert preauction_context.phase == "pre_auction"
    assert preauction_context.ready is False

    unconfirmed = SimpleNamespace(
        server_datetime=datetime(2026, 6, 12, 9, 26, tzinfo=ZoneInfo("Asia/Shanghai")),
        server_date_1=date(2026, 6, 11),
        server_date_2=date(2026, 6, 11),
    )
    unconfirmed_context = resolve_market_date_context(
        SimpleNamespace(transport=SimpleNamespace(last_handshake=unconfirmed)),
        request_interface=calendar,
    )
    assert unconfirmed_context.phase == "market_date_unconfirmed"
    assert unconfirmed_context.ready is False


def test_market_date_context_uses_last_open_day_when_market_is_closed():
    from axdata_source_tdx.market_dates import resolve_market_date_context

    handshake = SimpleNamespace(
        server_datetime=datetime(2026, 6, 13, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        server_date_1=date(2026, 6, 12),
        server_date_2=date(2026, 6, 12),
    )

    def calendar(interface_name, *, params, fields, persist):
        return SimpleNamespace(
            records=[
                {"cal_date": "20260611", "is_open": "1", "pretrade_date": "20260610"},
                {"cal_date": "20260612", "is_open": "1", "pretrade_date": "20260611"},
                {"cal_date": "20260613", "is_open": "0", "pretrade_date": "20260612"},
            ]
        )

    context = resolve_market_date_context(
        SimpleNamespace(transport=SimpleNamespace(last_handshake=handshake)),
        request_interface=calendar,
    )

    assert context.target_trade_date == "20260612"
    assert context.previous_trade_date == "20260611"
    assert context.phase == "closed"
    assert context.ready is True


def test_market_date_context_reuses_calendar_rows_within_process(monkeypatch):
    import axdata_core.source_request as source_request_module
    from axdata_source_tdx import market_dates

    monkeypatch.setattr(market_dates, "_CALENDAR_ROWS_BY_DATE", {})
    calls = []

    def calendar(interface_name, *, params, fields, persist):
        calls.append(interface_name)
        return SimpleNamespace(
            records=[
                {"cal_date": "20260611", "is_open": 1, "pretrade_date": "20260610"},
                {"cal_date": "20260612", "is_open": 1, "pretrade_date": "20260611"},
            ]
        )

    monkeypatch.setattr(source_request_module, "request_interface", calendar)
    handshake = SimpleNamespace(
        server_datetime=datetime(2026, 6, 12, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        server_date_1=date(2026, 6, 12),
        server_date_2=date(2026, 6, 12),
    )
    client = SimpleNamespace(transport=SimpleNamespace(last_handshake=handshake))

    first = market_dates.resolve_market_date_context(client)
    second = market_dates.resolve_market_date_context(client)

    assert first == second
    assert calls == ["stock_trade_calendar_exchange"]


def test_market_date_context_fallback_still_blocks_before_auction():
    from axdata_source_tdx.market_dates import resolve_market_date_context

    handshake = SimpleNamespace(
        server_datetime=datetime(2026, 6, 12, 9, 24, tzinfo=ZoneInfo("Asia/Shanghai")),
        server_date_1=date(2026, 6, 12),
        server_date_2=date(2026, 6, 12),
    )

    def unavailable_calendar(*args, **kwargs):
        raise OSError("calendar unavailable")

    context = resolve_market_date_context(
        SimpleNamespace(transport=SimpleNamespace(last_handshake=handshake)),
        request_interface=unavailable_calendar,
    )

    assert context.target_trade_date == "20260612"
    assert context.phase == "pre_auction_fallback"
    assert context.ready is False


def test_tdx_adapter_requests_stock_auction_indicators(tmp_path):
    stats_root = tmp_path / "tdxstats"
    stats_root.mkdir()
    stat_parts = [""] * 35
    stat_parts[0] = "0"
    stat_parts[1] = "000001"
    stat_parts[2] = "0.50"
    stat_parts[3] = "12.30"
    stat_parts[4] = "20260612"
    stat_parts[11] = "200.00"
    stat_parts[26] = "9"
    stat_parts[31] = "7"
    stat_parts[32] = "5"
    stat_parts[33] = "3"
    stat2_parts = [""] * 21
    stat2_parts[0] = "0"
    stat2_parts[1] = "000001"
    stat2_parts[2] = "20260612"
    stat2_parts[3] = "250.00"
    stat2_parts[4] = "25.00"
    stat2_parts[5] = "200.00"
    stat2_parts[6] = "50.00"
    stat2_parts[7] = "180.00"
    stat2_parts[8] = "40.00"
    stat2_parts[9] = "10"
    stat2_parts[10] = "150"
    stat2_parts[14] = "1.00"
    stat2_parts[15] = "80.00"
    (stats_root / "tdxstat.cfg").write_text("|".join(stat_parts), encoding="gbk")
    (stats_root / "tdxstat2.cfg").write_text("|".join(stat2_parts), encoding="gbk")

    class FiveDayKlineClient(FakeTdxClient):
        def get_kline(self, code, *, period="day", start=0, count=800, adjust=None, anchor_date=None):
            self.kline_calls.append(
                {
                    "code": code,
                    "period": period,
                    "start": start,
                    "count": count,
                    "adjust": adjust,
                    "anchor_date": anchor_date,
                }
            )
            bars = tuple(
                SimpleNamespace(
                    time=datetime(2026, 6, day, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                    open=10.0,
                    high=10.2,
                    low=9.8,
                    close=10.1,
                    volume_lots=12000.0 if day == 12 else 2400.0,
                    amount=2400000.0,
                )
                for day in [7, 8, 9, 10, 11, 12]
            )
            return SimpleNamespace(
                exchange=code[:2],
                code=code[2:],
                full_code=code,
                period_raw=13,
                period_param_raw=1,
                period_name=period,
                start=start,
                request_count=count,
                adjust_mode_raw=0,
                adjust_mode=adjust or "none",
                anchor_date_raw=0,
                bars=bars,
            )

    client = FiveDayKlineClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_shortline_indicators_tdx", {"code": "000001.SZ", "stats_root": str(stats_root)})

    assert client.explicit_quote_calls == [{"securities": [("sz", "000001")], "batch_size": 80}]
    assert client.kline_calls == [
        {"code": "sz000001", "period": "day", "start": 0, "count": 8, "adjust": "none", "anchor_date": None}
    ]
    assert client.finance_calls == [{"code": ["sz000001"], "include_raw": False}]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "stats_date": "20260612",
            "open_price": 10.13,
            "pre_close": 10.28,
            "open_change_pct": -1.459144,
            "open_amount": 10000.0,
            "open_volume_hand": 9.871668,
            "open_volume_ratio": 0.987167,
            "open_turnover_z": 0.049358,
            "open_prev_amount_ratio": 0.5,
            "auction_prev_volume_ratio": 0.065811,
            "opening_rush": 0.12,
            "open_prev_seal_ratio": 2.0,
            "prev_amount": 2000000.0,
            "prev_seal_amount": 500000.0,
            "prev2_seal_amount": 400000.0,
            "prev_open_volume_hand": 150.0,
            "prev_open_amount": 800000.0,
            "float_shares": 19405601250.0,
            "float_market_value": 196772796675.0,
            "free_float_shares": 2000000.0,
            "free_float_market_value": 20280000.0,
            "seal_amount": 324160.0,
            "seal_to_amount_ratio": 0.319684,
            "seal_to_float_ratio": 1.598422,
            "seal_prev_ratio": 0.64832,
            "limit_stat_days": 7,
            "limit_up_count_in_stat_days": 5,
            "limit_board_text": "7天5板",
            "limit_up_streak_days": 3,
            "year_limit_up_days": 9,
        }
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x054c+0x052d+0x0010+tdxstat"
    assert adapter.last_meta["tdx_stats_source_path"] == str(stats_root)
    assert adapter.last_meta["tdx_stats_refreshed"] is False


def test_tdx_auction_indicators_previous_date_resource_uses_c0_and_includes_s(
    monkeypatch,
    tmp_path,
):
    stats_root = tmp_path / "tdxstats"
    stats_root.mkdir()
    stat_parts = [""] * 35
    stat_parts[0] = "0"
    stat_parts[1] = "000001"
    stat_parts[4] = "20260611"
    stat_parts[11] = "200.00"
    stat2_parts = [""] * 21
    stat2_parts[0] = "0"
    stat2_parts[1] = "000001"
    stat2_parts[2] = "20260611"
    stat2_parts[3] = "300.00"
    stat2_parts[4] = "30.00"
    stat2_parts[5] = "200.00"
    stat2_parts[6] = "50.00"
    stat2_parts[7] = "180.00"
    stat2_parts[8] = "40.00"
    stat2_parts[9] = "10"
    stat2_parts[10] = "150"
    stat2_parts[14] = "90.00"
    stat2_parts[15] = "80.00"
    (stats_root / "tdxstat.cfg").write_text("|".join(stat_parts), encoding="gbk")
    (stats_root / "tdxstat2.cfg").write_text("|".join(stat2_parts), encoding="gbk")

    class PreviousDateKlineClient(FakeTdxClient):
        def get_kline(
            self,
            code,
            *,
            period="day",
            start=0,
            count=800,
            adjust=None,
            anchor_date=None,
        ):
            bars = tuple(
                SimpleNamespace(
                    time=datetime(2026, 6, day, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                    volume_lots=12000.0 if day == 11 else 2400.0,
                    amount=2400000.0,
                )
                for day in [5, 8, 9, 10, 11, 12]
            )
            return SimpleNamespace(bars=bars)

    monkeypatch.setattr(
        "axdata_source_tdx.market_dates.resolve_market_date_context",
        lambda *args, **kwargs: SimpleNamespace(
            target_trade_date="20260612",
            previous_trade_date="20260611",
            server_datetime=None,
            phase="trading",
            ready=True,
            source="test_calendar",
        ),
    )
    adapter = TdxRequestAdapter(client=PreviousDateKlineClient())

    rows = adapter.request(
        "stock_shortline_indicators_tdx",
        {"code": "000001.SZ", "stats_root": str(stats_root)},
    )

    row = rows[0]
    assert row["stats_date"] == "20260611"
    assert row["prev_amount"] == 3000000.0
    assert row["prev_seal_amount"] == 300000.0
    assert row["prev2_seal_amount"] == 500000.0
    assert row["prev_open_volume_hand"] == 10.0
    assert row["prev_open_amount"] == 900000.0
    assert row["open_prev_amount_ratio"] == 0.333333
    assert row["open_volume_ratio"] == 0.548426
    assert adapter.last_meta["tdx_stats_alignment_counts"] == {
        "previous_trading_day": 1
    }
    assert adapter.last_meta["partial"] is False


def test_tdx_adapter_refreshes_stock_auction_indicator_stats_from_source(monkeypatch, tmp_path):
    stats_cache = tmp_path / "stats-cache"
    monkeypatch.setenv("AXDATA_TDX_STATS_ROOT", str(stats_cache))
    stat_parts = [""] * 35
    stat_parts[0] = "0"
    stat_parts[1] = "000001"
    stat_parts[4] = "20260612"
    stat_parts[11] = "200.00"
    stat_parts[26] = "9"
    stat_parts[31] = "7"
    stat_parts[32] = "5"
    stat_parts[33] = "3"
    stat2_parts = [""] * 21
    stat2_parts[0] = "0"
    stat2_parts[1] = "000001"
    stat2_parts[2] = "20260612"
    stat2_parts[3] = "250.00"
    stat2_parts[4] = "25.00"
    stat2_parts[5] = "200.00"
    stat2_parts[6] = "50.00"
    stat2_parts[9] = "10"
    stat2_parts[10] = "150"
    stat2_parts[14] = "1.00"
    stat2_parts[15] = "80.00"

    class FiveDayKlineClient(FakeTdxClient):
        def get_kline(self, code, *, period="day", start=0, count=800, adjust=None, anchor_date=None):
            self.kline_calls.append(
                {
                    "code": code,
                    "period": period,
                    "start": start,
                    "count": count,
                    "adjust": adjust,
                    "anchor_date": anchor_date,
                }
            )
            bars = tuple(
                SimpleNamespace(
                    time=datetime(2026, 6, day, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                    open=10.0,
                    high=10.2,
                    low=9.8,
                    close=10.1,
                    volume_lots=2400.0,
                    amount=2400000.0,
                )
                for day in [7, 8, 9, 10, 11, 12]
            )
            return SimpleNamespace(
                exchange=code[:2],
                code=code[2:],
                full_code=code,
                period_raw=13,
                period_param_raw=1,
                period_name=period,
                start=start,
                request_count=count,
                adjust_mode_raw=0,
                adjust_mode=adjust or "none",
                anchor_date_raw=0,
                bars=bars,
            )

    client = FiveDayKlineClient()
    client.resource_payloads["zhb.zip"] = _stats_zip_bytes("|".join(stat_parts), "|".join(stat2_parts))
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_shortline_indicators_tdx", {"code": "000001.SZ", "refresh_stats": True})

    assert client.download_file_calls == [{"path": "zhb.zip", "chunk_size": 30000, "max_bytes": None}]
    assert default_tdx_stats_resource_path().read_bytes() == client.resource_payloads["zhb.zip"]
    assert default_tdx_stats_metadata_path().exists()
    metadata = json.loads(default_tdx_stats_metadata_path().read_text(encoding="utf-8"))
    assert metadata["stats_date"] == "20260612"
    assert metadata["size_bytes"] == len(client.resource_payloads["zhb.zip"])
    assert metadata["stat_rows"] == 1000
    assert metadata["stat2_rows"] == 1000
    assert rows[0]["stats_date"] == "20260612"
    assert rows[0]["open_prev_amount_ratio"] == 0.5
    assert adapter.last_meta["tdx_stats_source_path"] == str(default_tdx_stats_resource_path())
    assert adapter.last_meta["tdx_stats_refreshed"] is True
    assert adapter.last_meta["tdx_stats_date"] == "20260612"


def test_tdx_stats_resource_request_downloads_without_cache_writes(tmp_path):
    payload = _stats_zip_bytes(
        _minimal_stat_line("20260612"),
        _minimal_stat2_line("20260612", prev_amount="200.00"),
    )
    client = FakeTdxClient()
    client.resource_payloads["zhb.zip"] = payload

    resource = request_tdx_stats_resource(client)

    assert client.download_file_calls == [{"path": "zhb.zip", "chunk_size": 30000, "max_bytes": None}]
    assert resource.stats_date == "20260612"
    assert resource.source_path == "tdx://zhb.zip"
    assert resource.metadata is not None
    assert resource.metadata["cache_path"] is None
    assert resource.metadata["size_bytes"] == len(payload)
    assert resource.metadata["stat_rows"] == 1000
    assert resource.metadata["stat2_rows"] == 1000
    assert not tmp_path.joinpath("zhb.zip").exists()


def test_tdx_adapter_reuses_same_day_stock_auction_indicator_stats_cache(monkeypatch, tmp_path):
    stats_cache = tmp_path / "stats-cache"
    monkeypatch.setenv("AXDATA_TDX_STATS_ROOT", str(stats_cache))
    stats_cache.mkdir()
    stat_parts = [""] * 35
    stat_parts[0] = "0"
    stat_parts[1] = "000001"
    stat_parts[4] = "20260612"
    stat_parts[11] = "200.00"
    stat2_parts = [""] * 21
    stat2_parts[0] = "0"
    stat2_parts[1] = "000001"
    stat2_parts[2] = "20260612"
    stat2_parts[3] = "250.00"
    stat2_parts[4] = "25.00"
    stat2_parts[5] = "200.00"
    stat2_parts[6] = "50.00"
    stat2_parts[9] = "10"
    stat2_parts[10] = "150"
    stat2_parts[14] = "1.00"
    stat2_parts[15] = "80.00"
    payload = _stats_zip_bytes("|".join(stat_parts), "|".join(stat2_parts))
    default_tdx_stats_resource_path().write_bytes(payload)
    default_tdx_stats_metadata_path().write_text(
        json.dumps(
            {
                "downloaded_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds"),
                "stats_date": "20260612",
                "size_bytes": len(payload),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    class FiveDayKlineClient(FakeTdxClient):
        def get_kline(self, code, *, period="day", start=0, count=800, adjust=None, anchor_date=None):
            bars = tuple(
                SimpleNamespace(
                    time=datetime(2026, 6, day, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                    open=10.0,
                    high=10.2,
                    low=9.8,
                    close=10.1,
                    volume_lots=2400.0,
                    amount=2400000.0,
                )
                for day in [7, 8, 9, 10, 11, 12]
            )
            return SimpleNamespace(
                exchange=code[:2],
                code=code[2:],
                full_code=code,
                period_name=period,
                bars=bars,
            )

    client = FiveDayKlineClient()
    client.resource_payloads["zhb.zip"] = payload
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_shortline_indicators_tdx", {"code": "000001.SZ"})

    assert client.download_file_calls == []
    assert rows[0]["stats_date"] == "20260612"
    assert adapter.last_meta["tdx_stats_refreshed"] is False
    assert adapter.last_meta["tdx_stats_date"] == "20260612"


def test_tdx_stats_cache_reuses_previous_trade_date_without_downloading(tmp_path):
    from axdata_source_tdx.stats_resource import ensure_tdx_stats_resource

    stats_cache = tmp_path / "stats-cache"
    stats_cache.mkdir()
    payload = _stats_zip_bytes(
        _minimal_stat_line("20260611"),
        _minimal_stat2_line("20260611", prev_amount="100.00"),
    )
    (stats_cache / "zhb.zip").write_bytes(payload)
    client = FakeTdxClient()

    resource, refreshed = ensure_tdx_stats_resource(
        client,
        cache_root=stats_cache,
        target_trade_date="20260612",
        previous_trade_date="20260611",
    )

    assert resource.stats_date == "20260611"
    assert refreshed is False
    assert client.download_file_calls == []


def test_tdx_stats_cache_replaces_incomplete_cache_only_after_valid_download(tmp_path):
    from axdata_source_tdx.stats_resource import ensure_tdx_stats_resource

    stats_cache = tmp_path / "stats-cache"
    stats_cache.mkdir()
    incomplete = _stats_zip_bytes(
        _minimal_stat_line("20260610"),
        _minimal_stat2_line("20260610", prev_amount="100.00"),
        minimum_rows=None,
    )
    valid = _stats_zip_bytes(
        _minimal_stat_line("20260612"),
        _minimal_stat2_line("20260612", prev_amount="200.00"),
    )
    (stats_cache / "zhb.zip").write_bytes(incomplete)
    client = FakeTdxClient()
    client.resource_payloads["zhb.zip"] = valid

    resource, refreshed = ensure_tdx_stats_resource(
        client,
        cache_root=stats_cache,
        target_trade_date="20260612",
        previous_trade_date="20260611",
    )

    assert resource.stats_date == "20260612"
    assert refreshed is True
    assert (stats_cache / "zhb.zip").read_bytes() == valid


def test_tdx_stats_cache_preserves_old_file_and_defers_after_bad_refresh(tmp_path):
    from axdata_source_tdx.stats_errors import (
        TdxStatsRefreshDeferredError,
        TdxStatsValidationError,
    )
    from axdata_source_tdx.stats_resource import ensure_tdx_stats_resource

    stats_cache = tmp_path / "stats-cache"
    stats_cache.mkdir()
    stale = _stats_zip_bytes(
        _minimal_stat_line("20260610"),
        _minimal_stat2_line("20260610", prev_amount="100.00"),
    )
    bad_download = _stats_zip_bytes(
        _minimal_stat_line("20260612"),
        _minimal_stat2_line("20260612", prev_amount="200.00"),
        minimum_rows=None,
    )
    (stats_cache / "zhb.zip").write_bytes(stale)
    client = FakeTdxClient()
    client.resource_payloads["zhb.zip"] = bad_download

    with pytest.raises(TdxStatsValidationError, match="expected at least 1000 rows"):
        ensure_tdx_stats_resource(
            client,
            cache_root=stats_cache,
            target_trade_date="20260612",
            previous_trade_date="20260611",
        )

    assert (stats_cache / "zhb.zip").read_bytes() == stale
    assert len(client.download_file_calls) == 1
    metadata = json.loads((stats_cache / "zhb.meta.json").read_text(encoding="utf-8"))
    assert metadata["last_refresh_error_code"] == "TDX_STATS_RESOURCE_INVALID"

    with pytest.raises(TdxStatsRefreshDeferredError) as exc_info:
        ensure_tdx_stats_resource(
            client,
            cache_root=stats_cache,
            target_trade_date="20260612",
            previous_trade_date="20260611",
        )

    assert exc_info.value.code == "TDX_STATS_REFRESH_DEFERRED"
    assert exc_info.value.details["retry_after_seconds"] > 0
    assert len(client.download_file_calls) == 1


@pytest.mark.parametrize(
    ("failure_kind", "error_match"),
    [
        ("bad_zip", "not a valid ZIP"),
        ("old_date", "has not published a resource usable"),
    ],
)
def test_tdx_stats_refresh_preserves_usable_cache_on_bad_zip_or_old_date(
    tmp_path,
    failure_kind,
    error_match,
):
    from axdata_source_tdx.stats_errors import TdxStatsError
    from axdata_source_tdx.stats_resource import ensure_tdx_stats_resource

    stats_cache = tmp_path / "stats-cache"
    stats_cache.mkdir()
    usable = _stats_zip_bytes(
        _minimal_stat_line("20260611"),
        _minimal_stat2_line("20260611", prev_amount="100.00"),
    )
    (stats_cache / "zhb.zip").write_bytes(usable)
    client = FakeTdxClient()
    download_payload = b"not-a-zip"
    if failure_kind == "old_date":
        download_payload = _stats_zip_bytes(
            _minimal_stat_line("20260610"),
            _minimal_stat2_line("20260610", prev_amount="100.00"),
        )
    client.resource_payloads["zhb.zip"] = download_payload

    with pytest.raises(TdxStatsError, match=error_match):
        ensure_tdx_stats_resource(
            client,
            cache_root=stats_cache,
            refresh=True,
            target_trade_date="20260612",
            previous_trade_date="20260611",
        )

    assert (stats_cache / "zhb.zip").read_bytes() == usable


def test_tdx_stats_resource_reuses_parsed_resource_in_process(tmp_path):
    from axdata_source_tdx.stats_resource import load_tdx_stats_resource

    stats_zip = tmp_path / "zhb.zip"
    stats_zip.write_bytes(
        _stats_zip_bytes(
            _minimal_stat_line("20260612"),
            _minimal_stat2_line("20260612", prev_amount="200.00"),
        )
    )

    first = load_tdx_stats_resource(stats_zip)
    second = load_tdx_stats_resource(stats_zip)

    assert second is first


def _expected_default_tdx_stats_cache_root(local_app_data: Path) -> Path:
    """Expected default TDX stats cache root for the current OS.

    Mirrors ``axdata_source_tdx.stats_cache.user_tdx_stats_cache_root`` so the
    assertion matches the running platform instead of assuming the Windows
    ``LOCALAPPDATA`` layout.
    """

    if os.name == "nt":
        return (local_app_data / "AxData" / "cache" / "tdx" / "stats").resolve()
    if sys.platform == "darwin":
        return (Path.home() / "Library" / "Caches" / "AxData" / "tdx" / "stats").resolve()
    base = Path(os.getenv("XDG_CACHE_HOME") or (Path.home() / ".cache"))
    return (base / "axdata" / "tdx" / "stats").resolve()


def test_tdx_direct_adapter_default_stats_cache_is_independent_of_cwd(monkeypatch, tmp_path):
    from axdata_source_tdx.stats_cache import default_tdx_stats_cache_root

    local_app_data = tmp_path / "local-app-data"
    first_cwd = tmp_path / "first"
    second_cwd = tmp_path / "second"
    first_cwd.mkdir()
    second_cwd.mkdir()
    monkeypatch.delenv("AXDATA_TDX_STATS_ROOT", raising=False)
    monkeypatch.setenv("LOCALAPPDATA", str(local_app_data))

    monkeypatch.chdir(first_cwd)
    first = default_tdx_stats_cache_root()
    monkeypatch.chdir(second_cwd)
    second = default_tdx_stats_cache_root()

    assert first == second
    assert first == _expected_default_tdx_stats_cache_root(local_app_data)


def test_tdx_adapter_refreshes_stats_cache_older_than_previous_trade_date(monkeypatch, tmp_path):
    stats_cache = tmp_path / "stats-cache"
    monkeypatch.setenv("AXDATA_TDX_STATS_ROOT", str(stats_cache))
    stats_cache.mkdir()
    old_payload = _stats_zip_bytes(_minimal_stat_line("20260610"), _minimal_stat2_line("20260610", prev_amount="100.00"))
    new_payload = _stats_zip_bytes(_minimal_stat_line("20260612"), _minimal_stat2_line("20260612", prev_amount="200.00"))
    default_tdx_stats_resource_path().write_bytes(old_payload)
    default_tdx_stats_metadata_path().write_text(
        json.dumps(
            {
                "downloaded_at": (datetime.now(ZoneInfo("Asia/Shanghai")) - timedelta(days=1)).isoformat(timespec="seconds"),
                "stats_date": "20260610",
                "size_bytes": len(old_payload),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    class FiveDayKlineClient(FakeTdxClient):
        def get_kline(self, code, *, period="day", start=0, count=800, adjust=None, anchor_date=None):
            bars = tuple(
                SimpleNamespace(
                    time=datetime(2026, 6, day, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                    open=10.0,
                    high=10.2,
                    low=9.8,
                    close=10.1,
                    volume_lots=2400.0,
                    amount=2400000.0,
                )
                for day in [8, 9, 10, 11, 12]
            )
            return SimpleNamespace(
                exchange=code[:2],
                code=code[2:],
                full_code=code,
                period_name=period,
                bars=bars,
            )

    client = FiveDayKlineClient()
    client.resource_payloads["zhb.zip"] = new_payload
    adapter = TdxRequestAdapter(client=client)
    monkeypatch.setattr(
        "axdata_source_tdx.market_dates.resolve_market_date_context",
        lambda *args, **kwargs: SimpleNamespace(
            target_trade_date="20260612",
            previous_trade_date="20260611",
            server_datetime=None,
            phase="trading",
            ready=True,
            source="test_calendar",
        ),
    )

    rows = adapter.request("stock_shortline_indicators_tdx", {"code": "000001.SZ"})

    assert client.download_file_calls == [{"path": "zhb.zip", "chunk_size": 30000, "max_bytes": None}]
    assert default_tdx_stats_resource_path().read_bytes() == new_payload
    assert rows[0]["stats_date"] == "20260612"
    assert rows[0]["open_prev_amount_ratio"] == 0.5
    assert adapter.last_meta["tdx_stats_refreshed"] is True
    assert adapter.last_meta["tdx_stats_date"] == "20260612"


def test_tdx_adapter_uses_custom_stock_auction_indicator_stats_cache_root(tmp_path):
    stats_cache = tmp_path / "custom-cache"
    payload = _stats_zip_bytes(_minimal_stat_line("20260612"), _minimal_stat2_line("20260612", prev_amount="200.00"))

    class FiveDayKlineClient(FakeTdxClient):
        def get_kline(self, code, *, period="day", start=0, count=800, adjust=None, anchor_date=None):
            bars = tuple(
                SimpleNamespace(
                    time=datetime(2026, 6, day, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                    open=10.0,
                    high=10.2,
                    low=9.8,
                    close=10.1,
                    volume_lots=2400.0,
                    amount=2400000.0,
                )
                for day in [8, 9, 10, 11, 12]
            )
            return SimpleNamespace(
                exchange=code[:2],
                code=code[2:],
                full_code=code,
                period_name=period,
                bars=bars,
            )

    client = FiveDayKlineClient()
    client.resource_payloads["zhb.zip"] = payload
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_shortline_indicators_tdx",
        {"code": "000001.SZ", "stats_cache_root": str(stats_cache)},
    )

    assert (stats_cache / "zhb.zip").read_bytes() == payload
    assert (stats_cache / "zhb.meta.json").exists()
    assert rows[0]["stats_date"] == "20260612"
    assert adapter.last_meta["tdx_stats_source_path"] == str(stats_cache / "zhb.zip")
    assert adapter.last_meta["tdx_stats_refreshed"] is True


def test_tdx_adapter_uses_option_stats_cache_root_when_param_is_missing(tmp_path):
    stats_cache = tmp_path / "option-cache"
    payload = _stats_zip_bytes(_minimal_stat_line("20260612"), _minimal_stat2_line("20260612", prev_amount="200.00"))

    class FiveDayKlineClient(FakeTdxClient):
        def get_kline(self, code, *, period="day", start=0, count=800, adjust=None, anchor_date=None):
            bars = tuple(
                SimpleNamespace(
                    time=datetime(2026, 6, day, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                    open=10.0,
                    high=10.2,
                    low=9.8,
                    close=10.1,
                    volume_lots=2400.0,
                    amount=2400000.0,
                )
                for day in [8, 9, 10, 11, 12]
            )
            return SimpleNamespace(
                exchange=code[:2],
                code=code[2:],
                full_code=code,
                period_name=period,
                bars=bars,
            )

    client = FiveDayKlineClient()
    client.resource_payloads["zhb.zip"] = payload
    adapter = TdxRequestAdapter(client=client, options={"stats_cache_root": str(stats_cache)})

    rows = adapter.request("stock_shortline_indicators_tdx", {"code": "000001.SZ"})

    assert (stats_cache / "zhb.zip").read_bytes() == payload
    assert (stats_cache / "zhb.meta.json").exists()
    assert rows[0]["stats_date"] == "20260612"
    assert adapter.last_meta["tdx_stats_source_path"] == str(stats_cache / "zhb.zip")
    assert adapter.last_meta["tdx_stats_refreshed"] is True


def test_tdx_adapter_explicit_stats_cache_root_overrides_option_default(tmp_path):
    option_cache = tmp_path / "option-cache"
    explicit_cache = tmp_path / "explicit-cache"
    payload = _stats_zip_bytes(_minimal_stat_line("20260612"), _minimal_stat2_line("20260612", prev_amount="200.00"))

    class FiveDayKlineClient(FakeTdxClient):
        def get_kline(self, code, *, period="day", start=0, count=800, adjust=None, anchor_date=None):
            return SimpleNamespace(
                exchange=code[:2],
                code=code[2:],
                full_code=code,
                period_name=period,
                bars=(
                    SimpleNamespace(
                        time=datetime(2026, 6, 12, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                        open=10.0,
                        high=10.2,
                        low=9.8,
                        close=10.1,
                        volume_lots=2400.0,
                        amount=2400000.0,
                    ),
                ),
            )

    client = FiveDayKlineClient()
    client.resource_payloads["zhb.zip"] = payload
    adapter = TdxRequestAdapter(client=client, options={"stats_cache_root": str(option_cache)})

    adapter.request(
        "stock_shortline_indicators_tdx",
        {"code": "000001.SZ", "stats_cache_root": str(explicit_cache)},
    )

    assert not option_cache.exists()
    assert (explicit_cache / "zhb.zip").read_bytes() == payload
    assert adapter.last_meta["tdx_stats_source_path"] == str(explicit_cache / "zhb.zip")


def test_tdx_adapter_requests_stock_daily_share_from_stats_and_finance(tmp_path):
    stats_root = tmp_path / "stats"
    stats_root.mkdir()
    (stats_root / "tdxstat.cfg").write_text(_minimal_stat_line("20260612"), encoding="gbk")
    (stats_root / "tdxstat2.cfg").write_text(_minimal_stat2_line("20260612", prev_amount="200.00"), encoding="gbk")

    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_daily_share_tdx", {"code": "000001.SZ", "stats_root": str(stats_root)})

    assert client.finance_calls == [{"code": ["sz000001"], "include_raw": False}]
    assert rows == [
        {
            "trade_date": "20260612",
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "total_share": 19405918750.0,
            "float_share": 19405601250.0,
            "free_float_share_z": 2000000.0,
            "finance_updated_date": "20260425",
            "share_source": "finance_snapshot+tdxstat",
        }
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x0010+tdxstat"
    assert adapter.last_meta["tdx_stats_date"] == "20260612"
    assert adapter.last_meta["tdx_stats_refreshed"] is False
    assert adapter.last_meta["tdx_finance_batch_count"] == 1
    assert adapter.last_meta["tdx_finance_batch_size"] == 80


def test_tdx_adapter_requests_stock_daily_share_all_expands_scope_and_batches_finance(tmp_path):
    stats_root = tmp_path / "stats"
    stats_root.mkdir()
    (stats_root / "tdxstat.cfg").write_text(_minimal_stat_line("20260612"), encoding="gbk")
    (stats_root / "tdxstat2.cfg").write_text(_minimal_stat2_line("20260612", prev_amount="200.00"), encoding="gbk")

    class ManyStockClient(FakeTdxClient):
        def get_codes(self, market, *, start=0, limit=None):
            self.calls.append({"market": market, "start": start, "limit": limit})
            if market != "sz" or start > 0:
                return []
            return [
                SimpleNamespace(
                    full_code=f"sz30{index:04d}",
                    code=f"30{index:04d}",
                    exchange="sz",
                    name=f"样本{index}",
                    category="a_share",
                    category_reason="test",
                    board="szse_chinext",
                    board_reason="test",
                    decimal=2,
                    multiple=100,
                    previous_close_price=10.0,
                    volume_ratio_base=1.0,
                )
                for index in range(85)
            ]

    client = ManyStockClient()
    progress_events = []
    adapter = TdxRequestAdapter(
        client=client,
        progress_callback=lambda percent, message, **details: progress_events.append(
            {"percent": percent, "message": message, **details}
        ),
    )

    rows = adapter.request("stock_daily_share_tdx", {"code": "all", "scope": "chinext", "stats_root": str(stats_root)})

    assert len(rows) == 85
    assert [len(call["code"]) for call in client.finance_calls] == [80, 5]
    assert client.finance_calls[0]["code"][0] == "sz300000"
    assert client.finance_calls[1]["code"][-1] == "sz300084"
    assert adapter.last_meta["tdx_code_expansion_source"] == "stock_codes_tdx"
    assert adapter.last_meta["tdx_stock_scope"] == "chinext"
    assert adapter.last_meta["tdx_requested_code_count"] == 85
    assert adapter.last_meta["tdx_returned_count"] == 85
    assert adapter.last_meta["tdx_finance_batch_count"] == 2
    assert adapter.last_meta["tdx_finance_batch_size"] == 80
    finance_progress = [event for event in progress_events if "请求财务快照" in event["message"]]
    assert [event["progress_current"] for event in finance_progress] == [1, 2]
    assert finance_progress[-1]["progress_total"] == 2
    assert finance_progress[-1]["progress_unit"] == "批"


def test_tdx_adapter_requests_stock_daily_share_finance_batches_in_parallel(tmp_path):
    stats_root = tmp_path / "stats"
    stats_root.mkdir()
    (stats_root / "tdxstat.cfg").write_text(_minimal_stat_line("20260612"), encoding="gbk")
    (stats_root / "tdxstat2.cfg").write_text(_minimal_stat2_line("20260612", prev_amount="200.00"), encoding="gbk")

    class ParallelFinanceClient(FakeTdxClient):
        pool_size = 3

        def __init__(self) -> None:
            super().__init__()
            self._active_finance_calls = 0
            self.max_active_finance_calls = 0
            self._finance_lock = threading.Lock()

        def get_codes(self, market, *, start=0, limit=None):
            self.calls.append({"market": market, "start": start, "limit": limit})
            if market != "sz" or start > 0:
                return []
            return [
                SimpleNamespace(
                    full_code=f"sz30{index:04d}",
                    code=f"30{index:04d}",
                    exchange="sz",
                    name=f"样本{index}",
                    category="a_share",
                    category_reason="test",
                    board="szse_chinext",
                    board_reason="test",
                    decimal=2,
                    multiple=100,
                    previous_close_price=10.0,
                    volume_ratio_base=1.0,
                )
                for index in range(170)
            ]

        def get_finance_info(self, code, *, include_raw=False):
            with self._finance_lock:
                self._active_finance_calls += 1
                self.max_active_finance_calls = max(self.max_active_finance_calls, self._active_finance_calls)
            try:
                time.sleep(0.02)
                return super().get_finance_info(code, include_raw=include_raw)
            finally:
                with self._finance_lock:
                    self._active_finance_calls -= 1

    client = ParallelFinanceClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_daily_share_tdx", {"code": "all", "scope": "chinext", "stats_root": str(stats_root)})

    assert len(rows) == 170
    assert sorted(len(call["code"]) for call in client.finance_calls) == [10, 80, 80]
    assert client.max_active_finance_calls > 1
    assert adapter.last_meta["tdx_finance_batch_count"] == 3
    assert adapter.last_meta["tdx_finance_batch_size"] == 80
    assert adapter.last_meta["tdx_finance_concurrency"] == 3


def test_tdx_adapter_requests_stock_daily_share_uses_global_stats_date_when_symbol_row_missing(tmp_path):
    stats_root = tmp_path / "stats"
    stats_root.mkdir()
    (stats_root / "tdxstat.cfg").write_text(_minimal_stat_line("20260612"), encoding="gbk")
    (stats_root / "tdxstat2.cfg").write_text(_minimal_stat2_line("20260612", prev_amount="200.00"), encoding="gbk")

    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_daily_share_tdx", {"code": "600000.SH", "stats_root": str(stats_root)})

    assert rows[0]["trade_date"] == "20260612"
    assert rows[0]["tdx_code"] == "sh600000"
    assert rows[0]["free_float_share_z"] is None
    assert rows[0]["share_source"] == "finance_snapshot"


def test_tdx_adapter_requests_stock_daily_price_limit_from_daily_kline_and_rules(tmp_path):
    stats_root = tmp_path / "stats"
    stats_root.mkdir()
    (stats_root / "tdxstat.cfg").write_text(_minimal_stat_line("20260612"), encoding="gbk")
    (stats_root / "tdxstat2.cfg").write_text(_minimal_stat2_line("20260612", prev_amount="200.00"), encoding="gbk")

    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_daily_price_limit_tdx",
        {"code": "000001.SZ", "trade_date": "20260617"},
    )

    assert client.explicit_quote_calls == []
    assert client.price_limit_calls == []
    assert client.kline_calls == [
        {"code": "sz000001", "period": "day", "start": 0, "count": 800, "adjust": "none", "anchor_date": None, "kind": "stock"}
    ]
    assert rows == [
        {
            "trade_date": "20260617",
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "name": "平安银行",
            "name_flag": None,
            "pre_close_trade_date": "20240603",
            "pre_close": 8.0,
            "pre_close_source": "tdx_daily_kline",
            "limit_up_price": 8.8,
            "limit_down_price": 7.2,
            "limit_ratio_pct": 10.0,
            "limit_rule": "main_10pct",
            "limit_status": "normal",
        }
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x052d"
    assert adapter.last_meta["tdx_price_limit_mode"] == "historical_kline"
    assert adapter.last_meta["tdx_target_trade_date"] == "20260617"
    assert adapter.last_meta["snapshot_date"] == "20260617"
    assert adapter.last_meta["tdx_stock_scope"] is None
    assert adapter.last_meta["tdx_code_expansion_source"] is None
    assert adapter.last_meta["tdx_kline_page_count"] == 1


def test_tdx_adapter_requests_stock_daily_price_limit_all_expands_scope_and_parallelizes_kline():
    class ParallelPriceLimitClient(FakeTdxClient):
        pool_size = 3

        def __init__(self) -> None:
            super().__init__()
            self._active_kline_calls = 0
            self.max_active_kline_calls = 0
            self._kline_lock = threading.Lock()

        def get_codes(self, market, *, start=0, limit=None):
            self.calls.append({"market": market, "start": start, "limit": limit})
            if market != "sz" or start > 0:
                return []
            return [
                SimpleNamespace(
                    full_code=f"sz30{index:04d}",
                    code=f"30{index:04d}",
                    exchange="sz",
                    name=f"样本{index}",
                    category="a_share",
                    category_reason="test",
                    board="szse_chinext",
                    board_reason="test",
                    decimal=2,
                    multiple=100,
                    previous_close_price=10.0,
                    volume_ratio_base=1.0,
                )
                for index in range(5)
            ]

        def get_kline(self, code, *, period="day", start=0, count=800, adjust=None, anchor_date=None):
            with self._kline_lock:
                self._active_kline_calls += 1
                self.max_active_kline_calls = max(self.max_active_kline_calls, self._active_kline_calls)
            try:
                time.sleep(0.02)
                self.kline_calls.append(
                    {
                        "code": code,
                        "period": period,
                        "start": start,
                        "count": count,
                        "adjust": adjust,
                        "anchor_date": anchor_date,
                    }
                )
                return SimpleNamespace(
                    exchange=code[:2],
                    code=code[2:],
                    full_code=code,
                    period_raw=13,
                    period_param_raw=1,
                    period_name=period,
                    start=start,
                    request_count=count,
                    adjust_mode_raw=0,
                    adjust_mode=adjust or "none",
                    anchor_date_raw=0,
                    bars=(
                        SimpleNamespace(
                            time=datetime(2026, 6, 11, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                            close=20.0,
                        ),
                    ),
                )
            finally:
                with self._kline_lock:
                    self._active_kline_calls -= 1

    client = ParallelPriceLimitClient()
    progress_events = []
    adapter = TdxRequestAdapter(
        client=client,
        progress_callback=lambda percent, message, **details: progress_events.append(
            {"percent": percent, "message": message, **details}
        ),
    )

    rows = adapter.request("stock_daily_price_limit_tdx", {"scope": "chinext", "trade_date": "20260612"})

    assert len(rows) == 5
    assert [row["tdx_code"] for row in rows] == [f"sz30{index:04d}" for index in range(5)]
    assert len(client.kline_calls) == 5
    assert client.max_active_kline_calls > 1
    assert adapter.last_meta["tdx_protocol"] == "0x052d"
    assert adapter.last_meta["tdx_price_limit_mode"] == "historical_kline"
    assert adapter.last_meta["tdx_target_trade_date"] == "20260612"
    assert adapter.last_meta["snapshot_date"] == "20260612"
    assert adapter.last_meta["tdx_stock_scope"] == "chinext"
    assert adapter.last_meta["tdx_code_expansion_source"] == "stock_codes_tdx"
    assert adapter.last_meta["tdx_requested_code_count"] == 5
    assert adapter.last_meta["tdx_kline_page_count"] == 5
    assert adapter.last_meta["tdx_kline_concurrency"] == 3
    kline_progress = [event for event in progress_events if "计算涨跌停价格" in event["message"]]
    assert kline_progress
    assert kline_progress[0]["progress_total"] == 5
    assert kline_progress[-1]["progress_current"] == 5
    assert kline_progress[-1]["progress_total"] == 5
    assert kline_progress[-1]["progress_unit"] == "只"
    assert kline_progress[-1]["percent"] == 68


def test_tdx_adapter_requests_stock_daily_price_limit_default_uses_snapshot(monkeypatch):
    calendar_dates = tdx_request._PriceLimitCalendarDates(
        target_trade_date="20260622",
        pre_close_trade_date="20260618",
        source="exchange_calendar",
        snapshot_base_field="last_price",
    )
    monkeypatch.setattr(
        tdx_request,
        "_latest_daily_price_limit_calendar_dates",
        lambda: calendar_dates,
    )
    provider_request_methods = _optional_tdx_provider_module("request_methods")
    if provider_request_methods is not None:
        monkeypatch.setattr(
            provider_request_methods,
            "_latest_daily_price_limit_calendar_dates",
            lambda: calendar_dates,
        )

    class SnapshotPriceLimitClient(FakeTdxClient):
        pool_size = 2

        def __init__(self) -> None:
            super().__init__()
            self.code_scan_calls = 0

        def get_codes(self, market, *, start=0, limit=None):
            self.code_scan_calls += 1
            if market != "sz" or start > 0:
                return []
            return [
                SimpleNamespace(
                    full_code=f"sz00000{index}",
                    code=f"00000{index}",
                    exchange="sz",
                    name=f"样本{index}",
                    category="a_share",
                    category_reason="test",
                    board="szse_main_board",
                    board_reason="test",
                    decimal=2,
                    multiple=100,
                    previous_close_price=10.0,
                    volume_ratio_base=1.0,
                )
                for index in range(1, 4)
            ]

    client = SnapshotPriceLimitClient()
    progress_events = []
    adapter = TdxRequestAdapter(
        client=client,
        progress_callback=lambda percent, message, **details: progress_events.append(
            {"percent": percent, "message": message, **details}
        ),
    )

    rows = adapter.request("stock_daily_price_limit_tdx", {"scope": "main"})

    assert len(rows) == 3
    assert client.kline_calls == []
    assert client.explicit_quote_calls == [
        {
            "securities": [("sz", "000001"), ("sz", "000002"), ("sz", "000003")],
            "batch_size": 80,
        }
    ]
    assert rows[0]["trade_date"] == "20260622"
    assert rows[0]["pre_close_trade_date"] == "20260618"
    assert rows[0]["pre_close"] == 10.14
    assert rows[0]["name"] == "样本1"
    assert rows[0]["pre_close_source"] == "tdx_realtime_snapshot"
    assert rows[0]["limit_up_price"] == 11.15
    assert rows[0]["limit_down_price"] == 9.13
    assert client.code_scan_calls == 2
    assert adapter.last_meta["tdx_protocol"] == "0x054c+exchange_calendar"
    assert adapter.last_meta["tdx_price_limit_mode"] == "latest_snapshot"
    assert adapter.last_meta["tdx_target_trade_date"] == "20260622"
    assert adapter.last_meta["tdx_pre_close_trade_date"] == "20260618"
    assert adapter.last_meta["snapshot_date"] == "20260622"
    assert adapter.last_meta["tdx_trade_calendar_source"] == "exchange_calendar"
    assert adapter.last_meta["tdx_snapshot_base_field"] == "last_price"
    assert adapter.last_meta["tdx_stock_scope"] == "main"
    assert adapter.last_meta["tdx_kline_page_count"] == 0
    assert adapter.last_meta["tdx_quote_count"] == 3
    assert adapter.last_meta["tdx_quote_batch_count"] == 1
    snapshot_progress = [event for event in progress_events if "请求快照昨收价" in event["message"]]
    assert snapshot_progress
    assert snapshot_progress[-1]["progress_current"] == 1
    assert snapshot_progress[-1]["progress_total"] == 1
    assert snapshot_progress[-1]["progress_unit"] == "批"


def test_tdx_adapter_requests_stock_daily_price_limit_pages_daily_kline_for_old_trade_date():
    class PagedKlineClient(FakeTdxClient):
        def get_kline(self, code, *, period="day", start=0, count=800, adjust=None, anchor_date=None):
            self.kline_calls.append(
                {
                    "code": code,
                    "period": period,
                    "start": start,
                    "count": count,
                    "adjust": adjust,
                    "anchor_date": anchor_date,
                }
            )
            if start == 0:
                page_bars = tuple(
                    SimpleNamespace(
                        time=datetime(2026, 6, 17, 15, 0, tzinfo=timezone(timedelta(hours=8))) - timedelta(days=index),
                        close=10.0,
                    )
                    for index in range(count)
                )
            elif start == 800:
                page_bars = (
                    SimpleNamespace(time=datetime(2020, 1, 2, 15, 0, tzinfo=timezone(timedelta(hours=8))), close=8.0),
                    SimpleNamespace(time=datetime(2019, 12, 31, 15, 0, tzinfo=timezone(timedelta(hours=8))), close=7.0),
                )
            else:
                page_bars = ()
            return SimpleNamespace(
                exchange=code[:2],
                code=code[2:],
                full_code=code,
                period_raw=13,
                period_param_raw=1,
                period_name=period,
                start=start,
                request_count=count,
                adjust_mode_raw=0,
                adjust_mode=adjust or "none",
                anchor_date_raw=0,
                bars=page_bars,
            )

    client = PagedKlineClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_daily_price_limit_tdx", {"code": "000001.SZ", "trade_date": "20200102"})

    assert client.download_file_calls == []
    assert client.kline_calls == [
        {"code": "sz000001", "period": "day", "start": 0, "count": 800, "adjust": "none", "anchor_date": None},
        {"code": "sz000001", "period": "day", "start": 800, "count": 800, "adjust": "none", "anchor_date": None},
    ]
    assert rows[0]["trade_date"] == "20200102"
    assert rows[0]["pre_close_trade_date"] == "20191231"
    assert rows[0]["pre_close"] == 7.0
    assert rows[0]["limit_up_price"] == 7.7
    assert rows[0]["limit_down_price"] == 6.3
    assert adapter.last_meta["tdx_protocol"] == "0x052d"
    assert adapter.last_meta["tdx_price_limit_mode"] == "historical_kline"
    assert adapter.last_meta["tdx_kline_page_count"] == 2


def test_tdx_adapter_requests_stock_daily_price_limit_marks_no_price_limit_for_new_stock(tmp_path):
    stats_root = tmp_path / "stats"
    stats_root.mkdir()
    (stats_root / "tdxstat.cfg").write_text(_minimal_stat_line("20260612"), encoding="gbk")
    (stats_root / "tdxstat2.cfg").write_text(_minimal_stat2_line("20260612", prev_amount="200.00"), encoding="gbk")

    class NewStockClient(FakeTdxClient):
        def get_codes(self, market, *, start=0, limit=None):
            if market == "sz":
                return [
                    SimpleNamespace(
                        full_code="sz301001",
                        code="301001",
                        exchange="sz",
                        name="C测试",
                        category="a_share",
                        board="szse_chinext",
                    )
                ]
            return []

        def get_kline(self, code, *, period="day", start=0, count=800, adjust=None, anchor_date=None):
            self.kline_calls.append(
                {
                    "code": code,
                    "period": period,
                    "start": start,
                    "count": count,
                    "adjust": adjust,
                    "anchor_date": anchor_date,
                }
            )
            return SimpleNamespace(
                exchange=code[:2],
                code=code[2:],
                full_code=code,
                period_raw=13,
                period_param_raw=1,
                period_name=period,
                start=start,
                request_count=count,
                adjust_mode_raw=0,
                adjust_mode=adjust or "none",
                anchor_date_raw=0,
                bars=(
                    SimpleNamespace(
                        time=datetime(2026, 6, 11, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                        close=20.0,
                    ),
                ),
            )

    client = NewStockClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_daily_price_limit_tdx", {"code": "301001.SZ", "trade_date": "20260612"})

    assert rows[0]["name"] == "C测试"
    assert rows[0]["name_flag"] == "C"
    assert rows[0]["pre_close_trade_date"] == "20260611"
    assert rows[0]["pre_close"] == 20.0
    assert rows[0]["limit_up_price"] is None
    assert rows[0]["limit_down_price"] is None
    assert rows[0]["limit_ratio_pct"] is None
    assert rows[0]["limit_rule"] == "ipo_first_5_days"
    assert rows[0]["limit_status"] == "no_price_limit"


def _stats_zip_bytes(
    stat_text: str,
    stat2_text: str,
    *,
    minimum_rows: int | None = 1000,
) -> bytes:
    stat_lines = stat_text.splitlines()
    stat2_lines = stat2_text.splitlines()
    if minimum_rows is not None:
        stat_date = next(
            (parts[4] for line in stat_lines if len(parts := line.split("|")) > 4 and parts[4]),
            "20260612",
        )
        stat2_date = next(
            (parts[2] for line in stat2_lines if len(parts := line.split("|")) > 2 and parts[2]),
            stat_date,
        )
        existing_keys = {
            (parts[0], parts[1])
            for line in (*stat_lines, *stat2_lines)
            if len(parts := line.split("|")) > 1
        }
        candidate = 100000
        while len(stat_lines) < minimum_rows or len(stat2_lines) < minimum_rows:
            code = f"{candidate:06d}"
            candidate += 1
            if ("0", code) in existing_keys:
                continue
            existing_keys.add(("0", code))
            stat_parts = [""] * 35
            stat_parts[0] = "0"
            stat_parts[1] = code
            stat_parts[4] = stat_date
            stat2_parts = [""] * 21
            stat2_parts[0] = "0"
            stat2_parts[1] = code
            stat2_parts[2] = stat2_date
            if len(stat_lines) < minimum_rows:
                stat_lines.append("|".join(stat_parts))
            if len(stat2_lines) < minimum_rows:
                stat2_lines.append("|".join(stat2_parts))

    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr("tdxstat.cfg", "\n".join(stat_lines).encode("gbk"))
        archive.writestr("tdxstat2.cfg", "\n".join(stat2_lines).encode("gbk"))
    return buffer.getvalue()


def _minimal_stat_line(stats_date: str) -> str:
    parts = [""] * 35
    parts[0] = "0"
    parts[1] = "000001"
    parts[4] = stats_date
    parts[11] = "200.00"
    parts[26] = "9"
    parts[31] = "7"
    parts[32] = "5"
    parts[33] = "3"
    return "|".join(parts)


def _minimal_stat_line_for_code(stats_date: str, code: str) -> str:
    parts = _minimal_stat_line(stats_date).split("|")
    parts[1] = code
    return "|".join(parts)


def _minimal_stat2_line(stats_date: str, *, prev_amount: str) -> str:
    parts = [""] * 21
    parts[0] = "0"
    parts[1] = "000001"
    parts[2] = stats_date
    parts[3] = "250.00"
    parts[4] = "25.00"
    parts[5] = prev_amount
    parts[6] = "50.00"
    parts[7] = "180.00"
    parts[8] = "40.00"
    parts[9] = "10"
    parts[10] = "150"
    parts[14] = "1.00"
    parts[15] = "80.00"
    return "|".join(parts)


def _minimal_stat2_line_for_code(stats_date: str, code: str) -> str:
    parts = _minimal_stat2_line(stats_date, prev_amount="200.00").split("|")
    parts[1] = code
    return "|".join(parts)


def test_tdx_adapter_rejects_invalid_stock_trades_history_trade_date():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    with pytest.raises(SourceRequestValidationError, match="trade_date must be a valid date"):
        adapter.request(
            "stock_trades_history_tdx",
            {"code": "000001.SZ", "trade_date": "20260231"},
        )


def test_tdx_adapter_requests_stock_finance_summary_and_normalizes_rows():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_finance_summary_tdx", {"code": "000001.SZ"})

    assert client.finance_calls == [{"code": ["sz000001"], "include_raw": False}]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "updated_date": "20260425",
            "ipo_date": "19910403",
            "total_share": 19405918750.0,
            "float_share": 19405601250.0,
            "eps": 0.67,
            "bps": 23.91,
            "total_assets": 35277000000.0,
            "net_assets": 7000000.0,
            "revenue": 35277000000.0,
            "net_profit": 14523000000.0,
            "operating_cashflow": 12000000.0,
            "shareholder_count": 457610,
        }
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x0010"
    assert adapter.last_meta["tdx_requested_code_count"] == 1
    assert adapter.last_meta["tdx_finance_record_count"] == 1
    assert adapter.last_meta["tdx_finance_view"] == "stock_finance_summary_tdx"


@pytest.mark.parametrize(
    ("interface_name", "expected_fields"),
    [
        ("stock_share_capital_tdx", {"total_share", "float_share", "state_share", "shareholder_count"}),
        ("stock_balance_summary_tdx", {"total_assets", "current_assets", "net_assets", "inventory"}),
        ("stock_profit_cashflow_summary_tdx", {"revenue", "operating_profit", "net_profit", "eps"}),
        ("stock_finance_profile_tdx", {"updated_date", "ipo_date", "province_raw", "industry_raw"}),
    ],
)
def test_tdx_adapter_returns_finance_views(interface_name, expected_fields):
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    rows = adapter.request(interface_name, {"code": "000001.SZ"})

    assert len(rows) == 1
    assert rows[0]["instrument_id"] == "000001.SZ"
    assert expected_fields.issubset(rows[0])
    assert "record_hex" not in rows[0]
    assert "reserved_2" not in rows[0]
    assert adapter.last_meta["tdx_finance_view"] == interface_name


def test_tdx_adapter_enriches_finance_profile_from_builtin_maps(monkeypatch):
    def fake_lookup(code, *, market_id, province_raw, local_maps=None):
        assert code == "000001"
        assert market_id == 0
        assert province_raw == 18
        assert local_maps is None
        return {
            "province_name": "深圳",
            "province_board_name": "深圳板块",
            "province_board_code": "880218",
            "tdx_industry_code": "T1001",
            "tdx_industry_name": "银行",
            "tdx_industry_path": "金融 / 银行",
            "tdx_research_industry_code": "X500102",
            "tdx_research_industry_name": "股份制银行",
            "tdx_research_industry_path": "银行 / 全国性银行 / 股份制银行",
        }

    monkeypatch.setattr(tdx_request, "lookup_finance_profile_maps", fake_lookup)
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    rows = adapter.request("stock_finance_profile_tdx", {"code": "000001.SZ"})

    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "tdx_code": "sz000001",
            "exchange": "SZSE",
            "updated_date": "20260425",
            "ipo_date": "19910403",
            "province_raw": 18,
            "province_name": "深圳",
            "province_board_name": "深圳板块",
            "province_board_code": "880218",
            "industry_raw": 101,
            "tdx_industry_code": "T1001",
            "tdx_industry_name": "银行",
            "tdx_industry_path": "金融 / 银行",
            "tdx_research_industry_code": "X500102",
            "tdx_research_industry_name": "股份制银行",
            "tdx_research_industry_path": "银行 / 全国性银行 / 股份制银行",
        }
    ]
    assert adapter.last_meta["tdx_finance_map_source"] == "builtin"


def test_tdx_adapter_uses_map_root_for_finance_profile_maps(monkeypatch):
    fake_maps = SimpleNamespace(loaded=True)

    def fake_load(root):
        assert root == r"C:\APP\tdx"
        return fake_maps

    def fake_lookup(code, *, market_id, province_raw, local_maps=None):
        assert code == "000001"
        assert market_id == 0
        assert province_raw == 18
        assert local_maps is fake_maps
        return {
            "province_name": "自定义地区",
            "province_board_name": "自定义板块",
            "province_board_code": "889999",
            "tdx_industry_code": "T9999",
            "tdx_industry_name": "自定义行业",
            "tdx_industry_path": "自定义 / 行业",
            "tdx_research_industry_code": "X999999",
            "tdx_research_industry_name": "自定义研究行业",
            "tdx_research_industry_path": "自定义 / 研究行业",
        }

    monkeypatch.setattr(tdx_request, "load_finance_local_maps_from_root", fake_load)
    monkeypatch.setattr(tdx_request, "lookup_finance_profile_maps", fake_lookup)
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    rows = adapter.request(
        "stock_finance_profile_tdx",
        {"code": "000001.SZ", "map_root": r"C:\APP\tdx"},
    )

    assert rows[0]["province_name"] == "自定义地区"
    assert rows[0]["tdx_industry_name"] == "自定义行业"
    assert rows[0]["tdx_research_industry_name"] == "自定义研究行业"
    assert adapter.last_meta["tdx_finance_map_source"] == "map_root"
    assert adapter.last_meta["tdx_finance_map_loaded"] is True


def test_tdx_adapter_skips_empty_finance_records():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    rows = adapter.request("stock_finance_summary_tdx", {"code": ["000001.SZ", "sz999999"]})

    assert [row["instrument_id"] for row in rows] == ["000001.SZ"]
    assert adapter.last_meta["tdx_finance_record_count"] == 2
    assert adapter.last_meta["tdx_skipped_empty_record_count"] == 1


def test_source_request_gateway_filters_stock_finance_summary_fields():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    result = request_interface(
        "stock_finance_summary_tdx",
        params={"code": "000001.SZ"},
        fields=["instrument_id", "updated_date", "net_profit"],
        adapter=adapter,
    )

    assert result.records == [
        {
            "instrument_id": "000001.SZ",
            "updated_date": "20260425",
            "net_profit": 14523000000.0,
        }
    ]
    assert result.meta["interface_name"] == "stock_finance_summary_tdx"
    assert result.meta["tdx_protocol"] == "0x0010"


def test_tdx_adapter_requests_f10_ipo_listing_profile_and_normalizes_rows():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_gsgk": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["T035", "T031", "T042", "T044", "zcxs"],
                        "Content": [["A股", "1994-05-09", "5.2", "4680", "测试证券"]],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_ipo_listing_profile_tdx", {"code": "000034.SZ"})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_gsgk", "body": {"Params": ["8", "000034", ""]}}
    ]
    assert rows[0]["instrument_id"] == "000034.SZ"
    assert rows[0]["symbol"] == "000034"
    assert rows[0]["stock_type"] == "A股"
    assert rows[0]["list_date"] == "19940509"
    assert rows[0]["issue_price"] == 5.2
    assert rows[0]["issue_volume"] == 4680.0
    assert rows[0]["lead_underwriter"] == "测试证券"
    assert adapter.last_meta["tdx_protocol_family"] == "7615"


def test_tdx_adapter_requests_f10_index_constituent_changes_and_filters_rows():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_gsgk": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["N001", "N002", "N003", "N004", "N005", "N006"],
                        "Content": [
                            ["2024-11-29", "调入", "中证500", "1.9496101", "2024-12-16", "-1.9343494"],
                            ["2024-08-27", "调入", "中证A500", "-2.0188413", "2024-09-23", "12.602739"],
                        ],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_index_constituent_changes_tdx",
        {"code": "000034.SZ", "start_date": "20240901", "count": 1},
    )

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_gsgk", "body": {"Params": ["9", "000034", ""]}}
    ]
    assert rows == [
        {
            "instrument_id": "000034.SZ",
            "symbol": "000034",
            "publish_date": "20241129",
            "change_direction": "调入",
            "index_name": "中证500",
            "publish_date_change_pct": 1.9496101,
            "effective_date": "20241216",
            "effective_date_change_pct": -1.9343494,
        }
    ]


def test_tdx_adapter_requests_f10_interface_for_multiple_codes_and_merges_rows():
    client = FakeTqlexClient({})

    def request(entry, body):
        client.calls.append({"entry": entry, "body": body})
        symbol = body["Params"][1]
        return {
            "ErrorCode": 0,
            "ResultSets": [
                {
                    "ColName": ["T035", "T031", "T042", "T044", "zcxs"],
                    "Content": [["A股", "1994-05-09", "5.2", "4680", f"{symbol}承销商"]],
                }
            ],
        }

    client.request = request
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_ipo_listing_profile_tdx", {"code": ["000034.SZ", "000001.SZ", "000034.SZ"]})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_gsgk", "body": {"Params": ["8", "000034", ""]}},
        {"entry": "CWServ.tdxf10_gg_gsgk", "body": {"Params": ["8", "000001", ""]}},
    ]
    assert [row["instrument_id"] for row in rows] == ["000034.SZ", "000001.SZ"]
    assert [row["lead_underwriter"] for row in rows] == ["000034承销商", "000001承销商"]
    assert adapter.last_meta["tdx_requested_code_count"] == 2
    assert adapter.last_meta["tdx_returned_count"] == 2


def test_tdx_adapter_keeps_legacy_company_profile_interface_compatible():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_gsgk": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["N001", "N002", "N003", "N004", "N005", "N006"],
                        "Content": [["2024-11-29", "调入", "中证500", "1.9496101", "2024-12-16", "-1.9343494"]],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_company_profile_tdx", {"code": "000034.SZ", "type": "index_change"})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_gsgk", "body": {"Params": ["9", "000034", ""]}}
    ]
    assert rows[0]["publish_date"] == "20241129"
    assert rows[0]["publish_date_change_pct"] == 1.9496101
    assert rows[0]["effective_date_change_pct"] == -1.9343494


def test_tdx_adapter_requests_f10_disclosure_cache_without_protocol_leak():
    client = FakeTqlexClient(
        {
            "CWSearch.tzx_rcache": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": [
                            "issue_date",
                            "title",
                            "tableid",
                            "rec_id",
                            "typecode",
                            "typename",
                            "url",
                            "source",
                            "summary",
                        ],
                        "Content": [
                            [
                                "2026-06-14",
                                "年度报告",
                                "tb_gg_abg",
                                "1001",
                                "010301",
                                "定期报告",
                                "http://example.test/a.pdf",
                                "交易所",
                                "null",
                            ]
                        ],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_disclosure_feed_tdx", {"code": "000034.SZ", "category": "announcement"})

    assert client.calls == [
        {
            "entry": "CWSearch.tzx_rcache",
            "body": {"action": "get", "key": "gg:0_000034", "bin": "1", "qsid": "tdx"},
        }
    ]
    assert rows[0]["category"] == "announcement"
    assert rows[0]["issue_date"] == "20260614"
    assert rows[0]["title"] == "年度报告"
    assert rows[0]["symbol"] == "000034"
    assert rows[0]["detail_table"] == "tb_gg_abg"
    assert rows[0]["detail_id"] == "1001"
    assert rows[0]["type_code"] == "010301"
    assert rows[0]["type_name"] == "定期报告"
    assert rows[0]["url"] == "http://example.test/a.pdf"
    assert rows[0]["summary"] is None


def test_tdx_adapter_requests_f10_disclosure_roadshow_fields():
    client = FakeTqlexClient(
        {
            "CWSearch.tzx_rcache": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["title", "start_date", "start_time", "end_time", "summary", "url"],
                        "Content": [["业绩说明会", "2026-03-23", "15:00", "17:00", "null", "https://example.test/roadshow"]],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_disclosure_feed_tdx", {"code": "000034.SZ", "category": "roadshow"})

    assert client.calls == [
        {
            "entry": "CWSearch.tzx_rcache",
            "body": {"action": "get", "key": "ly:0_000034", "bin": "1", "qsid": "tdx"},
        }
    ]
    assert rows[0]["category"] == "roadshow"
    assert rows[0]["issue_date"] is None
    assert rows[0]["source"] is None
    assert rows[0]["detail_id"] is None
    assert rows[0]["summary"] is None
    assert rows[0]["start_date"] == "20260323"
    assert rows[0]["start_time"] == "15:00"
    assert rows[0]["end_time"] == "17:00"


def test_tdx_adapter_requests_event_drivers_with_optional_detail_text():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_rdtc": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["cjrq", "sjmc", "rec_id", "sjxz", "cjrzdf"],
                        "Content": [["2015-06-25", "商业银行75%存贷比取消", "2304", "利好", "-1.6534"]],
                    }
                ],
            },
            "CWServ.tdxf10_gg_idreq": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["Title", "Txt"],
                        "Content": [["事件背景", "存贷比由法定监管指标转为流动性监测指标。"]],
                    }
                ],
            },
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_event_drivers_tdx", {"code": "000001.SZ", "include_detail": True})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_rdtc", "body": {"Params": ["000001", "sjcd"]}},
        {"entry": "CWServ.tdxf10_gg_idreq", "body": {"Params": ["sjqd", "2304"]}},
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "event_date": "20150625",
            "event_name": "商业银行75%存贷比取消",
            "detail_id": "2304",
            "event_nature": "利好",
            "creation_change_pct": -1.6534,
            "has_detail": True,
            "detail_title": "事件背景",
            "detail_text": "存贷比由法定监管指标转为流动性监测指标。",
        }
    ]


def test_tdx_adapter_requests_f10_valuation_metrics_and_filters_fields():
    client = FakeTqlexClient(
        {
            "HQServ.hq_nlp_gpsj": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColDes": [{"Name": "num"}, {"Name": "total_num"}],
                        "Content": [["20", "1211"]],
                    },
                    {
                        "ColDes": [{"Name": "DATE"}, {"Name": "PETTM"}, {"Name": "PBMRQ"}, {"Name": "ALIQMV"}],
                        "Content": [
                            ["20260614", "8.5", "0.91", "1234567"],
                            ["20260613", "8.6", "0.92", "1234568"],
                        ],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    result = request_interface(
        "stock_valuation_metrics_tdx",
        params={"code": "000034.SZ", "count": 1},
        fields=["instrument_id", "date", "pe_ttm", "pb_mrq", "total_market_cap"],
        adapter=adapter,
    )

    assert client.calls[0]["entry"] == "HQServ.hq_nlp_gpsj"
    assert client.calls[0]["body"][0]["ReqId"] == "200191"
    assert client.calls[0]["body"][0]["Code"] == "000034|0"
    assert client.calls[0]["body"][0]["PageSize"] == 1
    assert result.records == [
        {
            "instrument_id": "000034.SZ",
            "date": "20260614",
            "pe_ttm": 8.5,
            "pb_mrq": 0.91,
            "total_market_cap": 1234567.0,
        }
    ]


def test_tdx_adapter_requests_f10_valuation_metrics_sends_date_range():
    client = FakeTqlexClient(
        {
            "HQServ.hq_nlp_gpsj": {
                "ErrorCode": 0,
                "ResultSets": [
                    {"ColName": ["num", "total_num"], "Content": [[3, 1211]]},
                    {
                        "ColName": ["DATE", "PETTM", "PEBFW"],
                        "Content": [
                            ["20260601", "4.95", "37.32"],
                            ["20260605", "4.98", "41.21"],
                        ],
                    },
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_valuation_metrics_tdx",
        {"code": "000001.SZ", "start_date": "20260601", "end_date": "20260605", "count": 3},
    )

    assert client.calls[0]["body"][0]["BeginDate"] == "20260601"
    assert client.calls[0]["body"][0]["EndDate"] == "20260605"
    assert [row["date"] for row in rows] == ["20260601", "20260605"]
    assert [row["pe_ttm"] for row in rows] == [4.95, 4.98]


def test_tdx_adapter_requests_f10_regulatory_actions_with_confirmed_params():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_gszx": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["N001", "N002", "N003", "N004", "N005"],
                        "Content": [
                            ["2022-08-17", "上市公司董监高", "监管函", "关于对叶海强的监管函", "http://example.test/a.pdf"],
                            ["2016-12-13", "上市公司", "监管工作函", "对独立董事任职资格事项明确监管要求", ""],
                        ],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_regulatory_actions_tdx", {"code": "000034.SZ", "count": 20})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_gszx", "body": {"Params": ["000034", "jgcs", "", "0", 1, 20]}}
    ]
    assert rows[0]["publish_date"] == "20220817"
    assert rows[0]["target"] == "上市公司董监高"
    assert rows[0]["action"] == "监管函"
    assert rows[0]["link"] == "http://example.test/a.pdf"
    assert rows[1]["publish_date"] == "20161213"
    assert rows[1]["link"] is None


def test_tdx_adapter_f10_regulatory_actions_filters_by_publish_date():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_gszx": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["N001", "N002", "N003", "N004", "N005"],
                        "Content": [
                            ["2022-08-17", "上市公司董监高", "监管函", "关于对叶海强的监管函", "http://example.test/a.pdf"],
                            ["2011-08-24", "姜欣", "交易所通报批评", "关于对重庆润江基础设施投资有限公司及相关当事人给予处分的决定", "http://example.test/b.doc"],
                            ["2008-08-25", "王迎", "交易所通报批评", "关于对深圳市深信泰丰（集团）股份有限公司及相关当事人给予处分的决定", "http://example.test/c.doc"],
                        ],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_regulatory_actions_tdx",
        {"code": "000034.SZ", "start_date": "20100101", "end_date": "20191231", "count": 20},
    )

    assert [row["publish_date"] for row in rows] == ["20110824"]


def test_tdx_adapter_f10_forecast_consensus_uses_forecast_and_summary_tables():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_ybpj": {
                "ErrorCode": 0,
                "ResultSets": [
                    {"ColName": ["nyear", "flag"], "Content": [["2026", 0]]},
                    {
                        "ColName": [
                            "T036",
                            "T037",
                            "T038",
                            "T033",
                            "T034",
                            "T035",
                            "T021",
                            "T022",
                            "T023",
                        ],
                        "Content": [["2.166", "2.227", "2.308", "4311468.3", "4397870.5", "4523682.1", "13224131.6", "13565032.4", "13999891.6"]],
                    },
                    {"ColName": ["T002"], "Content": [["2025"]]},
                    {"ColName": ["rq", "T019"], "Content": [["20251231", "42633000000"]]},
                    {"ColName": ["rq", "t023", "T003"], "Content": [["20260616", "12", "平安银行"]]},
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_forecast_consensus_tdx", {"code": "000001.SZ"})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_ybpj", "body": {"Params": ["000001", "ylyctj"]}}
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "forecast_start_year": 2026,
            "align_flag": 0,
            "eps_year1": 2.166,
            "eps_year2": 2.227,
            "eps_year3": 2.308,
            "net_profit_year1": 4311468.3,
            "net_profit_year2": 4397870.5,
            "net_profit_year3": 4523682.1,
            "revenue_year1": 13224131.6,
            "revenue_year2": 13565032.4,
            "revenue_year3": 13999891.6,
            "forecast_institution_count": 12,
        }
    ]


def test_tdx_adapter_f10_dividend_metrics_trend_keeps_view_specific_fields_clean():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_fhrz": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["N001", "N002", "N003"],
                        "Content": [["2026-06-15", "5.39", "0.838"]],
                    },
                    {"ColName": ["N004"], "Content": [["2026-06-12"]]},
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_dividend_metrics_tdx", {"code": "000001.SZ", "metric": "dividend_yield"})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_fhrz", "body": {"Params": ["000001", "fhlszs_gxl"]}}
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "date": "20260615",
            "metric_value": 5.39,
            "benchmark_value": 0.838,
            "rank": None,
            "stock_name": None,
            "stock_code": None,
            "summary_total": None,
            "cash_dividend_total": None,
        }
    ]


def test_tdx_adapter_f10_dividend_metrics_ranking_uses_view_param():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_fhrz": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["N001", "N002", "dm", "sc", "N003"],
                        "Content": [["4", "平安银行", "000001", "0", "5.39"]],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_dividend_metrics_tdx",
        {"code": "000001.SZ", "metric": "dividend_yield", "view": "ranking"},
    )

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_fhrz", "body": {"Params": ["000001", "fhpm_gxl"]}}
    ]
    assert rows[0]["date"] is None
    assert rows[0]["metric_value"] == 5.39
    assert rows[0]["rank"] == 4
    assert rows[0]["stock_name"] == "平安银行"
    assert rows[0]["stock_code"] == "000001"


def test_tdx_adapter_f10_dividend_metrics_cash_financing_defaults_to_summary():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_fhrz": {
                "ErrorCode": 0,
                "ResultSets": [
                    {"ColName": ["total", "sum"], "Content": [["28", "71769901608"]]},
                    {"ColName": ["total", "sum"], "Content": [["1", "27000000"]]},
                    {"ColName": ["total", "sum", "zfcnt"], "Content": [["4", "60793816755", "2"]]},
                    {"ColName": ["total", "sum", "pgcnt"], "Content": [["5", "3885272601", "1"]]},
                    {"ColName": ["ssy"], "Content": [["35"]]},
                    {"ColName": ["total", "sum"], "Content": [["1", "25914825000"]]},
                    {
                        "ColName": ["gxl", "glzfl", "ljxjfh", "njgmjlrfrom", "xjfhnl"],
                        "Content": [["5.39", "27.13", "37318072939", "44532000000", "83.80057698"]],
                    },
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_dividend_metrics_tdx", {"code": "000001.SZ", "metric": "cash_financing"})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_fhrz", "body": {"Params": ["000001", "pxmz"]}}
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "date": None,
            "metric_value": 83.80057698,
            "benchmark_value": None,
            "rank": None,
            "stock_name": None,
            "stock_code": None,
            "summary_total": 28.0,
            "cash_dividend_total": 37318072939.0,
        }
    ]


def test_tdx_adapter_f10_equity_financing_events_normalizes_placement_tables():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_fhrz": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": [
                            "T003",
                            "T005",
                            "T006",
                            "T011",
                            "T012",
                            "T017",
                            "T025",
                            "T026",
                            "T111",
                            "T110",
                            "T037",
                            "T038",
                            "T039",
                            "T040",
                            "T080",
                        ],
                        "Content": [
                            [
                                "2016-03-01",
                                29609.6903,
                                29609.6903,
                                1,
                                7.43,
                                "发行价格不低于定价基准日前20个交易日公司股票均价的90%。",
                                219999.9989,
                                219999.9989,
                                None,
                                "非公开发行",
                                None,
                                None,
                                35797.3531,
                                65407.0434,
                                "2016-03-02",
                            ]
                        ],
                    },
                    {
                        "ColName": ["T005", "T008", "T002", "T007", "T016", "T006", "T009"],
                        "Content": [
                            [
                                2664.3353,
                                41910,
                                "2016-06-22",
                                "发行价格为15.73元/股。",
                                "已终止",
                                "发行对象说明",
                                "募集资金用途",
                            ]
                        ],
                    },
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_equity_financing_events_tdx", {"code": "000034.SZ", "event_type": "placement"})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_fhrz", "body": {"Params": ["000034", "zf"]}}
    ]
    assert rows == [
        {
            "instrument_id": "000034.SZ",
            "symbol": "000034",
            "event_date": "20160301",
            "event_type": "placement",
            "plan": "发行价格不低于定价基准日前20个交易日公司股票均价的90%。",
            "amount": 219999.9989,
            "price": 7.43,
            "volume": 29609.6903,
            "progress": "非公开发行",
        },
        {
            "instrument_id": "000034.SZ",
            "symbol": "000034",
            "event_date": "20160622",
            "event_type": "placement",
            "plan": "发行价格为15.73元/股。",
            "amount": 41910.0,
            "price": None,
            "volume": 2664.3353,
            "progress": "已终止",
        },
    ]


def test_tdx_adapter_f10_equity_financing_events_normalizes_incentive_and_bond():
    client = FakeTqlexClient()

    def request(entry, body):
        client.calls.append({"entry": entry, "body": body})
        function = body["Params"][1]
        if function == "gqjl":
            return {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": [
                            "N001",
                            "N002",
                            "N003",
                            "N004",
                            "N005",
                            "N006",
                            "N007",
                            "N008",
                            "N009",
                            "N010",
                            "N011",
                        ],
                        "Content": [
                            [
                                "2018-09-18",
                                "实施",
                                "限制性股票授予",
                                2258.04,
                                1.04,
                                35.15,
                                "授予价格说明",
                                "2018-07-26",
                                "2018-08-30",
                                "2018-09-19",
                                "激励对象说明",
                            ]
                        ],
                    }
                ],
            }
        return {
            "ErrorCode": 0,
            "ResultSets": [
                {
                    "ColName": [
                        "N001",
                        "N002",
                        "N003",
                        "N004",
                        "N005",
                        "N006",
                        "N007",
                        "N008",
                        "N009",
                        "N010",
                        "N011",
                        "N012",
                        "N013",
                        "N014",
                        "N015",
                        "N016",
                    ],
                    "Content": [
                        [
                            "127100",
                            "神码转债",
                            6,
                            "2023-12-21",
                            "2023-12-21",
                            "2024-01-19",
                            13.38999,
                            13.39,
                            13.277037,
                            "2024-06-27",
                            "2025-03-27",
                            "2025-04-08",
                            32.51,
                            "2025-03-28",
                            "补充流动资金",
                            0,
                        ]
                    ],
                }
            ],
        }

    client.request = request
    adapter = TdxRequestAdapter(client=client)

    incentive = adapter.request("stock_equity_financing_events_tdx", {"code": "300750.SZ", "event_type": "incentive"})
    bond = adapter.request(
        "stock_equity_financing_events_tdx",
        {"code": "000034.SZ", "event_type": "convertible_bond"},
    )

    assert incentive[0]["event_type"] == "incentive"
    assert incentive[0]["event_date"] == "20180918"
    assert incentive[0]["plan"] == "限制性股票授予"
    assert incentive[0]["price"] == 35.15
    assert incentive[0]["volume"] == 2258.04
    assert incentive[0]["progress"] == "实施"
    assert bond[0]["event_type"] == "convertible_bond"
    assert bond[0]["event_date"] == "20231221"
    assert bond[0]["plan"] == "补充流动资金"
    assert bond[0]["amount"] == 13.277037
    assert bond[0]["price"] == 32.51
    assert bond[0]["volume"] == 13.38999
    assert bond[0]["progress"] == "未退市"


def test_tdx_adapter_f10_private_placement_allocations_uses_explicit_event_date():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_fhrz_zfhpmx": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["T004", "T009", "T007", "T008", "T006", "T012", "jjrq", "T002", "T003", "id"],
                        "Content": [
                            ["郭为", 36, 154777803, 154777803, "特定投资者", 1149999076.29, "2019-03-01", "GD020109", None, 2]
                        ],
                    },
                    {"ColName": ["mx"], "Content": [["2016-03-01"]]},
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_private_placement_allocations_tdx", {"code": "000034.SZ", "event_date": "20160301"})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_fhrz_zfhpmx", "body": {"Params": ["zfpg", "000034", "2016-03-01"]}}
    ]
    assert rows == [
        {
            "instrument_id": "000034.SZ",
            "symbol": "000034",
            "event_date": "20160301",
            "allocator": "郭为",
            "allocated_volume": 154777803.0,
            "subscribed_volume": 154777803.0,
            "allocated_amount": 1149999076.29,
            "lock_months": 36.0,
            "institution_type": "特定投资者",
            "unlock_date": "20190301",
            "shareholder_id": "GD020109",
        }
    ]


def test_tdx_adapter_f10_private_placement_allocations_fetches_all_event_dates_when_omitted():
    client = FakeTqlexClient()

    def request(entry, body):
        client.calls.append({"entry": entry, "body": body})
        if body == {"Params": ["zfpg_bgq", "000034", ""]}:
            return {
                "ErrorCode": 0,
                "ResultSets": [{"ColName": ["rq"], "Content": [["2016-03-01"], ["2016-06-22"]]}],
            }
        event_date = body["Params"][2]
        return {
            "ErrorCode": 0,
            "ResultSets": [
                {
                    "ColName": ["T004", "T009", "T007", "T008", "T006", "T012", "jjrq", "T002", "T003", "id"],
                    "Content": [[f"机构{event_date}", 12, 100, 200, "基金管理公司", 3000, "2019-03-01", None, None, 2]],
                },
                {"ColName": ["mx"], "Content": [[event_date]]},
            ],
        }

    client.request = request
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_private_placement_allocations_tdx", {"code": "000034.SZ"})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_fhrz_zfhpmx", "body": {"Params": ["zfpg_bgq", "000034", ""]}},
        {"entry": "CWServ.tdxf10_gg_fhrz_zfhpmx", "body": {"Params": ["zfpg", "000034", "2016-03-01"]}},
        {"entry": "CWServ.tdxf10_gg_fhrz_zfhpmx", "body": {"Params": ["zfpg", "000034", "2016-06-22"]}},
    ]
    assert [row["event_date"] for row in rows] == ["20160301", "20160622"]
    assert [row["allocator"] for row in rows] == ["机构2016-03-01", "机构2016-06-22"]


def test_tdx_adapter_f10_shareholder_change_plans_filters_direction_locally():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_gdyj": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": [
                            "N001",
                            "N002",
                            "N003",
                            "N004",
                            "N005",
                            "N006",
                            "N007",
                            "N008",
                            "N009",
                            "N010",
                            "N011",
                            "N012",
                        ],
                        "Content": [
                            ["2026-03-13", "拟减持", "陈振坤", "董事", 220937, 0.0305, None, None, "2026-04-07", "2026-07-06", "进行中", 1078327],
                            ["2025-12-30", "拟增持", "集团公司", "控股股东", None, None, None, 3300000000, "2025-09-01", "2026-02-28", "完成", 1000001],
                        ],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    increase = adapter.request("stock_shareholder_change_plans_tdx", {"code": "000034.SZ", "direction": "increase"})
    decrease = adapter.request("stock_shareholder_change_plans_tdx", {"code": "000034.SZ", "direction": "decrease"})
    all_rows = adapter.request("stock_shareholder_change_plans_tdx", {"code": "000034.SZ"})

    assert [row["direction"] for row in increase] == ["拟增持"]
    assert [row["direction"] for row in decrease] == ["拟减持"]
    assert [row["direction"] for row in all_rows] == ["拟减持", "拟增持"]


def test_tdx_adapter_f10_shareholder_change_plans_auto_fetches_all_pages():
    client = FakeTqlexClient()

    def request(entry, body):
        client.calls.append({"entry": entry, "body": body})
        page = int(body["Params"][4])
        row_count = 100 if page == 1 else 2 if page == 2 else 0
        rows = [
            ["2026-03-13", "拟减持", f"股东{page}-{index}", "董事", 100 + index, 0.01, None, None, "2026-04-07", "2026-07-06", "进行中", page * 1000 + index]
            for index in range(row_count)
        ]
        return {
            "ErrorCode": 0,
            "ResultSets": [
                {
                    "ColName": [
                        "N001",
                        "N002",
                        "N003",
                        "N004",
                        "N005",
                        "N006",
                        "N007",
                        "N008",
                        "N009",
                        "N010",
                        "N011",
                        "N012",
                    ],
                    "Content": rows,
                }
            ],
        }

    client.request = request
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_shareholder_change_plans_tdx", {"code": "000034.SZ"})

    assert len(rows) == 102
    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_gdyj", "body": {"Params": ["000034", "gdzjcjh", "", "", 1, 1, 100]}},
        {"entry": "CWServ.tdxf10_gg_gdyj", "body": {"Params": ["000034", "gdzjcjh", "", "", 2, 2, 100]}},
    ]


def test_tdx_adapter_f10_northbound_holding_auto_fetches_all_pages_and_filters_dates():
    client = FakeTqlexClient()

    def request(entry, body):
        client.calls.append({"entry": entry, "body": body})
        page = int(body["Params"][3])
        row_count = 100 if page == 1 else 2 if page == 2 else 0
        rows = []
        for index in range(row_count):
            date_text = "2026-03-31" if page == 1 and index == 0 else f"2025-{((index + page) % 12) + 1:02d}-{(index % 28) + 1:02d}"
            rows.append([date_text, "3.0", page * 1000 + index, 10 + index, "1.2", "11.08"])
        return {
            "ErrorCode": 0,
            "ResultSets": [
                {
                    "ColName": ["N001", "N002", "N003", "N004", "N005", "N006"],
                    "Content": rows,
                }
            ],
        }

    client.request = request
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_northbound_holding_tdx", {"code": "000001.SZ", "start_date": "20260101"})

    assert len(rows) == 1
    assert rows[0]["date"] == "20260331"
    assert rows[0]["channel_type"] == "深股通"
    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_zlcc", "body": {"Params": ["000001", "bszj", "", 1, 1, 100]}},
        {"entry": "CWServ.tdxf10_gg_zlcc", "body": {"Params": ["000001", "bszj", "", 2, 2, 100]}},
    ]


def test_tdx_adapter_f10_chip_distribution_maps_90_and_70_cost_fields():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_ggzp": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["N001", "N002", "N003", "N004", "N005", "N006"],
                        "Content": [
                            ["2026-06-12", "73.2641", "11.9776", "10.32~11.85", "7.092", "10.47~11.38"],
                            ["2026-06-11", "55.4947", "11.6482", "10.68~12.21", "6.9736", "10.83~11.75"],
                        ],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_chip_distribution_tdx",
        {"code": "000001.SZ", "start_date": "20260612"},
    )

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_ggzp", "body": {"Params": ["cmfb", "000001", "", ""]}}
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "date": "20260612",
            "profit_ratio_pct": 73.2641,
            "cost90_concentration": 11.9776,
            "cost90_range": "10.32~11.85",
            "cost70_concentration": 7.092,
            "cost70_range": "10.47~11.38",
        }
    ]


def test_tdx_adapter_f10_institution_holding_uses_trend_rows_and_filters_dates():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_ggzp": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["N001", "N002", "N003"],
                        "Content": [
                            ["2026-03-31", 2.94, 11.08],
                            ["2025-12-31", 3.23, 11.41],
                        ],
                    },
                    {
                        "ColName": ["N004", "N005", "N006", "N007", "N008", "N009"],
                        "Content": [
                            ["2026一季报", 1.9, None, None, None, None],
                            ["2025年报", 4.91, None, None, None, None],
                        ],
                    },
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_institution_holding_tdx", {"code": "000001.SZ", "start_date": "20260101"})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_ggzp", "body": {"Params": ["jgcc_tb", "000001", "", ""]}}
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "date": "20260331",
            "report_period": "2026一季报",
            "institution_holding_ratio_pct": 2.94,
            "close": 11.08,
        }
    ]


def test_tdx_adapter_f10_analyst_rating_merges_rating_counts_and_target_price():
    client = FakeTqlexClient()

    def request(entry, body):
        client.calls.append({"entry": entry, "body": body})
        if body == {"Params": ["jgpj", "000001", "", ""]}:
            return {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["N001", "N002", "N003", "N004", "N005", "N006"],
                        "Content": [["2026-06-11", 13, 7, 2, 0, 0]],
                    }
                ],
            }
        if body == {"Params": ["jgmbj", "000001", "", ""]}:
            return {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["N001", "N002"],
                        "Content": [["2026-06-11", 11.06]],
                    },
                    {
                        "ColName": ["N004", "N005", "N006", "N007", "N008"],
                        "Content": [[32.1880651, 14.162143, 11.06, 13.52, 14.62]],
                    },
                ],
            }
        return {"ErrorCode": 0, "ResultSets": []}

    client.request = request
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_analyst_rating_tdx", {"code": "000001.SZ", "start_date": "20260601"})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_ggzp", "body": {"Params": ["jgpj", "000001", "", ""]}},
        {"entry": "CWServ.tdxf10_gg_ggzp", "body": {"Params": ["jgmbj", "000001", "", ""]}},
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "date": "20260611",
            "buy_count": 13,
            "overweight_count": 7,
            "neutral_count": 2,
            "underweight_count": 0,
            "sell_count": 0,
            "target_price": 14.162143,
            "target_price_low": 13.52,
            "target_price_high": 14.62,
            "current_price": 11.06,
            "upside_pct": 32.1880651,
        }
    ]


def test_tdx_adapter_f10_guarantees_uses_latest_period_when_omitted():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_zbyz": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["bgq"],
                        "Content": [["2025-12-31"], ["2025-06-30"]],
                    }
                ],
            }
        }
    )

    def request(entry, body):
        client.calls.append({"entry": entry, "body": body})
        if body == {"Params": ["dbmxbgq", "000034", ""]}:
            return {
                "ErrorCode": 0,
                "ResultSets": [{"ColName": ["bgq"], "Content": [["2025-12-31"], ["2025-06-30"]]}],
            }
        return {
            "ErrorCode": 0,
            "ResultSets": [
                {
                    "ColName": ["N001", "N002", "N003", "N004", "N005", "N008", "N009", "N010", "N011", "N012"],
                    "Content": [
                        ["2025-12-31", "担保方", "被担保方", "12345", "人民币", "连带责任保证", "否", "是", "2025-09-11", "2"],
                        ["2025-12-31", "第二担保方", "第二被担保方", "67890", "人民币", "一般保证", "否", "否", "2025-09-12", "3"],
                    ],
                }
            ],
        }

    client.request = request
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_governance_guarantees_tdx", {"code": "000034.SZ", "count": 1})

    assert client.calls[0] == {"entry": "CWServ.tdxf10_gg_zbyz", "body": {"Params": ["dbmxbgq", "000034", ""]}}
    assert client.calls[1] == {"entry": "CWServ.tdxf10_gg_zbyz", "body": {"Params": ["dbmx", "000034", "20251231"]}}
    assert len(rows) == 1
    assert rows[0]["report_period"] == "20251231"
    assert rows[0]["amount"] == 12345.0
    assert rows[0]["currency"] == "人民币"


def test_tdx_adapter_f10_violation_cases_filters_by_case_date_when_publish_date_missing():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_zbyz": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["T003", "T004", "T007", "T008", "rec_id", "T006", "T009"],
                        "Content": [
                            ["2026-03-13", "被留置调查", None, None, 1002375, None, "董监高违法违规"],
                            ["2024-01-02", "已结案", "处罚决定", "2024-02-01", 1000001, None, "信息披露违规"],
                        ],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_violation_cases_tdx", {"code": "600519.SH", "end_date": "20250101"})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_zbyz", "body": {"Params": ["wgcl", "600519", ""]}}
    ]
    assert rows == [
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "case_date": "20240102",
            "case_type": "信息披露违规",
            "publish_date": "20240201",
            "progress": "已结案",
            "detail_id": 1000001,
            "decision": "处罚决定",
        }
    ]


def test_tdx_adapter_f10_business_composition_fetches_all_periods_when_omitted():
    client = FakeTqlexClient()

    def request(entry, body):
        client.calls.append({"entry": entry, "body": body})
        if entry == "CWServ.tdxf10_gg_comreq":
            return {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["T001", "T002"],
                        "Content": [["年报", "2025-12-31"], ["中报", "2025-06-30"]],
                    }
                ],
            }
        period = body["Params"][2]
        return {
            "ErrorCode": 0,
            "ResultSets": [
                {
                    "ColName": ["N000", "N001", "N002", "N003", "N004", "N005", "N006", "N007", "N008", "N009"],
                    "Content": [["按产品", "1", f"项目{period}", "100", "60", "40", "50", "60", "70", "60"]],
                }
            ],
        }

    client.request = request
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_business_composition_tdx", {"code": "000034.SZ"})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_comreq", "body": {"Params": ["zygcfx", "000034"]}},
        {"entry": "CWServ.tdxf10_gg_jyfx", "body": {"Params": ["000034", "zygc", "20251231"]}},
        {"entry": "CWServ.tdxf10_gg_jyfx", "body": {"Params": ["000034", "zygc", "20250630"]}},
    ]
    assert [row["report_period"] for row in rows] == ["20251231", "20250630"]
    assert rows[0]["item_name"] == "项目20251231"
    assert rows[1]["item_name"] == "项目20250630"


def test_tdx_adapter_f10_business_composition_uses_explicit_period_only():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_jyfx": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["N000", "N001", "N002", "N003", "N004", "N005", "N006", "N007", "N008", "N009"],
                        "Content": [["按地区", "1", "总部", "100", "60", "40", "50", "60", "70", "60"]],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_business_composition_tdx", {"code": "000034.SZ", "period": "2025-12-31"})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_jyfx", "body": {"Params": ["000034", "zygc", "20251231"]}}
    ]
    assert rows[0]["report_period"] == "20251231"
    assert rows[0]["dimension"] == "按地区"


def test_tdx_adapter_f10_valuation_series_uses_confirmed_body_and_scales_values():
    client = FakeTqlexClient(
        {
            "HQServ.hq_nlp_gpsj": {
                "ErrorCode": 0,
                "ResultSets": [
                    {"ColName": ["num", "total_num"], "Content": [[1211, 1211]]},
                    {"ColName": ["DATE", "value", "bfw"], "Content": [[20210616, 1479, 9992]]},
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_valuation_series_tdx", {"code": "000001.SZ", "metric": "pe", "count": 5})

    assert client.calls[0]["body"][0] == {
        "ReqId": "200192",
        "Code": "000001|0",
        "BeginDate": "0",
        "EndDate": "0",
        "Type": "0",
        "Page": "-1",
        "PageSize": 5,
        "modname": "mod_gpsj.dll",
    }
    assert rows[0]["date"] == "20210616"
    assert rows[0]["metric"] == "pe"
    assert rows[0]["value"] == 14.79
    assert rows[0]["percentile"] == 99.92


def test_tdx_adapter_f10_valuation_series_sends_date_range_and_rejects_raw_metric_code():
    client = FakeTqlexClient(
        {
            "HQServ.hq_nlp_gpsj": {
                "ErrorCode": 0,
                "ResultSets": [
                    {"ColName": ["num", "total_num"], "Content": [[5, 1211]]},
                    {
                        "ColName": ["DATE", "value", "bfw"],
                        "Content": [
                            [20260601, 520, 3911],
                            [20260605, 498, 4121],
                        ],
                    },
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_valuation_series_tdx",
        {
            "code": "000001.SZ",
            "metric": "pe",
            "start_date": "20260601",
            "end_date": "20260605",
            "count": 5,
        },
    )

    assert client.calls[0]["body"][0]["BeginDate"] == "20260601"
    assert client.calls[0]["body"][0]["EndDate"] == "20260605"
    assert [row["date"] for row in rows] == ["20260601", "20260605"]
    assert [row["metric"] for row in rows] == ["pe", "pe"]

    with pytest.raises(SourceRequestValidationError, match="metric must be one of"):
        adapter.request("stock_valuation_series_tdx", {"code": "000001.SZ", "metric": "0"})


def test_tdx_adapter_f10_valuation_band_merges_summary_table():
    client = FakeTqlexClient(
        {
            "HQServ.hq_nlp_gpsj": {
                "ErrorCode": 0,
                "ResultSets": [
                    {"ColName": ["N001", "N002", "N003", "N004"], "Content": [["20250616", "5.2390", "11.7900", "11.7900"]]},
                    {"ColName": ["TotalNum", "Min", "Mid"], "Content": [["243", "4.7567", "5.1667"]]},
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_valuation_band_tdx", {"code": "000001.SZ", "metric": "pe", "count": 20})

    assert client.calls[0]["body"][0]["ReqId"] == "200124"
    assert client.calls[0]["body"][0]["code"] == "000001"
    assert client.calls[0]["body"][0]["setcode"] == 0
    assert client.calls[0]["body"][0]["zb"] == "0"
    assert rows[0]["date"] == "20250616"
    assert rows[0]["band_value_1"] == 5.239
    assert rows[0]["total_count"] == 243
    assert rows[0]["min_value"] == 4.7567
    assert rows[0]["mid_value"] == 5.1667


def test_tdx_adapter_f10_valuation_band_sends_date_range():
    client = FakeTqlexClient(
        {
            "HQServ.hq_nlp_gpsj": {
                "ErrorCode": 0,
                "ResultSets": [
                    {"ColName": ["N001", "N002", "N003", "N004"], "Content": [["20250616", "5.2390", "11.7900", "11.7900"]]},
                    {"ColName": ["TotalNum", "Min", "Mid"], "Content": [["5", "5.199", "5.2301"]]},
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_valuation_band_tdx",
        {"code": "000001.SZ", "metric": "pe", "start_date": "20250616", "end_date": "20250620", "count": 5},
    )

    assert client.calls[0]["body"][0]["sdate"] == "20250616"
    assert client.calls[0]["body"][0]["edate"] == "20250620"
    assert rows[0]["date"] == "20250616"
    assert rows[0]["total_count"] == 5
    assert rows[0]["min_value"] == 5.199
    assert rows[0]["mid_value"] == 5.2301


def test_tdx_adapter_f10_market_rankings_adds_instrument_id_and_exchange():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_zxts_rqpm": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["pm", "prepm", "zqdm", "zqjc", "sc", "modtime"],
                        "Content": [["118", "-611", "000001", "平安银行", "0", "2026-06-16 07:35:00"]],
                    },
                    {
                        "ColName": ["pm", "prepm", "zqdm", "gpdm", "zqjc", "sc", "modtime"],
                        "Content": [
                            ["1", "-3", "000001", "000001", "平安银行", "0", "2026-06-16 07:35:00"],
                            ["2", "1", "601288", "601288", "农业银行", "1", "2026-06-16 07:35:00"],
                        ],
                    },
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_market_rankings_tdx", {"code": "000001.SZ", "scope": "industry", "count": 2})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_zxts_rqpm", "body": {"Params": ["000001", "hypmdelat"]}}
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "rank": 1,
            "rank_change": -3.0,
            "symbol": "000001",
            "name": "平安银行",
            "exchange": "SZSE",
            "market_code": "0",
            "updated_time": "20260616",
        },
        {
            "instrument_id": "601288.SH",
            "rank": 2,
            "rank_change": 1.0,
            "symbol": "601288",
            "name": "农业银行",
            "exchange": "SSE",
            "market_code": "1",
            "updated_time": "20260616",
        },
    ]


def test_tdx_adapter_f10_empty_response_returns_empty_rows():
    adapter = TdxRequestAdapter(client=FakeTqlexClient())

    rows = adapter.request("stock_company_profile_tdx", {"code": "000034.SZ"})

    assert rows == []
    assert adapter.last_meta["tdx_returned_count"] == 0


def test_tdx_adapter_f10_error_response_raises_validation_error():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_gsgk": {
                "ErrorCode": 1,
                "ResultSets": [],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    with pytest.raises(ValueError, match="ErrorCode=1"):
        adapter.request("stock_company_profile_tdx", {"code": "000034.SZ"})


def test_tdx_adapter_f10_multitable_response_uses_configured_main_table():
    client = FakeTqlexClient(
        {
            "HQServ.hq_nlp_tcihq": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColDes": [{"Name": "hy_code"}, {"Name": "hy_name"}, {"Name": "rank"}],
                        "Content": [["HY01", "银行", "5"]],
                    },
                    {
                        "ColDes": [{"Name": "N001"}, {"Name": "N002"}],
                        "Content": [["20260614", "12.3"]],
                    },
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("concept_control_series_tdx", {"code": "000034.SZ"})

    assert rows == [
        {
            "date": "20260614",
            "control_ratio_pct": 12.3,
            "board_code": "HY01",
            "board_name": "银行",
            "rank": 5,
        }
    ]


def test_tdx_adapter_f10_related_boards_adds_stock_identity():
    client = FakeTqlexClient(
        {
            "HQServ.hq_nlp_tcihq": {
                "ErrorCode": 0,
                "ResultSets": [
                    {"ColName": ["dummy"], "Content": []},
                    {
                        "ColName": ["N001", "N002", "N003", "N004", "N005"],
                        "Content": [["1", "881386", "全国性银行", "-1.09", "0"]],
                    },
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("concept_related_boards_tdx", {"code": "000001.SZ"})

    assert client.calls == [
        {
            "entry": "HQServ.hq_nlp_tcihq",
            "body": [
                {
                    "ReqId": "200743",
                    "setcode": 0,
                    "code": "000001",
                    "zq_num": "30",
                    "Page": -1,
                    "PageSize": 20,
                    "modname": "mod_tcihq.dll",
                }
            ],
        }
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "board_market": "1",
            "board_code": "881386",
            "board_name": "全国性银行",
            "change_pct": -1.09,
            "limit_up_count": 0,
        }
    ]


def test_tdx_adapter_f10_concept_constituents_adds_stock_identity():
    client = FakeTqlexClient(
        {
            "HQServ.hq_nlp_tcihq": {
                "ErrorCode": 0,
                "ResultSets": [
                    {"ColName": ["dummy"], "Content": []},
                    {
                        "ColName": ["N001", "N002", "N003", "N004", "N005"],
                        "Content": [
                            ["0", "000001", "平安银行", "-0.09", "11.05"],
                            ["1", "600000", "浦发银行", "0.10", "9.54"],
                        ],
                    },
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("concept_constituents_tdx", {"concept_code": "881386", "count": 2})

    assert client.calls == [
        {
            "entry": "HQServ.hq_nlp_tcihq",
            "body": [
                {
                    "ReqId": "200744",
                    "setcode": 0,
                    "code": "881386",
                    "zq_num": "30",
                    "Page": -1,
                    "PageSize": 2,
                    "modname": "mod_tcihq.dll",
                }
            ],
        }
    ]
    assert rows == [
        {
            "instrument_id": "000001.SZ",
            "symbol": "000001",
            "exchange": "SZSE",
            "market_code": "0",
            "name": "平安银行",
            "change_pct": -0.09,
            "last_price": 11.05,
        },
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "market_code": "1",
            "name": "浦发银行",
            "change_pct": 0.1,
            "last_price": 9.54,
        },
    ]


def test_tdx_adapter_f10_concept_capital_flow_filters_date_and_count():
    client = FakeTqlexClient(
        {
            "HQServ.hq_nlp_tcihq": {
                "ErrorCode": 0,
                "ResultSets": [
                    {"ColName": ["dummy"], "Content": []},
                    {
                        "ColName": ["N001", "N002", "N003", "N004", "N005", "N006"],
                        "Content": [
                            ["化学制药", "20260529", "-1", "-2", "-3", "-4"],
                            ["化学制药", "20260601", "10", "20", "30", "40"],
                            ["化学制药", "20260603", "11", "21", "31", "41"],
                            ["化学制药", "20260610", "12", "22", "32", "42"],
                            ["化学制药", "20260611", "13", "23", "33", "43"],
                        ],
                    },
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "concept_capital_flow_tdx",
        {
            "concept_code": "2817",
            "start_date": "20260601",
            "end_date": "20260610",
            "count": 2,
        },
    )

    assert client.calls == [
        {
            "entry": "HQServ.hq_nlp_tcihq",
            "body": [
                {
                    "ReqId": "200742",
                    "setcode": 0,
                    "code": "2817",
                    "zq_num": "30",
                    "Page": -1,
                    "PageSize": 2,
                    "modname": "mod_tcihq.dll",
                }
            ],
        }
    ]
    assert rows == [
        {
            "board_name": "化学制药",
            "date": "20260601",
            "main_amount": 10.0,
            "main_buy_amount": 20.0,
            "avg_main_amount": 30.0,
            "avg_main_buy_amount": 40.0,
        },
        {
            "board_name": "化学制药",
            "date": "20260603",
            "main_amount": 11.0,
            "main_buy_amount": 21.0,
            "avg_main_amount": 31.0,
            "avg_main_buy_amount": 41.0,
        },
    ]


def test_tdx_adapter_f10_control_series_filters_date_and_count():
    client = FakeTqlexClient(
        {
            "HQServ.hq_nlp_tcihq": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColDes": [{"Name": "hy_code"}, {"Name": "hy_name"}, {"Name": "rank"}],
                        "Content": [["881386", "全国性银行", "6"]],
                    },
                    {
                        "ColDes": [{"Name": "N001"}, {"Name": "N002"}],
                        "Content": [
                            ["20260531", "39.11"],
                            ["20260601", "41.58"],
                            ["20260603", "42.88"],
                            ["20260611", "40.22"],
                        ],
                    },
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "concept_control_series_tdx",
        {"code": "000001.SZ", "start_date": "20260601", "end_date": "20260610", "count": 2},
    )

    assert client.calls == [
        {
            "entry": "HQServ.hq_nlp_tcihq",
            "body": [
                {
                    "ReqId": "200745",
                    "setcode": 0,
                    "code": "000001",
                    "zq_num": "30",
                    "Page": -1,
                    "PageSize": 2,
                    "modname": "mod_tcihq.dll",
                }
            ],
        }
    ]
    assert rows == [
        {
            "date": "20260601",
            "control_ratio_pct": 41.58,
            "board_code": "881386",
            "board_name": "全国性银行",
            "rank": 6,
        },
        {
            "date": "20260603",
            "control_ratio_pct": 42.88,
            "board_code": "881386",
            "board_name": "全国性银行",
            "rank": 6,
        },
    ]


def test_tdx_adapter_f10_control_ranking_flattens_nested_rows():
    client = FakeTqlexClient(
        {
            "HQServ.hq_nlp_tcihq": {
                "ErrorCode": 0,
                "ResultSets": [
                    {"ColName": ["dummy"], "Content": []},
                    {
                        "ColName": ["N001", "N002"],
                        "Content": [
                            [
                                "20260506",
                                [
                                    [1, 0, "002102", "ST能特", 57.8],
                                    [2, 1, "688166", "博瑞医药", 47.05],
                                ],
                            ],
                            [
                                "20260507",
                                [
                                    {
                                        "pm": 1,
                                        "sc": 0,
                                        "code": "000766",
                                        "name": "通化金马",
                                        "zlkp": 51.95,
                                    }
                                ],
                            ],
                        ],
                    },
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("concept_control_ranking_tdx", {"concept_code": "2817", "count": 2})

    assert client.calls == [
        {
            "entry": "HQServ.hq_nlp_tcihq",
            "body": [
                {
                    "ReqId": "200747",
                    "setcode": 0,
                    "code": "2817",
                    "zq_num": "30",
                    "Page": -1,
                    "PageSize": 2,
                    "modname": "mod_tcihq.dll",
                }
            ],
        }
    ]
    assert rows == [
        {
            "date": "20260506",
            "instrument_id": "002102.SZ",
            "exchange": "SZSE",
            "rank": 1,
            "market_code": 0,
            "symbol": "002102",
            "name": "ST能特",
            "control_ratio_pct": 57.8,
        },
        {
            "date": "20260506",
            "instrument_id": "688166.SH",
            "exchange": "SSE",
            "rank": 2,
            "market_code": 1,
            "symbol": "688166",
            "name": "博瑞医药",
            "control_ratio_pct": 47.05,
        },
    ]


def test_tdx_adapter_f10_concept_constituent_comparison_keeps_financial_period():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_rdtc_gndb": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["pm", "sc", "zqdm", "zqjc", "bgq", "T013", "T012", "T003", "T019"],
                        "Content": [["1", "0", "301171", "易点天下", "20260331", "12235300000", "8932100000", "389000000", "42200000"]],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "concept_constituent_comparison_tdx",
        {
            "code": "000001.SZ",
            "concept_code": "2817",
            "compare_type": "financial",
            "sort_by": "revenue",
        },
    )

    assert client.calls == [
        {
            "entry": "CWServ.tdxf10_gg_rdtc_gndb",
            "body": {"Params": ["gndbcwsj", "000001", "2817", "revenue"]},
        }
    ]
    assert rows == [
        {
            "rank": 1,
            "market_code": 0,
            "symbol": "301171",
            "name": "易点天下",
            "report_period": "20260331",
            "change_pct": None,
            "change_pct_3d": None,
            "change_pct_5d": None,
            "change_pct_20d": None,
            "change_pct_60d": None,
            "total_market_cap": 12235300000.0,
            "float_market_cap": 8932100000.0,
            "revenue": 389000000.0,
            "net_profit": 42200000.0,
            "revenue_yoy_pct": None,
            "net_profit_yoy_pct": None,
        }
    ]


def test_tdx_adapter_f10_duplicate_source_columns_are_preserved_by_position():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_cwfx": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["rq", "T007", "T007"],
                        "Content": [["20251231", "100", "200"]],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_financial_statement_tdx", {"code": "000034.SZ"})

    assert rows[0]["report_period"] == "20251231"
    assert rows[0]["cash"] == 100.0


@pytest.mark.parametrize(
    ("topic_type", "function"),
    [("sector", "zttzbkz"), ("theme", "zttzztk")],
)
def test_tdx_adapter_f10_topic_exposure_uses_user_facing_topic_type(topic_type, function):
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_rdtc": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["bflag", "ztrq", "ztmc", "gld", "rxsj", "ztnr", "id", "sslb"],
                        "Content": [["1", "2022-02-28", "跨境支付CIPS", "3", "2022-02-28", "公司是首批参与者", "2817", "2"]],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_topic_exposure_tdx", {"code": "000001.SZ", "topic_type": topic_type})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_rdtc", "body": {"Params": ["000001", function]}}
    ]
    assert rows[0]["topic_type"] == topic_type
    assert rows[0]["created_date"] == "20220228"
    assert rows[0]["topic_name"] == "跨境支付CIPS"
    assert rows[0]["relevance"] == 3.0
    assert rows[0]["topic_id"] == "2817"
    assert rows[0]["group_code"] == "2"


def test_tdx_adapter_f10_topic_exposure_defaults_to_theme():
    client = FakeTqlexClient(
        {
            "CWServ.tdxf10_gg_rdtc": {
                "ErrorCode": 0,
                "ResultSets": [
                    {
                        "ColName": ["bflag", "ztrq", "ztmc", "gld", "rxsj", "ztnr", "id", "sslb"],
                        "Content": [["1", "2016-12-02", "平安保险持股", "5", "2026-03-23", "中国平安保险持股", "900", None]],
                    }
                ],
            }
        }
    )
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_topic_exposure_tdx", {"code": "000001.SZ"})

    assert client.calls == [
        {"entry": "CWServ.tdxf10_gg_rdtc", "body": {"Params": ["000001", "zttzztk"]}}
    ]
    assert rows[0]["topic_type"] == "theme"
    assert rows[0]["topic_id"] == "900"
    assert rows[0]["group_code"] is None


def test_tdx_adapter_requests_multiple_f10_codes_sequentially_by_default():
    client = EchoTqlexClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_topic_exposure_tdx",
        {"code": ["000034.SZ", "600000.SH"]},
    )

    assert len(rows) == 2
    assert adapter.last_meta["tdx_f10_workers"] == 1
    assert adapter.last_meta["tdx_requested_code_count"] == 2
    assert [call["body"]["Params"][0] for call in client.calls] == ["000034", "600000"]


def test_tdx_adapter_requests_multiple_f10_codes_with_workers_and_keeps_order():
    class SlowEchoTqlexClient:
        def __init__(self):
            self.calls = []
            self.active = 0
            self.max_active = 0
            self.lock = threading.Lock()

        def request(self, entry, body):
            code = body["Params"][0]
            with self.lock:
                self.calls.append({"entry": entry, "body": body})
                self.active += 1
                self.max_active = max(self.max_active, self.active)
            try:
                if code == "000034":
                    time.sleep(0.04)
                else:
                    time.sleep(0.01)
                return {
                    "ErrorCode": 0,
                    "ResultSets": [
                        {
                            "ColName": ["GPDM", "GPMC"],
                            "Content": [[code, f"name-{code}"]],
                        }
                    ],
                }
            finally:
                with self.lock:
                    self.active -= 1

    client = SlowEchoTqlexClient()
    adapter = TdxRequestAdapter(client=client, options={"f10_workers": 2})

    rows = adapter.request(
        "stock_topic_exposure_tdx",
        {"code": ["000034.SZ", "600000.SH"]},
    )

    assert client.max_active == 2
    assert adapter.last_meta["tdx_f10_workers"] == 2
    assert adapter.last_meta["tdx_requested_code_count"] == 2
    assert [row["instrument_id"] for row in rows] == ["000034.SZ", "600000.SH"]


def test_tdx_adapter_accepts_f10_workers_for_single_code_without_parallelism():
    client = EchoTqlexClient()
    adapter = TdxRequestAdapter(client=client, options={"f10_workers": 6})

    rows = adapter.request("stock_topic_exposure_tdx", {"code": "000034.SZ"})

    assert len(rows) == 1
    assert len(client.calls) == 1
    assert adapter.last_meta["tdx_f10_workers"] == 1
    assert adapter.last_meta["tdx_requested_code_count"] == 1


def test_tdx_adapter_f10_existing_client_does_not_create_default_client(monkeypatch):
    from axdata_core.adapters.tdx import f10_executor

    class FailingDefaultTqlexClient:
        def __init__(self):
            raise AssertionError("default TQLEX client should not be created")

    monkeypatch.setattr(f10_executor, "TdxTqlexClient", FailingDefaultTqlexClient)

    client = EchoTqlexClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_topic_exposure_tdx", {"code": "000034.SZ"})

    assert len(rows) == 1
    assert len(client.calls) == 1
    assert adapter.last_meta["tdx_protocol_family"] == "7615"
    assert adapter.last_meta["tdx_requested_interface"] == "stock_topic_exposure_tdx"


def test_tdx_adapter_deduplicates_repeated_f10_codes_before_requesting():
    client = EchoTqlexClient()
    adapter = TdxRequestAdapter(client=client, options={"f10_workers": 3})

    rows = adapter.request(
        "stock_topic_exposure_tdx",
        {"code": ["000034.SZ", "000034.SZ", "600000.SH", "600000.SH"]},
    )

    assert len(rows) == 2
    assert adapter.last_meta["tdx_f10_workers"] == 2
    assert adapter.last_meta["tdx_requested_code_count"] == 2
    assert [call["body"]["Params"][0] for call in client.calls] == ["000034", "600000"]


def test_tdx_adapter_rejects_invalid_f10_workers_option():
    adapter = TdxRequestAdapter(client=EchoTqlexClient(), options={"f10_workers": 0})

    with pytest.raises(SourceRequestValidationError, match="f10_workers must be >= 1"):
        adapter.request("stock_topic_exposure_tdx", {"code": ["000034.SZ", "600000.SH"]})


def test_tdx_adapter_rejects_invalid_f10_workers_option_for_single_code():
    adapter = TdxRequestAdapter(client=EchoTqlexClient(), options={"f10_workers": 0})

    with pytest.raises(SourceRequestValidationError, match="f10_workers must be >= 1"):
        adapter.request("stock_topic_exposure_tdx", {"code": "000034.SZ"})


@pytest.mark.parametrize("interface_name", sorted(F10_INTERFACE_SPECS))
def test_tdx_adapter_can_request_every_f10_spec_with_minimal_params(interface_name):
    spec = F10_INTERFACE_SPECS[interface_name]
    client = EchoTqlexClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(interface_name, _minimal_f10_params(spec))

    assert client.calls
    assert client.calls[-1]["entry"] == spec.entry
    assert rows
    assert set(rows[0]) == {field.name for field in spec.fields}
    assert adapter.last_meta["tdx_protocol_family"] == "7615"
    assert adapter.last_meta["tdx_requested_interface"] == interface_name


def test_tdx_adapter_normalizes_qfq_adj_factor_to_anchor_date():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_adj_factor_tdx",
        {"code": "000001.SZ", "adjust": "qfq", "anchor_date": "20240531"},
    )

    assert [row["adj_factor"] for row in rows] == [1.0, 1.2121212121]
    assert adapter.last_meta["tdx_anchor_date"] == "20240531"


def test_tdx_adapter_rejects_hfq_adj_factor_anchor_date():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    with pytest.raises(SourceRequestValidationError, match="anchor_date is only supported"):
        adapter.request(
            "stock_adj_factor_tdx",
            {"code": "000001.SZ", "adjust": "hfq", "anchor_date": "20240531"},
        )


def test_tdx_adapter_rejects_invalid_adj_factor_anchor_date():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    with pytest.raises(SourceRequestValidationError, match="anchor_date must be a valid date"):
        adapter.request(
            "stock_adj_factor_tdx",
            {"code": "000001.SZ", "adjust": "qfq", "anchor_date": "20240231"},
        )


def test_tdx_adapter_rejects_adj_factor_anchor_date_before_history():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    with pytest.raises(SourceRequestValidationError, match="earlier than the first available"):
        adapter.request(
            "stock_adj_factor_tdx",
            {"code": "000001.SZ", "adjust": "qfq", "anchor_date": "20240501"},
        )


@pytest.mark.parametrize(("interface_name", "params", "expected_period"), KLINE_PERIOD_CASES)
def test_tdx_adapter_maps_all_kline_interfaces_to_clean_rows(interface_name, params, expected_period):
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(interface_name, params)

    assert client.kline_calls[-1]["period"] == expected_period
    assert client.kline_calls[-1]["count"] == 2
    assert rows[0]["period"] == expected_period
    assert set(rows[0]) == {
        "instrument_id",
        "symbol",
        "tdx_code",
        "exchange",
        "trade_time",
        "period",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
    }
    assert adapter.last_meta["tdx_full_history"] is True
    assert adapter.last_meta["tdx_requested_code_count"] == 1


def test_tdx_adapter_accepts_batch_kline_codes_with_injected_client():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "stock_kline_second_tdx",
        {"code": ["000001.SZ", "600000.SH"], "seconds": 5, "count": 2},
    )

    assert client.kline_calls == [
        {
            "code": "sz000001",
            "period": "5s",
            "start": 0,
            "count": 2,
            "adjust": "none",
            "anchor_date": None,
            "kind": "stock",
        },
        {
            "code": "sh600000",
            "period": "5s",
            "start": 0,
            "count": 2,
            "adjust": "none",
            "anchor_date": None,
            "kind": "stock",
        },
    ]
    assert [row["instrument_id"] for row in rows] == ["000001.SZ", "600000.SH"]
    assert adapter.last_meta["tdx_requested_code_count"] == 2
    assert adapter.last_meta["tdx_concurrency_limit"] == 1


def test_tdx_adapter_requests_index_kline_with_index_kind_and_breadth_counts():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "index_kline_tdx",
        {"code": "000001.SH", "period": "day", "count": 2},
    )

    assert client.kline_calls[0] == {
        "code": "sh000001",
        "period": "day",
        "start": 0,
        "count": 2,
        "adjust": "none",
        "anchor_date": None,
        "kind": "index",
    }
    assert {call["kind"] for call in client.kline_calls} == {"index"}
    assert len(rows) == 2
    assert rows[0]["instrument_id"] == "000001.SH"
    assert rows[0]["up_count"] == 1087
    assert rows[0]["down_count"] == 1222
    assert set(rows[0]) == {
        "instrument_id",
        "symbol",
        "tdx_code",
        "exchange",
        "trade_time",
        "period",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "up_count",
        "down_count",
    }
    assert adapter.last_meta["tdx_kline_kind"] == "index"


def test_tdx_adapter_requests_full_kline_history_with_internal_paging():
    class PagingKlineClient(FakeTdxClient):
        def get_kline(
            self,
            code,
            *,
            period="day",
            start=0,
            count=800,
            adjust=None,
            anchor_date=None,
        ):
            self.kline_calls.append(
                {
                    "code": code,
                    "period": period,
                    "start": start,
                    "count": count,
                    "adjust": adjust,
                    "anchor_date": anchor_date,
                }
            )
            page_times = {
                0: (
                    datetime(2026, 5, 3, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                    datetime(2026, 5, 4, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                ),
                2: (
                    datetime(2026, 5, 1, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                    datetime(2026, 5, 2, 15, 0, tzinfo=timezone(timedelta(hours=8))),
                ),
            }
            return SimpleNamespace(
                exchange=code[:2],
                code=code[2:],
                full_code=code,
                period_raw=4,
                period_param_raw=1,
                period_name=period,
                start=start,
                request_count=count,
                adjust_mode_raw=0,
                adjust_mode=adjust or "none",
                anchor_date_raw=0,
                bars=tuple(
                    SimpleNamespace(
                        time=item_time,
                        open=10.0,
                        high=10.2,
                        low=9.9,
                        close=10.1,
                        volume_lots=100.0,
                        amount=101000.0,
                    )
                    for item_time in page_times.get(start, ())
                ),
            )

    client = PagingKlineClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("stock_kline_daily_tdx", {"code": "000001.SZ", "count": 2})

    assert [call["start"] for call in client.kline_calls] == [0, 2, 4]
    assert [row["trade_time"] for row in rows] == [
        "2026-05-01T15:00:00+08:00",
        "2026-05-02T15:00:00+08:00",
        "2026-05-03T15:00:00+08:00",
        "2026-05-04T15:00:00+08:00",
    ]
    assert adapter.last_meta["tdx_full_history"] is True
    assert adapter.last_meta["tdx_page_size"] == 2
    assert adapter.last_meta["tdx_page_count"] == 3


def test_tdx_adapter_batches_kline_codes_across_two_hosts(monkeypatch):
    created_clients = []

    def fake_create_tdx_client(*, hosts=None, pool_size=None, heartbeat_interval=None):
        client = FakeTdxClient()
        host_values = tuple(hosts or ())
        client.hosts = host_values
        client.pool_size = pool_size
        client.transport = SimpleNamespace(
            hosts=host_values,
            connected_host=host_values[0] if host_values else None,
            connected_hosts=tuple(f"{host_values[0]}#{index}" for index in range(pool_size or 1))
            if host_values
            else (),
            pool_size=pool_size,
            heartbeat_interval=heartbeat_interval,
        )
        created_clients.append(client)
        return client

    monkeypatch.setenv("AXDATA_TDX_HOSTS", "10.0.0.1:7709,10.0.0.2:7709,10.0.0.3:7709")
    monkeypatch.setattr("axdata_core.adapters.tdx.request.create_tdx_client", fake_create_tdx_client)
    provider_request_methods = _optional_tdx_provider_module("request_methods")
    if provider_request_methods is not None:
        monkeypatch.setattr(provider_request_methods, "create_tdx_client", fake_create_tdx_client)
    adapter = TdxRequestAdapter()

    rows = adapter.request(
        "stock_kline_second_tdx",
        {
            "code": [
                "000001.SZ",
                "000002.SZ",
                "000003.SZ",
                "000004.SZ",
                "000005.SZ",
                "600000.SH",
                "600001.SH",
                "600002.SH",
                "600003.SH",
            ],
            "seconds": 5,
            "count": 2,
        },
    )

    assert sorted(client.hosts for client in created_clients) == [("10.0.0.1:7709",), ("10.0.0.2:7709",)]
    assert [client.pool_size for client in created_clients] == [4, 4]
    assert sum(len(client.kline_calls) for client in created_clients) == 9
    assert [row["instrument_id"] for row in rows] == [
        "000001.SZ",
        "000002.SZ",
        "000003.SZ",
        "000004.SZ",
        "000005.SZ",
        "600000.SH",
        "600001.SH",
        "600002.SH",
        "600003.SH",
    ]
    assert adapter.last_meta["tdx_requested_code_count"] == 9
    assert adapter.last_meta["tdx_kline_host_count"] == 2
    assert adapter.last_meta["tdx_kline_pool_size_per_host"] == 4
    assert adapter.last_meta["tdx_concurrency_capacity"] == 8
    assert adapter.last_meta["tdx_concurrency_limit"] == 8


def test_tdx_adapter_applies_options_to_parallel_kline_codes(monkeypatch):
    created_clients = []

    def fake_create_tdx_client(*, hosts=None, pool_size=None, heartbeat_interval=None):
        client = FakeTdxClient()
        host_values = tuple(hosts or ())
        client.hosts = host_values
        client.pool_size = pool_size
        client.transport = SimpleNamespace(
            hosts=host_values,
            connected_host=host_values[0] if host_values else None,
            connected_hosts=tuple(f"{host_values[0]}#{index}" for index in range(pool_size or 1))
            if host_values
            else (),
            pool_size=pool_size,
            heartbeat_interval=heartbeat_interval,
        )
        created_clients.append(client)
        return client

    monkeypatch.setenv("AXDATA_TDX_HOSTS", "10.0.0.1:7709,10.0.0.2:7709,10.0.0.3:7709,10.0.0.4:7709")
    monkeypatch.setattr("axdata_core.adapters.tdx.request.create_tdx_client", fake_create_tdx_client)
    provider_request_methods = _optional_tdx_provider_module("request_methods")
    if provider_request_methods is not None:
        monkeypatch.setattr(provider_request_methods, "create_tdx_client", fake_create_tdx_client)
    adapter = TdxRequestAdapter(options={"source_server_count": 3, "connections_per_server": 2})

    rows = adapter.request(
        "stock_kline_second_tdx",
        {
            "code": [
                "000001.SZ",
                "000002.SZ",
                "000003.SZ",
                "000004.SZ",
                "600000.SH",
                "600001.SH",
            ],
            "seconds": 5,
            "count": 2,
        },
    )

    assert sorted(client.hosts for client in created_clients) == [
        ("10.0.0.1:7709",),
        ("10.0.0.2:7709",),
        ("10.0.0.3:7709",),
    ]
    assert [client.pool_size for client in created_clients] == [2, 2, 2]
    assert sum(len(client.kline_calls) for client in created_clients) == 6
    assert [row["instrument_id"] for row in rows] == [
        "000001.SZ",
        "000002.SZ",
        "000003.SZ",
        "000004.SZ",
        "600000.SH",
        "600001.SH",
    ]
    assert adapter.last_meta["tdx_kline_host_count"] == 3
    assert adapter.last_meta["tdx_kline_pool_size_per_host"] == 2
    assert adapter.last_meta["tdx_concurrency_capacity"] == 6
    assert adapter.last_meta["tdx_concurrency_limit"] == 6


def test_source_request_gateway_filters_second_kline_fields():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    result = request_interface(
        "stock_kline_second_tdx",
        params={"code": "000001.SZ", "seconds": 5},
        fields=["instrument_id", "trade_time", "period", "close"],
        adapter=adapter,
    )

    assert result.records == [
        {
            "instrument_id": "000001.SZ",
            "trade_time": "2026-05-19T13:39:50+08:00",
            "period": "5s",
            "close": 10.14,
        }
    ]
    assert result.meta["interface_name"] == "stock_kline_second_tdx"
    assert result.meta["source"] == "tdx"


def test_source_request_gateway_rejects_suspension_limit_param():
    adapter = TdxRequestAdapter(client=FakeTdxClient(include_extra_stocks=True))

    with pytest.raises(SourceRequestValidationError, match="Unknown param"):
        request_interface(
            "stock_suspensions_tdx",
            params={"scope": "all", "limit": 1},
            adapter=adapter,
        )


def test_source_request_gateway_accepts_custom_second_kline_seconds():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    result = request_interface(
        "stock_kline_second_tdx",
        params={"code": "000001.SZ", "seconds": 7},
        fields=["instrument_id", "period"],
        adapter=adapter,
    )

    assert client.kline_calls[-1]["period"] == "7s"
    assert result.records == [{"instrument_id": "000001.SZ", "period": "7s"}]


@pytest.mark.parametrize(("seconds", "message"), [(0, "seconds must be >= 1"), (61, "seconds must be <= 60")])
def test_source_request_gateway_rejects_second_kline_seconds_out_of_range(seconds, message):
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    with pytest.raises(SourceRequestValidationError, match=message):
        request_interface(
            "stock_kline_second_tdx",
            params={"code": "000001.SZ", "seconds": seconds},
            adapter=adapter,
        )


@pytest.mark.parametrize(("interface_name", "params"), KLINE_PUBLIC_PARAM_CASES)
def test_source_request_gateway_rejects_public_kline_count_param(interface_name, params):
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    with pytest.raises(SourceRequestValidationError, match="Unknown param"):
        request_interface(
            interface_name,
            params={**params, "count": 2},
            adapter=adapter,
        )


@pytest.mark.parametrize(
    ("interface_name", "params"),
    [
        ("stock_trades_today_tdx", {"code": "000001.SZ"}),
        ("stock_trades_history_tdx", {"code": "000001.SZ", "trade_date": "20260511"}),
        ("stock_auction_result_tdx", {"code": "000001.SZ"}),
        ("stock_auction_result_history_tdx", {"code": "000001.SZ", "trade_date": "20260511"}),
    ],
)
def test_source_request_gateway_rejects_public_trade_paging_params(interface_name, params):
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    with pytest.raises(SourceRequestValidationError, match="Unknown param"):
        request_interface(
            interface_name,
            params={**params, "start": 0, "count": 100},
            adapter=adapter,
        )


def test_source_request_gateway_rejects_score_summary_trade_date_param():
    adapter = TdxRequestAdapter(client=FakeTqlexClient())

    with pytest.raises(SourceRequestValidationError, match="Unknown param"):
        request_interface(
            "stock_score_summary_tdx",
            params={"code": "000001.SZ", "trade_date": "20260616"},
            adapter=adapter,
        )


@pytest.mark.parametrize(
    ("interface_name", "params", "message"),
    [
        ("stock_kline_nminute_tdx", {"code": "000001.SZ"}, "minutes"),
        ("stock_kline_nday_tdx", {"code": "000001.SZ"}, "days"),
    ],
)
def test_source_request_gateway_rejects_required_custom_kline_period_params(
    interface_name,
    params,
    message,
):
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    with pytest.raises(SourceRequestValidationError, match=message):
        request_interface(interface_name, params=params, adapter=adapter)


def test_source_request_gateway_accepts_fixed_qfq_anchor_date():
    client = FakeTdxClient()
    adapter = TdxRequestAdapter(client=client)

    result = request_interface(
        "stock_kline_daily_tdx",
        params={"code": "000001.SZ", "adjust": "fixed_qfq", "anchor_date": "20260519"},
        fields=["instrument_id"],
        adapter=adapter,
    )

    assert client.kline_calls[-1]["adjust"] == "fixed_qfq"
    assert client.kline_calls[-1]["anchor_date"] == "20260519"
    assert result.meta["tdx_adjust_mode"] == "fixed_qfq"


def test_source_request_gateway_rejects_fixed_hfq_public_kline_adjust():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    with pytest.raises(SourceRequestValidationError, match="adjust must be one of"):
        request_interface(
            "stock_kline_daily_tdx",
            params={"code": "000001.SZ", "adjust": "fixed_hfq", "anchor_date": "20260519"},
            adapter=adapter,
        )


def test_source_request_gateway_rejects_kline_anchor_without_fixed_qfq():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    with pytest.raises(SourceRequestValidationError, match="anchor_date is only supported"):
        request_interface(
            "stock_kline_daily_tdx",
            params={"code": "000001.SZ", "adjust": "qfq", "anchor_date": "20260519"},
            adapter=adapter,
        )


def test_source_request_gateway_rejects_fixed_qfq_without_anchor_date():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    with pytest.raises(SourceRequestValidationError, match="anchor_date is required"):
        request_interface(
            "stock_kline_daily_tdx",
            params={"code": "000001.SZ", "adjust": "fixed_qfq"},
            adapter=adapter,
        )


def test_source_request_gateway_rejects_unknown_public_params():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    with pytest.raises(SourceRequestValidationError, match="Unknown param"):
        request_interface(
            "stock_codes_tdx",
            params={"exchange": "SZSE"},
            adapter=adapter,
        )


def test_tdx_source_preview_client_disables_heartbeat_by_default(monkeypatch):
    captured = {}

    def fake_from_hosts(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace()

    monkeypatch.delenv("AXDATA_TDX_HEARTBEAT_INTERVAL", raising=False)
    monkeypatch.setattr("axdata_core._tdx_wire.client.TdxClient.from_hosts", fake_from_hosts)
    provider_client = _optional_tdx_provider_module("_tdx_wire.client")
    if provider_client is not None:
        monkeypatch.setattr(provider_client.TdxClient, "from_hosts", fake_from_hosts)

    tdx_request.create_tdx_client()

    assert captured["heartbeat_interval"] is None


def test_tdx_source_preview_client_allows_explicit_heartbeat(monkeypatch):
    captured = {}

    def fake_from_hosts(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace()

    monkeypatch.setenv("AXDATA_TDX_HEARTBEAT_INTERVAL", "15")
    monkeypatch.setattr("axdata_core._tdx_wire.client.TdxClient.from_hosts", fake_from_hosts)
    provider_client = _optional_tdx_provider_module("_tdx_wire.client")
    if provider_client is not None:
        monkeypatch.setattr(provider_client.TdxClient, "from_hosts", fake_from_hosts)

    tdx_request.create_tdx_client()

    assert captured["heartbeat_interval"] == 15.0


def test_source_request_gateway_rejects_unregistered_interfaces_before_adapter():
    adapter = TdxRequestAdapter(client=FakeTdxClient())

    with pytest.raises(SourceInterfaceNotFound):
        request_interface(
            "custom_interface",
            params={"code": "demo"},
            adapter=adapter,
        )


def test_tdx_code_helpers_remain_available_for_future_interfaces():
    assert instrument_id_to_tdx_code("600000.SH") == "sh600000"
    assert instrument_id_to_tdx_code("000001.SZ") == "sz000001"
    assert tdx_code_to_instrument_id("sh600000") == "600000.SH"
    assert tdx_code_to_instrument_id("sz000001") == "000001.SZ"


def test_tdx_etf_codes_unit():
    client = FakeTdxClient(include_etfs=True)
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("etf_codes_tdx", {})

    instrument_ids = {row["instrument_id"] for row in rows}
    assert instrument_ids == {
        "510050.SH",
        "588000.SH",
        "159001.SZ",
        "159915.SZ",
        "159919.SZ",
    }
    # 只返回 ETF 代码段，不含股票/指数
    assert "000001.SH" not in instrument_ids
    assert "399001.SZ" not in instrument_ids
    assert "000001.SZ" not in instrument_ids
    by_id = {row["instrument_id"]: row for row in rows}
    assert by_id["510050.SH"]["name"] == "50ETF"
    assert by_id["510050.SH"]["previous_close"] == 2.86
    assert by_id["159915.SZ"]["tdx_code"] == "sz159915"
    assert adapter.last_meta["tdx_returned_count"] == 5


def test_tdx_etf_codes_unit_filters_by_code():
    client = FakeTdxClient(include_etfs=True)
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("etf_codes_tdx", {"code": ["510050.SH", "159915.SZ"]})

    assert [row["instrument_id"] for row in rows] == ["510050.SH", "159915.SZ"]


def test_tdx_etf_realtime_snapshot_unit():
    client = FakeTdxClient(include_etfs=True)
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("etf_realtime_snapshot_tdx", {"code": "510050.SH"})

    assert client.explicit_quote_calls == [
        {"securities": [("sh", "510050")], "batch_size": 80}
    ]
    assert rows[0]["instrument_id"] == "510050.SH"
    # decimal=3 ETF: wire decoded 8.42 as raw/100, adapter rescales to raw/1000
    assert rows[0]["last_price"] == 0.842
    assert rows[0]["change_pct"] == 0.357568
    # ETF 快照复用指数字段形状，不含股票专有字段
    assert "bid1_price" not in rows[0]
    assert "locked_amount" not in rows[0]
    assert adapter.last_meta["tdx_protocol"] == "0x054c"
    assert adapter.last_meta["tdx_asset_type"] == "etf"


def test_tdx_etf_realtime_rank_unit():
    client = FakeTdxClient(include_etfs=True)
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("etf_realtime_rank_tdx", {"sort": "change_pct", "count": 5})

    # ETF 排行使用 0x2afd category
    assert client.category_quote_calls[-1]["category"] == 0x2AFD
    assert client.category_quote_calls[-1]["category"] == 11005
    assert rows[0]["rank"] == 1
    assert rows[0]["instrument_id"] == "000001.SZ"
    assert rows[0]["last_price"] == 10.14
    assert rows[1]["rank"] == 2
    assert rows[1]["instrument_id"] == "600000.SH"
    assert adapter.last_meta["tdx_protocol"] == "0x054b"
    assert adapter.last_meta["tdx_category"] == "0x2afd"
    assert adapter.last_meta["tdx_category_name"] == "etf"


def test_tdx_etf_kline_unit():
    client = FakeTdxClient(include_etfs=True)
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("etf_kline_tdx", {"code": "510050.SH", "period": "day", "count": 3})

    assert client.kline_calls == [
        {
            "code": "sh510050",
            "period": "day",
            "start": 0,
            "count": 3,
            "adjust": "none",
            "anchor_date": None,
            "kind": "stock",
        }
    ]
    assert [row["instrument_id"] for row in rows] == ["510050.SH", "510050.SH"]
    assert rows[0]["trade_time"] == "2024-05-31T15:00:00+08:00"
    assert rows[0]["open"] == 10.0
    assert rows[0]["close"] == 10.0
    assert rows[1]["open"] == 8.0
    assert adapter.last_meta["tdx_kline_kind"] == "etf"


def test_tdx_index_and_etf_kline_default_count_is_preview_sized():
    client = FakeTdxClient(include_etfs=True)
    adapter = TdxRequestAdapter(client=client)

    adapter.request("index_kline_tdx", {"code": "000001.SH", "period": "day"})
    adapter.request("etf_kline_tdx", {"code": "510050.SH", "period": "day"})

    assert client.kline_calls[-2]["count"] == 120
    assert client.kline_calls[-1]["count"] == 120


def test_tdx_etf_intraday_today_unit():
    client = FakeTdxClient(include_etfs=True)
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("etf_intraday_today_tdx", {"code": "510050.SH"})

    assert client.today_intraday_calls == [{"code": "sh510050", "include_raw": False}]
    assert [row["instrument_id"] for row in rows] == ["510050.SH", "510050.SH"]
    assert rows[0]["time_label"] == "09:31"
    assert rows[0]["price"] == 10.86
    assert rows[0]["avg_price"] == 10.8417
    assert rows[0]["volume"] == 120
    assert adapter.last_meta["tdx_asset_type"] == "etf"


def test_tdx_etf_intraday_history_unit():
    client = FakeTdxClient(include_etfs=True)
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "etf_intraday_history_tdx",
        {"code": "510050.SH", "trade_date": "20260519"},
    )

    assert client.intraday_calls == [
        {"code": "sh510050", "trade_date": "20260519", "include_raw": False}
    ]
    assert [row["instrument_id"] for row in rows] == ["510050.SH", "510050.SH"]
    assert rows[0]["trade_time"] == "2026-05-19T09:31:00+08:00"
    assert rows[0]["price"] == 10.13
    assert rows[0]["volume"] == 120
    assert adapter.last_meta["tdx_asset_type"] == "etf"


def test_tdx_etf_trades_today_unit():
    client = FakeTdxClient(include_etfs=True)
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("etf_trades_today_tdx", {"code": "510050.SH"})

    assert client.today_trade_calls == [
        {"code": "sh510050", "start": 0, "count": 1800, "include_raw": False}
    ]
    assert [row["instrument_id"] for row in rows] == ["510050.SH", "510050.SH"]
    assert rows[0]["trade_time"] == "14:08"
    assert rows[0]["price"] == 10.86
    assert rows[0]["side"] == "buy"
    assert rows[1]["side"] == "sell"
    assert adapter.last_meta["tdx_asset_type"] == "etf"


def test_tdx_etf_trades_history_unit():
    client = FakeTdxClient(include_etfs=True)
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request(
        "etf_trades_history_tdx",
        {"code": "510050.SH", "trade_date": "2026-05-11"},
    )

    assert client.historical_trade_calls == [
        {
            "code": "sh510050",
            "trade_date": "20260511",
            "start": 0,
            "count": 1800,
            "include_raw": False,
        }
    ]
    assert [row["instrument_id"] for row in rows] == ["510050.SH", "510050.SH"]
    assert rows[0]["trade_date"] == "20260511"
    assert rows[0]["trade_time"] == "14:12"
    assert rows[0]["trade_datetime"] == "2026-05-11T14:12:00+08:00"
    assert rows[0]["price"] == 10.86
    assert adapter.last_meta["tdx_asset_type"] == "etf"


def test_tdx_etf_auction_process_unit():
    client = FakeTdxClient(include_etfs=True)
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("etf_auction_process_tdx", {"code": "510050.SH"})

    assert client.auction_calls == [
        {
            "code": "sh510050",
            "mode_or_selector_raw": 3,
            "start": 0,
            "count": 500,
            "include_raw": False,
        }
    ]
    assert [row["instrument_id"] for row in rows] == ["510050.SH", "510050.SH"]
    assert rows[0]["auction_time"] == "09:15:00"
    assert rows[0]["price"] == 162.12
    assert rows[0]["matched_volume"] == 2568
    assert rows[1]["auction_time"] == "09:15:09"
    assert adapter.last_meta["tdx_asset_type"] == "etf"


def test_tdx_etf_auction_result_unit():
    client = FakeAuctionResultTradeClient()
    adapter = TdxRequestAdapter(client=client)

    rows = adapter.request("etf_auction_result_tdx", {"code": "510050.SH"})

    assert client.today_trade_calls == [
        {"code": "sh510050", "start": 0, "count": 1800, "include_raw": False}
    ]
    assert rows == [
        {
            "instrument_id": "510050.SH",
            "symbol": "510050",
            "tdx_code": "sh510050",
            "exchange": "SSE",
            "auction_time": "09:25",
            "trade_index": 1,
            "price": 10.86,
            "volume": 1200,
            "amount": 1303200.0,
            "order_count": 28,
        }
    ]
    assert adapter.last_meta["tdx_protocol"] == "0x0fc5"
    assert adapter.last_meta["tdx_result_time"] == "09:25"
    assert adapter.last_meta["tdx_asset_type"] == "etf"


def test_tdx_price_decimal_resolution():
    """_price_decimal reads the code table: ETF=3, stock=2, fallback=2."""
    client = FakeTdxClient(include_etfs=True)
    assert tdx_request._price_decimal(client, "sh510050") == 3
    assert tdx_request._price_decimal(client, "sh600000") == 2
    assert tdx_request._price_decimal(client, "sz000001") == 2
    # Unknown / malformed codes degrade to the legacy default.
    assert tdx_request._price_decimal(client, "sh999999") == 2
    assert tdx_request._price_decimal(client, "") == 2
    assert tdx_request._price_decimal(client, None) == 2


def test_tdx_price_from_raw_scaling():
    """Trade/intraday prices rescale by 10**decimal from the raw integer."""
    # raw 3018 -> 3.018 for ETF (decimal=3), 30.18 for decimal=2.
    assert tdx_request._price_from_raw(3018, 3, None) == 3.018
    assert tdx_request._price_from_raw(3018, 2, None) == 30.18
    assert tdx_request._price_from_raw(911, 2, None) == 9.11
    # No raw available: fall back to the already-decoded value.
    assert tdx_request._price_from_raw(None, 3, 9.09) == 9.09
    assert tdx_request._price_from_raw(None, 3, None) is None


def test_tdx_avg_price_from_raw_scaling():
    """Average prices carry two extra decimals: divisor is 10**(decimal+2)."""
    # raw 300757 -> 3.00757 for ETF (decimal=3), raw 91789 -> 9.1789 for stock.
    assert tdx_request._avg_price_from_raw(300757, 3, None) == 3.00757
    assert tdx_request._avg_price_from_raw(91789, 2, None) == 9.1789
    assert tdx_request._avg_price_from_raw(None, 3, 9.18) == 9.18


def test_tdx_rescale_price_snapshot():
    """Snapshot prices (already /100 by the wire) rescale by 10**(2-decimal)."""
    # decimal=2 is a no-op; decimal=3 divides by another 10.
    assert tdx_request._rescale_price(9.09, 2) == 9.09
    assert tdx_request._rescale_price(30.17, 3) == 3.017
    assert tdx_request._rescale_price(None, 3) is None


def test_tdx_trade_row_decimal_rescaling():
    """_normalize_trade_row honors decimal when price_acc_raw is present."""
    series = SimpleNamespace(full_code="sh510050", exchange="sh", code="510050", trade_date=None)
    record = SimpleNamespace(
        trade_time=datetime(2026, 5, 19, 9, 25).time(),
        trade_datetime=None,
        trade_date=None,
        index=0,
        absolute_index=0,
        price=0.3018,  # wrongly /10000 by the wire
        price_acc_raw=3018,
        volume=100,
        order_count=1,
        side="buy",
    )
    etf_row = tdx_request._normalize_trade_row(series, record, decimal=3)
    stock_row = tdx_request._normalize_trade_row(series, record, decimal=2)
    assert etf_row["price"] == 3.018
    assert stock_row["price"] == 30.18


@pytest.mark.integration
def test_tdx_etf_codes():
    adapter = TdxRequestAdapter()
    result = adapter.request("etf_codes_tdx", {"code": ["510050.SH", "159915.SZ"]})
    assert len(result) == 2
    assert result[0]["instrument_id"] == "510050.SH"
    assert result[1]["instrument_id"] == "159915.SZ"
    assert "name" in result[0]
    assert "previous_close" in result[0]


@pytest.mark.integration
def test_tdx_etf_realtime_snapshot():
    adapter = TdxRequestAdapter()
    result = adapter.request("etf_realtime_snapshot_tdx", {"code": "510050.SH"})
    assert len(result) >= 1
    assert result[0]["instrument_id"] == "510050.SH"
    assert "last_price" in result[0]
    assert "change_pct" in result[0]


@pytest.mark.integration
def test_tdx_etf_realtime_rank():
    adapter = TdxRequestAdapter()
    result = adapter.request("etf_realtime_rank_tdx", {"sort": "change_pct", "count": 5})
    assert len(result) <= 5
    if len(result) > 0:
        assert "rank" in result[0]
        assert "instrument_id" in result[0]
        assert "last_price" in result[0]


@pytest.mark.integration
def test_tdx_etf_kline():
    adapter = TdxRequestAdapter()
    result = adapter.request("etf_kline_tdx", {"code": "510050.SH", "period": "day", "count": 3})
    assert len(result) <= 3
    if len(result) > 0:
        assert result[0]["instrument_id"] == "510050.SH"
        assert "trade_time" in result[0]
        assert "open" in result[0]
        assert "close" in result[0]


@pytest.mark.integration
def test_tdx_etf_intraday_today():
    adapter = TdxRequestAdapter()
    result = adapter.request("etf_intraday_today_tdx", {"code": "510050.SH"})
    assert isinstance(result, list)
    if len(result) > 0:
        assert result[0]["instrument_id"] == "510050.SH"
        assert "time_label" in result[0]
        assert "price" in result[0]


@pytest.mark.integration
def test_tdx_etf_intraday_history():
    adapter = TdxRequestAdapter()
    trade_date = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    result = adapter.request("etf_intraday_history_tdx", {"code": "510050.SH", "trade_date": trade_date})
    assert isinstance(result, list)


@pytest.mark.integration
def test_tdx_etf_trades_today():
    adapter = TdxRequestAdapter()
    result = adapter.request("etf_trades_today_tdx", {"code": "510050.SH"})
    assert isinstance(result, list)
    if len(result) > 0:
        assert result[0]["instrument_id"] == "510050.SH"
        assert "trade_time" in result[0]
        assert "price" in result[0]


@pytest.mark.integration
def test_tdx_etf_trades_history():
    adapter = TdxRequestAdapter()
    trade_date = (date.today() - timedelta(days=3)).strftime("%Y%m%d")
    result = adapter.request("etf_trades_history_tdx", {"code": "510050.SH", "trade_date": trade_date})
    assert isinstance(result, list)


@pytest.mark.integration
def test_tdx_etf_auction_process():
    adapter = TdxRequestAdapter()
    result = adapter.request("etf_auction_process_tdx", {"code": "510050.SH"})
    assert isinstance(result, list)


@pytest.mark.integration
def test_tdx_etf_auction_result():
    adapter = TdxRequestAdapter()
    result = adapter.request("etf_auction_result_tdx", {"code": "510050.SH"})
    assert isinstance(result, list)

