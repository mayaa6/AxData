from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TDX_PACKAGE_ROOT = REPO_ROOT / "packages" / "axdata-source-tdx"
TDX_PROVIDER_ID = "axdata.source.tdx_external"
TDX_DOWNLOADER_INTERFACE_NAMES = [
    "stock_codes_tdx",
    "stock_suspensions_tdx",
    "stock_st_list_tdx",
    "stock_daily_share_tdx",
    "stock_daily_price_limit_tdx",
    "stock_capital_changes_tdx",
    "stock_kline_daily_tdx",
    "stock_adj_factor_tdx",
    "stock_limit_ladder_tdx",
    "stock_theme_strength_rank_tdx",
]
BUILTIN_GENERIC_INTERFACE_NAMES = {
    "stock_trade_calendar_exchange",
    "stock_historical_list_exchange",
    "stock_basic_info_exchange",
}
sys.path.insert(0, str(TDX_PACKAGE_ROOT / "src"))
from tests.tdx_plugin_helpers import build_registry_with_local_tdx_plugins, ensure_local_tdx_plugin_paths

from axdata_core import SourceRequestResult, get_downloader_profile, list_downloader_profiles
from axdata_core import downloaders as downloaders_module
from axdata_core.plugins import (
    InterfaceCollectionSpec,
    DownloaderProfile as PluginDownloaderProfile,
    InterfaceSpec,
    ProviderInfo,
    ProviderManifest,
)
from axdata_core.provider_registry import ProviderRegistry


def _core_pythonpath() -> str:
    return str(REPO_ROOT / "libs" / "axdata_core")


def _tdx_provider_pythonpath() -> str:
    return os.pathsep.join([str(TDX_PACKAGE_ROOT / "src"), _core_pythonpath()])


def _assert_quality_ok(quality: dict, *, row_count: int | None = None) -> None:
    assert quality["schema"] == "pass"
    assert quality["primary_key"] == "pass"
    assert quality["row_count"] == "pass"
    assert quality["quality_status"] == "ok"
    assert quality["required_columns_present"] is True
    assert quality["missing_required_columns"] == []
    assert quality["duplicate_key_count"] == 0
    if row_count is not None:
        assert quality["row_count_value"] == row_count


def _write_trade_calendar(data_root: Path, dates: list[str], *, exchange: str = "SZSE") -> None:
    from axdata_core.storage import core_table_partition_path

    partition = core_table_partition_path("trade_cal", data_root) / f"exchange={exchange}"
    partition.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "exchange": exchange,
                "cal_date": date,
                "is_open": True,
                "pretrade_date": None,
            }
            for date in dates
        ]
    ).to_parquet(partition / "part-0.parquet", engine="pyarrow", index=False)


@pytest.fixture(autouse=True)
def _enable_local_tdx_provider(monkeypatch, tmp_path):
    from axdata_core.plugin_config import enable_provider
    import axdata_core.provider_catalog as provider_catalog

    ensure_local_tdx_plugin_paths()
    base_builder = provider_catalog.build_builtin_provider_registry

    def build_registry(**kwargs):
        return build_registry_with_local_tdx_plugins(
            base_builder=base_builder,
            **kwargs,
        )

    monkeypatch.setattr(provider_catalog, "build_builtin_provider_registry", build_registry)

    data_root = tmp_path / "axdata"
    monkeypatch.setenv("AXDATA_DATA_DIR", str(data_root))
    for root in (data_root, tmp_path, tmp_path / "data"):
        enable_provider(TDX_PROVIDER_ID, data_root=root)


def test_axdata_core_import_does_not_eager_load_tdx_downloader_profiles():
    code = (
        "import sys\n"
        "import axdata_core\n"
        "print('builtin=' + str('axdata_core.builtin_providers' in sys.modules))\n"
        "print('downloaders=' + str('axdata_core.downloaders' in sys.modules))\n"
        "print('downloader_registry=' + str('axdata_core.downloader_registry' in sys.modules))\n"
        "print('provider_registry=' + str('axdata_core.provider_registry' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
        "print('tdx_profiles=' + str('axdata_core.adapters.tdx.downloader_profiles' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('tdx_f10_specs=' + str('axdata_core.tdx_f10_specs' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _core_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "builtin=False" in result.stdout
    assert "downloaders=False" in result.stdout
    assert "downloader_registry=False" in result.stdout
    assert "provider_registry=False" in result.stdout
    assert "source_request=False" in result.stdout
    assert "tdx_profiles=False" in result.stdout
    assert "tdx_request=False" in result.stdout
    assert "tdx_f10_specs=False" in result.stdout


def test_downloaders_import_does_not_eager_load_source_request_gateway():
    code = (
        "import sys\n"
        "import axdata_core.downloaders\n"
        "print('downloaders=' + str('axdata_core.downloaders' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "downloaders=True" in result.stdout
    assert "source_request=False" in result.stdout


def test_downloader_engine_import_does_not_eager_load_source_request_or_tdx_runtime():
    code = (
        "import sys\n"
        "import axdata_core.downloader_engine\n"
        "tracked = [\n"
        "    'axdata_core.source_request',\n"
        "    'axdata_core.downloader_registry',\n"
        "    'axdata_core.adapters.tdx.downloader',\n"
        "    'axdata_core.adapters.tdx.request',\n"
        "    'axdata_core._tdx_wire.client',\n"
        "]\n"
        "print('loaded=' + ','.join(name for name in tracked if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "loaded=\n" in result.stdout


def test_root_download_request_plan_export_uses_engine_without_loading_downloaders():
    code = (
        "import sys\n"
        "import axdata_core\n"
        "print('downloaders_before=' + str('axdata_core.downloaders' in sys.modules))\n"
        "_ = axdata_core.DownloadRequestPlan\n"
        "_ = axdata_core.DownloadWriter\n"
        "print('engine_after=' + str('axdata_core.downloader_engine' in sys.modules))\n"
        "print('downloaders_after=' + str('axdata_core.downloaders' in sys.modules))\n"
        "print('source_request_after=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "libs" / "axdata_core"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "downloaders_before=False" in result.stdout
    assert "engine_after=True" in result.stdout
    assert "downloaders_after=False" in result.stdout
    assert "source_request_after=False" in result.stdout


def test_downloader_registry_tdx_factories_are_lazy():
    code = (
        "import sys\n"
        "from axdata_core.downloader_registry import (\n"
        "    load_builtin_downloader_adapter_factories,\n"
        "    load_builtin_runtime_source_server_max_factories,\n"
        ")\n"
        "adapter_factories = load_builtin_downloader_adapter_factories()\n"
        "runtime_factories = load_builtin_runtime_source_server_max_factories()\n"
        "print('adapter_factories=' + ','.join(sorted(adapter_factories)))\n"
        "print('runtime_factories=' + ','.join(sorted(runtime_factories)))\n"
        "print('downloaders=' + str('axdata_core.downloaders' in sys.modules))\n"
        "print('downloader_registry=' + str('axdata_core.downloader_registry' in sys.modules))\n"
        "print('tdx_downloader=' + str('axdata_core.adapters.tdx.downloader' in sys.modules))\n"
        "print('tdx_profiles=' + str('axdata_core.adapters.tdx.downloader_profiles' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('tdx_wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
        "print('tdx_f10_specs=' + str('axdata_core.tdx_f10_specs' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "adapter_factories=tdx" in result.stdout
    assert "runtime_factories=tdx" in result.stdout
    assert "downloaders=False" in result.stdout
    assert "downloader_registry=True" in result.stdout
    assert "tdx_downloader=False" in result.stdout
    assert "tdx_profiles=False" in result.stdout
    assert "tdx_request=False" in result.stdout
    assert "tdx_wire_client=False" in result.stdout
    assert "tdx_f10_specs=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_downloader_compat_option_helpers_do_not_load_request_runtime():
    code = (
        "import sys\n"
        "from axdata_core.adapters.tdx.downloader import tdx_adapter_options\n"
        "from axdata_core.adapters.tdx.server_cache import tdx_server_cache_root\n"
        "options = tdx_adapter_options(\n"
        "    interface_name='stock_limit_ladder_tdx',\n"
        "    pool_size=1,\n"
        "    server_cache_root='server-cache',\n"
        "    stats_cache_root='stats-cache',\n"
        ")\n"
        "server_cache_root = tdx_server_cache_root('demo-root')\n"
        "print('option_keys=' + ','.join(sorted(options or {})))\n"
        "print('server_cache_root=' + server_cache_root.replace('\\\\', '/'))\n"
        "print('tdx_downloader=' + str('axdata_core.adapters.tdx.downloader' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('tdx_client_factory=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "print('tdx_host_config=' + str('axdata_core.adapters.tdx.host_config' in sys.modules))\n"
        "print('tdx_server_config=' + str('axdata_core.tdx_server_config' in sys.modules))\n"
        "print('tdx_wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
        "print('tdx_f10_specs=' + str('axdata_core.tdx_f10_specs' in sys.modules))\n"
        "print('source_request=' + str('axdata_core.source_request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert (
        "option_keys=f10_topic_refill_rounds,f10_topic_refill_workers,"
        "f10_topic_workers,server_cache_root,stats_cache_root"
    ) in result.stdout
    assert "server_cache_root=" in result.stdout
    assert "/demo-root/cache/tdx_servers" in result.stdout
    assert "tdx_downloader=True" in result.stdout
    assert "tdx_request=False" in result.stdout
    assert "tdx_client_factory=False" in result.stdout
    assert "tdx_host_config=False" in result.stdout
    assert "tdx_server_config=False" in result.stdout
    assert "tdx_wire_client=False" in result.stdout
    assert "tdx_f10_specs=False" in result.stdout
    assert "source_request=False" in result.stdout


def test_tdx_request_limits_import_does_not_load_wire_protocol_constants():
    code = (
        "import sys\n"
        "from axdata_core.adapters.tdx import request_limits\n"
        "print('batch_size=' + str(request_limits.DEFAULT_QUOTE_BATCH_SIZE))\n"
        "print('wire_constants=' + str('axdata_core._tdx_wire.protocol.constants' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "batch_size=80" in result.stdout
    assert "wire_constants=False" in result.stdout


def test_stock_codes_downloader_profile_is_registered():
    profiles = list_downloader_profiles()

    names = [profile["interface_name"] for profile in profiles]
    assert names[: len(TDX_DOWNLOADER_INTERFACE_NAMES)] == TDX_DOWNLOADER_INTERFACE_NAMES
    assert set(names) == {*TDX_DOWNLOADER_INTERFACE_NAMES, *BUILTIN_GENERIC_INTERFACE_NAMES}
    profile = get_downloader_profile("stock_codes_tdx")
    assert profile.downloader_type == "full_snapshot"
    assert profile.output_layer == "snapshot"
    assert profile.default_params == {"scope": "all"}
    assert profile.primary_key == "instrument_id"
    assert profile.supported_formats == ["parquet", "csv", "jsonl"]
    assert profile.default_connection_mode == "long_connection"
    assert profile.default_connection_count == 3
    assert profile.connection_count_editable is False
    assert profile.max_connection_count == 3
    profile_dict = profile.to_dict()
    assert profile_dict["concurrency"] == {
        "mode": "fixed",
        "mode_editable": False,
        "default_source_server_count": 1,
        "source_server_count_editable": False,
        "max_source_server_count": 1,
        "default_connections_per_server": 3,
        "connections_per_server_editable": False,
        "max_connections_per_server": 3,
        "default_max_concurrent_tasks": 3,
        "max_concurrent_tasks_editable": False,
        "max_max_concurrent_tasks": 3,
        "default_batch_size": 1,
        "batch_size_editable": False,
        "max_batch_size": 1,
        "default_request_interval_ms": 0,
        "request_interval_ms_editable": False,
        "min_request_interval_ms": 0,
        "max_request_interval_ms": 0,
        "default_retry_count": 0,
        "retry_count_editable": False,
        "max_retry_count": 0,
        "default_timeout_ms": 30000,
        "timeout_ms_editable": False,
        "min_timeout_ms": 30000,
        "max_timeout_ms": 30000,
        "default_connection_count": 3,
        "connection_count_editable": False,
        "max_connection_count": 3,
        "description": "这个接口会并行扫描沪、深、北三个市场；采集完成后会自动断开连接。",
    }


def test_downloader_profile_projects_resolved_provider_manifest(monkeypatch, tmp_path):
    manifest = ProviderManifest(
        provider=ProviderInfo(
            provider_id="axdata.source.runtime_demo",
            source_code="runtime_demo",
            source_name_zh="运行时示例",
            version="0.1.0",
        ),
        interfaces=(
                InterfaceSpec(
                    name="stock_codes_tdx",
                    display_name_zh="运行时股票列表",
                    source_code="runtime_demo",
                    source_name_zh="运行时示例",
                    asset_class="stock",
                    collection=InterfaceCollectionSpec(
                        supported=True,
                        default_profile="runtime_demo.stock_codes.snapshot",
                    ),
                ),
            ),
            downloaders=(
            PluginDownloaderProfile(
                name="runtime_demo.stock_codes.snapshot",
                interface_name="stock_codes_tdx",
                display_name_zh="运行时股票采集",
                resource_group="runtime.demo.quote",
                mode="snapshot",
                default_options={
                    "params": {"scope": "stock"},
                    "fields": ["instrument_id", "symbol"],
                    "formats": ["jsonl"],
                    "connection_mode": "connection_pool",
                },
                default_limits={"max_connections_total": 7},
                output={
                    "supported_formats": ["jsonl", "csv"],
                    "output_layer": "staging",
                },
            ),
        ),
    )
    registry = ProviderRegistry(enabled_provider_ids={"axdata.source.runtime_demo"})
    registry.register_manifest(manifest)

    import axdata_core.provider_catalog as provider_catalog

    monkeypatch.setattr(
        provider_catalog,
        "build_builtin_provider_registry",
        lambda **_kwargs: registry,
    )

    profile = get_downloader_profile("stock_codes_tdx", data_root=tmp_path)
    profiles = list_downloader_profiles(data_root=tmp_path)

    assert [item["interface_name"] for item in profiles][:1] == ["stock_codes_tdx"]
    assert profile.provider_id == "axdata.source.runtime_demo"
    assert profile.manifest_downloader_name == "runtime_demo.stock_codes.snapshot"
    assert profile.effective_trust_level == "community"
    assert profile.built_in_provider is False
    assert profile.display_name == "运行时股票采集"
    assert profile.resource_group == "runtime.demo.quote"
    assert profile.default_params == {"scope": "stock"}
    assert profile.default_fields == ["instrument_id", "symbol"]
    assert profile.output_format == "jsonl"
    assert profile.supported_formats == ["jsonl", "csv"]
    assert profile.output_layer == "staging"
    assert profile.default_connection_mode == "connection_pool"
    assert profile.adapter_factory is None
    assert profile.runtime_source_server_max_factory is None
    assert profile.default_connection_count == 3
    assert profile.max_connection_count == 3
    assert profile.manifest_default_limits == {"max_connections_total": 7}
    assert profiles[0]["provider_id"] == "axdata.source.runtime_demo"
    assert profiles[0]["adapter_factory"] is None


def test_get_downloader_profile_respects_provider_disable(tmp_path):
    from axdata_core.plugin_config import disable_provider

    data_root = tmp_path / "data"
    disable_provider(TDX_PROVIDER_ID, data_root=data_root)

    try:
        get_downloader_profile("stock_codes_tdx", data_root=data_root)
    except downloaders_module.DownloaderError as exc:
        assert "not available" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("DownloaderError was not raised")

    profiles = list_downloader_profiles(data_root=data_root)
    names = {profile["interface_name"] for profile in profiles}
    assert "stock_codes_tdx" not in names
    assert names == BUILTIN_GENERIC_INTERFACE_NAMES


def test_builtin_generic_downloader_profiles_are_registered_without_tdx(tmp_path):
    from axdata_core.plugin_config import disable_provider

    data_root = tmp_path / "data"
    disable_provider(TDX_PROVIDER_ID, data_root=data_root)
    profiles = {profile["interface_name"]: profile for profile in list_downloader_profiles(data_root=data_root)}

    assert set(profiles) == BUILTIN_GENERIC_INTERFACE_NAMES
    historical_list = get_downloader_profile("stock_historical_list_exchange", data_root=data_root)
    assert historical_list.provider_id == "axdata.source.exchange"
    assert historical_list.manifest_downloader_name == "stock_historical_list_exchange.snapshot"
    assert historical_list.downloader_type == "snapshot"
    assert historical_list.resource_group == "exchange.http"
    assert historical_list.default_params == {"trade_date": "20260102"}
    assert historical_list.output_layer == "snapshot"
    assert historical_list.primary_key == ("trade_date", "instrument_id")
    assert historical_list.supported_formats == ["parquet", "csv", "jsonl"]
    assert historical_list.default_connection_mode == "long_connection"
    assert historical_list.default_connection_count == 1
    assert historical_list.default_output_path_parts == ["交易所", "基础数据", "stock_historical_list_exchange"]
    assert historical_list.manifest_output["file_name_template"] == "{interface_name}_{data_date}_{run_time}"

    stock_basic = profiles["stock_basic_info_exchange"]
    assert stock_basic["provider_id"] == "axdata.source.exchange"
    assert stock_basic["primary_key"] == "instrument_id"
    assert stock_basic["output_layer"] == "core"

    with pytest.raises(downloaders_module.DownloaderError, match="not available"):
        get_downloader_profile("tencent_realtime_snapshot", data_root=data_root)


def test_builtin_generic_downloader_writes_formats_and_duckdb_can_read(monkeypatch, tmp_path):
    calls = []

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        calls.append(
            {
                "interface_name": interface_name,
                "params": dict(params),
                "fields": list(fields) if fields is not None else None,
                "persist": persist,
                "adapter": adapter,
                "options": dict(options or {}),
                "data_root": data_root,
            }
        )
        return SourceRequestResult(
            records=[
                {
                    "trade_date": "20260102",
                    "instrument_id": "000001.SZ",
                    "symbol": "000001",
                    "exchange": "SZSE",
                    "name": "平安银行",
                    "market": "主板",
                    "list_date": "19910403",
                    "delist_date": None,
                    "listing_status": "listed",
                }
            ],
            meta={"source": "exchange", "snapshot_date": "20260102"},
        )

    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    result = downloaders_module.run_downloader(
        "stock_historical_list_exchange",
        data_root=tmp_path / "data",
        output_root=tmp_path / "out",
        formats=["parquet", "csv", "jsonl"],
    )

    assert result["status"] == "success"
    assert result["row_count"] == 1
    assert result["output_formats"] == ["parquet", "csv", "jsonl"]
    assert set(result["output_paths"]) == {"parquet", "csv", "jsonl"}
    _assert_quality_ok(result["quality"], row_count=1)
    assert result["quality"]["date_range"] == {
        "min": "20260102",
        "max": "20260102",
    }
    assert calls == [
        {
            "interface_name": "stock_historical_list_exchange",
            "params": {"trade_date": "20260102"},
            "fields": [
                "trade_date",
                "instrument_id",
                "symbol",
                "exchange",
                "name",
                "market",
                "list_date",
                "delist_date",
                "listing_status",
            ],
            "persist": False,
            "adapter": None,
            "options": {"source_server_count": 1, "connections_per_server": 1},
            "data_root": tmp_path / "data",
        }
    ]

    parquet_path = Path(result["output_paths"]["parquet"])
    csv_path = Path(result["output_paths"]["csv"])
    jsonl_path = Path(result["output_paths"]["jsonl"])
    assert parquet_path.is_file()
    assert csv_path.is_file()
    assert jsonl_path.is_file()
    assert parquet_path.parent == tmp_path / "out" / "交易所" / "基础数据" / "stock_historical_list_exchange" / "parquet"
    assert Path(result["log_path"]).is_file()

    import duckdb

    rows = duckdb.connect(database=":memory:").execute(
        "SELECT instrument_id, name FROM read_parquet(?)",
        [str(parquet_path)],
    ).fetchall()
    assert rows == [("000001.SZ", "平安银行")]


def test_download_writer_writes_duckdb_output(tmp_path):
    from axdata_core.downloader_engine import DownloadWriter

    frame = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "20260618", "adj_factor": 1.0},
            {"ts_code": "600000.SH", "trade_date": "20260618", "adj_factor": 0.99},
        ]
    )
    output_path = tmp_path / "adj_factor.duckdb"

    DownloadWriter().write_frame_atomic(frame, output_path, "duckdb")

    assert output_path.is_file()
    import duckdb

    rows = duckdb.connect(str(output_path)).execute(
        "SELECT ts_code, adj_factor FROM data ORDER BY ts_code"
    ).fetchall()
    assert rows == [("000001.SZ", 1.0), ("600000.SH", 0.99)]


def test_plugin_declared_downloader_without_builtin_profile_can_run(monkeypatch, tmp_path):
    manifest = ProviderManifest(
        provider=ProviderInfo(
            provider_id="axdata.source.generic_demo",
            source_code="generic_demo",
            source_name_zh="通用示例",
            version="0.1.0",
        ),
        interfaces=(
            InterfaceSpec(
                name="generic_snapshot_demo",
                display_name_zh="通用快照",
                source_code="generic_demo",
                source_name_zh="通用示例",
                asset_class="stock",
                collection=InterfaceCollectionSpec(
                    supported=True,
                    default_profile="generic_demo.snapshot",
                ),
            ),
        ),
        downloaders=(
            PluginDownloaderProfile(
                name="generic_demo.snapshot",
                interface_name="generic_snapshot_demo",
                display_name_zh="通用示例采集",
                resource_group="generic.http",
                mode="snapshot",
                default_options={
                    "params": {"scope": "all"},
                    "fields": ["instrument_id", "name"],
                    "formats": ["jsonl"],
                    "source_server_count": 2,
                    "connections_per_server": 3,
                    "max_concurrent_tasks": 5,
                    "batch_size": 50,
                    "request_interval_ms": 100,
                    "retry_count": 2,
                    "timeout_ms": 12000,
                },
                default_limits={"max_connections_total": 6, "request_interval_ms": 100, "max_retries": 2},
                output={
                    "default_dir_name": "generic_snapshot_demo",
                    "file_name_template": "{interface_name}_{data_date}_{run_time}",
                    "supported_formats": ["jsonl", "csv"],
                    "output_layer": "staging",
                    "primary_key": "instrument_id",
                },
            ),
        ),
    )
    registry = ProviderRegistry(enabled_provider_ids={"axdata.source.generic_demo"})
    registry.register_manifest(manifest)

    import axdata_core.provider_catalog as provider_catalog

    monkeypatch.setattr(
        provider_catalog,
        "build_builtin_provider_registry",
        lambda **_kwargs: registry,
    )

    calls = []

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        calls.append(
            {
                "interface_name": interface_name,
                "params": dict(params),
                "fields": list(fields) if fields is not None else None,
                "persist": persist,
                "adapter": adapter,
                "options": dict(options or {}),
                "data_root": data_root,
            }
        )
        return SourceRequestResult(
            records=[{"instrument_id": "000001.SZ", "name": "平安银行"}],
            meta={"source": "generic_demo", "snapshot_date": "20260620"},
        )

    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    profile = get_downloader_profile("generic_snapshot_demo", data_root=tmp_path / "data")
    profiles = list_downloader_profiles(data_root=tmp_path / "data")
    result = downloaders_module.run_downloader(
        "generic_snapshot_demo",
        data_root=tmp_path / "data",
        output_root=tmp_path / "out",
    )

    assert profile.provider_id == "axdata.source.generic_demo"
    assert profile.manifest_downloader_name == "generic_demo.snapshot"
    assert profile.adapter_factory is None
    assert profile.resource_group == "generic.http"
    assert profile.default_connection_count == 5
    assert profile.concurrency.default_source_server_count == 2
    assert profile.concurrency.default_connections_per_server == 3
    assert profile.concurrency.default_max_concurrent_tasks == 5
    assert [item["interface_name"] for item in profiles] == ["generic_snapshot_demo"]
    assert result["status"] == "success"
    assert result["output_formats"] == ["jsonl"]
    output_path = Path(result["output_path"])
    assert output_path.parent == tmp_path / "out" / "通用示例" / "staging" / "generic_snapshot_demo" / "jsonl"
    assert output_path.name.startswith("generic_snapshot_demo_20260620_")
    assert output_path.name.endswith(".jsonl")
    assert calls == [
        {
            "interface_name": "generic_snapshot_demo",
            "params": {"scope": "all"},
            "fields": ["instrument_id", "name"],
            "persist": False,
            "adapter": None,
            "options": {"source_server_count": 2, "connections_per_server": 3},
            "data_root": tmp_path / "data",
        }
    ]


def test_stock_suspensions_downloader_profile_is_registered():
    profile = get_downloader_profile("stock_suspensions_tdx")

    assert profile.display_name == "最新停牌列表"
    assert profile.downloader_type == "full_snapshot"
    assert profile.default_params == {"scope": "all"}
    assert profile.primary_key == "instrument_id"
    assert profile.supported_formats == ["parquet", "csv", "jsonl"]
    assert profile.default_connection_mode == "long_connection"
    assert profile.default_connection_count == 8
    assert profile.connection_count_editable is False
    assert profile.max_connection_count == 8
    assert profile.concurrency.default_source_server_count == 4
    assert profile.concurrency.default_connections_per_server == 2
    assert profile.concurrency.default_max_concurrent_tasks == 8
    assert profile.concurrency.default_batch_size == 80
    assert "停牌状态" in profile.concurrency.description
    assert "4 × 2" in profile.concurrency.description


def test_stock_st_list_downloader_profile_is_registered():
    profile = get_downloader_profile("stock_st_list_tdx")

    assert profile.display_name == "最新ST股票列表"
    assert profile.downloader_type == "full_snapshot"
    assert profile.default_params == {"scope": "all"}
    assert profile.primary_key == "instrument_id"
    assert profile.supported_formats == ["parquet", "csv", "jsonl"]
    assert profile.default_connection_mode == "long_connection"
    assert profile.default_connection_count == 3
    assert profile.connection_count_editable is False
    assert profile.max_connection_count == 3


def test_stock_daily_share_downloader_profile_is_registered():
    profile = get_downloader_profile("stock_daily_share_tdx")

    assert profile.display_name == "每日股本（盘前）"
    assert profile.downloader_type == "full_snapshot"
    assert profile.default_params == {"scope": "all"}
    assert profile.default_fields == [
        "trade_date",
        "instrument_id",
        "symbol",
        "tdx_code",
        "exchange",
        "total_share",
        "float_share",
        "free_float_share_z",
        "finance_updated_date",
        "share_source",
    ]
    assert profile.primary_key == ("trade_date", "instrument_id")
    assert profile.supported_formats == ["parquet", "csv", "jsonl"]
    assert profile.default_connection_mode == "long_connection"
    assert profile.default_connection_count == 8
    assert profile.connection_count_editable is False
    assert profile.max_connection_count == 8
    assert profile.concurrency.mode == "fixed"
    assert profile.concurrency.mode_editable is False
    assert profile.concurrency.default_source_server_count == 4
    assert profile.concurrency.source_server_count_editable is False
    assert profile.concurrency.max_source_server_count == 4
    assert profile.concurrency.default_connections_per_server == 2
    assert profile.concurrency.connections_per_server_editable is False
    assert profile.concurrency.max_connections_per_server == 2
    assert profile.concurrency.default_max_concurrent_tasks == 8
    assert profile.concurrency.max_concurrent_tasks_editable is False
    assert profile.concurrency.max_max_concurrent_tasks == 8
    assert profile.concurrency.default_batch_size == 80
    assert profile.concurrency.batch_size_editable is False
    assert profile.params is not None
    assert profile.params[0][0] == "scope"
    assert profile.params[1] == ["code", "string/list", "否", "证券代码：可选；不填则按股票范围拉取全量"]


def test_stock_daily_price_limit_downloader_profile_is_registered():
    profile = get_downloader_profile("stock_daily_price_limit_tdx")

    assert profile.display_name == "涨跌停价格"
    assert profile.downloader_type == "full_snapshot"
    assert profile.default_params == {"scope": "all"}
    assert profile.default_fields == [
        "trade_date",
        "instrument_id",
        "symbol",
        "tdx_code",
        "exchange",
        "name",
        "name_flag",
        "pre_close_trade_date",
        "pre_close",
        "pre_close_source",
        "limit_up_price",
        "limit_down_price",
        "limit_ratio_pct",
        "limit_rule",
        "limit_status",
    ]
    assert profile.primary_key == ("trade_date", "instrument_id")
    assert profile.supported_formats == ["parquet", "csv", "jsonl"]
    assert profile.default_connection_mode == "long_connection"
    assert profile.default_connection_count == 8
    assert profile.connection_count_editable is False
    assert profile.max_connection_count == 8
    assert profile.concurrency.mode == "fixed"
    assert profile.concurrency.default_source_server_count == 4
    assert profile.concurrency.default_connections_per_server == 2
    assert profile.concurrency.default_max_concurrent_tasks == 8
    assert profile.concurrency.default_batch_size == 80
    assert profile.concurrency.batch_size_editable is False
    assert profile.params is not None
    assert len(profile.params) == 2
    assert profile.params[0][0] == "scope"
    assert profile.params[1] == ["code", "string/list", "否", "证券代码：可选；不填则按股票范围拉取全量"]


def test_stock_capital_changes_downloader_profile_is_registered():
    profile = get_downloader_profile("stock_capital_changes_tdx")

    assert profile.display_name == "股本变迁"
    assert profile.downloader_type == "full_snapshot"
    assert profile.default_params == {"scope": "all"}
    assert profile.default_fields == [
        "instrument_id",
        "ts_code",
        "symbol",
        "tdx_code",
        "exchange",
        "event_date",
        "category_raw",
        "category_name",
        "c1",
        "c2",
        "c3",
        "c4",
        "c1_raw_hex",
        "c2_raw_hex",
        "c3_raw_hex",
        "c4_raw_hex",
        "record_hex",
    ]
    assert profile.primary_key == ("instrument_id", "record_hex")
    assert profile.supported_formats == ["parquet", "csv", "jsonl"]
    assert profile.default_connection_mode == "long_connection"
    assert profile.default_connection_count == 16
    assert profile.connection_count_editable is False
    assert profile.max_connection_count == 16
    assert profile.concurrency.mode == "fixed"
    assert profile.concurrency.default_source_server_count == 8
    assert profile.concurrency.default_connections_per_server == 2
    assert profile.concurrency.default_max_concurrent_tasks == 16
    assert profile.concurrency.default_batch_size == 1
    assert profile.concurrency.batch_size_editable is False
    assert profile.params is not None
    assert [row[0] for row in profile.params] == ["scope", "code", "category"]


def test_stock_kline_daily_downloader_profile_is_registered():
    profile = get_downloader_profile("stock_kline_daily_tdx")

    assert profile.display_name == "日K线"
    assert profile.downloader_type == "history"
    assert profile.default_params == {
        "code": "000001.SZ",
        "count": 800,
        "adjust": "none",
    }
    assert profile.default_fields == [
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
    ]
    assert profile.primary_key == ("instrument_id", "trade_time", "period")
    assert profile.output_layer == "core"
    assert profile.supported_formats == ["parquet", "csv", "jsonl"]
    assert profile.default_connection_mode == "long_connection"
    assert profile.default_connection_count == 1
    assert profile.connection_count_editable is False
    assert profile.max_connection_count == 1
    assert profile.concurrency.default_source_server_count == 1
    assert profile.concurrency.default_connections_per_server == 1
    assert profile.concurrency.default_max_concurrent_tasks == 1
    assert profile.concurrency.default_batch_size == 1
    assert profile.write_mode == "snapshot"
    assert profile.partition_by == ["trade_date"]
    assert profile.params is not None
    assert [row[0] for row in profile.params] == ["code", "count", "adjust", "anchor_date"]


def test_stock_adj_factor_downloader_profile_is_registered():
    profile = get_downloader_profile("stock_adj_factor_tdx")

    assert profile.display_name == "复权因子"
    assert profile.downloader_type == "history"
    assert profile.default_params == {
        "code": "000001.SZ",
        "adjust": "qfq",
    }
    assert profile.default_fields == [
        "instrument_id",
        "ts_code",
        "symbol",
        "tdx_code",
        "exchange",
        "trade_date",
        "adj_factor",
    ]
    assert profile.primary_key == ("ts_code", "trade_date")
    assert profile.output_layer == "core"
    assert profile.supported_formats == ["parquet", "csv", "jsonl"]
    assert profile.default_connection_mode == "long_connection"
    assert profile.default_connection_count == 1
    assert profile.connection_count_editable is False
    assert profile.max_connection_count == 1
    assert profile.concurrency.default_source_server_count == 1
    assert profile.concurrency.default_connections_per_server == 1
    assert profile.concurrency.default_max_concurrent_tasks == 1
    assert profile.concurrency.default_batch_size == 1
    assert profile.write_mode == "upsert_by_key"
    assert profile.partition_by == ["trade_date"]
    assert profile.params is not None
    assert [row[0] for row in profile.params] == ["code", "adjust", "anchor_date"]


def test_stock_limit_ladder_downloader_profile_is_registered():
    profile = get_downloader_profile("stock_limit_ladder_tdx")

    assert profile.display_name == "连板天梯"
    assert profile.downloader_type == "full_snapshot"
    assert profile.default_params == {
        "count": "all",
        "scope": "main",
        "include_touched": False,
        "topic_type": "theme",
    }
    assert profile.default_fields == [
        "trade_date",
        "ladder_level",
        "limit_board_text",
        "instrument_id",
        "name",
        "last_price",
        "change_pct",
        "limit_status",
        "amount",
        "seal_amount",
        "seal_to_amount_ratio",
        "free_float_market_value",
        "primary_theme",
        "secondary_themes",
        "year_limit_up_days",
        "symbol",
        "exchange",
        "pre_close",
        "limit_up_price",
    ]
    assert profile.primary_key == ("trade_date", "ladder_level", "instrument_id")
    assert profile.supported_formats == ["parquet", "csv", "jsonl"]
    assert profile.default_connection_mode == "long_connection"
    assert profile.default_connection_count == 1
    assert profile.connection_count_editable is False
    assert profile.max_connection_count == 1
    assert profile.concurrency.mode == "fixed"
    assert profile.concurrency.default_source_server_count == 1
    assert profile.concurrency.default_connections_per_server == 1
    assert profile.concurrency.default_max_concurrent_tasks == 1
    assert profile.concurrency.default_batch_size == 1
    assert "6 个 F10 题材查询 worker" in profile.concurrency.description
    assert "6 个 worker 补漏 1 轮" in profile.concurrency.description
    assert profile.params is not None
    assert [row[0] for row in profile.params] == ["count", "scope", "include_touched", "topic_type"]


def test_stock_theme_strength_rank_downloader_profile_is_registered():
    profile = get_downloader_profile("stock_theme_strength_rank_tdx")

    assert profile.display_name == "题材强度排行"
    assert profile.downloader_type == "full_snapshot"
    assert profile.default_params == {
        "count": "all",
        "scope": "main",
        "topic_type": "theme",
    }
    assert profile.default_fields == [
        "rank",
        "trade_date",
        "topic_type",
        "topic_name",
        "topic_id",
        "theme_strength_score",
        "limit_up_count",
        "highest_ladder_level",
        "lianban_stock_count",
        "first_board_count",
        "leader_instrument_id",
        "leader_name",
        "leader_ladder_level",
        "leader_limit_board_text",
        "leader_seal_amount",
        "seal_amount_sum",
        "amount_sum",
        "top_stock_summary",
    ]
    assert profile.primary_key == ("trade_date", "topic_type", "topic_name")
    assert profile.supported_formats == ["parquet", "csv", "jsonl"]
    assert profile.default_connection_mode == "long_connection"
    assert profile.default_connection_count == 1
    assert profile.connection_count_editable is False
    assert profile.max_connection_count == 1
    assert profile.concurrency.mode == "fixed"
    assert profile.concurrency.default_source_server_count == 1
    assert profile.concurrency.default_connections_per_server == 1
    assert profile.concurrency.default_max_concurrent_tasks == 1
    assert profile.concurrency.default_batch_size == 1
    assert "6 个 F10 题材查询 worker" in profile.concurrency.description
    assert "6 个 worker 补漏 1 轮" in profile.concurrency.description
    assert profile.params is not None
    assert [row[0] for row in profile.params] == ["count", "scope", "topic_type"]


def test_build_request_plan_filters_downloader_params_and_records_concurrency():
    profile = get_downloader_profile("stock_daily_price_limit_tdx")
    concurrency = downloaders_module._normalize_concurrency(profile)

    plan = downloaders_module.build_request_plan(
        profile,
        params={"scope": "all", "trade_date": "20260617", "code": "000001.SZ"},
        fields=None,
        concurrency=concurrency,
    )

    assert plan.interface_name == "stock_daily_price_limit_tdx"
    assert plan.params == {"scope": "all", "code": "000001.SZ"}
    assert plan.fields == profile.default_fields
    assert plan.options["concurrency"] == concurrency.to_dict()


@pytest.mark.parametrize(
    "interface_name",
    [
        "stock_kline_daily_tdx",
        "stock_daily_price_limit_tdx",
        "stock_codes_tdx",
        "stock_limit_ladder_tdx",
    ],
)
def test_build_request_plan_only_forwards_params_the_source_contract_accepts(interface_name):
    """Planned params must pass the real gateway validation, not just a mock.

    The daily-kline profile declares ``count`` as a sample size while the source
    gateway deliberately rejects paging params, so forwarding the profile's
    defaults verbatim made client.download() fail at runtime. The download tests
    mock request_interface, so only checking against the live contract catches it.
    """

    from axdata_core.source_request import _contract_for_interface, validate_params

    profile = get_downloader_profile(interface_name)
    plan = downloaders_module.build_request_plan(
        profile,
        params=None,
        fields=None,
        concurrency=downloaders_module._normalize_concurrency(profile),
    )

    validate_params(_contract_for_interface(interface_name), plan.params)


def test_build_request_plan_keeps_kline_count_as_a_local_sample_size():
    profile = get_downloader_profile("stock_kline_daily_tdx")

    plan = downloaders_module.build_request_plan(
        profile,
        params={"code": "000001.SZ", "count": 5},
        fields=None,
        concurrency=downloaders_module._normalize_concurrency(profile),
    )

    assert "count" not in plan.params
    assert plan.local_params == {"count": 5}


def test_local_row_limit_keeps_most_recent_rows_per_instrument():
    profile = get_downloader_profile("stock_kline_daily_tdx")
    records = [
        {"instrument_id": code, "trade_time": f"2026-06-{day:02d}T15:00:00+08:00", "close": day}
        for code in ("000001.SZ", "600000.SH")
        for day in (16, 17, 18)
    ]

    limited = downloaders_module._apply_local_row_limit(records, {"count": 2}, profile)

    assert len(limited) == 4
    assert [row["trade_time"][:10] for row in limited if row["instrument_id"] == "000001.SZ"] == [
        "2026-06-17",
        "2026-06-18",
    ]
    # A non-numeric limit (e.g. the "all" some profiles default to) trims nothing.
    assert downloaders_module._apply_local_row_limit(records, {"count": "all"}, profile) == records


def test_downloader_engine_writer_quality_and_metadata_are_source_neutral(tmp_path):
    from axdata_core.downloader_engine import (
        DownloadMetadataWriter,
        DownloadQualityChecker,
        DownloadWriter,
    )

    frame = pd.DataFrame.from_records(
        [{"instrument_id": "000001.SZ", "name": "平安银行"}]
    )
    output_dir = tmp_path / "out"
    writer = DownloadWriter()
    quality = DownloadQualityChecker()
    metadata = DownloadMetadataWriter()

    output_paths = writer.write_outputs(
        get_downloader_profile("stock_codes_tdx"),
        frame,
        output_dir=output_dir,
        file_stem="demo_snapshot",
        formats=["csv", "jsonl"],
    )
    quality_result = quality.evaluate(
        frame,
        primary_key="instrument_id",
        rules=["schema", "primary_key", "row_count"],
    )
    log_path = metadata.write_run_log(
        output_dir,
        file_stem="demo_snapshot",
        result={
            "job_id": "run_demo",
            "status": "success",
            "source_meta": {"source": "demo"},
        },
    )

    assert set(output_paths) == {"csv", "jsonl"}
    assert output_paths["csv"].exists()
    assert output_paths["jsonl"].exists()
    _assert_quality_ok(quality_result, row_count=1)
    payload = json.loads(log_path.read_text(encoding="utf-8"))
    assert payload["job_id"] == "run_demo"
    assert payload["source_meta"] == {"source": "demo"}


def test_downloader_engine_upsert_by_key_rewrites_existing_parquet_without_duplicates(tmp_path):
    from axdata_core.downloader_engine import DownloadWriter

    writer = DownloadWriter()
    output_path = tmp_path / "daily.parquet"
    pd.DataFrame.from_records(
        [
            {"ts_code": "000001.SZ", "trade_date": "20240102", "close": 10.0},
            {"ts_code": "000002.SZ", "trade_date": "20240102", "close": 20.0},
        ]
    ).to_parquet(output_path, engine="pyarrow", index=False)

    metadata = writer.write_parquet_with_mode(
        pd.DataFrame.from_records(
            [
                {"ts_code": "000001.SZ", "trade_date": "20240102", "close": 10.8},
                {"ts_code": "000003.SZ", "trade_date": "20240102", "close": 30.0},
            ]
        ),
        output_path,
        write_mode="upsert_by_key",
        primary_key=("ts_code", "trade_date"),
        date_field="trade_date",
    )

    rows = pd.read_parquet(output_path, engine="pyarrow").sort_values("ts_code").to_dict(orient="records")
    assert rows == [
        {"ts_code": "000001.SZ", "trade_date": "20240102", "close": 10.8},
        {"ts_code": "000002.SZ", "trade_date": "20240102", "close": 20.0},
        {"ts_code": "000003.SZ", "trade_date": "20240102", "close": 30.0},
    ]
    assert metadata.to_dict() | {"formats": {}} == {
        "write_mode": "upsert_by_key",
        "partition_by": [],
        "primary_key": ["ts_code", "trade_date"],
        "date_field": "trade_date",
        "replace_range_start": None,
        "replace_range_end": None,
        "rows_before": 2,
        "rows_written": 2,
        "rows_after": 3,
        "duplicate_rows_dropped": 1,
        "partitions_touched": [],
        "formats": {},
    }


def test_downloader_engine_upsert_by_key_removes_stale_partition_rows(tmp_path):
    from axdata_core.downloader_engine import DownloadWriter

    writer = DownloadWriter()
    output_dir = tmp_path / "adj_factor"
    stale_partition = output_dir / "trade_date=20240102"
    stale_partition.mkdir(parents=True)
    pd.DataFrame.from_records(
        [{"ts_code": "000001.SZ", "adj_factor": 1.0}]
    ).to_parquet(stale_partition / "part-0.parquet", engine="pyarrow", index=False)

    metadata = writer.write_parquet_with_mode(
        pd.DataFrame.from_records(
            [{"ts_code": "000001.SZ", "trade_date": "20240103", "adj_factor": 1.1}]
        ),
        output_dir,
        write_mode="upsert_by_key",
        primary_key=("ts_code",),
        partition_by=("trade_date",),
        date_field="trade_date",
    )

    assert not list(stale_partition.glob("*.parquet"))
    assert (output_dir / "20240103.parquet").is_file()
    rows = pd.read_parquet(output_dir, engine="pyarrow").to_dict(orient="records")
    assert len(rows) == 1
    assert rows[0]["ts_code"] == "000001.SZ"
    assert rows[0]["adj_factor"] == 1.1
    assert str(rows[0]["trade_date"]) == "20240103"
    assert metadata.rows_before == 1
    assert metadata.rows_after == 1
    assert metadata.duplicate_rows_dropped == 1


def test_downloader_engine_upsert_by_key_requires_primary_key(tmp_path):
    from axdata_core.downloader_engine import DownloadWriter, WriteStrategyError

    writer = DownloadWriter()

    with pytest.raises(WriteStrategyError, match="requires primary_key"):
        writer.write_parquet_with_mode(
            pd.DataFrame.from_records([{"ts_code": "000001.SZ"}]),
            tmp_path / "daily.parquet",
            write_mode="upsert_by_key",
        )


def test_downloader_engine_replace_range_replaces_only_target_dates(tmp_path):
    from axdata_core.downloader_engine import DownloadWriter

    writer = DownloadWriter()
    output_path = tmp_path / "daily.parquet"
    pd.DataFrame.from_records(
        [
            {"ts_code": "000001.SZ", "trade_date": "20240102", "close": 10.0},
            {"ts_code": "000001.SZ", "trade_date": "20240103", "close": 11.0},
            {"ts_code": "000001.SZ", "trade_date": "20240104", "close": 12.0},
        ]
    ).to_parquet(output_path, engine="pyarrow", index=False)

    metadata = writer.write_parquet_with_mode(
        pd.DataFrame.from_records(
            [
                {"ts_code": "000001.SZ", "trade_date": "20240103", "close": 11.8},
            ]
        ),
        output_path,
        write_mode="replace_range",
        primary_key=("ts_code", "trade_date"),
        date_field="trade_date",
    )

    rows = pd.read_parquet(output_path, engine="pyarrow").sort_values("trade_date").to_dict(orient="records")
    assert rows == [
        {"ts_code": "000001.SZ", "trade_date": "20240102", "close": 10.0},
        {"ts_code": "000001.SZ", "trade_date": "20240103", "close": 11.8},
        {"ts_code": "000001.SZ", "trade_date": "20240104", "close": 12.0},
    ]
    assert metadata.replace_range_start == "20240103"
    assert metadata.replace_range_end == "20240103"
    assert metadata.rows_before == 3
    assert metadata.rows_after == 3


def test_downloader_engine_replace_range_rewrites_non_date_partitions_without_stale_rows(tmp_path):
    from axdata_core.downloader_engine import DownloadWriter

    writer = DownloadWriter()
    output_dir = tmp_path / "daily"
    partition_dir = output_dir / "exchange=SZSE"
    partition_dir.mkdir(parents=True)
    pd.DataFrame.from_records(
        [
            {"ts_code": "000001.SZ", "trade_date": "20240102", "close": 10.0},
            {"ts_code": "000001.SZ", "trade_date": "20240103", "close": 11.0},
            {"ts_code": "000001.SZ", "trade_date": "20240104", "close": 12.0},
        ]
    ).to_parquet(partition_dir / "part-0.parquet", engine="pyarrow", index=False)

    metadata = writer.write_parquet_with_mode(
        pd.DataFrame.from_records(
            [{"ts_code": "000001.SZ", "exchange": "SZSE", "trade_date": "20240103", "close": 11.8}]
        ),
        output_dir,
        write_mode="replace_range",
        primary_key=("ts_code", "trade_date"),
        partition_by=("exchange",),
        date_field="trade_date",
    )

    rows = (
        pd.read_parquet(output_dir, engine="pyarrow")
        .sort_values("trade_date")
        .to_dict(orient="records")
    )
    assert rows == [
        {"ts_code": "000001.SZ", "trade_date": "20240102", "close": 10.0, "exchange": "SZSE"},
        {"ts_code": "000001.SZ", "trade_date": "20240103", "close": 11.8, "exchange": "SZSE"},
        {"ts_code": "000001.SZ", "trade_date": "20240104", "close": 12.0, "exchange": "SZSE"},
    ]
    assert metadata.rows_before == 3
    assert metadata.rows_after == 3


def test_downloader_engine_replace_range_requires_date_field(tmp_path):
    from axdata_core.downloader_engine import DownloadWriter, WriteStrategyError

    writer = DownloadWriter()

    with pytest.raises(WriteStrategyError, match="requires date_field"):
        writer.write_parquet_with_mode(
            pd.DataFrame.from_records([{"ts_code": "000001.SZ"}]),
            tmp_path / "daily.parquet",
            write_mode="replace_range",
        )


def test_downloader_engine_overwrite_partition_replaces_only_touched_partition(tmp_path):
    from axdata_core.downloader_engine import DownloadWriter

    writer = DownloadWriter()
    output_dir = tmp_path / "daily"
    old_file = output_dir / "20240102.parquet"
    keep_file = output_dir / "20240103.parquet"
    output_dir.mkdir(parents=True)
    pd.DataFrame.from_records([{"ts_code": "old", "trade_date": "20240102", "close": 1.0}]).to_parquet(
        old_file,
        engine="pyarrow",
        index=False,
    )
    pd.DataFrame.from_records([{"ts_code": "keep", "trade_date": "20240103", "close": 2.0}]).to_parquet(
        keep_file,
        engine="pyarrow",
        index=False,
    )

    metadata = writer.write_parquet_with_mode(
        pd.DataFrame.from_records(
            [
                {"ts_code": "new", "trade_date": "20240102", "close": 3.0},
            ]
        ),
        output_dir,
        write_mode="overwrite_partition",
        primary_key=("ts_code", "trade_date"),
        partition_by=("trade_date",),
        date_field="trade_date",
    )

    touched_rows = pd.read_parquet(old_file, engine="pyarrow").to_dict(orient="records")
    kept_rows = pd.read_parquet(keep_file, engine="pyarrow").to_dict(orient="records")
    assert touched_rows == [{"ts_code": "new", "trade_date": "20240102", "close": 3.0}]
    assert kept_rows == [{"ts_code": "keep", "trade_date": "20240103", "close": 2.0}]
    assert metadata.partitions_touched == ("trade_date=20240102",)


def test_downloader_engine_append_keeps_repeated_snapshot_files(tmp_path):
    from axdata_core.downloader_engine import DownloadWriter

    writer = DownloadWriter()
    output_path = tmp_path / "snapshot.parquet"

    first = writer.write_parquet_with_mode(
        pd.DataFrame.from_records([{"instrument_id": "000001.SZ"}]),
        output_path,
        write_mode="append",
        primary_key=("instrument_id",),
    )
    second = writer.write_parquet_with_mode(
        pd.DataFrame.from_records([{"instrument_id": "000001.SZ"}]),
        output_path,
        write_mode="append",
        primary_key=("instrument_id",),
    )

    files = sorted(path.name for path in tmp_path.glob("snapshot*.parquet"))
    assert files == ["snapshot.parquet", "snapshot_0001.parquet"]
    assert first.rows_after == 1
    assert second.rows_after == 1


def test_downloader_engine_append_reports_actual_output_path(tmp_path):
    from axdata_core.downloader_engine import DownloadWriter

    class Profile:
        write_mode = "append"
        primary_key = ("instrument_id",)
        partition_by: list[str] = []
        date_field = None
        datetime_field = None

    writer = DownloadWriter()
    frame = pd.DataFrame.from_records([{"instrument_id": "000001.SZ"}])

    first = writer.write_outputs_with_metadata(
        Profile(),
        frame,
        output_dir=tmp_path,
        file_stem="snapshot",
        formats=["parquet"],
    )
    second = writer.write_outputs_with_metadata(
        Profile(),
        frame,
        output_dir=tmp_path,
        file_stem="snapshot",
        formats=["parquet"],
    )

    assert first.output_paths["parquet"] == tmp_path / "parquet" / "snapshot.parquet"
    assert second.output_paths["parquet"] == tmp_path / "parquet" / "snapshot_0001.parquet"


def test_downloader_engine_quality_reports_primary_key_problems():
    from axdata_core.downloader_engine import DownloadQualityChecker

    checker = DownloadQualityChecker()

    missing = checker.evaluate(
        pd.DataFrame.from_records([{"name": "平安银行"}]),
        primary_key="instrument_id",
    )
    duplicate = checker.evaluate(
        pd.DataFrame.from_records(
            [
                {"instrument_id": "000001.SZ", "name": "平安银行"},
                {"instrument_id": "000001.SZ", "name": "平安银行"},
            ]
        ),
        primary_key="instrument_id",
    )
    missing_value = checker.evaluate(
        pd.DataFrame.from_records([{"instrument_id": None, "name": "平安银行"}]),
        primary_key="instrument_id",
    )

    assert missing["primary_key"] == "missing"
    assert duplicate["primary_key"] == "duplicate"
    assert missing_value["primary_key"] == "missing_value"


def test_downloader_engine_quality_reports_schema_dates_and_numeric_problems():
    from axdata_core.downloader_engine import DownloadQualityChecker

    checker = DownloadQualityChecker()

    quality = checker.evaluate(
        pd.DataFrame.from_records(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240102",
                    "close": 10.2,
                    "vol": 1000.0,
                    "unexpected": "x",
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240102",
                    "close": -1.0,
                    "vol": 1100.0,
                    "unexpected": "y",
                },
            ]
        ),
        primary_key=("ts_code", "trade_date"),
        required_columns=["ts_code", "trade_date", "amount"],
        expected_columns=["ts_code", "trade_date", "close", "vol", "amount"],
        date_field="trade_date",
        numeric_positive_columns=["close", "vol", "amount"],
    )

    assert quality["schema"] == "missing_required"
    assert quality["primary_key"] == "duplicate"
    assert quality["quality_status"] == "error"
    assert quality["missing_required_columns"] == ["amount"]
    assert quality["duplicate_key_count"] == 2
    assert quality["unexpected_columns"] == ["unexpected"]
    assert quality["date_range"] == {"min": "20240102", "max": "20240102"}
    assert quality["numeric_positive_checks"]["close"]["status"] == "negative"
    assert quality["numeric_positive_checks"]["close"]["negative_count"] == 1
    assert quality["numeric_positive_checks"]["amount"]["status"] == "missing"


def test_downloader_engine_quality_reports_calendar_gaps_and_samples():
    from axdata_core.downloader_engine import DownloadQualityChecker

    checker = DownloadQualityChecker()

    quality = checker.evaluate(
        pd.DataFrame.from_records(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240102",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.8,
                    "close": 10.2,
                    "vol": 1000.0,
                    "amount": 10200.0,
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240104",
                    "open": 10.1,
                    "high": 10.6,
                    "low": 10.0,
                    "close": 10.3,
                    "vol": 1100.0,
                    "amount": 11330.0,
                },
            ]
        ),
        primary_key=("ts_code", "trade_date"),
        date_field="trade_date",
        calendar_check=True,
        trade_calendar_dates={"SZSE": ["20240102", "20240103", "20240104"]},
    )

    assert quality["quality_status"] == "warn"
    assert quality["calendar_coverage_status"] == "warn"
    assert quality["expected_trading_day_count"] == 3
    assert quality["actual_trading_day_count"] == 2
    assert quality["date_gap_count"] == 1
    assert quality["missing_trading_dates"] == ["20240103"]
    assert quality["missing_date_samples"] == ["20240103"]
    assert quality["unexplained_missing_dates"] == [{"symbol": "000001.SZ", "date": "20240103"}]
    assert quality["per_symbol_date_coverage"][0]["missing_date_count"] == 1


def test_downloader_engine_quality_reports_extra_non_trading_dates():
    from axdata_core.downloader_engine import DownloadQualityChecker

    checker = DownloadQualityChecker()

    quality = checker.evaluate(
        pd.DataFrame.from_records(
            [
                {"ts_code": "000001.SZ", "trade_date": "20240102"},
                {"ts_code": "000001.SZ", "trade_date": "20240106"},
            ]
        ),
        primary_key=("ts_code", "trade_date"),
        date_field="trade_date",
        calendar_check=True,
        trade_calendar_dates={"SZSE": ["20240102", "20240103", "20240104", "20240105"]},
    )

    assert quality["quality_status"] == "error"
    assert quality["calendar_coverage_status"] == "error"
    assert quality["extra_non_trading_dates"] == ["20240106"]


def test_downloader_engine_quality_warns_when_calendar_is_missing():
    from axdata_core.downloader_engine import DownloadQualityChecker

    checker = DownloadQualityChecker()

    quality = checker.evaluate(
        pd.DataFrame.from_records(
            [{"ts_code": "000001.SZ", "trade_date": "20240102"}]
        ),
        primary_key=("ts_code", "trade_date"),
        date_field="trade_date",
        calendar_check=True,
        trade_calendar_dates=None,
    )

    assert quality["quality_status"] == "warn"
    assert quality["calendar_coverage_status"] == "warn"
    assert "trade_cal" in quality["calendar_next_action"]


def test_downloader_engine_quality_reports_ohlc_and_adj_factor_anomalies():
    from axdata_core.downloader_engine import DownloadQualityChecker

    checker = DownloadQualityChecker()

    quality = checker.evaluate(
        pd.DataFrame.from_records(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240102",
                    "open": 10.0,
                    "high": 9.9,
                    "low": 9.8,
                    "close": 10.2,
                    "vol": -1.0,
                    "amount": 100.0,
                    "adj_factor": 1.0,
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240103",
                    "open": 10.2,
                    "high": 10.5,
                    "low": 10.1,
                    "close": 10.3,
                    "vol": 10.0,
                    "amount": -5.0,
                    "adj_factor": 0.0,
                },
            ]
        ),
        primary_key=("ts_code", "trade_date"),
        date_field="trade_date",
        numeric_positive_columns=["vol", "amount", "adj_factor"],
    )

    assert quality["quality_status"] == "error"
    assert quality["price_ohlc_anomaly_count"] == 1
    assert quality["negative_volume_count"] == 1
    assert quality["negative_amount_count"] == 1
    assert quality["invalid_adj_factor_count"] == 1
    assert quality["invalid_adj_factor_samples"] == [
        {"ts_code": "000001.SZ", "trade_date": "20240103", "adj_factor": 0.0}
    ]


def test_run_downloader_uses_engine_writer_quality_and_metadata(tmp_path, monkeypatch):
    fake_adapter = object()
    calls = []

    class FakeWriter:
        def write_outputs(self, profile, frame, *, output_dir, file_stem, formats, progress_callback=None):
            calls.append(
                {
                    "component": "writer",
                    "profile": profile.interface_name,
                    "row_count": len(frame),
                    "formats": list(formats),
                    "file_stem": file_stem,
                }
            )
            return {"parquet": tmp_path / "engine-output.parquet"}

    class FakeQuality:
        def evaluate(
            self,
            frame,
            *,
            primary_key,
            rules=None,
            required_columns=None,
            expected_columns=None,
            date_field=None,
            datetime_field=None,
            numeric_positive_columns=None,
            field_mappings=None,
            calendar_check=False,
            trade_calendar_dates=None,
            symbol_field=None,
            suspension_dates=None,
        ):
            calls.append(
                {
                    "component": "quality",
                    "primary_key": primary_key,
                    "rules": list(rules or []),
                    "required_columns": list(required_columns or []),
                    "expected_columns": list(expected_columns or []),
                    "date_field": date_field,
                    "datetime_field": datetime_field,
                    "numeric_positive_columns": list(numeric_positive_columns or []),
                    "field_mappings": dict(field_mappings or {}),
                    "calendar_check": calendar_check,
                    "trade_calendar_dates": trade_calendar_dates,
                }
            )
            return {"engine_quality": "pass"}

    class FakeMetadata:
        def write_run_log(self, output_dir, *, file_stem, result):
            calls.append(
                {
                    "component": "metadata",
                    "file_stem": file_stem,
                    "quality": dict(result["quality"]),
                }
            )
            return tmp_path / "engine-log.json"

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        assert interface_name == "stock_codes_tdx"
        assert adapter is fake_adapter
        assert persist is False
        return SourceRequestResult(
            records=[{"instrument_id": "000001.SZ", "symbol": "000001", "name": "平安银行"}],
            meta={"source": "tdx", "snapshot_date": "20260620"},
        )

    monkeypatch.setattr(downloaders_module, "_DOWNLOAD_WRITER", FakeWriter())
    monkeypatch.setattr(downloaders_module, "_QUALITY_CHECKER", FakeQuality())
    monkeypatch.setattr(downloaders_module, "_METADATA_WRITER", FakeMetadata())
    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    result = downloaders_module.run_downloader(
        "stock_codes_tdx",
        output_root=tmp_path / "data",
        formats=["parquet"],
        adapter=fake_adapter,
    )

    assert result["output_paths"] == {"parquet": str(tmp_path / "engine-output.parquet")}
    assert result["output_path"] == str(tmp_path / "engine-output.parquet")
    assert result["quality"]["engine_quality"] == "pass"
    assert result["quality"]["write_mode"] == "snapshot"
    assert result["quality"]["write_primary_key"] == ["instrument_id"]
    assert result["quality"]["rows_written"] == 1
    assert result["write_mode"] == "snapshot"
    assert result["primary_key"] == ["instrument_id"]
    assert result["log_path"] == str(tmp_path / "engine-log.json")
    assert [call["component"] for call in calls] == ["writer", "quality", "metadata"]


def test_tdx_downloader_profile_declares_runtime_factories(monkeypatch):
    monkeypatch.setattr(downloaders_module, "_registry_downloader_projections", lambda *, data_root=None: None)

    profile = get_downloader_profile("stock_codes_tdx")

    assert profile.adapter_factory == "tdx"
    assert profile.runtime_source_server_max_factory == "tdx"
    profile_dict = profile.to_dict()
    assert profile_dict["adapter_factory"] == "tdx"
    assert profile_dict["runtime_source_server_max_factory"] == "tdx"


def test_downloader_uses_declared_adapter_factory(tmp_path, monkeypatch):
    fake_adapter = object()
    fake_client = object()
    calls = []

    def fake_download_adapter(profile, *, source_server_count, pool_size, data_root=None, progress_callback=None):
        calls.append(
            {
                "profile": profile.interface_name,
                "factory": profile.adapter_factory,
                "source_server_count": source_server_count,
                "pool_size": pool_size,
                "data_root": data_root,
            }
        )
        return fake_adapter, fake_client

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        assert interface_name == "stock_codes_tdx"
        assert adapter is fake_adapter
        assert data_root == tmp_path
        return SourceRequestResult(
            records=[{"instrument_id": "000001.SZ", "symbol": "000001", "name": "平安银行"}],
            meta={"source": "tdx", "snapshot_date": "20260620"},
        )

    closed = []
    monkeypatch.setattr(downloaders_module, "_download_adapter", fake_download_adapter)
    monkeypatch.setattr(downloaders_module, "_close_client", lambda client: closed.append(client))
    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    result = downloaders_module.run_downloader(
        "stock_codes_tdx",
        data_root=tmp_path,
        formats=["parquet"],
    )

    assert result["status"] == "success"
    assert calls == [
        {
            "profile": "stock_codes_tdx",
            "factory": "tdx",
            "source_server_count": 1,
            "pool_size": 3,
            "data_root": tmp_path,
        }
    ]
    assert closed == [fake_client]


def test_downloader_adapter_factory_can_be_registered(tmp_path, monkeypatch):
    profile = replace(get_downloader_profile("stock_codes_tdx"), adapter_factory="demo")
    fake_adapter = object()
    fake_client = object()
    calls = []

    def fake_registry():
        def demo_factory(**kwargs):
            calls.append(kwargs)
            return fake_adapter, fake_client

        return {"demo": demo_factory}

    monkeypatch.setattr(
        downloaders_module,
        "load_builtin_downloader_adapter_factories",
        fake_registry,
    )

    adapter, client = downloaders_module._download_adapter(
        profile,
        source_server_count=2,
        pool_size=4,
        data_root=tmp_path,
        progress_callback=None,
    )

    assert adapter is fake_adapter
    assert client is fake_client
    assert calls == [
        {
            "interface_name": "stock_codes_tdx",
            "source_server_count": 2,
            "pool_size": 4,
            "data_root": tmp_path,
            "progress_callback": None,
        }
    ]


def test_runtime_source_server_max_factory_can_be_registered(tmp_path, monkeypatch):
    base_profile = get_downloader_profile("stock_daily_share_tdx")
    profile = replace(
        base_profile,
        concurrency=replace(
            base_profile.concurrency,
            source_server_count_editable=True,
            default_source_server_count=2,
            max_source_server_count=4,
        ),
        runtime_source_server_max_factory="demo",
    )
    calls = []

    def fake_registry():
        def demo_factory(interface_name, *, configured_max, source_server_count_editable, data_root=None):
            calls.append(
                {
                    "interface_name": interface_name,
                    "configured_max": configured_max,
                    "source_server_count_editable": source_server_count_editable,
                    "data_root": data_root,
                }
            )
            return 3

        return {"demo": demo_factory}

    monkeypatch.setattr(
        downloaders_module,
        "load_builtin_runtime_source_server_max_factories",
        fake_registry,
    )

    resolved = downloaders_module._normalize_concurrency(
        profile,
        data_root=tmp_path,
        source_server_count=3,
    )

    assert resolved.source_server_count == 3
    assert calls == [
        {
            "interface_name": "stock_daily_share_tdx",
            "configured_max": 4,
            "source_server_count_editable": True,
            "data_root": tmp_path,
        }
    ]


def test_downloader_registry_prefers_tdx_provider_package_when_available():
    code = (
        "import sys\n"
        "from axdata_core.downloader_registry import (\n"
        "    load_builtin_downloader_adapter_factories,\n"
        "    load_builtin_runtime_source_server_max_factories,\n"
        ")\n"
        "adapter_factories = load_builtin_downloader_adapter_factories()\n"
        "runtime_factories = load_builtin_runtime_source_server_max_factories()\n"
        "print('adapter_module=' + adapter_factories['tdx'].__module__)\n"
        "print('runtime_module=' + runtime_factories['tdx'].__module__)\n"
        "print('provider_registry=' + str('axdata_source_tdx.downloader_registry' in sys.modules))\n"
        "print('core_tdx_registry=' + str('axdata_core.adapters.tdx.downloader_registry' in sys.modules))\n"
        "print('tdx_profiles=' + str('axdata_core.adapters.tdx.downloader_profiles' in sys.modules))\n"
        "print('tdx_downloader=' + str('axdata_core.adapters.tdx.downloader' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('tdx_wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(REPO_ROOT / "packages" / "axdata-source-tdx" / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "adapter_module=axdata_source_tdx.downloader_registry" in result.stdout
    assert "runtime_module=axdata_source_tdx.downloader_registry" in result.stdout
    assert "provider_registry=True" in result.stdout
    assert "core_tdx_registry=False" in result.stdout
    assert "tdx_profiles=False" in result.stdout
    assert "tdx_downloader=False" in result.stdout
    assert "tdx_request=False" in result.stdout
    assert "tdx_wire_client=False" in result.stdout


def test_tdx_provider_package_downloader_factory_runs_without_core_downloader(tmp_path):
    code = (
        "import sys\n"
        "from axdata_core.downloader_registry import (\n"
        "    load_builtin_downloader_adapter_factories,\n"
        "    load_builtin_runtime_source_server_max_factories,\n"
        ")\n"
        "import axdata_source_tdx.client_factory as provider_client_factory\n"
        "import axdata_source_tdx.downloader as provider_downloader\n"
        "created = {}\n"
        "seen_cache_roots = []\n"
        "class FakeAdapter:\n"
        "    def __init__(self, client, progress_callback=None, use_parallel_suspension_quotes=None, options=None):\n"
        "        self.client = client\n"
        "        self.progress_callback = progress_callback\n"
        "        self.use_parallel_suspension_quotes = use_parallel_suspension_quotes\n"
        "        self.options = options\n"
        "def fake_effective_host_strings(kind, *, cache_root=None):\n"
        "    seen_cache_roots.append(cache_root)\n"
        "    return ['host1:7709', 'host2:7709', 'host3:7709']\n"
        "def fake_create_tdx_client(*, hosts=None, pool_size=None, heartbeat_interval=None):\n"
        "    created.update({'hosts': hosts, 'pool_size': pool_size, 'heartbeat_interval': heartbeat_interval})\n"
        "    return object()\n"
        "provider_downloader.TdxRequestAdapter = FakeAdapter\n"
        "provider_client_factory.create_tdx_client = fake_create_tdx_client\n"
        "provider_downloader.effective_host_strings = fake_effective_host_strings\n"
        "adapter_factory = load_builtin_downloader_adapter_factories()['tdx']\n"
        "runtime_factory = load_builtin_runtime_source_server_max_factories()['tdx']\n"
        f"adapter, client = adapter_factory(interface_name='stock_limit_ladder_tdx', source_server_count=2, pool_size=5, data_root={str(tmp_path)!r})\n"
        f"runtime_max = runtime_factory('stock_daily_share_tdx', configured_max=8, source_server_count_editable=True, data_root={str(tmp_path)!r})\n"
        "print('adapter_module=' + adapter_factory.__module__)\n"
        "print('runtime_module=' + runtime_factory.__module__)\n"
        "print('provider_downloader=' + str('axdata_source_tdx.downloader' in sys.modules))\n"
        "print('provider_client_factory=' + str('axdata_source_tdx.client_factory' in sys.modules))\n"
        "print('provider_interface_sets=' + str('axdata_source_tdx.downloader_interface_sets' in sys.modules))\n"
        "print('provider_server_cache=' + str('axdata_source_tdx.server_cache' in sys.modules))\n"
        "print('provider_host_config=' + str('axdata_source_tdx.host_config' in sys.modules))\n"
        "print('core_tdx_downloader=' + str('axdata_core.adapters.tdx.downloader' in sys.modules))\n"
        "print('core_client_factory=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "print('core_interface_sets=' + str('axdata_core.adapters.tdx.downloader_interface_sets' in sys.modules))\n"
        "print('core_server_cache=' + str('axdata_core.adapters.tdx.server_cache' in sys.modules))\n"
        "print('core_host_config=' + str('axdata_core.adapters.tdx.host_config' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('hosts=' + ','.join(created['hosts']))\n"
        "print('pool_size=' + str(created['pool_size']))\n"
        "print('heartbeat=' + str(created['heartbeat_interval']))\n"
        "print('parallel=' + str(adapter.use_parallel_suspension_quotes))\n"
        "print('option_keys=' + ','.join(sorted(adapter.options or {})))\n"
        "print('runtime_max=' + str(runtime_max))\n"
        "print('last_cache_root=' + str(seen_cache_roots[-1]).replace('\\\\', '/'))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(REPO_ROOT / "packages" / "axdata-source-tdx" / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    expected_cache_root = str((tmp_path / "cache" / "tdx_servers").resolve()).replace("\\", "/")
    assert "adapter_module=axdata_source_tdx.downloader_registry" in result.stdout
    assert "runtime_module=axdata_source_tdx.downloader_registry" in result.stdout
    assert "provider_downloader=True" in result.stdout
    assert "provider_client_factory=True" in result.stdout
    assert "provider_interface_sets=True" in result.stdout
    assert "provider_server_cache=True" in result.stdout
    assert "provider_host_config=False" in result.stdout
    assert "core_tdx_downloader=False" in result.stdout
    assert "core_client_factory=False" in result.stdout
    assert "core_interface_sets=False" in result.stdout
    assert "core_server_cache=False" in result.stdout
    assert "core_host_config=False" in result.stdout
    assert "tdx_request=False" in result.stdout
    assert "hosts=host1:7709,host2:7709" in result.stdout
    assert "pool_size=5" in result.stdout
    assert "heartbeat=None" in result.stdout
    assert "parallel=False" in result.stdout
    assert (
        "option_keys=f10_topic_refill_rounds,f10_topic_refill_workers,"
        "f10_topic_workers,server_cache_root,stats_cache_root"
    ) in result.stdout
    assert "runtime_max=3" in result.stdout
    assert f"last_cache_root={expected_cache_root}" in result.stdout


def test_tdx_downloader_interface_sets_prefers_provider_package_when_available():
    code = (
        "import sys\n"
        "from axdata_core.adapters.tdx import downloader_interface_sets as interface_sets\n"
        "print('provider_interface_sets_before=' + str('axdata_source_tdx.downloader_interface_sets' in sys.modules))\n"
        "print('core_interface_sets=' + str('axdata_core.adapters.tdx.downloader_interface_sets' in sys.modules))\n"
        "print('parallel=' + ','.join(sorted(interface_sets.DOWNLOADER_PARALLEL_SUSPENSION_INTERFACES)))\n"
        "print('stats=' + ','.join(sorted(interface_sets.DOWNLOADER_STATS_RESOURCE_INTERFACES)))\n"
        "print('f10_prefill=' + ','.join(sorted(interface_sets.DOWNLOADER_F10_TOPIC_PREFILL_INTERFACES)))\n"
        "print('runtime=' + ','.join(sorted(interface_sets.DOWNLOADER_RUNTIME_SOURCE_SERVER_MAX_INTERFACES)))\n"
        "print('provider_interface_sets_after=' + str('axdata_source_tdx.downloader_interface_sets' in sys.modules))\n"
        "print('tdx_downloader=' + str('axdata_core.adapters.tdx.downloader' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [
                    str(REPO_ROOT / "packages" / "axdata-source-tdx" / "src"),
                    str(REPO_ROOT / "libs" / "axdata_core"),
                ]
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "provider_interface_sets_before=False" in result.stdout
    assert "provider_interface_sets_after=True" in result.stdout
    assert "core_interface_sets=True" in result.stdout
    assert "parallel=stock_suspensions_tdx" in result.stdout
    assert "stock_daily_share_tdx" in result.stdout
    assert "stock_limit_ladder_tdx" in result.stdout
    assert "stock_theme_strength_rank_tdx" in result.stdout
    assert "f10_prefill=stock_limit_ladder_tdx,stock_theme_strength_rank_tdx" in result.stdout
    assert "runtime=stock_daily_price_limit_tdx,stock_daily_share_tdx" in result.stdout
    assert "tdx_downloader=False" in result.stdout
    assert "tdx_request=False" in result.stdout


def test_downloader_factory_registry_does_not_eager_load_tdx_modules():
    code = (
        "import sys\n"
        "from axdata_core.downloader_registry import (\n"
        "    load_builtin_downloader_adapter_factories,\n"
        "    load_builtin_runtime_source_server_max_factories,\n"
        ")\n"
        "load_builtin_downloader_adapter_factories()\n"
        "load_builtin_runtime_source_server_max_factories()\n"
        "print('tdx_downloader_registry=' + str('axdata_core.adapters.tdx.downloader_registry' in sys.modules))\n"
        "print('tdx_downloader=' + str('axdata_core.adapters.tdx.downloader' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "tdx_downloader_registry=False" in result.stdout
    assert "tdx_downloader=False" in result.stdout
    assert "tdx_request=False" in result.stdout


def test_tdx_downloader_registry_import_does_not_load_runtime_modules():
    code = (
        "import sys\n"
        "import axdata_core.adapters.tdx.downloader_registry as registry\n"
        "adapter_factories = registry.tdx_downloader_adapter_factories()\n"
        "runtime_factories = registry.tdx_runtime_source_server_max_factories()\n"
        "profile_declarations, declared_adapter_factories, declared_runtime_factories = "
        "registry.tdx_downloader_declarations()\n"
        "print('adapter_keys=' + ','.join(sorted(adapter_factories)))\n"
        "print('runtime_keys=' + ','.join(sorted(runtime_factories)))\n"
        "print('profile_declarations=' + profile_declarations.__name__)\n"
        "print('declared_adapter_keys=' + ','.join(sorted(declared_adapter_factories)))\n"
        "print('declared_runtime_keys=' + ','.join(sorted(declared_runtime_factories)))\n"
        "print('tdx_downloader_registry=' + str('axdata_core.adapters.tdx.downloader_registry' in sys.modules))\n"
        "print('tdx_profiles=' + str('axdata_core.adapters.tdx.downloader_profiles' in sys.modules))\n"
        "print('tdx_downloader=' + str('axdata_core.adapters.tdx.downloader' in sys.modules))\n"
        "print('tdx_request=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('tdx_client_factory=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "print('tdx_host_config=' + str('axdata_core.adapters.tdx.host_config' in sys.modules))\n"
        "print('tdx_wire_client=' + str('axdata_core._tdx_wire.client' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "adapter_keys=tdx" in result.stdout
    assert "runtime_keys=tdx" in result.stdout
    assert "profile_declarations=tdx_downloader_profile_declarations" in result.stdout
    assert "declared_adapter_keys=tdx" in result.stdout
    assert "declared_runtime_keys=tdx" in result.stdout
    assert "tdx_downloader_registry=True" in result.stdout
    assert "tdx_profiles=False" in result.stdout
    assert "tdx_downloader=False" in result.stdout
    assert "tdx_request=False" in result.stdout
    assert "tdx_client_factory=False" in result.stdout
    assert "tdx_host_config=False" in result.stdout
    assert "tdx_wire_client=False" in result.stdout


def test_stock_codes_downloader_writes_selected_formats(tmp_path, monkeypatch):
    fake_adapter = object()

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        assert interface_name == "stock_codes_tdx"
        assert params == {"scope": "all"}
        assert fields is None
        assert persist is False
        assert adapter is fake_adapter
        assert data_root is None
        return SourceRequestResult(
            records=[
                {"instrument_id": "000001.SZ", "symbol": "000001", "name": "平安银行"},
                {"instrument_id": "600000.SH", "symbol": "600000", "name": "浦发银行"},
            ],
            meta={"source": "tdx"},
        )

    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    result = downloaders_module.run_downloader(
        "stock_codes_tdx",
        output_dir=tmp_path / "custom" / "final",
        formats=["parquet", "csv", "jsonl"],
        collect_mode="incremental",
        connection_mode="long_connection",
        connection_count=2,
        source_server_count=3,
        connections_per_server=4,
        max_concurrent_tasks=12,
        batch_size=99,
        request_interval_ms=100,
        retry_count=5,
        timeout_ms=1000,
        adapter=fake_adapter,
    )

    assert result["status"] == "success"
    assert result["collect_mode"] == "incremental"
    assert result["row_count"] == 2
    assert result["snapshot_date"].isdigit()
    assert len(result["snapshot_date"]) == 8
    assert result["snapshot_date_source"] == "collected_at"
    assert result["collection_time"].isdigit() is False
    assert len(result["collection_time"]) == 13
    assert result["file_stem"] == f"stock_codes_tdx_{result['collection_time']}"
    assert result["connection_mode"] == "long_connection"
    assert result["connection_count"] == 3
    assert set(result["duration_breakdown_ms"]) == {
        "connection",
        "source_request",
        "transform",
        "write",
        "quality",
        "total",
    }
    assert result["duration_breakdown_ms"]["total"] == result["duration_ms"]
    assert result["concurrency"] == {
        "mode": "fixed",
        "source_server_count": 1,
        "connections_per_server": 3,
        "max_concurrent_tasks": 3,
        "connection_count": 3,
        "batch_size": 1,
        "request_interval_ms": 0,
        "retry_count": 0,
        "timeout_ms": 30000,
    }
    assert result["request_plan"]["interface_name"] == "stock_codes_tdx"
    assert result["request_plan"]["params"] == {"scope": "all"}
    assert result["request_plan"]["fields"] is None
    assert result["request_plan"]["options"]["concurrency"] == result["concurrency"]
    assert result["output_formats"] == ["parquet", "csv", "jsonl"]
    _assert_quality_ok(result["quality"], row_count=2)
    output_path = tmp_path / "custom" / "final"
    assert len(list((output_path / "parquet").glob("*.parquet"))) == 1
    assert len(list((output_path / "csv").glob("*.csv"))) == 1
    assert len(list((output_path / "jsonl").glob("*.jsonl"))) == 1
    assert (output_path / "parquet" / f"{result['file_stem']}.parquet").exists()
    assert (output_path / "csv" / f"{result['file_stem']}.csv").exists()
    assert (output_path / "jsonl" / f"{result['file_stem']}.jsonl").exists()
    assert (output_path / "logs" / f"{result['file_stem']}.json").exists()
    assert set(result["output_paths"]) == {"parquet", "csv", "jsonl"}
    assert result["output_path"] == result["output_paths"]["parquet"]
    assert result["log_path"] == str(output_path / "logs" / f"{result['file_stem']}.json")


def test_stock_st_list_downloader_writes_snapshot(tmp_path, monkeypatch):
    fake_adapter = object()

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        assert interface_name == "stock_st_list_tdx"
        assert params == {"scope": "all"}
        assert fields is None
        assert persist is False
        assert adapter is fake_adapter
        return SourceRequestResult(
            records=[
                {
                    "instrument_id": "000004.SZ",
                    "symbol": "000004",
                    "tdx_code": "sz000004",
                    "exchange": "SZSE",
                    "name": "ST国华",
                    "market": "主板",
                    "st_type": "ST",
                },
                {
                    "instrument_id": "600122.SH",
                    "symbol": "600122",
                    "tdx_code": "sh600122",
                    "exchange": "SSE",
                    "name": "*ST宏图",
                    "market": "主板",
                    "st_type": "*ST",
                },
            ],
            meta={"source": "tdx", "snapshot_date": "20260620"},
        )

    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    result = downloaders_module.run_downloader(
        "stock_st_list_tdx",
        output_root=tmp_path / "data",
        formats=["parquet", "csv"],
        adapter=fake_adapter,
    )

    assert result["status"] == "success"
    assert result["row_count"] == 2
    assert result["snapshot_date"] == "20260620"
    assert result["snapshot_date_source"] == "snapshot_date"
    assert result["file_stem"] == f"stock_st_list_tdx_{result['collection_time']}"
    assert result["output_path"].startswith(
        str(tmp_path / "data" / "通达信" / "股票数据" / "基础数据" / "stock_st_list_tdx" / "parquet")
    )
    assert set(result["output_paths"]) == {"parquet", "csv"}
    assert result["quality"]["primary_key"] == "pass"


def test_stock_suspensions_downloader_writes_snapshot(tmp_path, monkeypatch):
    fake_adapter = object()

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        assert interface_name == "stock_suspensions_tdx"
        assert params == {"scope": "all"}
        assert fields is None
        assert persist is False
        assert adapter is fake_adapter
        return SourceRequestResult(
            records=[
                {
                    "instrument_id": "000004.SZ",
                    "symbol": "000004",
                    "tdx_code": "sz000004",
                    "exchange": "SZSE",
                    "name": "*ST国华",
                    "market": "主板",
                },
                {
                    "instrument_id": "600717.SH",
                    "symbol": "600717",
                    "tdx_code": "sh600717",
                    "exchange": "SSE",
                    "name": "天津港",
                    "market": "主板",
                },
            ],
            meta={"source": "tdx", "snapshot_date": "20260620"},
        )

    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    result = downloaders_module.run_downloader(
        "stock_suspensions_tdx",
        output_root=tmp_path / "data",
        formats=["parquet", "jsonl"],
        adapter=fake_adapter,
    )

    assert result["status"] == "success"
    assert result["row_count"] == 2
    assert result["snapshot_date"] == "20260620"
    assert result["snapshot_date_source"] == "snapshot_date"
    assert result["file_stem"] == f"stock_suspensions_tdx_{result['collection_time']}"
    assert result["output_path"].startswith(
        str(tmp_path / "data" / "通达信" / "股票数据" / "基础数据" / "stock_suspensions_tdx" / "parquet")
    )
    assert set(result["output_paths"]) == {"parquet", "jsonl"}
    assert result["quality"]["primary_key"] == "pass"


def test_stock_daily_share_downloader_delegates_full_scope_to_interface_and_writes_snapshot(tmp_path, monkeypatch):
    fake_adapter = object()
    calls = []

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        calls.append(
            {
                "interface_name": interface_name,
                "params": dict(params),
                "fields": list(fields) if fields is not None else None,
                "persist": persist,
                "adapter": adapter,
            }
        )
        assert adapter is fake_adapter
        assert persist is False
        if interface_name == "stock_daily_share_tdx":
            assert params["refresh_stats"] == "true"
            assert params["scope"] == "all"
            assert "code" not in params
            assert fields == get_downloader_profile("stock_daily_share_tdx").default_fields
            return SourceRequestResult(
                records=[
                    {
                        "trade_date": "20260620",
                        "instrument_id": instrument_id,
                        "symbol": instrument_id.split(".")[0],
                        "tdx_code": f"sz{instrument_id.split('.')[0]}",
                        "exchange": "SZSE",
                        "total_share": 19405918750.0,
                        "float_share": 19405601250.0,
                        "free_float_share_z": 8160481200.0,
                        "finance_updated_date": "20260425",
                        "share_source": "finance_snapshot+tdxstat",
                    }
                    for instrument_id in ("000001.SZ", "000002.SZ")
                ],
                meta={
                    "source": "tdx",
                    "tdx_stats_date": "20260620",
                    "tdx_code_expansion_source": "stock_codes_tdx",
                    "tdx_requested_code_count": 2,
                    "tdx_finance_batch_count": 1,
                    "tdx_finance_batch_size": 80,
                },
            )
        raise AssertionError(interface_name)

    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    result = downloaders_module.run_downloader(
        "stock_daily_share_tdx",
        params={"scope": "all", "refresh_stats": "true"},
        output_root=tmp_path / "data",
        formats=["parquet"],
        adapter=fake_adapter,
    )

    assert result["status"] == "success"
    assert result["row_count"] == 2
    assert result["snapshot_date"] == "20260620"
    assert result["snapshot_date_source"] == "tdx_stats_date"
    assert result["file_stem"] == "stock_daily_share_tdx_20260620"
    assert result["output_path"].startswith(
        str(tmp_path / "data" / "通达信" / "股票数据" / "基础数据" / "stock_daily_share_tdx" / "parquet")
    )
    assert result["output_path"].endswith("stock_daily_share_tdx_20260620.parquet")
    assert result["quality"]["primary_key"] == "pass"
    assert result["params"] == {"scope": "all", "refresh_stats": "true"}
    assert [call["interface_name"] for call in calls] == ["stock_daily_share_tdx"]
    assert result["source_meta"]["tdx_code_expansion_source"] == "stock_codes_tdx"
    assert result["source_meta"]["tdx_finance_batch_size"] == 80


def test_stock_daily_price_limit_downloader_delegates_full_scope_to_interface_and_writes_snapshot(
    tmp_path, monkeypatch
):
    fake_adapter = object()
    calls = []

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        calls.append(
            {
                "interface_name": interface_name,
                "params": dict(params),
                "fields": list(fields) if fields is not None else None,
                "persist": persist,
                "adapter": adapter,
            }
        )
        assert adapter is fake_adapter
        assert persist is False
        if interface_name == "stock_daily_price_limit_tdx":
            assert params["scope"] == "all"
            assert "trade_date" not in params
            assert "code" not in params
            assert fields == get_downloader_profile("stock_daily_price_limit_tdx").default_fields
            return SourceRequestResult(
                records=[
                    {
                        "trade_date": "20260617",
                        "instrument_id": "000001.SZ",
                        "symbol": "000001",
                        "tdx_code": "sz000001",
                        "exchange": "SZSE",
                        "name": "平安银行",
                        "name_flag": None,
                        "pre_close_trade_date": "20260616",
                        "pre_close": 10.94,
                        "pre_close_source": "tdx_realtime_snapshot",
                        "limit_up_price": 12.03,
                        "limit_down_price": 9.85,
                        "limit_ratio_pct": 10.0,
                        "limit_rule": "main_10pct",
                        "limit_status": "normal",
                    }
                ],
                meta={
                    "source": "tdx",
                    "snapshot_date": "20260617",
                    "tdx_code_expansion_source": "stock_codes_tdx",
                    "tdx_requested_code_count": 1,
                    "tdx_price_limit_mode": "latest_snapshot",
                    "tdx_quote_batch_count": 1,
                },
            )
        raise AssertionError(interface_name)

    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    result = downloaders_module.run_downloader(
        "stock_daily_price_limit_tdx",
        params={"scope": "all", "trade_date": "20260617"},
        output_root=tmp_path / "data",
        formats=["parquet"],
        adapter=fake_adapter,
    )

    assert result["status"] == "success"
    assert result["row_count"] == 1
    assert result["snapshot_date"] == "20260617"
    assert result["snapshot_date_source"] == "snapshot_date"
    assert result["file_stem"] == "stock_daily_price_limit_tdx_20260617"
    assert result["output_path"].startswith(
        str(
            tmp_path
            / "data"
            / "通达信"
            / "股票数据"
            / "基础数据"
            / "stock_daily_price_limit_tdx"
            / "parquet"
        )
    )
    assert result["output_path"].endswith("stock_daily_price_limit_tdx_20260617.parquet")
    assert result["quality"]["primary_key"] == "pass"
    assert result["params"] == {"scope": "all"}
    assert [call["interface_name"] for call in calls] == ["stock_daily_price_limit_tdx"]
    assert result["source_meta"]["tdx_code_expansion_source"] == "stock_codes_tdx"


def test_stock_capital_changes_downloader_delegates_full_scope_to_interface_and_writes_snapshot(
    tmp_path, monkeypatch
):
    fake_adapter = object()
    calls = []

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        calls.append(
            {
                "interface_name": interface_name,
                "params": dict(params),
                "fields": list(fields) if fields is not None else None,
                "persist": persist,
                "adapter": adapter,
            }
        )
        assert adapter is fake_adapter
        assert persist is False
        if interface_name == "stock_capital_changes_tdx":
            assert params["scope"] == "all"
            assert params["category"] == "xdxr"
            assert "trade_date" not in params
            assert "code" not in params
            assert fields == get_downloader_profile("stock_capital_changes_tdx").default_fields
            return SourceRequestResult(
                records=[
                    {
                        "instrument_id": "000001.SZ",
                        "ts_code": "000001.SZ",
                        "symbol": "000001",
                        "tdx_code": "sz000001",
                        "exchange": "SZSE",
                        "event_date": "20240614",
                        "category_raw": 1,
                        "category_name": "除权除息",
                        "c1": 7.19,
                        "c2": 0.0,
                        "c3": 0.0,
                        "c4": 0.0,
                        "c1_raw_hex": "0000e640",
                        "c2_raw_hex": "00000000",
                        "c3_raw_hex": "00000000",
                        "c4_raw_hex": "00000000",
                        "record_hex": "row-1",
                    }
                ],
                meta={
                    "source": "tdx",
                    "tdx_code_expansion_source": "stock_codes_tdx",
                    "tdx_requested_code_count": 1,
                    "tdx_capital_change_concurrency": 1,
                },
            )
        raise AssertionError(interface_name)

    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    result = downloaders_module.run_downloader(
        "stock_capital_changes_tdx",
        params={"scope": "all", "category": "xdxr", "trade_date": "20260617"},
        output_root=tmp_path / "data",
        formats=["parquet"],
        adapter=fake_adapter,
    )

    assert result["status"] == "success"
    assert result["row_count"] == 1
    assert result["snapshot_date_source"] == "collected_at"
    assert result["file_stem"] == f"stock_capital_changes_tdx_{result['collection_time']}"
    assert result["output_path"].startswith(
        str(
            tmp_path
            / "data"
            / "通达信"
            / "股票数据"
            / "基础数据"
            / "stock_capital_changes_tdx"
            / "parquet"
        )
    )
    assert result["output_path"].endswith(f"{result['file_stem']}.parquet")
    assert result["quality"]["primary_key"] == "pass"
    assert result["params"] == {"scope": "all", "category": "xdxr"}
    assert [call["interface_name"] for call in calls] == ["stock_capital_changes_tdx"]
    assert result["source_meta"]["tdx_code_expansion_source"] == "stock_codes_tdx"


def test_stock_kline_daily_downloader_writes_core_sample_and_duckdb_can_read(tmp_path, monkeypatch):
    fake_adapter = object()
    data_root = tmp_path / "data"
    _write_trade_calendar(data_root, ["20260617", "20260618"])
    calls = []

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        calls.append(
            {
                "interface_name": interface_name,
                "params": dict(params),
                "fields": list(fields) if fields is not None else None,
                "persist": persist,
                "adapter": adapter,
            }
        )
        assert interface_name == "stock_kline_daily_tdx"
        # `count` is a downloader-level sample size applied after the request;
        # the kline source contract rejects paging params, so it is not forwarded.
        assert params == {"code": "000001.SZ", "adjust": "none"}
        assert fields == get_downloader_profile("stock_kline_daily_tdx").default_fields
        assert persist is False
        assert adapter is fake_adapter
        return SourceRequestResult(
            records=[
                {
                    "instrument_id": "000001.SZ",
                    "symbol": "000001",
                    "tdx_code": "sz000001",
                    "exchange": "SZSE",
                    "trade_time": "2026-06-17T15:00:00+08:00",
                    "period": "day",
                    "open": 10.0,
                    "high": 10.3,
                    "low": 9.9,
                    "close": 10.2,
                    "volume": 1200.0,
                    "amount": 12240.0,
                },
                {
                    "instrument_id": "000001.SZ",
                    "symbol": "000001",
                    "tdx_code": "sz000001",
                    "exchange": "SZSE",
                    "trade_time": "2026-06-18T15:00:00+08:00",
                    "period": "day",
                    "open": 10.2,
                    "high": 10.4,
                    "low": 10.1,
                    "close": 10.3,
                    "volume": 1300.0,
                    "amount": 13390.0,
                },
            ],
            meta={"source": "tdx", "trade_date": "20260618"},
        )

    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    result = downloaders_module.run_downloader(
        "stock_kline_daily_tdx",
        params={"count": 2},
        data_root=data_root,
        output_root=data_root,
        formats=["parquet", "csv", "jsonl"],
        adapter=fake_adapter,
    )

    assert result["status"] == "success"
    assert result["row_count"] == 2
    assert result["snapshot_date"] == "20260618"
    assert result["snapshot_date_source"] == "trade_date"
    assert result["file_stem"] == "stock_kline_daily_tdx_20260618"
    _assert_quality_ok(result["quality"], row_count=2)
    assert result["quality"]["date_range"] == {
        "min": "2026-06-17T15:00:00+08:00",
        "max": "2026-06-18T15:00:00+08:00",
    }
    assert result["quality"]["field_mappings"] == {
        "instrument_id": "ts_code",
        "trade_time": "trade_date",
        "volume": "vol",
    }
    assert result["quality"]["calendar_coverage_status"] == "ok"
    assert result["quality"]["expected_trading_day_count"] == 2
    assert result["output_path"].startswith(
        str(data_root / "通达信" / "股票数据" / "K线数据" / "stock_kline_daily_tdx" / "parquet")
    )
    assert set(result["output_paths"]) == {"parquet", "csv", "jsonl"}
    assert calls == [
        {
            "interface_name": "stock_kline_daily_tdx",
            "params": {"code": "000001.SZ", "adjust": "none"},
            "fields": get_downloader_profile("stock_kline_daily_tdx").default_fields,
            "persist": False,
            "adapter": fake_adapter,
        }
    ]

    import duckdb

    rows = duckdb.connect(database=":memory:").execute(
        "SELECT instrument_id, close FROM read_parquet(?) ORDER BY trade_time",
        [result["output_paths"]["parquet"]],
    ).fetchall()
    assert rows == [("000001.SZ", 10.2), ("000001.SZ", 10.3)]


def test_stock_adj_factor_downloader_writes_core_sample_and_duckdb_can_read(tmp_path, monkeypatch):
    fake_adapter = object()
    data_root = tmp_path / "data"
    _write_trade_calendar(data_root, ["20260617", "20260618"])

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        assert interface_name == "stock_adj_factor_tdx"
        assert params == {"code": "000001.SZ", "adjust": "qfq"}
        assert fields == get_downloader_profile("stock_adj_factor_tdx").default_fields
        assert persist is False
        assert adapter is fake_adapter
        return SourceRequestResult(
            records=[
                {
                    "instrument_id": "000001.SZ",
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "tdx_code": "sz000001",
                    "exchange": "SZSE",
                    "trade_date": "20260617",
                    "adj_factor": 0.98,
                },
                {
                    "instrument_id": "000001.SZ",
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "tdx_code": "sz000001",
                    "exchange": "SZSE",
                    "trade_date": "20260618",
                    "adj_factor": 1.0,
                },
            ],
            meta={"source": "tdx", "trade_date": "20260618"},
        )

    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    result = downloaders_module.run_downloader(
        "stock_adj_factor_tdx",
        data_root=data_root,
        output_root=data_root,
        formats=["parquet", "csv", "jsonl"],
        adapter=fake_adapter,
    )

    assert result["status"] == "success"
    assert result["row_count"] == 2
    assert result["snapshot_date"] == "20260618"
    assert result["file_stem"] == "stock_adj_factor_tdx_20260618"
    _assert_quality_ok(result["quality"], row_count=2)
    assert result["quality"]["date_range"] == {"min": "20260617", "max": "20260618"}
    assert result["quality"]["calendar_coverage_status"] == "ok"
    assert result["write_mode"] == "upsert_by_key"
    assert result["primary_key"] == ["ts_code", "trade_date"]
    assert result["partition_by"] == ["trade_date"]
    assert result["rows_before"] == 0
    assert result["rows_written"] == 2
    assert result["rows_after"] == 2
    assert result["duplicate_rows_dropped"] == 0
    assert result["partitions_touched"] == ["trade_date=20260617", "trade_date=20260618"]
    assert result["quality"]["write_mode"] == "upsert_by_key"
    assert result["quality"]["rows_after"] == 2
    assert result["output_path"] == str(
        data_root / "core" / "table=adj_factor" / "parquet"
    )
    assert set(result["output_paths"]) == {"parquet", "csv", "jsonl"}
    assert (Path(result["output_paths"]["parquet"]) / "20260617.parquet").is_file()
    assert (Path(result["output_paths"]["parquet"]) / "20260618.parquet").is_file()

    import duckdb

    rows = duckdb.connect(database=":memory:").execute(
        "SELECT ts_code, trade_date, adj_factor FROM read_parquet(?, hive_partitioning = true) ORDER BY trade_date",
        [str(Path(result["output_paths"]["parquet"]) / "**" / "*.parquet")],
    ).fetchall()
    assert rows == [("000001.SZ", "20260617", 0.98), ("000001.SZ", "20260618", 1.0)]


def test_stock_adj_factor_downloader_upsert_by_key_rerun_does_not_duplicate(tmp_path, monkeypatch):
    fake_adapter = object()
    data_root = tmp_path / "data"
    _write_trade_calendar(data_root, ["20260617", "20260618", "20260619"])
    payloads = [
        [
            {
                "instrument_id": "000001.SZ",
                "ts_code": "000001.SZ",
                "symbol": "000001",
                "tdx_code": "sz000001",
                "exchange": "SZSE",
                "trade_date": "20260617",
                "adj_factor": 0.98,
            },
            {
                "instrument_id": "000001.SZ",
                "ts_code": "000001.SZ",
                "symbol": "000001",
                "tdx_code": "sz000001",
                "exchange": "SZSE",
                "trade_date": "20260618",
                "adj_factor": 1.0,
            },
        ],
        [
            {
                "instrument_id": "000001.SZ",
                "ts_code": "000001.SZ",
                "symbol": "000001",
                "tdx_code": "sz000001",
                "exchange": "SZSE",
                "trade_date": "20260618",
                "adj_factor": 1.01,
            },
            {
                "instrument_id": "000001.SZ",
                "ts_code": "000001.SZ",
                "symbol": "000001",
                "tdx_code": "sz000001",
                "exchange": "SZSE",
                "trade_date": "20260619",
                "adj_factor": 1.02,
            },
        ],
    ]

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        assert interface_name == "stock_adj_factor_tdx"
        rows = payloads.pop(0)
        return SourceRequestResult(records=rows, meta={"source": "tdx", "trade_date": rows[-1]["trade_date"]})

    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    first = downloaders_module.run_downloader(
        "stock_adj_factor_tdx",
        data_root=data_root,
        output_root=data_root,
        formats=["parquet"],
        adapter=fake_adapter,
    )
    second = downloaders_module.run_downloader(
        "stock_adj_factor_tdx",
        data_root=data_root,
        output_root=data_root,
        formats=["parquet"],
        adapter=fake_adapter,
    )

    assert first["rows_before"] == 0
    assert first["rows_after"] == 2
    assert second["rows_before"] == 2
    assert second["rows_written"] == 2
    assert second["rows_after"] == 3
    assert second["duplicate_rows_dropped"] == 1
    assert (Path(second["output_paths"]["parquet"]) / "20260617.parquet").is_file()
    assert (Path(second["output_paths"]["parquet"]) / "20260618.parquet").is_file()
    assert (Path(second["output_paths"]["parquet"]) / "20260619.parquet").is_file()

    import duckdb

    rows = duckdb.connect(database=":memory:").execute(
        "SELECT ts_code, trade_date, adj_factor FROM read_parquet(?, hive_partitioning = true) ORDER BY trade_date",
        [str(Path(second["output_paths"]["parquet"]) / "**" / "*.parquet")],
    ).fetchall()
    assert rows == [
        ("000001.SZ", "20260617", 0.98),
        ("000001.SZ", "20260618", 1.01),
        ("000001.SZ", "20260619", 1.02),
    ]


def test_stock_adj_factor_downloader_warns_without_local_trade_calendar(tmp_path, monkeypatch):
    fake_adapter = object()

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        assert interface_name == "stock_adj_factor_tdx"
        assert persist is False
        assert adapter is fake_adapter
        return SourceRequestResult(
            records=[
                {
                    "instrument_id": "000001.SZ",
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "tdx_code": "sz000001",
                    "exchange": "SZSE",
                    "trade_date": "20260618",
                    "adj_factor": 1.0,
                },
            ],
            meta={"source": "tdx", "trade_date": "20260618"},
        )

    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    result = downloaders_module.run_downloader(
        "stock_adj_factor_tdx",
        data_root=tmp_path / "data",
        output_root=tmp_path / "data",
        formats=["parquet"],
        adapter=fake_adapter,
    )

    assert result["status"] == "success"
    assert result["quality"]["quality_status"] == "warn"
    assert result["quality"]["calendar_coverage_status"] == "warn"
    assert "trade_cal" in result["quality"]["calendar_next_action"]


def test_stock_limit_ladder_downloader_delegates_to_interface_and_writes_shortline_snapshot(
    tmp_path, monkeypatch
):
    fake_adapter = object()
    calls = []

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        calls.append(
            {
                "interface_name": interface_name,
                "params": dict(params),
                "fields": list(fields) if fields is not None else None,
                "persist": persist,
                "adapter": adapter,
            }
        )
        assert adapter is fake_adapter
        assert persist is False
        if interface_name == "stock_limit_ladder_tdx":
            assert params == {
                "count": "all",
                "scope": "main",
                "include_touched": False,
                "topic_type": "theme",
            }
            assert fields == get_downloader_profile("stock_limit_ladder_tdx").default_fields
            return SourceRequestResult(
                records=[
                    {
                        "trade_date": "20260617",
                        "ladder_level": 4,
                        "limit_board_text": "8天6板",
                        "instrument_id": "002971.SZ",
                        "name": "和远气体",
                        "last_price": 55.76,
                        "change_pct": 10.001973,
                        "limit_status": "sealed",
                        "amount": 2521050000.0,
                        "seal_amount": 453790000.0,
                        "seal_to_amount_ratio": 0.18,
                        "free_float_market_value": 6520440000.0,
                        "primary_theme": "氢能源",
                        "secondary_themes": "机器人+储能",
                        "year_limit_up_days": 9,
                        "symbol": "002971",
                        "exchange": "SZSE",
                        "pre_close": 50.69,
                        "limit_up_price": 55.76,
                    }
                ],
                meta={
                    "source": "tdx",
                    "tdx_rank_page_count": 2,
                    "tdx_returned_count": 1,
                },
            )
        raise AssertionError(interface_name)

    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    result = downloaders_module.run_downloader(
        "stock_limit_ladder_tdx",
        output_root=tmp_path / "data",
        formats=["parquet", "jsonl"],
        adapter=fake_adapter,
    )

    assert result["status"] == "success"
    assert result["row_count"] == 1
    assert result["snapshot_date"] == "20260617"
    assert result["snapshot_date_source"] == "record_trade_date"
    assert result["file_stem"] == f"stock_limit_ladder_tdx_{result['collection_time']}"
    assert result["output_path"].startswith(
        str(tmp_path / "data" / "通达信" / "股票数据" / "短线数据" / "stock_limit_ladder_tdx" / "parquet")
    )
    assert result["output_path"].endswith(f"{result['file_stem']}.parquet")
    assert set(result["output_paths"]) == {"parquet", "jsonl"}
    assert result["quality"]["primary_key"] == "pass"
    assert result["params"] == {
        "count": "all",
        "scope": "main",
        "include_touched": False,
        "topic_type": "theme",
    }
    assert [call["interface_name"] for call in calls] == ["stock_limit_ladder_tdx"]
    assert result["source_meta"]["tdx_returned_count"] == 1


def test_tdx_download_adapter_uses_selected_servers_and_connections(monkeypatch, tmp_path):
    code = (
        "from axdata_core.downloader_registry import load_builtin_downloader_adapter_factories\n"
        "import axdata_source_tdx.downloader as tdx_downloader\n"
        "created = {}\n"
        "seen_cache_roots = []\n"
        "class FakeAdapter:\n"
        "    def __init__(self, client, progress_callback=None, use_parallel_suspension_quotes=None, options=None):\n"
        "        self.client = client\n"
        "        self.progress_callback = progress_callback\n"
        "        self.use_parallel_suspension_quotes = use_parallel_suspension_quotes\n"
        "        self.options = options\n"
        "def fake_effective_host_strings(kind, *, cache_root=None):\n"
        "    assert kind == 'quote'\n"
        "    seen_cache_roots.append(cache_root)\n"
        "    return ['host1:7709', 'host2:7709', 'host3:7709', 'host4:7709', 'host5:7709']\n"
        "def fake_create_tdx_client(*, hosts=None, pool_size=None, heartbeat_interval=None):\n"
        "    created.update({'hosts': hosts, 'pool_size': pool_size, 'heartbeat_interval': heartbeat_interval})\n"
        "    return object()\n"
        "tdx_downloader.TdxRequestAdapter = FakeAdapter\n"
        "tdx_downloader.create_tdx_client = fake_create_tdx_client\n"
        "tdx_downloader.effective_host_strings = fake_effective_host_strings\n"
        "def norm(value):\n"
        "    return str(value).replace('\\\\', '/')\n"
        "factory = load_builtin_downloader_adapter_factories()['tdx']\n"
        f"data_root = {str(tmp_path)!r}\n"
        "adapter, client = factory(interface_name='stock_codes_tdx', source_server_count=2, pool_size=6, data_root=data_root)\n"
        "print('module=' + factory.__module__)\n"
        "print('first_same_client=' + str(adapter.client is client))\n"
        "print('first_parallel=' + str(adapter.use_parallel_suspension_quotes))\n"
        "print('first_server_cache_root=' + norm(adapter.options['server_cache_root']))\n"
        "print('first_created=' + repr(created))\n"
        "print('first_cache_root=' + norm(seen_cache_roots[-1]))\n"
        "adapter, client = factory(interface_name='stock_suspensions_tdx', source_server_count=4, pool_size=8)\n"
        "print('second_same_client=' + str(adapter.client is client))\n"
        "print('second_parallel=' + str(adapter.use_parallel_suspension_quotes))\n"
        "print('second_options=' + repr(adapter.options))\n"
        "print('second_created=' + repr(created))\n"
        "adapter, _client = factory(interface_name='stock_limit_ladder_tdx', source_server_count=1, pool_size=1, data_root=data_root)\n"
        "print('ladder_server_cache_root=' + norm(adapter.options['server_cache_root']))\n"
        "print('ladder_stats_cache_root=' + norm(adapter.options['stats_cache_root']))\n"
        "print('ladder_topic_workers=' + str(adapter.options['f10_topic_workers']))\n"
        "print('ladder_refill_workers=' + str(adapter.options['f10_topic_refill_workers']))\n"
        "print('ladder_refill_rounds=' + str(adapter.options['f10_topic_refill_rounds']))\n"
        "adapter, _client = factory(interface_name='stock_daily_share_tdx', source_server_count=1, pool_size=1, data_root=data_root)\n"
        "print('daily_server_cache_root=' + norm(adapter.options['server_cache_root']))\n"
        "print('daily_stats_cache_root=' + norm(adapter.options['stats_cache_root']))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    server_cache_root = str((tmp_path / "cache" / "tdx_servers").resolve()).replace("\\", "/")
    stats_cache_root = str((tmp_path / "cache" / "tdx" / "stats").resolve()).replace("\\", "/")
    assert "module=axdata_source_tdx.downloader_registry" in result.stdout
    assert "first_same_client=True" in result.stdout
    assert "first_parallel=False" in result.stdout
    assert f"first_server_cache_root={server_cache_root}" in result.stdout
    assert "first_created={'hosts': ['host1:7709', 'host2:7709'], 'pool_size': 6, 'heartbeat_interval': None}" in result.stdout
    assert f"first_cache_root={server_cache_root}" in result.stdout
    assert "second_same_client=True" in result.stdout
    assert "second_parallel=True" in result.stdout
    assert "second_options=None" in result.stdout
    assert "second_created={'hosts': ['host1:7709', 'host2:7709', 'host3:7709', 'host4:7709'], 'pool_size': 8, 'heartbeat_interval': None}" in result.stdout
    assert f"ladder_server_cache_root={server_cache_root}" in result.stdout
    assert f"ladder_stats_cache_root={stats_cache_root}" in result.stdout
    assert "ladder_topic_workers=6" in result.stdout
    assert "ladder_refill_workers=6" in result.stdout
    assert "ladder_refill_rounds=1" in result.stdout
    assert f"daily_server_cache_root={server_cache_root}" in result.stdout
    assert f"daily_stats_cache_root={stats_cache_root}" in result.stdout


def test_tdx_runtime_source_server_max_uses_downloader_host_config_seam(monkeypatch, tmp_path):
    code = (
        "from axdata_source_tdx import downloader as tdx_downloader\n"
        "calls = []\n"
        "def fake_effective_host_strings(kind, *, cache_root=None):\n"
        "    calls.append({'kind': kind, 'cache_root': cache_root})\n"
        "    return ['host1:7709', 'host2:7709', 'host3:7709']\n"
        "def norm(value):\n"
        "    return str(value).replace('\\\\', '/')\n"
        "tdx_downloader.effective_host_strings = fake_effective_host_strings\n"
        f"data_root = {str(tmp_path)!r}\n"
        "resolved = tdx_downloader.tdx_runtime_source_server_max(\n"
        "    'stock_daily_share_tdx',\n"
        "    configured_max=8,\n"
        "    source_server_count_editable=True,\n"
        "    data_root=data_root,\n"
        ")\n"
        "print('resolved=' + str(resolved))\n"
        "print('call_kind=' + calls[0]['kind'])\n"
        "print('call_cache_root=' + norm(calls[0]['cache_root']))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    server_cache_root = str((tmp_path / "cache" / "tdx_servers").resolve()).replace("\\", "/")
    assert "resolved=3" in result.stdout
    assert "call_kind=quote" in result.stdout
    assert f"call_cache_root={server_cache_root}" in result.stdout


def test_tdx_host_config_effective_hosts_reads_current_server_config(monkeypatch):
    code = (
        "from axdata_source_tdx import host_config, tdx_server_config\n"
        "def fake_effective_host_strings(kind, *, cache_root=None):\n"
        "    return [f'{kind}:{cache_root or \"default\"}']\n"
        "tdx_server_config.effective_host_strings = fake_effective_host_strings\n"
        "print('hosts=' + repr(host_config.effective_host_strings('quote', cache_root='demo-cache')))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "hosts=['quote:demo-cache']" in result.stdout


def test_tdx_host_config_env_hosts_override_configured_server_list(monkeypatch):
    from axdata_core.adapters.tdx import host_config

    def unexpected_resolver(kind, *, cache_root=None):
        raise AssertionError("server list resolver should not run when AXDATA_TDX_HOSTS is set")

    monkeypatch.setenv("AXDATA_TDX_HOSTS", " env-a:7709,env-b:7709 ,, ")

    assert host_config.configured_tdx_hosts_from_options(
        {"server_cache_root": "demo-cache"},
        effective_host_strings_func=unexpected_resolver,
        default_hosts=["fallback:7709"],
    ) == ["env-a:7709", "env-b:7709"]


def test_tdx_host_config_configured_hosts_passes_cache_root_to_resolver(monkeypatch):
    from axdata_core.adapters.tdx import host_config

    calls = []

    def fake_resolver(kind, *, cache_root=None):
        calls.append({"kind": kind, "cache_root": cache_root})
        return ["fast-a:7709", "fast-b:7709"]

    monkeypatch.delenv("AXDATA_TDX_HOSTS", raising=False)

    assert host_config.configured_tdx_hosts_from_options(
        {"server_cache_root": "demo-cache"},
        effective_host_strings_func=fake_resolver,
        default_hosts=["fallback:7709"],
    ) == ["fast-a:7709", "fast-b:7709"]
    assert calls == [{"kind": "quote", "cache_root": "demo-cache"}]


def test_tdx_host_config_configured_hosts_fallback_on_resolver_failure(monkeypatch):
    from axdata_core.adapters.tdx import host_config

    def failing_resolver(kind, *, cache_root=None):
        raise RuntimeError("temporary server config failure")

    monkeypatch.delenv("AXDATA_TDX_HOSTS", raising=False)

    assert host_config.configured_tdx_hosts_from_options(
        {"server_cache_root": "demo-cache"},
        effective_host_strings_func=failing_resolver,
        default_hosts=["fallback-a:7709", "fallback-b:7709"],
    ) == ["fallback-a:7709", "fallback-b:7709"]


def test_tdx_host_config_suspension_scan_hosts_uses_leading_hosts():
    from axdata_core.adapters.tdx import host_config

    assert host_config.suspension_scan_hosts(
        ["fast-a:7709", "fast-b:7709", "fast-c:7709"],
        host_count=2,
    ) == ["fast-a:7709", "fast-b:7709"]


def test_tdx_downloader_module_import_does_not_load_request_module():
    code = (
        "import sys\n"
        "import axdata_core.adapters.tdx.downloader as downloader\n"
        "tracked = [\n"
            "    'axdata_core.adapters.tdx.request',\n"
            "    'axdata_core.adapters.tdx.downloader_profiles',\n"
            "    'axdata_core.adapters.tdx.client_factory',\n"
            "    'axdata_core.adapters.tdx.host_config',\n"
            "    'axdata_core.adapters.tdx.f10_request',\n"
            "    'axdata_core.adapters.tdx.tqlex',\n"
            "    'axdata_core.tdx_f10_specs',\n"
            "    'axdata_core.tdx_server_config',\n"
            "    'axdata_core.source_request',\n"
        "]\n"
        "print('loaded_before=' + ','.join(name for name in tracked if name in sys.modules))\n"
        "print('request_before=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
        "print('client_factory_before=' + str('axdata_core.adapters.tdx.client_factory' in sys.modules))\n"
        "print('host_config_before=' + str('axdata_core.adapters.tdx.host_config' in sys.modules))\n"
        "print('server_config_before=' + str('axdata_core.tdx_server_config' in sys.modules))\n"
        "print('interface_sets_before=' + str('axdata_core.adapters.tdx.interface_sets' in sys.modules))\n"
        "print('f10_specs_before=' + str('axdata_core.tdx_f10_specs' in sys.modules))\n"
        "_ = downloader.TdxRequestAdapter\n"
        "print('request_after=' + str('axdata_core.adapters.tdx.request' in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "PYTHONPATH": _tdx_provider_pythonpath(),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert "loaded_before=\n" in result.stdout
    assert "request_before=False" in result.stdout
    assert "client_factory_before=False" in result.stdout
    assert "host_config_before=False" in result.stdout
    assert "server_config_before=False" in result.stdout
    assert "interface_sets_before=False" in result.stdout
    assert "f10_specs_before=False" in result.stdout
    assert "request_after=True" in result.stdout


def test_stock_daily_share_concurrency_is_fixed_recommended_default(monkeypatch):
    profile = get_downloader_profile("stock_daily_share_tdx")

    monkeypatch.setattr(
        "axdata_core.tdx_server_config.effective_host_strings",
        lambda kind, *, cache_root=None: ["host1:7709", "host2:7709", "host3:7709"] if kind == "quote" else [],
    )

    resolved = downloaders_module._normalize_concurrency(
        profile,
        mode="custom",
        source_server_count=99,
        connections_per_server=99,
    )
    assert resolved.to_dict() == {
        "mode": "fixed",
        "source_server_count": 4,
        "connections_per_server": 2,
        "max_concurrent_tasks": 8,
        "connection_count": 8,
        "batch_size": 80,
        "request_interval_ms": 0,
        "retry_count": 0,
        "timeout_ms": 30000,
    }


def test_stock_codes_downloader_uses_menu_directory_when_only_root_is_set(tmp_path, monkeypatch):
    fake_adapter = object()

    def fake_request_interface(interface_name, *, params, fields, persist, adapter=None, options=None, data_root=None):
        return SourceRequestResult(
            records=[{"instrument_id": "000001.SZ", "symbol": "000001", "name": "平安银行"}],
            meta={"source": "tdx"},
        )

    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    result = downloaders_module.run_downloader(
        "stock_codes_tdx",
        output_root=tmp_path / "data",
        formats=["parquet"],
        adapter=fake_adapter,
    )

    assert result["output_path"].startswith(
        str(tmp_path / "data" / "通达信" / "股票数据" / "基础数据" / "stock_codes_tdx" / "parquet")
    )


def test_stock_codes_downloader_rejects_unknown_format(tmp_path, monkeypatch):
    from axdata_core.plugin_config import enable_provider

    enable_provider(TDX_PROVIDER_ID, data_root=tmp_path / "data")

    def fake_request_interface(*args, **kwargs):  # pragma: no cover - should not be called
        raise AssertionError("request_interface should not be called")

    monkeypatch.setattr(downloaders_module, "request_interface", fake_request_interface)

    try:
        downloaders_module.run_downloader("stock_codes_tdx", data_root=tmp_path, formats=["xlsx"])
    except downloaders_module.DownloaderError as exc:
        assert "Unsupported output format" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("DownloaderError was not raised")
