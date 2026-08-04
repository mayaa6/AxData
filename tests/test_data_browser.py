from __future__ import annotations

from pathlib import Path

import pandas as pd

from axdata_core.collector_scheduler import CollectorRun, CollectorSchedulerStore
from axdata_core.data_browser import DatasetSummary, delete_dataset, get_dataset, list_datasets, preview_dataset


DAILY_ROWS = [
    {
        "ts_code": "000001.SZ",
        "trade_date": "20240102",
        "open": 10.0,
        "close": 10.2,
        "vol": 1000.0,
    },
    {
        "ts_code": "000001.SZ",
        "trade_date": "20240103",
        "open": 10.2,
        "close": 10.6,
        "vol": 1200.0,
    },
    {
        "ts_code": "600000.SH",
        "trade_date": "20240102",
        "open": 8.0,
        "close": 8.1,
        "vol": 900.0,
    },
]


def _write_daily_run(tmp_path: Path) -> tuple[Path, Path]:
    data_root = tmp_path / "data"
    output_dir = tmp_path / "export" / "loop_demo" / "core" / "daily"
    parquet_path = output_dir / "parquet" / "daily_20240102.parquet"
    parquet_path.parent.mkdir(parents=True)
    pd.DataFrame(DAILY_ROWS).to_parquet(parquet_path, engine="pyarrow", index=False)

    store = CollectorSchedulerStore(data_root=data_root)
    store.create_run(
        CollectorRun(
            run_id="run_browser_daily",
            task_id="daily_sample",
            collector_name="core.daily.sample",
            trigger_type="manual",
            status="success",
            provider_id="axdata.source.loop_demo",
            downloader_profile="loop_demo.daily.snapshot",
            output_paths={"parquet": str(parquet_path)},
            result={
                "target_interface": "daily",
                "download_result": {
                    "interface_name": "daily",
                    "row_count": 3,
                    "snapshot_date": "20240102",
                    "output_paths": {"parquet": str(parquet_path)},
                    "quality": {
                        "quality_status": "ok",
                        "row_count_value": 3,
                        "date_field": "trade_date",
                        "write_mode": "upsert_by_key",
                        "partition_by": ["trade_date"],
                        "primary_key": "pass",
                        "write_primary_key": ["ts_code", "trade_date"],
                        "rows_before": 2,
                        "rows_written": 3,
                        "rows_after": 3,
                        "duplicate_rows_dropped": 1,
                        "date_range": {"min": "20240102", "max": "20240103"},
                        "schema_columns": ["ts_code", "trade_date", "open", "close", "vol"],
                    },
                },
            },
            quality={
                "quality_status": "ok",
                "row_count_value": 3,
                "date_field": "trade_date",
                "write_mode": "upsert_by_key",
                "partition_by": ["trade_date"],
                "primary_key": "pass",
                "write_primary_key": ["ts_code", "trade_date"],
                "rows_before": 2,
                "rows_written": 3,
                "rows_after": 3,
                "duplicate_rows_dropped": 1,
                "date_range": {"min": "20240102", "max": "20240103"},
                "schema_columns": ["ts_code", "trade_date", "open", "close", "vol"],
            },
            finished_at="2026-06-29T01:00:00+00:00",
            created_at="2026-06-29T00:59:00+00:00",
            updated_at="2026-06-29T01:00:00+00:00",
        )
    )
    return data_root, parquet_path


def test_data_browser_discovers_collector_run_and_previews_filtered_rows(tmp_path) -> None:
    data_root, parquet_path = _write_daily_run(tmp_path)

    datasets = list_datasets(data_root=data_root)
    daily = next(item for item in datasets if item.dataset == "daily")

    assert daily.interface_name == "daily"
    assert daily.provider == "axdata.source.loop_demo"
    assert daily.layer == "core"
    assert daily.output_paths == {"parquet": str(parquet_path)}
    assert daily.row_count == 3
    assert daily.date_min == "20240102"
    assert daily.date_max == "20240103"
    assert daily.columns == ["ts_code", "trade_date", "open", "close", "vol"]
    assert daily.quality_status == "ok"
    assert daily.latest_run_id == "run_browser_daily"
    assert daily.write_mode == "upsert_by_key"
    assert daily.partition_by == ["trade_date"]
    assert daily.primary_key == ["ts_code", "trade_date"]
    assert daily.date_field == "trade_date"
    assert daily.rows_before == 2
    assert daily.rows_written == 3
    assert daily.rows_after == 3
    assert daily.duplicate_rows_dropped == 1

    inspected = get_dataset("daily", data_root=data_root)
    assert inspected.latest_run_status == "success"

    preview = preview_dataset(
        "daily",
        data_root=data_root,
        fields=["ts_code", "trade_date", "close"],
        symbol="000001.SZ",
        start="2024-01-03",
        end="2024-01-31",
        limit=1000,
    )

    assert preview.limit == 100
    assert preview.columns == ["ts_code", "trade_date", "close"]
    assert preview.preview_format == "parquet"
    assert preview.preview_paths == [str(parquet_path.resolve())]
    assert preview.rows == [
        {"ts_code": "000001.SZ", "trade_date": "20240103", "close": 10.6}
    ]


def test_data_browser_reports_stale_output_paths(tmp_path) -> None:
    data_root, parquet_path = _write_daily_run(tmp_path)
    parquet_path.unlink()

    dataset = get_dataset("daily", data_root=data_root)

    assert dataset.missing_paths == [str(parquet_path.resolve())]


def test_data_browser_reanchors_output_paths_recorded_under_another_root(tmp_path) -> None:
    data_root, parquet_path = _write_daily_run(tmp_path)
    local_path = data_root / "core" / "table=daily" / "parquet" / "daily_20240102.parquet"
    local_path.parent.mkdir(parents=True)
    parquet_path.replace(local_path)

    store = CollectorSchedulerStore(data_root=data_root)
    run = store.list_runs(limit=1)[0]
    foreign_path = "/app/data/core/table=daily/parquet/daily_20240102.parquet"
    result = dict(run.result)
    result["download_result"] = {
        **result["download_result"],
        "output_paths": {"parquet": foreign_path},
    }
    store.update_run(
        run.run_id,
        output_paths={"parquet": foreign_path},
        result=result,
    )

    dataset = get_dataset("daily", data_root=data_root)

    assert dataset.missing_paths == []
    assert dataset.output_paths == {"parquet": str(local_path)}
    assert dataset.metadata["recorded_output_paths"] == {"parquet": foreign_path}

    preview = preview_dataset("daily", data_root=data_root, symbol="000001.SZ", limit=10)

    assert preview.preview_paths == [str(local_path.resolve())]
    assert len(preview.rows) == 2


def test_data_browser_omits_declared_only_datasets(monkeypatch, tmp_path) -> None:
    data_root = tmp_path / "data"

    import axdata_core.data_browser as data_browser

    monkeypatch.setattr(
        data_browser,
        "_declared_dataset_summaries",
        lambda root: [
            DatasetSummary(
                dataset="tdx.uncollected",
                interface_name="tdx.uncollected",
                display_name_zh="未采集数据表",
                provider="axdata.collector.demo",
                layer="snapshot",
                metadata={
                    "declared_only": True,
                    "expected_output_paths": {
                        "parquet": str(root / "snapshot" / "tdx.uncollected" / "parquet")
                    },
                },
            )
        ],
    )

    assert list_datasets(data_root=data_root) == []


def test_data_browser_delete_removes_snapshot_dataset_directory(tmp_path) -> None:
    data_root = tmp_path / "data"
    dataset_root = data_root / "snapshot" / "tdx.stock_codes"
    csv_path = dataset_root / "csv" / "tdx.stock_codes_20260703.csv"
    parquet_path = dataset_root / "parquet" / "tdx.stock_codes_20260703.parquet"
    log_path = dataset_root / "logs" / "tdx.stock_codes_20260703.json"
    csv_path.parent.mkdir(parents=True)
    parquet_path.parent.mkdir(parents=True)
    log_path.parent.mkdir(parents=True)
    csv_path.write_text("symbol\n000001\n", encoding="utf-8")
    pd.DataFrame([{"instrument_id": "000001.SZ", "name": "平安银行"}]).to_parquet(
        parquet_path,
        engine="pyarrow",
        index=False,
    )
    log_path.write_text("{}", encoding="utf-8")

    store = CollectorSchedulerStore(data_root=data_root)
    store.create_run(
        CollectorRun(
            run_id="run_delete_stock_codes",
            task_id="stock_codes_task",
            collector_name="tdx.stock_codes_tdx.snapshot",
            trigger_type="manual",
            status="success",
            provider_id="axdata.collector.tdx",
            output_paths={"csv": str(csv_path), "parquet": str(parquet_path)},
            result={
                "download_result": {
                    "interface_name": "tdx.stock_codes",
                    "output_paths": {"csv": str(csv_path), "parquet": str(parquet_path)},
                    "log_path": str(log_path),
                    "quality": {
                        "quality_status": "ok",
                        "schema_columns": ["instrument_id", "name"],
                    },
                },
            },
            quality={"quality_status": "ok", "schema_columns": ["instrument_id", "name"]},
            finished_at="2026-07-03T01:00:00+00:00",
        )
    )

    result = delete_dataset("tdx.stock_codes", data_root=data_root)

    assert result["deleted_paths"] == [str(dataset_root.resolve())]
    assert not dataset_root.exists()
    assert store.get_run("run_delete_stock_codes") is None


def test_data_browser_uses_parquet_metadata_for_partitioned_stats(tmp_path) -> None:
    data_root = tmp_path / "data"
    first_partition = data_root / "core" / "table=daily" / "trade_date=20240102"
    second_partition = data_root / "core" / "table=daily" / "trade_date=20240103"
    first_partition.mkdir(parents=True)
    second_partition.mkdir(parents=True)
    pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "close": 10.2},
            {"ts_code": "600000.SH", "close": 8.1},
        ]
    ).to_parquet(first_partition / "part-0.parquet", engine="pyarrow", index=False)
    pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "close": 10.6},
        ]
    ).to_parquet(second_partition / "part-0.parquet", engine="pyarrow", index=False)

    datasets = list_datasets(data_root=data_root)
    daily = next(item for item in datasets if item.dataset == "daily")

    assert daily.row_count == 3
    assert daily.date_min == "20240102"
    assert daily.date_max == "20240103"
    assert {"ts_code", "trade_date", "close"} <= set(daily.columns)
    assert "table" not in daily.columns


def test_data_browser_uses_axdata_data_dir_for_core_tables(monkeypatch, tmp_path) -> None:
    data_root = tmp_path / "data"
    partition = data_root / "core" / "table=adj_factor" / "parquet"
    partition.mkdir(parents=True)
    pd.DataFrame(
        [{"ts_code": "000001.SZ", "trade_date": "20260618", "adj_factor": 101.25}]
    ).to_parquet(partition / "20260618.parquet", engine="pyarrow", index=False)

    monkeypatch.setenv("AXDATA_DATA_DIR", str(data_root))

    dataset = get_dataset("adj_factor")
    assert dataset.dataset == "adj_factor"
    assert dataset.row_count == 1
    assert dataset.date_min == "20260618"
    assert dataset.date_max == "20260618"

    preview = preview_dataset("adj_factor", symbol="000001.SZ", start="20260618", end="20260618")
    assert preview.rows == [
        {"ts_code": "000001.SZ", "trade_date": "20260618", "adj_factor": 101.25}
    ]


def test_data_browser_reports_metadata_only_quality_for_core_tables(monkeypatch, tmp_path) -> None:
    data_root = tmp_path / "data"
    core_dir = data_root / "core"
    core_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {"exchange": "SSE", "cal_date": "20240102", "is_open": True, "pretrade_date": "20231229"},
            {"exchange": "SSE", "cal_date": "20240103", "is_open": True, "pretrade_date": "20240102"},
        ]
    ).to_parquet(core_dir / "trade_cal.parquet", engine="pyarrow", index=False)

    monkeypatch.setenv("AXDATA_DATA_DIR", str(data_root))

    dataset = get_dataset("trade_cal")
    assert dataset.quality_status == "ok"
    assert dataset.quality["quality_check_scope"] == "metadata_only"
    assert dataset.quality["row_count_value"] == 2
    assert dataset.quality["missing_required_columns"] == []
    assert dataset.quality_errors == []


def test_data_browser_flags_empty_core_table_as_error(monkeypatch, tmp_path) -> None:
    data_root = tmp_path / "data"
    core_dir = data_root / "core"
    core_dir.mkdir(parents=True)
    pd.DataFrame(
        {"exchange": [], "cal_date": [], "is_open": [], "pretrade_date": []}
    ).to_parquet(core_dir / "trade_cal.parquet", engine="pyarrow", index=False)

    monkeypatch.setenv("AXDATA_DATA_DIR", str(data_root))

    dataset = get_dataset("trade_cal")
    assert dataset.quality_status == "error"
    assert dataset.quality_errors == ["Table is empty."]


def test_data_browser_keeps_downloader_quality_over_core_table_scan(tmp_path) -> None:
    data_root, _ = _write_daily_run(tmp_path)
    partition = data_root / "core" / "table=daily" / "parquet"
    partition.mkdir(parents=True)
    pd.DataFrame(DAILY_ROWS).to_parquet(partition / "daily.parquet", engine="pyarrow", index=False)

    daily = get_dataset("daily", data_root=data_root)
    assert daily.quality_status == "ok"
    assert daily.quality.get("quality_check_scope") is None
    assert daily.latest_run_id == "run_browser_daily"


def test_data_browser_merge_prefers_downloader_quality_on_timestamp_tie() -> None:
    from axdata_core.data_browser import _merge_summary

    run_summary = DatasetSummary(
        dataset="daily",
        interface_name="daily",
        output_paths={"parquet": "/tmp/run.parquet"},
        quality={"quality_status": "error", "quality_errors": ["boom"]},
        quality_status="error",
        quality_errors=["boom"],
    )
    core_summary = DatasetSummary(
        dataset="daily",
        interface_name="daily",
        output_paths={"parquet": "/tmp/core.parquet"},
        quality={"quality_status": "ok", "quality_check_scope": "metadata_only"},
        quality_status="ok",
    )

    entries: dict[str, DatasetSummary] = {}
    _merge_summary(entries, run_summary)
    _merge_summary(entries, core_summary)

    merged = entries["daily"]
    assert merged.quality_status == "error"
    assert merged.quality_errors == ["boom"]
    assert merged.quality.get("quality_check_scope") is None


def test_data_browser_stats_marks_large_directory_as_limited(monkeypatch, tmp_path) -> None:
    data_root = tmp_path / "data"
    partition_root = data_root / "core" / "table=daily"
    for index in range(3):
        partition = partition_root / f"trade_date=2024010{index + 1}"
        partition.mkdir(parents=True)
        pd.DataFrame([{"ts_code": "000001.SZ", "close": 10.0 + index}]).to_parquet(
            partition / "part-0.parquet",
            engine="pyarrow",
            index=False,
        )

    import axdata_core.data_browser as data_browser

    monkeypatch.setattr(data_browser, "MAX_PARQUET_STATS_FILES", 2)

    daily = get_dataset("daily", data_root=data_root)

    assert daily.row_count is None
    assert daily.date_min == "20240101"
    assert daily.date_max == "20240103"
    assert daily.metadata["parquet_stats_limited"] is True
    assert daily.metadata["parquet_stats_file_limit"] == 2


def test_data_browser_directory_probe_is_bounded(monkeypatch, tmp_path) -> None:
    data_root = tmp_path / "data"
    partition_root = data_root / "core" / "table=daily"
    for index in range(3):
        (partition_root / f"empty={index}").mkdir(parents=True)

    import axdata_core.data_browser as data_browser

    monkeypatch.setattr(data_browser, "MAX_PARQUET_STATS_DIRS", 2)

    daily = get_dataset("daily", data_root=data_root)

    assert daily.output_paths == {"parquet": str(partition_root)}
    assert daily.row_count is None
    assert daily.metadata["parquet_stats_limited"] is True
    assert daily.metadata["parquet_stats_dir_limit"] == 2


def test_data_browser_listing_does_not_count_parquet_rows_with_duckdb(monkeypatch, tmp_path) -> None:
    data_root, _parquet_path = _write_daily_run(tmp_path)

    class FailingDuckDB:
        def connect(self, *args, **kwargs):  # pragma: no cover - should never be called
            raise AssertionError("dataset discovery should not use DuckDB COUNT(*)")

    monkeypatch.setitem(__import__("sys").modules, "duckdb", FailingDuckDB())

    daily = get_dataset("daily", data_root=data_root)

    assert daily.row_count == 3
    assert daily.columns == ["ts_code", "trade_date", "open", "close", "vol"]


def test_data_browser_uses_run_metadata_without_enumerating_parquet_files(monkeypatch, tmp_path) -> None:
    data_root, _parquet_path = _write_daily_run(tmp_path)

    def fail_stats(*args, **kwargs):  # pragma: no cover - should never be called
        raise AssertionError("complete run metadata should avoid parquet stats enumeration")

    import axdata_core.data_browser as data_browser

    monkeypatch.setattr(data_browser, "_parquet_stats", fail_stats)

    daily = get_dataset("daily", data_root=data_root)

    assert daily.row_count == 3
    assert daily.date_min == "20240102"
    assert daily.date_max == "20240103"
    assert daily.columns == ["ts_code", "trade_date", "open", "close", "vol"]


def test_data_browser_preview_can_read_directory_output_path(tmp_path) -> None:
    data_root = tmp_path / "data"
    output_dir = tmp_path / "export" / "daily" / "parquet"
    first_partition = output_dir / "trade_date=20240102"
    second_partition = output_dir / "trade_date=20240103"
    first_partition.mkdir(parents=True)
    second_partition.mkdir(parents=True)
    pd.DataFrame([{"ts_code": "000001.SZ", "close": 10.2}]).to_parquet(
        first_partition / "part-0.parquet",
        engine="pyarrow",
        index=False,
    )
    pd.DataFrame([{"ts_code": "000001.SZ", "close": 10.6}]).to_parquet(
        second_partition / "part-0.parquet",
        engine="pyarrow",
        index=False,
    )
    store = CollectorSchedulerStore(data_root=data_root)
    store.create_run(
        CollectorRun(
            run_id="run_browser_dir_daily",
            task_id="daily_sample",
            collector_name="core.daily.sample",
            trigger_type="manual",
            status="success",
            output_paths={"parquet": str(output_dir)},
            result={"target_interface": "daily"},
            quality={
                "quality_status": "ok",
                "row_count_value": 2,
                "date_field": "trade_date",
                "date_range": {"min": "20240102", "max": "20240103"},
                "schema_columns": ["ts_code", "trade_date", "close"],
            },
            finished_at="2026-06-29T01:00:00+00:00",
        )
    )

    preview = preview_dataset("daily", data_root=data_root, start="20240103", end="20240103")

    assert preview.rows == [{"ts_code": "000001.SZ", "trade_date": 20240103, "close": 10.6}]


def test_data_browser_preview_prunes_date_partition_directory(tmp_path) -> None:
    data_root = tmp_path / "data"
    output_dir = tmp_path / "export" / "daily" / "parquet"
    good_partition = output_dir / "trade_date=20240103"
    bad_partition = output_dir / "trade_date=20240104"
    good_partition.mkdir(parents=True)
    bad_partition.mkdir(parents=True)
    pd.DataFrame([{"ts_code": "000001.SZ", "close": 10.6}]).to_parquet(
        good_partition / "part-0.parquet",
        engine="pyarrow",
        index=False,
    )
    (bad_partition / "part-0.parquet").write_text("not parquet", encoding="utf-8")
    store = CollectorSchedulerStore(data_root=data_root)
    store.create_run(
        CollectorRun(
            run_id="run_browser_pruned_daily",
            task_id="daily_sample",
            collector_name="core.daily.sample",
            trigger_type="manual",
            status="success",
            output_paths={"parquet": str(output_dir)},
            result={"target_interface": "daily"},
            quality={
                "quality_status": "ok",
                "row_count_value": 1,
                "date_field": "trade_date",
                "date_range": {"min": "20240103", "max": "20240103"},
                "schema_columns": ["ts_code", "trade_date", "close"],
            },
            finished_at="2026-06-29T01:00:00+00:00",
        )
    )

    preview = preview_dataset("daily", data_root=data_root, start="20240103", end="20240103")
    missing = preview_dataset("daily", data_root=data_root, start="20240105", end="20240105")

    assert preview.rows == [{"ts_code": "000001.SZ", "trade_date": 20240103, "close": 10.6}]
    assert missing.rows == []
    assert missing.columns == ["ts_code", "trade_date", "close"]
