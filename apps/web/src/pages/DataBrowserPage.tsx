import { useEffect, useMemo, useState } from "react";
import { AlertCircle, CheckCircle2, Database, Download, FileArchive, Loader2, RefreshCw, Search, Table2, Trash2, X } from "lucide-react";

import { apiFetch } from "../api";
import { DataTable, Metric } from "../components/common";
import { catalogItems } from "../data/catalog";
import type { TableRow } from "../types";

type DatasetSummary = {
  dataset: string;
  interface_name: string;
  display_name_zh?: string | null;
  description?: string;
  provider?: string | null;
  source?: string | null;
  layer?: string | null;
  output_paths?: Record<string, string>;
  row_count?: number | null;
  date_min?: string | null;
  date_max?: string | null;
  datetime_min?: string | null;
  datetime_max?: string | null;
  columns?: string[];
  quality_status?: string | null;
  quality_warnings?: string[];
  quality_errors?: string[];
  quality?: Record<string, unknown>;
  latest_run_id?: string | null;
  latest_run_status?: string | null;
  updated_at?: string | null;
  missing_paths?: string[];
  write_mode?: string | null;
  partition_by?: string[];
  primary_key?: string[];
  date_field?: string | null;
  replace_range_start?: string | null;
  replace_range_end?: string | null;
  rows_before?: number | null;
  rows_written?: number | null;
  rows_after?: number | null;
  duplicate_rows_dropped?: number | null;
  partitions_touched?: string[];
  field_schema?: Array<Record<string, unknown>>;
  logical_table?: string | null;
  storage_layout?: string | null;
  default_query_fields?: string[];
  default_filter_fields?: string[];
  available_formats?: string[];
  metadata?: Record<string, unknown>;
};

type DatasetPreviewPayload = {
  data?: Array<Record<string, unknown>>;
  meta?: {
    dataset?: DatasetSummary;
    columns?: string[];
    count?: number;
    limit?: number;
    filters?: Record<string, unknown>;
    preview_format?: string;
    preview_paths?: string[];
  };
  success?: boolean;
  error?: { message?: string };
};

type DatasetPreviewSource = {
  format: string;
  paths: string[];
};

const DATASET_LABEL_OVERRIDES: Record<string, string> = {
  "adj_factor": "复权因子",
  "daily": "日线行情",
  "stock_basic": "股票基础信息",
  "trade_cal": "交易日历",
  "tdx.stock_codes": "最新股票列表",
  "tdx.stock_suspensions": "最新停牌列表",
  "tdx.stock_st_list": "最新ST股票列表",
  "tdx.stock_daily": "日线行情",
  "tdx.stock_daily_share": "每日股本（盘前）",
  "tdx.stock_daily_price_limit": "涨跌停价格",
  "tdx.stock_capital_changes": "股本变迁",
  "tdx.stock_adj_factor": "复权因子",
  "tdx.stock_limit_ladder": "连板天梯",
  "tdx.stock_theme_strength_rank": "题材强度排行",
  "exchange.trade_calendar": "交易日历",
  "exchange.stock_historical_list": "历史股票列表",
  "exchange.stock_basic_info": "股票基础信息"
};

const CATALOG_LABELS = new Map<string, string>();
for (const item of catalogItems) {
  CATALOG_LABELS.set(item.id, item.title);
  CATALOG_LABELS.set(item.name, item.title);
}

export function DataBrowserPage({
  apiBase,
  onOpenCollector
}: {
  apiBase: string;
  onOpenCollector?: () => void;
}) {
  const [datasets, setDatasets] = useState<DatasetSummary[]>([]);
  const [activeDatasetId, setActiveDatasetId] = useState<string>("");
  const [datasetQuery, setDatasetQuery] = useState("");
  const [previewRows, setPreviewRows] = useState<Array<Record<string, unknown>>>([]);
  const [previewColumns, setPreviewColumns] = useState<string[]>([]);
  const [previewSource, setPreviewSource] = useState<DatasetPreviewSource | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isPreviewLoading, setIsPreviewLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [symbol, setSymbol] = useState("");
  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");
  const [limit, setLimit] = useState("3");
  const [deleteBusyDataset, setDeleteBusyDataset] = useState<string | null>(null);
  const [deleteMessage, setDeleteMessage] = useState<string | null>(null);
  const [deleteError, setDeleteError] = useState<string | null>(null);

  const filteredDatasets = useMemo(() => {
    return datasets.filter((dataset) => datasetMatchesQuery(dataset, datasetQuery));
  }, [datasets, datasetQuery]);
  const activeDataset = useMemo(() => {
    const selected = datasets.find((item) => item.dataset === activeDatasetId);
    if (selected && datasetMatchesQuery(selected, datasetQuery)) {
      return selected;
    }
    return filteredDatasets[0] ?? null;
  }, [activeDatasetId, datasets, datasetQuery, filteredDatasets]);
  const formatBadgeLabel = useMemo(() => datasetFormatLabel(datasets), [datasets]);

  async function loadDatasets(signal?: AbortSignal) {
    setIsLoading(true);
    setError(null);
    try {
      const response = await apiFetch(`${apiBase}/v1/data/datasets`, {
        signal,
        headers: { Accept: "application/json" }
      });
      const payload = await response.json();
      if (!response.ok || payload?.success === false) {
        throw new Error(payload?.error?.message ?? payload?.detail ?? `HTTP ${response.status}`);
      }
      const rows = Array.isArray(payload.data) ? payload.data : [];
      setDatasets(rows);
      setActiveDatasetId((current) => current && rows.some((item: DatasetSummary) => item.dataset === current)
        ? current
        : rows[0]?.dataset ?? "");
    } catch (loadError) {
      if (!signal?.aborted) {
        setDatasets([]);
        setActiveDatasetId("");
        setError(loadError instanceof Error ? loadError.message : "本地数据集读取失败");
      }
    } finally {
      if (!signal?.aborted) {
        setIsLoading(false);
      }
    }
  }

  useEffect(() => {
    const controller = new AbortController();
    loadDatasets(controller.signal);
    return () => controller.abort();
  }, [apiBase]);

  useEffect(() => {
    if (!activeDataset) {
      setPreviewRows([]);
      setPreviewColumns([]);
      setPreviewSource(null);
      return;
    }
    const controller = new AbortController();
    loadPreview(activeDataset, controller.signal);
    return () => controller.abort();
  }, [activeDataset?.dataset, apiBase]);

  async function loadPreview(dataset: DatasetSummary, signal?: AbortSignal) {
    setIsPreviewLoading(true);
    setPreviewError(null);
    try {
      const params = new URLSearchParams();
      if (symbol.trim()) {
        params.set("symbol", symbol.trim());
      }
      if (start.trim()) {
        params.set("start", start.trim());
      }
      if (end.trim()) {
        params.set("end", end.trim());
      }
      params.set("limit", limit.trim() || "20");
      const response = await apiFetch(`${apiBase}/v1/data/datasets/${encodeURIComponent(dataset.dataset)}/preview?${params}`, {
        signal,
        headers: { Accept: "application/json" }
      });
      const payload = (await response.json()) as DatasetPreviewPayload & { detail?: string };
      if (!response.ok || payload?.success === false) {
        throw new Error(payload?.error?.message ?? payload?.detail ?? `HTTP ${response.status}`);
      }
      const rows = Array.isArray(payload.data) ? payload.data : [];
      setPreviewRows(rows);
      setPreviewColumns(payload.meta?.columns ?? Object.keys(rows[0] ?? {}));
      setPreviewSource({
        format: payload.meta?.preview_format ?? "parquet",
        paths: payload.meta?.preview_paths ?? []
      });
    } catch (loadError) {
      if (!signal?.aborted) {
        setPreviewRows([]);
        setPreviewColumns([]);
        setPreviewSource(null);
        setPreviewError(loadError instanceof Error ? loadError.message : "预览读取失败");
      }
    } finally {
      if (!signal?.aborted) {
        setIsPreviewLoading(false);
      }
    }
  }

  function applyPreviewFilters() {
    if (activeDataset) {
      loadPreview(activeDataset);
    }
  }

  async function deleteDatasetAction(dataset: DatasetSummary) {
    const label = datasetDisplayName(dataset);
    const paths = datasetDeletePathHints(dataset);
    const pathText = paths.length ? `\n\n将删除数据集目录：\n${paths.join("\n")}` : "";
    const confirmed = window.confirm(
      `确认删除本地数据集「${label}」？\n\n数据集 ID：${dataset.dataset}\n这会删除该数据集目录下的 Parquet、CSV、DuckDB 等本地文件，并清理相关运行记录；不会删除采集任务配置。${pathText}`
    );
    if (!confirmed) {
      return;
    }
    setDeleteBusyDataset(dataset.dataset);
    setDeleteMessage(null);
    setDeleteError(null);
    try {
      const response = await apiFetch(`${apiBase}/v1/data/datasets/${encodeURIComponent(dataset.dataset)}`, {
        method: "DELETE",
        headers: { Accept: "application/json" }
      });
      const payload = await response.json();
      if (!response.ok || payload?.success === false) {
        throw new Error(payload?.error?.message ?? payload?.detail ?? `HTTP ${response.status}`);
      }
      setDeleteMessage(`已删除「${label}」。`);
      setPreviewRows([]);
      setPreviewColumns([]);
      setPreviewSource(null);
      await loadDatasets();
    } catch (deleteFailure) {
      setDeleteError(deleteFailure instanceof Error ? deleteFailure.message : "数据集删除失败");
    } finally {
      setDeleteBusyDataset(null);
    }
  }

  const totalRows = datasets.reduce((sum, item) => sum + (item.row_count ?? 0), 0);
  const showEmptyState = !isLoading && !error && datasets.length === 0;

  return (
    <>
      <section className="doc-hero single">
        <div className="doc-heading">
          <div className="endpoint-line">
            <span className="section-eyebrow">本地数据</span>
            <span className="ready-badge">
              <Database size={15} />
              已保存格式：
              {formatBadgeLabel}
            </span>
            <span className="ready-badge example">Parquet 主数据 / CSV 导出 / DuckDB 查询缓存</span>
          </div>
          <div className="title-row">
            <span className="title-icon">
              <Table2 size={26} />
            </span>
            <h1>数据中心</h1>
          </div>
          <p>按逻辑表查看本机数据、字段说明、输出路径和通用查询预览。</p>
          <div className="metrics-row">
            <Metric icon={FileArchive} label="数据集" value={String(datasets.length)} />
            <Metric icon={Database} label="行数" value={totalRows ? totalRows.toLocaleString("zh-CN") : "0"} />
            <Metric icon={CheckCircle2} label="正常" value={String(datasets.filter((item) => item.quality_status === "ok").length)} />
          </div>
        </div>
      </section>

      <section className={`data-browser-layout${showEmptyState ? " empty" : ""}`}>
        {showEmptyState ? (
          <div className="data-browser-empty-state">
            <div>
              <span className="empty-state-icon">
                <Database size={24} />
              </span>
              <h2>还没有本地数据</h2>
              <p>先去「采集」页面运行一个默认任务。</p>
              <p>采集完成后，这里会显示数据集、行数、质量状态和预览。</p>
            </div>
            <div className="empty-state-actions">
              {onOpenCollector ? (
                <button className="primary-action" onClick={onOpenCollector} type="button">
                  <Download size={17} />
                  去采集
                </button>
              ) : null}
              <button className="ghost-action" disabled={isLoading} onClick={() => loadDatasets()} type="button">
                {isLoading ? <Loader2 size={16} /> : <RefreshCw size={16} />}
                刷新
              </button>
            </div>
            <details className="empty-state-cli">
              <summary>查看命令行示例</summary>
              <code>axdata collector task run &lt;task_id&gt; --wait --json</code>
            </details>
          </div>
        ) : (
          <>
        <div className="dataset-list-panel">
          <div className="section-title compact-title">
            <Database size={19} />
            <h2>数据表目录</h2>
            <button className="ghost-action compact" disabled={isLoading} onClick={() => loadDatasets()} type="button">
              {isLoading ? <Loader2 size={15} /> : <RefreshCw size={15} />}
              刷新
            </button>
          </div>
          <div className="dataset-list-search">
            <Search size={16} />
            <input
              aria-label="搜索数据表"
              onChange={(event) => setDatasetQuery(event.target.value)}
              placeholder="搜索名称、表名、来源"
              value={datasetQuery}
            />
            {datasetQuery.trim() ? (
              <button aria-label="清空搜索" onClick={() => setDatasetQuery("")} title="清空搜索" type="button">
                <X size={14} />
              </button>
            ) : null}
          </div>
          {error ? (
            <div className="data-browser-message error">
              <AlertCircle size={17} />
              <span>{error}</span>
            </div>
          ) : null}
          {isLoading ? (
            <div className="data-browser-message">
              <Loader2 size={17} />
              <span>正在读取本地 metadata</span>
            </div>
          ) : filteredDatasets.length > 0 ? (
            <div className="dataset-list">
              {filteredDatasets.map((dataset) => (
                <button
                  className={dataset.dataset === activeDataset?.dataset ? "dataset-list-item active" : "dataset-list-item"}
                  key={dataset.dataset}
                  onClick={() => setActiveDatasetId(dataset.dataset)}
                  type="button"
                >
                  <strong>{datasetDisplayName(dataset)}</strong>
                  <small>{dataset.logical_table || dataset.dataset}</small>
                  <span>{dataset.layer || "数据表"} · {formatRows(dataset.row_count) || "未采集"}</span>
                  <em className={qualityClass(dataset.quality_status)} title={qualityTitle(dataset)}>
                    {formatQuality(dataset.quality_status)}
                  </em>
                </button>
              ))}
            </div>
          ) : datasets.length > 0 ? (
            <div className="data-browser-empty">
              <strong>没有匹配的数据表</strong>
              <span>换个关键词试试。</span>
            </div>
          ) : null}
        </div>

        <div className="dataset-detail-panel">
          {activeDataset ? (
            <>
              <section className="doc-section dataset-detail-section">
                <div className="section-title">
                  <Table2 size={20} />
                  <h2>{datasetDisplayName(activeDataset)}</h2>
                  <code className="dataset-title-id">{activeDataset.dataset}</code>
                  <span className={qualityBadgeClass(activeDataset.quality_status)} title={qualityTitle(activeDataset)}>
                    {formatQuality(activeDataset.quality_status)}
                  </span>
                  <button
                    className="ghost-action compact danger dataset-delete-action"
                    disabled={deleteBusyDataset === activeDataset.dataset}
                    onClick={() => deleteDatasetAction(activeDataset)}
                    type="button"
                  >
                    {deleteBusyDataset === activeDataset.dataset ? <Loader2 size={15} /> : <Trash2 size={15} />}
                    删除
                  </button>
                </div>
                {deleteMessage ? <p className="form-success">{deleteMessage}</p> : null}
                {deleteError ? <p className="form-error">{deleteError}</p> : null}
                <DataTable
                  columns={["项目", "当前值", "说明"]}
                  rows={datasetFacts(activeDataset)}
                />
                {activeDataset.quality_warnings?.length || activeDataset.quality_errors?.length ? (
                  <div className="quality-note-list">
                    {(activeDataset.quality_errors ?? []).map((item) => (
                      <p className="quality-note error" key={`error:${item}`}>{item}</p>
                    ))}
                    {(activeDataset.quality_warnings ?? []).map((item) => (
                      <p className="quality-note" key={`warning:${item}`}>{item}</p>
                    ))}
                  </div>
                ) : null}
              </section>

              <section className="doc-section dataset-preview-section">
                <div className="section-title">
                  <Search size={20} />
                  <h2>数据预览</h2>
                </div>
                <PreviewSourceNotice source={previewSource} />
                <div className="preview-filter-row">
                  <label>
                    <span>证券代码</span>
                    <input value={symbol} onChange={(event) => setSymbol(event.target.value)} placeholder="000001.SZ" />
                  </label>
                  <label>
                    <span>开始日期</span>
                    <input value={start} onChange={(event) => setStart(event.target.value)} placeholder="20240101" />
                  </label>
                  <label>
                    <span>结束日期</span>
                    <input value={end} onChange={(event) => setEnd(event.target.value)} placeholder="20240131" />
                  </label>
                  <label>
                    <span>行数</span>
                    <input value={limit} onChange={(event) => setLimit(event.target.value)} placeholder="3" />
                  </label>
                  <button className="primary-action" disabled={isPreviewLoading} onClick={applyPreviewFilters} type="button">
                    {isPreviewLoading ? <Loader2 size={17} /> : <Search size={17} />}
                    查询
                  </button>
                </div>
                {previewError ? (
                  <div className="data-browser-message error">
                    <AlertCircle size={17} />
                    <span>{previewError}</span>
                  </div>
                ) : isPreviewLoading ? (
                  <div className="data-browser-message">
                    <Loader2 size={17} />
                    <span>正在读取 Parquet 预览</span>
                  </div>
                ) : previewRows.length > 0 ? (
                  <DataTable columns={previewColumns} rows={previewRowsToTable(previewRows, previewColumns)} />
                ) : (
                  <div className="data-browser-empty">
                    <strong>当前筛选没有返回行</strong>
                  </div>
                )}
              </section>

              <section className="doc-section">
                <div className="section-title">
                  <FileArchive size={20} />
                  <h2>输出路径</h2>
                </div>
                <div className="path-list">
                  {Object.entries(activeDataset.output_paths ?? {}).map(([format, path]) => (
                    <code key={`${format}:${path}`}>{outputFormatLabel(format)}: {path}</code>
                  ))}
                  {(activeDataset.missing_paths ?? []).map((path) => (
                    <code className="missing" key={`missing:${path}`}>missing: {path}</code>
                  ))}
                </div>
              </section>

              <section className="doc-section">
                <div className="section-title">
                  <Database size={20} />
                  <h2>字段说明</h2>
                </div>
                <DataTable
                  columns={["字段", "类型", "中文含义"]}
                  rows={datasetFieldRows(activeDataset)}
                />
              </section>

              <section className="doc-section">
                <div className="section-title">
                  <Download size={20} />
                  <h2>通用查询</h2>
                </div>
                <code className="query-example-code">{datasetCurlExample(apiBase, activeDataset)}</code>
              </section>
            </>
          ) : (
            <div className="dataset-detail-empty">
              <span>选择一个数据集查看详情。</span>
            </div>
          )}
        </div>
          </>
        )}
      </section>
    </>
  );
}

function datasetFacts(dataset: DatasetSummary): TableRow[] {
  const quality = dataset.quality ?? {};
  return [
    ["逻辑表", dataset.logical_table || dataset.dataset, "用户查询时面对的数据表名"],
    ["来源", dataset.provider || dataset.source || "", "贡献这张表的采集器或数据源插件"],
    ["层级", dataset.layer || "", "raw / staging / core / factor / snapshot"],
    ["已保存格式", datasetFormatLabel([dataset]), "当前本地已经落盘的格式；Parquet 是主数据"],
    ["支持格式", formatList(dataset.available_formats), "Parquet 主数据，CSV 导出，DuckDB 查询缓存"],
    ["预计路径", formatExpectedPaths(dataset), "声明里的默认输出目录；未采集时也可以看到"],
    ["行数", formatRows(dataset.row_count), "优先使用质量元数据，必要时读取 Parquet footer/schema 补充"],
    ["日期范围", formatDateRange(dataset), "来自 quality.date_range 或 Parquet metadata/小范围查询"],
    ["质检范围", qualityScopeLabel(quality.quality_check_scope), "full=采集时完整质检；metadata_only=仅按 Parquet 元数据核对"],
    ["交易日历", cellText(quality.calendar_coverage_status), "ok / warn / error；无本地 trade_cal 时为 warn"],
    ["日期缺口", cellText(quality.date_gap_count), "交易日历范围内缺失的交易日数量"],
    ["非交易日", cellText(quality.extra_non_trading_dates), "样本化展示出现在非交易日的数据日期（仅日历覆盖区间内）"],
    ["日历未覆盖", cellText(quality.uncovered_date_count), "落在 trade_cal 覆盖区间之外、无法判定的数据日期数量"],
    ["K线异常", cellText(quality.price_ohlc_anomaly_count), "OHLC 高低开收关系异常行数"],
    ["复权异常", cellText(quality.invalid_adj_factor_count), "adj_factor <= 0 的行数"],
    ["最近 run", dataset.latest_run_id || "", dataset.latest_run_status || ""],
    ["更新时间", dataset.updated_at || "", "run finished_at 或 metadata 更新时间"]
  ];
}

function datasetFieldRows(dataset: DatasetSummary): TableRow[] {
  const schema = Array.isArray(dataset.field_schema) ? dataset.field_schema : [];
  if (schema.length > 0) {
    return schema.map((field) => [
      cellText(field.name),
      cellText(field.type || field.dtype),
      cellText(field.display_name_zh || field.description_zh || field.description)
    ]);
  }
  return (dataset.columns ?? []).map((column) => [column, "", ""]);
}

function PreviewSourceNotice({ source }: { source: DatasetPreviewSource | null }) {
  if (!source) {
    return null;
  }
  const paths = source.paths.slice(0, 2);
  return (
    <div className="preview-source-note">
      <span>预览来源</span>
      <strong>{outputFormatLabel(source.format)}</strong>
      {paths.map((path) => (
        <code key={path}>{path}</code>
      ))}
      {source.paths.length > paths.length ? <em>还有 {source.paths.length - paths.length} 个路径</em> : null}
    </div>
  );
}

function datasetDisplayName(dataset: DatasetSummary) {
  if (dataset.display_name_zh) {
    return dataset.display_name_zh;
  }
  return DATASET_LABEL_OVERRIDES[dataset.dataset] ||
    DATASET_LABEL_OVERRIDES[dataset.interface_name] ||
    CATALOG_LABELS.get(dataset.interface_name) ||
    CATALOG_LABELS.get(dataset.dataset) ||
    dataset.dataset;
}

function datasetMatchesQuery(dataset: DatasetSummary, query: string) {
  const tokens = query.trim().toLowerCase().split(/\s+/).filter(Boolean);
  if (tokens.length === 0) {
    return true;
  }
  const text = [
    datasetDisplayName(dataset),
    dataset.dataset,
    dataset.interface_name,
    dataset.logical_table,
    dataset.provider,
    dataset.source,
    dataset.layer,
    dataset.latest_run_id,
    dataset.latest_run_status,
    dataset.write_mode,
    ...(dataset.columns ?? []),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return tokens.every((token) => text.includes(token));
}

function datasetFormatLabel(datasets: DatasetSummary[]) {
  const formats = new Set<string>();
  for (const dataset of datasets) {
    for (const format of Object.keys(dataset.output_paths ?? {})) {
      const clean = format.trim().toLowerCase();
      if (!clean || clean === "log" || clean === "logs") {
        continue;
      }
      formats.add(clean);
    }
  }
  const ordered = Array.from(formats).sort(compareOutputFormat);
  if (ordered.length === 0 || ordered.length > 3) {
    return "多格式";
  }
  return ordered.map(outputFormatLabel).join(" / ");
}

function formatList(values: string[] | null | undefined) {
  const items = Array.isArray(values)
    ? values.map((item) => item.trim().toLowerCase()).filter(Boolean)
    : [];
  if (items.length === 0) {
    return "";
  }
  return Array.from(new Set(items)).sort(compareOutputFormat).map(outputFormatLabel).join(" / ");
}

function compareOutputFormat(left: string, right: string) {
  const order = ["parquet", "csv", "duckdb", "jsonl", "json", "arrow"];
  const leftIndex = order.indexOf(left);
  const rightIndex = order.indexOf(right);
  if (leftIndex >= 0 || rightIndex >= 0) {
    return (leftIndex >= 0 ? leftIndex : order.length) - (rightIndex >= 0 ? rightIndex : order.length);
  }
  return left.localeCompare(right);
}

function outputFormatLabel(format: string) {
  const labels: Record<string, string> = {
    parquet: "Parquet",
    csv: "CSV",
    duckdb: "DuckDB",
    jsonl: "JSONL",
    json: "JSON",
    arrow: "Arrow"
  };
  return labels[format] ?? format.toUpperCase();
}

function datasetCurlExample(apiBase: string, dataset: DatasetSummary) {
  const table = dataset.logical_table || dataset.dataset;
  const fields = dataset.default_query_fields?.length
    ? dataset.default_query_fields.slice(0, 5)
    : (dataset.columns ?? []).slice(0, 5);
  const payload: Record<string, unknown> = {
    table,
    fields,
    limit: 3,
  };
  if ((dataset.default_filter_fields ?? []).includes("ts_code") || (dataset.columns ?? []).includes("ts_code")) {
    payload.filters = { ts_code: "000001.SZ" };
  } else if ((dataset.default_filter_fields ?? []).includes("instrument_id") || (dataset.columns ?? []).includes("instrument_id")) {
    payload.filters = { instrument_id: "000001.SZ" };
  }
  if ((dataset.default_filter_fields ?? []).includes("start_date") || dataset.date_field) {
    payload.start_date = "20240101";
    payload.end_date = "20240131";
  }
  return `curl -X POST ${apiBase}/v1/query -H "Content-Type: application/json" -d '${JSON.stringify(payload)}'`;
}

function formatExpectedPaths(dataset: DatasetSummary) {
  const metadata = dataset.metadata ?? {};
  const expected = isRecord(metadata.expected_output_paths) ? metadata.expected_output_paths : {};
  const paths = Object.entries(expected).map(([format, path]) => `${outputFormatLabel(format)}: ${String(path)}`);
  return paths.join("\n");
}

function datasetDeletePathHints(dataset: DatasetSummary) {
  const paths = Object.values(dataset.output_paths ?? {}).filter(Boolean);
  const hints = paths.map((path) => datasetDeletePathHint(path, dataset));
  return Array.from(new Set(hints.filter(Boolean)));
}

function datasetDeletePathHint(path: string, dataset: DatasetSummary) {
  const normalized = path.replace(/\\/g, "/");
  const parts = normalized.split("/").filter(Boolean);
  const names = new Set(
    [dataset.dataset, dataset.interface_name]
      .flatMap((name) => {
        const clean = String(name || "").trim().toLowerCase();
        return clean ? [clean, `table=${clean}`, `interface=${clean}`] : [];
      })
  );
  const index = parts.findIndex((part) => {
    const clean = part.toLowerCase();
    if (names.has(clean)) {
      return true;
    }
    if (clean.includes("=")) {
      const [, value] = clean.split("=", 2);
      return names.has(value);
    }
    return false;
  });
  const hintParts = index >= 0 ? parts.slice(0, index + 1) : parts;
  const separator = path.includes("\\") ? "\\" : "/";
  const prefix = normalized.startsWith("/") ? separator : "";
  return `${prefix}${hintParts.join(separator)}`;
}

function previewRowsToTable(rows: Array<Record<string, unknown>>, columns: string[]): TableRow[] {
  return rows.map((row) => columns.map((column) => cellText(row[column])));
}

function cellText(value: unknown) {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function formatRows(value: number | null | undefined) {
  return value === null || value === undefined ? "" : value.toLocaleString("zh-CN");
}

function formatDateRange(dataset: DatasetSummary) {
  const start = dataset.date_min ?? dataset.datetime_min;
  const end = dataset.date_max ?? dataset.datetime_max;
  if (start && end) {
    return `${start} - ${end}`;
  }
  return start ?? end ?? "";
}

function qualityScopeLabel(scope: unknown) {
  if (scope === "metadata_only") {
    return "metadata_only（仅元数据）";
  }
  if (typeof scope === "string" && scope) {
    return scope;
  }
  return "full";
}

function qualityTitle(dataset: Pick<DatasetSummary, "quality" | "quality_status">) {
  const scope = (dataset.quality ?? {}).quality_check_scope;
  if (scope === "metadata_only") {
    return "仅按 Parquet 元数据（路径、列、行数）核对，未逐行质检";
  }
  return undefined;
}

function formatQuality(status: string | null | undefined) {
  if (status === "ok") {
    return "正常";
  }
  if (status === "warn") {
    return "提醒";
  }
  if (status === "error") {
    return "异常";
  }
  return "未知";
}

function qualityClass(status: string | null | undefined) {
  if (status === "ok") {
    return "ok";
  }
  if (status === "error") {
    return "error";
  }
  if (status === "warn") {
    return "warn";
  }
  return "";
}

function qualityBadgeClass(status: string | null | undefined) {
  return `provider-status-badge ${qualityClass(status) || "installed"}`;
}
