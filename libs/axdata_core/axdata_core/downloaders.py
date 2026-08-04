"""Small downloader layer for source interfaces."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, Callable
from uuid import uuid4

import pandas as pd

from .downloader_engine import (
    DefaultRequestPlanner,
    DownloadMetadataWriter,
    DownloadQualityChecker,
    DownloadRequestPlan,
    DownloadWriter,
    WriteStrategyError,
    emit_progress,
)
from .downloader_registry import (
    load_builtin_downloader_adapter_factories,
    load_builtin_downloader_profiles,
    load_builtin_runtime_source_server_max_factories,
)

if TYPE_CHECKING:
    from .source_request import SourceRequestAdapter, SourceRequestResult

LOCAL_TIMEZONE = timezone(timedelta(hours=8), name="Asia/Shanghai")
_DOWNLOADER_PROFILES_CACHE: dict[str, "DownloaderProfile"] | None = None
_REQUEST_PLANNER = DefaultRequestPlanner()
_DOWNLOAD_WRITER = DownloadWriter()
_QUALITY_CHECKER = DownloadQualityChecker()
_METADATA_WRITER = DownloadMetadataWriter()


class DownloaderError(RuntimeError):
    """Raised when a downloader cannot run."""


def request_interface(*args: Any, **kwargs: Any) -> "SourceRequestResult":
    """Call the source request gateway without importing it at module load."""

    from .source_request import request_interface as current_request_interface

    return current_request_interface(*args, **kwargs)


@dataclass(frozen=True)
class ConcurrencyProfile:
    """Static concurrency capability for one downloader."""

    mode: str = "fixed"
    mode_editable: bool = False
    default_source_server_count: int = 1
    source_server_count_editable: bool = False
    max_source_server_count: int = 1
    default_connections_per_server: int = 1
    connections_per_server_editable: bool = False
    max_connections_per_server: int = 1
    default_max_concurrent_tasks: int = 1
    max_concurrent_tasks_editable: bool = False
    max_max_concurrent_tasks: int = 1
    default_batch_size: int = 1
    batch_size_editable: bool = False
    max_batch_size: int = 1
    default_request_interval_ms: int = 0
    request_interval_ms_editable: bool = False
    min_request_interval_ms: int = 0
    max_request_interval_ms: int = 0
    default_retry_count: int = 0
    retry_count_editable: bool = False
    max_retry_count: int = 0
    default_timeout_ms: int = 30000
    timeout_ms_editable: bool = False
    min_timeout_ms: int = 30000
    max_timeout_ms: int = 30000
    description: str = ""

    @property
    def default_connection_count(self) -> int:
        return min(
            self.default_source_server_count * self.default_connections_per_server,
            self.default_max_concurrent_tasks,
        )

    @property
    def max_connection_count(self) -> int:
        return min(
            self.max_source_server_count * self.max_connections_per_server,
            self.max_max_concurrent_tasks,
        )

    @property
    def connection_count_editable(self) -> bool:
        return (
            self.source_server_count_editable
            or self.connections_per_server_editable
            or self.max_concurrent_tasks_editable
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "mode_editable": self.mode_editable,
            "default_source_server_count": self.default_source_server_count,
            "source_server_count_editable": self.source_server_count_editable,
            "max_source_server_count": self.max_source_server_count,
            "default_connections_per_server": self.default_connections_per_server,
            "connections_per_server_editable": self.connections_per_server_editable,
            "max_connections_per_server": self.max_connections_per_server,
            "default_max_concurrent_tasks": self.default_max_concurrent_tasks,
            "max_concurrent_tasks_editable": self.max_concurrent_tasks_editable,
            "max_max_concurrent_tasks": self.max_max_concurrent_tasks,
            "default_batch_size": self.default_batch_size,
            "batch_size_editable": self.batch_size_editable,
            "max_batch_size": self.max_batch_size,
            "default_request_interval_ms": self.default_request_interval_ms,
            "request_interval_ms_editable": self.request_interval_ms_editable,
            "min_request_interval_ms": self.min_request_interval_ms,
            "max_request_interval_ms": self.max_request_interval_ms,
            "default_retry_count": self.default_retry_count,
            "retry_count_editable": self.retry_count_editable,
            "max_retry_count": self.max_retry_count,
            "default_timeout_ms": self.default_timeout_ms,
            "timeout_ms_editable": self.timeout_ms_editable,
            "min_timeout_ms": self.min_timeout_ms,
            "max_timeout_ms": self.max_timeout_ms,
            "default_connection_count": self.default_connection_count,
            "connection_count_editable": self.connection_count_editable,
            "max_connection_count": self.max_connection_count,
            "description": self.description,
        }


@dataclass(frozen=True)
class ResolvedConcurrency:
    """Runtime concurrency settings after profile limits are applied."""

    mode: str
    source_server_count: int
    connections_per_server: int
    max_concurrent_tasks: int
    batch_size: int
    request_interval_ms: int
    retry_count: int
    timeout_ms: int

    @property
    def connection_count(self) -> int:
        return min(
            self.source_server_count * self.connections_per_server,
            self.max_concurrent_tasks,
        )

    def to_dict(self) -> dict[str, int | str]:
        return {
            "mode": self.mode,
            "source_server_count": self.source_server_count,
            "connections_per_server": self.connections_per_server,
            "max_concurrent_tasks": self.max_concurrent_tasks,
            "connection_count": self.connection_count,
            "batch_size": self.batch_size,
            "request_interval_ms": self.request_interval_ms,
            "retry_count": self.retry_count,
            "timeout_ms": self.timeout_ms,
        }


@dataclass(frozen=True)
class DownloaderProfile:
    """Static downloader capability for one source interface."""

    interface_name: str
    display_name: str
    downloader_type: str
    default_params: dict[str, Any]
    default_fields: list[str] | None
    output_layer: str
    output_format: str
    supported_formats: list[str]
    primary_key: str | tuple[str, ...]
    resource_group: str = "default"
    default_connection_mode: str = "long_connection"
    concurrency: ConcurrencyProfile = field(default_factory=ConcurrencyProfile)
    adapter_factory: str | None = None
    runtime_source_server_max_factory: str | None = None
    default_output_path_parts: list[str] = field(default_factory=list)
    snapshot_date_meta_keys: list[str] = field(
        default_factory=lambda: ["snapshot_date", "data_date", "trade_date", "date"]
    )
    file_stem_template: str = "{interface_name}_{collection_time}"
    write_mode: str = "snapshot"
    partition_by: list[str] = field(default_factory=list)
    quality_rules: list[str] = field(default_factory=lambda: ["schema", "primary_key", "row_count"])
    required_columns: list[str] = field(default_factory=list)
    expected_columns: list[str] = field(default_factory=list)
    date_field: str | None = None
    datetime_field: str | None = None
    numeric_positive_columns: list[str] = field(default_factory=list)
    field_mappings: dict[str, str] = field(default_factory=dict)
    params: list[list[str]] | None = None
    description: str = ""
    provider_id: str | None = None
    manifest_downloader_name: str | None = None
    effective_trust_level: str | None = None
    built_in_provider: bool | None = None
    manifest_default_options: dict[str, Any] = field(default_factory=dict)
    manifest_default_limits: dict[str, Any] = field(default_factory=dict)
    manifest_output: dict[str, Any] = field(default_factory=dict)

    @property
    def default_connection_count(self) -> int:
        return self.concurrency.default_connection_count

    @property
    def connection_count_editable(self) -> bool:
        return self.concurrency.connection_count_editable

    @property
    def max_connection_count(self) -> int:
        return self.concurrency.max_connection_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "interface_name": self.interface_name,
            "display_name": self.display_name,
            "downloader_type": self.downloader_type,
            "resource_group": self.resource_group,
            "default_params": dict(self.default_params),
            "default_fields": list(self.default_fields) if self.default_fields is not None else None,
            "output_layer": self.output_layer,
            "output_format": self.output_format,
            "supported_formats": list(self.supported_formats),
            "default_connection_mode": self.default_connection_mode,
            "default_connection_count": self.default_connection_count,
            "connection_count_editable": self.connection_count_editable,
            "max_connection_count": self.max_connection_count,
            "concurrency": self.concurrency.to_dict(),
            "adapter_factory": self.adapter_factory,
            "runtime_source_server_max_factory": self.runtime_source_server_max_factory,
            "default_output_path_parts": list(self.default_output_path_parts),
            "snapshot_date_meta_keys": list(self.snapshot_date_meta_keys),
            "file_stem_template": self.file_stem_template,
            "write_mode": self.write_mode,
            "primary_key": list(self.primary_key) if isinstance(self.primary_key, tuple) else self.primary_key,
            "partition_by": list(self.partition_by),
            "quality_rules": list(self.quality_rules),
            "required_columns": list(self.required_columns),
            "expected_columns": list(self.expected_columns),
            "date_field": self.date_field,
            "datetime_field": self.datetime_field,
            "numeric_positive_columns": list(self.numeric_positive_columns),
            "field_mappings": dict(self.field_mappings),
            "params": [list(row) for row in self.params] if self.params is not None else None,
            "description": self.description,
            "provider_id": self.provider_id,
            "manifest_downloader_name": self.manifest_downloader_name,
            "effective_trust_level": self.effective_trust_level,
            "built_in_provider": self.built_in_provider,
            "manifest_default_options": dict(self.manifest_default_options),
            "manifest_default_limits": dict(self.manifest_default_limits),
            "manifest_output": dict(self.manifest_output),
        }


class _LazyDownloaderProfiles:
    """Compatibility mapping that loads bundled profiles on first use."""

    def _profiles(self) -> dict[str, DownloaderProfile]:
        return _downloader_profiles()

    def __getitem__(self, key: str) -> DownloaderProfile:
        return self._profiles()[key]

    def __iter__(self):
        return iter(self._profiles())

    def __len__(self) -> int:
        return len(self._profiles())

    def values(self):
        return self._profiles().values()

    def keys(self):
        return self._profiles().keys()

    def items(self):
        return self._profiles().items()

    def get(self, key: str, default: Any = None) -> DownloaderProfile | Any:
        return self._profiles().get(key, default)


DOWNLOADER_PROFILES = _LazyDownloaderProfiles()


def _downloader_profiles() -> dict[str, DownloaderProfile]:
    global _DOWNLOADER_PROFILES_CACHE
    if _DOWNLOADER_PROFILES_CACHE is None:
        _DOWNLOADER_PROFILES_CACHE = load_builtin_downloader_profiles(
            ConcurrencyProfile,
            DownloaderProfile,
        )
    return _DOWNLOADER_PROFILES_CACHE


@dataclass(frozen=True)
class _RegistryDownloaderProjection:
    provider_id: str
    effective_trust_level: str
    built_in: bool
    downloader: Any
    interface: Any


def list_downloader_profiles(*, data_root: str | Path | None = None) -> tuple[dict[str, Any], ...]:
    """Return all currently supported downloader profiles."""

    profiles = _downloader_profiles()
    projections = _registry_downloader_projections(data_root=data_root)
    if projections is None:
        return tuple(profile.to_dict() for profile in profiles.values())
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for interface_name, profile in profiles.items():
        projection = projections.get(interface_name)
        if projection is None:
            continue
        rows.append(_project_downloader_profile(profile, projection, data_root=data_root).to_dict())
        seen.add(interface_name)
    for interface_name, projection in projections.items():
        if interface_name in seen:
            continue
        rows.append(_generic_downloader_profile(projection).to_dict())
    return tuple(rows)


def get_downloader_profile(interface_name: str, *, data_root: str | Path | None = None) -> DownloaderProfile:
    """Return one downloader profile by interface name."""

    projections = _registry_downloader_projections(data_root=data_root)
    if projections is not None:
        projection = projections.get(interface_name)
        if projection is None:
            known = ", ".join(projections) or "<empty>"
            raise DownloaderError(
                f"Downloader is not available for {interface_name!r} in the current Provider registry. "
                f"Known available downloaders: {known}."
            )
        profile = _downloader_profiles().get(interface_name)
        if profile is None:
            return _generic_downloader_profile(projection)
        return _project_downloader_profile(profile, projection, data_root=data_root)
    try:
        return _downloader_profiles()[interface_name]
    except KeyError as exc:
        known = ", ".join(_downloader_profiles()) or "<empty>"
        raise DownloaderError(
            f"Downloader is not configured for {interface_name!r}. Known downloaders: {known}."
        ) from exc


def _registry_downloader_projections(
    *,
    data_root: str | Path | None = None,
) -> dict[str, _RegistryDownloaderProjection] | None:
    try:
        from .provider_catalog import build_builtin_provider_registry

        registry = build_builtin_provider_registry(data_root=data_root)
    except Exception:
        return None

    snapshot = registry.snapshot()
    projections: dict[str, _RegistryDownloaderProjection] = {}
    for interface_name, route in snapshot.interfaces.items():
        provider = snapshot.providers.get(route.provider_id)
        if provider is None:
            continue
        for downloader in provider.manifest.downloaders:
            if downloader.interface_name == interface_name:
                projections[interface_name] = _RegistryDownloaderProjection(
                    provider_id=provider.provider_id,
                    effective_trust_level=route.effective_trust_level,
                    built_in=route.built_in,
                    downloader=downloader,
                    interface=route.interface,
                )
                break
    return projections


def _generic_downloader_profile(projection: _RegistryDownloaderProjection) -> DownloaderProfile:
    downloader = projection.downloader
    default_options = dict(getattr(downloader, "default_options", {}) or {})
    default_limits = dict(getattr(downloader, "default_limits", {}) or {})
    output = dict(getattr(downloader, "output", {}) or {})
    supported_formats = _string_list_option(output.get("supported_formats"), fallback=["parquet", "csv", "jsonl"])
    output_format = _first_format_option(
        default_options.get("formats"),
        supported_formats=supported_formats,
        fallback="parquet",
    )
    primary_key = _primary_key_option(output.get("primary_key", default_options.get("primary_key")))
    quality_rules = ["schema", "primary_key", "row_count"] if primary_key else ["schema", "row_count"]
    default_fields = _fields_option(
        default_options.get("fields"),
        fallback=[field.name for field in getattr(projection.interface, "fields", ())] or None,
    )
    expected_columns = _string_list_option(output.get("expected_columns"), fallback=default_fields or [])
    return DownloaderProfile(
        interface_name=str(getattr(downloader, "interface_name", projection.interface.name)),
        display_name=str(getattr(downloader, "display_name_zh", None) or projection.interface.display_name_zh),
        downloader_type=str(getattr(downloader, "mode", None) or "snapshot"),
        default_params=_mapping_option(default_options.get("params"), fallback={}),
        default_fields=default_fields,
        output_layer=_string_option(output.get("output_layer"), fallback="snapshot"),
        output_format=output_format,
        supported_formats=supported_formats,
        primary_key=primary_key or "__axdata_row__",
        resource_group=str(getattr(downloader, "resource_group", None) or "default"),
        default_connection_mode=_string_option(default_options.get("connection_mode"), fallback="long_connection"),
        concurrency=_generic_concurrency_profile(default_options=default_options, default_limits=default_limits),
        adapter_factory=None,
        runtime_source_server_max_factory=None,
        default_output_path_parts=_default_output_path_parts(projection, output=output),
        file_stem_template=_file_stem_template(output.get("file_name_template")),
        write_mode=_write_mode_option(output.get("write_mode"), fallback="snapshot"),
        partition_by=_string_list_option(output.get("partition_by"), fallback=[]),
        quality_rules=quality_rules,
        required_columns=_string_list_option(output.get("required_columns"), fallback=[]),
        expected_columns=expected_columns,
        date_field=_string_option(output.get("date_field"), fallback=None),
        datetime_field=_string_option(output.get("datetime_field"), fallback=None),
        numeric_positive_columns=_string_list_option(output.get("numeric_positive_columns"), fallback=[]),
        field_mappings=_mapping_option(output.get("field_mappings"), fallback={}),
        description=str(getattr(projection.interface, "notes", "") or ""),
        provider_id=projection.provider_id,
        manifest_downloader_name=str(getattr(downloader, "name", "") or ""),
        effective_trust_level=projection.effective_trust_level,
        built_in_provider=projection.built_in,
        manifest_default_options=default_options,
        manifest_default_limits=default_limits,
        manifest_output=output,
    )


def _project_downloader_profile(
    profile: DownloaderProfile,
    projection: _RegistryDownloaderProjection,
    *,
    data_root: str | Path | None = None,
) -> DownloaderProfile:
    downloader = projection.downloader
    default_options = dict(getattr(downloader, "default_options", {}) or {})
    default_limits = dict(getattr(downloader, "default_limits", {}) or {})
    output = dict(getattr(downloader, "output", {}) or {})

    default_params = _mapping_option(default_options.get("params"), fallback=profile.default_params)
    default_fields = _fields_option(default_options.get("fields"), fallback=profile.default_fields)
    supported_formats = _string_list_option(output.get("supported_formats"), fallback=profile.supported_formats)
    output_format = _first_format_option(
        default_options.get("formats"),
        supported_formats=supported_formats,
        fallback=profile.output_format,
    )
    output_layer = _string_option(output.get("output_layer"), fallback=profile.output_layer)
    write_mode = _write_mode_option(output.get("write_mode"), fallback=profile.write_mode)
    partition_by = _string_list_option(output.get("partition_by"), fallback=profile.partition_by)
    required_columns = _string_list_option(output.get("required_columns"), fallback=profile.required_columns)
    expected_columns = _string_list_option(output.get("expected_columns"), fallback=profile.expected_columns)
    date_field = _string_option(output.get("date_field"), fallback=profile.date_field)
    datetime_field = _string_option(output.get("datetime_field"), fallback=profile.datetime_field)
    numeric_positive_columns = _string_list_option(
        output.get("numeric_positive_columns"),
        fallback=profile.numeric_positive_columns,
    )
    field_mappings = _mapping_option(output.get("field_mappings"), fallback=profile.field_mappings)
    default_connection_mode = _string_option(
        default_options.get("connection_mode"),
        fallback=profile.default_connection_mode,
    )
    keep_tdx_factories = _projection_uses_tdx_factories(profile, projection)
    runtime_source_server_max = (
        _runtime_source_server_max(profile, profile.concurrency, data_root=data_root)
        if projection.built_in or keep_tdx_factories
        else profile.concurrency.max_source_server_count
    )
    concurrency = _project_concurrency_profile(
        profile.concurrency,
        default_options=default_options,
        default_limits=default_limits,
        runtime_source_server_max=runtime_source_server_max,
    )

    return replace(
        profile,
        display_name=str(getattr(downloader, "display_name_zh", None) or profile.display_name),
        resource_group=str(getattr(downloader, "resource_group", None) or profile.resource_group),
        default_params=default_params,
        default_fields=default_fields,
        output_layer=output_layer,
        write_mode=write_mode,
        partition_by=partition_by,
        output_format=output_format,
        supported_formats=supported_formats,
        required_columns=required_columns,
        expected_columns=expected_columns,
        date_field=date_field,
        datetime_field=datetime_field,
        numeric_positive_columns=numeric_positive_columns,
        field_mappings=field_mappings,
        default_connection_mode=default_connection_mode,
        concurrency=concurrency,
        adapter_factory=profile.adapter_factory if projection.built_in or keep_tdx_factories else None,
        runtime_source_server_max_factory=(
            profile.runtime_source_server_max_factory
            if projection.built_in or keep_tdx_factories
            else None
        ),
        provider_id=projection.provider_id,
        manifest_downloader_name=str(getattr(downloader, "name", "") or ""),
        effective_trust_level=projection.effective_trust_level,
        built_in_provider=projection.built_in,
        manifest_default_options=default_options,
        manifest_default_limits=default_limits,
        manifest_output=output,
    )


def _projection_uses_tdx_factories(
    profile: DownloaderProfile,
    projection: _RegistryDownloaderProjection,
) -> bool:
    if profile.adapter_factory != "tdx" and profile.runtime_source_server_max_factory != "tdx":
        return False
    source_code = str(getattr(projection.interface, "source_code", "") or "")
    if not (projection.provider_id.startswith("axdata.source.tdx") or source_code == "tdx"):
        return False
    factories = load_builtin_downloader_adapter_factories()
    runtime_factories = load_builtin_runtime_source_server_max_factories()
    return (
        profile.adapter_factory in factories
        and profile.runtime_source_server_max_factory in runtime_factories
    )


def _project_concurrency_profile(
    concurrency: ConcurrencyProfile,
    *,
    default_options: dict[str, Any],
    default_limits: dict[str, Any],
    runtime_source_server_max: int,
) -> ConcurrencyProfile:
    source_server_count = _bounded_int_option(
        default_options.get("source_server_count"),
        fallback=concurrency.default_source_server_count,
        minimum=1,
        maximum=runtime_source_server_max,
    )
    connections_per_server = _bounded_int_option(
        default_options.get("connections_per_server"),
        fallback=concurrency.default_connections_per_server,
        minimum=1,
        maximum=concurrency.max_connections_per_server,
    )
    max_concurrent_tasks = _bounded_int_option(
        default_options.get("max_concurrent_tasks"),
        fallback=concurrency.default_max_concurrent_tasks,
        minimum=1,
        maximum=concurrency.max_max_concurrent_tasks,
    )
    batch_size = _bounded_int_option(
        default_options.get("batch_size"),
        fallback=concurrency.default_batch_size,
        minimum=1,
        maximum=concurrency.max_batch_size,
    )
    request_interval_ms = _bounded_int_option(
        default_options.get("request_interval_ms"),
        fallback=concurrency.default_request_interval_ms,
        minimum=concurrency.min_request_interval_ms,
        maximum=concurrency.max_request_interval_ms,
    )
    retry_count = _bounded_int_option(
        default_options.get("retry_count", default_limits.get("max_retries")),
        fallback=concurrency.default_retry_count,
        minimum=0,
        maximum=concurrency.max_retry_count,
    )
    timeout_ms = _bounded_int_option(
        default_options.get("timeout_ms"),
        fallback=concurrency.default_timeout_ms,
        minimum=concurrency.min_timeout_ms,
        maximum=concurrency.max_timeout_ms,
    )
    return replace(
        concurrency,
        default_source_server_count=source_server_count,
        default_connections_per_server=connections_per_server,
        default_max_concurrent_tasks=max_concurrent_tasks,
        default_batch_size=batch_size,
        default_request_interval_ms=request_interval_ms,
        default_retry_count=retry_count,
        default_timeout_ms=timeout_ms,
    )


def _generic_concurrency_profile(
    *,
    default_options: dict[str, Any],
    default_limits: dict[str, Any],
) -> ConcurrencyProfile:
    default_connections = _positive_int_option(default_limits.get("max_connections_total"), fallback=1)
    source_server_count = _positive_int_option(default_options.get("source_server_count"), fallback=1)
    connections_per_server = _positive_int_option(default_options.get("connections_per_server"), fallback=1)
    max_concurrent_tasks = _positive_int_option(
        default_options.get("max_concurrent_tasks"),
        fallback=max(default_connections, source_server_count * connections_per_server, 1),
    )
    batch_size = _positive_int_option(default_options.get("batch_size"), fallback=1)
    request_interval_ms = _nonnegative_int_option(
        default_options.get("request_interval_ms", default_limits.get("request_interval_ms")),
        fallback=0,
    )
    retry_count = _nonnegative_int_option(
        default_options.get("retry_count", default_limits.get("max_retries")),
        fallback=0,
    )
    timeout_ms = _positive_int_option(default_options.get("timeout_ms"), fallback=30000)
    return ConcurrencyProfile(
        mode="fixed",
        default_source_server_count=source_server_count,
        max_source_server_count=source_server_count,
        default_connections_per_server=connections_per_server,
        max_connections_per_server=connections_per_server,
        default_max_concurrent_tasks=max_concurrent_tasks,
        max_max_concurrent_tasks=max_concurrent_tasks,
        default_batch_size=batch_size,
        max_batch_size=batch_size,
        default_request_interval_ms=request_interval_ms,
        min_request_interval_ms=request_interval_ms,
        max_request_interval_ms=request_interval_ms,
        default_retry_count=retry_count,
        max_retry_count=retry_count,
        default_timeout_ms=timeout_ms,
        min_timeout_ms=timeout_ms,
        max_timeout_ms=timeout_ms,
        description="插件声明的固定采集预算；当前由 AxData Downloader Engine 统一执行。",
    )


def _mapping_option(value: Any, *, fallback: dict[str, Any]) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return dict(fallback)


def _fields_option(value: Any, *, fallback: list[str] | None) -> list[str] | None:
    if value is None:
        return None if fallback is None else list(fallback)
    if isinstance(value, (str, bytes, bytearray)):
        return [str(value)]
    try:
        return [str(item) for item in value]
    except TypeError:
        return None if fallback is None else list(fallback)


def _string_list_option(value: Any, *, fallback: list[str]) -> list[str]:
    if isinstance(value, (str, bytes, bytearray)):
        items = [str(value)]
    else:
        try:
            items = [str(item) for item in value]
        except TypeError:
            items = []
    normalized = [item.strip().lower() for item in items if item and item.strip()]
    return normalized or list(fallback)


def _first_format_option(value: Any, *, supported_formats: list[str], fallback: str) -> str:
    candidates = _string_list_option(value, fallback=[fallback])
    for candidate in candidates:
        if candidate in supported_formats:
            return candidate
    return fallback if fallback in supported_formats else supported_formats[0]


def _string_option(value: Any, *, fallback: str | None) -> str | None:
    if value is None:
        return fallback
    normalized = str(value).strip()
    return normalized or fallback


def _bounded_int_option(value: Any, *, fallback: int, minimum: int, maximum: int) -> int:
    try:
        resolved = int(value) if value is not None else int(fallback)
    except (TypeError, ValueError):
        resolved = int(fallback)
    lower = int(minimum)
    upper = max(lower, int(maximum))
    return max(lower, min(resolved, upper))


def _positive_int_option(value: Any, *, fallback: int) -> int:
    try:
        return max(int(value), 1) if value is not None else max(int(fallback), 1)
    except (TypeError, ValueError):
        return max(int(fallback), 1)


def _nonnegative_int_option(value: Any, *, fallback: int) -> int:
    try:
        return max(int(value), 0) if value is not None else max(int(fallback), 0)
    except (TypeError, ValueError):
        return max(int(fallback), 0)


def _primary_key_option(value: Any) -> str | tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    if isinstance(value, (list, tuple)):
        items = tuple(str(item).strip() for item in value if str(item).strip())
        if not items:
            return None
        return items[0] if len(items) == 1 else items
    return None


def _write_mode_option(value: Any, *, fallback: str) -> str:
    normalized = str(value or fallback or "snapshot").strip().lower()
    if normalized not in {"append", "snapshot", "overwrite_partition", "replace_range", "upsert_by_key"}:
        return fallback
    return normalized


def _default_output_path_parts(
    projection: _RegistryDownloaderProjection,
    *,
    output: dict[str, Any],
) -> list[str]:
    default_dir_name = _string_option(output.get("default_dir_name"), fallback=projection.interface.name)
    source_name = getattr(projection.interface, "source_name_zh", "") or projection.provider_id
    grouping = getattr(projection.interface, "category", "") or output.get("output_layer") or ""
    if grouping:
        return [str(source_name), str(grouping), default_dir_name]
    return [str(source_name), default_dir_name]


def _file_stem_template(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "{interface_name}_{collection_time}"
    return (
        text.replace("{data_date}", "{snapshot_date}")
        .replace("{run_time}", "{collection_time}")
    )


def build_request_plan(
    profile: DownloaderProfile,
    *,
    params: dict[str, Any] | None = None,
    fields: list[str] | None = None,
    concurrency: ResolvedConcurrency | None = None,
) -> DownloadRequestPlan:
    """Build the source request plan for one downloader run.

    This is the first neutral planner seam. TDX-specific request expansion still
    lives inside the TDX adapter, but the downloader engine no longer builds
    request params inline in the execution body.
    """

    return _REQUEST_PLANNER.build(
        profile,
        params=params,
        fields=fields,
        concurrency=concurrency,
    )


def run_downloader(
    interface_name: str,
    *,
    params: dict[str, Any] | None = None,
    fields: list[str] | None = None,
    data_root: str | Path | None = None,
    output_root: str | Path | None = None,
    output_dir: str | Path | None = None,
    formats: list[str] | tuple[str, ...] | str | None = None,
    collect_mode: str | None = None,
    connection_mode: str | None = None,
    concurrency_mode: str | None = None,
    connection_count: int | None = None,
    source_server_count: int | None = None,
    connections_per_server: int | None = None,
    max_concurrent_tasks: int | None = None,
    batch_size: int | None = None,
    request_interval_ms: int | None = None,
    retry_count: int | None = None,
    timeout_ms: int | None = None,
    adapter: SourceRequestAdapter | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    """Run one supported downloader synchronously and write local snapshots."""

    _emit_progress(progress_callback, 1, "准备采集任务")
    profile = get_downloader_profile(interface_name, data_root=data_root)
    requested_fields = fields if fields is not None else profile.default_fields
    selected_formats = _normalize_formats(formats, profile)
    resolved_collect_mode = _normalize_collect_mode(collect_mode)
    resolved_connection_mode = _normalize_connection_mode(connection_mode, profile)
    resolved_concurrency = _normalize_concurrency(
        profile,
        data_root=data_root,
        mode=concurrency_mode,
        source_server_count=source_server_count,
        connections_per_server=connections_per_server,
        max_concurrent_tasks=max_concurrent_tasks,
        batch_size=batch_size,
        request_interval_ms=request_interval_ms,
        retry_count=retry_count,
        timeout_ms=timeout_ms,
        legacy_connection_count=connection_count,
    )

    started_at = _utc_now()
    started_perf = perf_counter()
    run_id = f"run_{started_at.strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    managed_client: Any | None = None
    request_adapter = adapter
    _emit_progress(progress_callback, 10, "准备长连接")
    connection_started_perf = perf_counter()
    if request_adapter is None and resolved_connection_mode in {"long_connection", "connection_pool"}:
        adapter_client = _download_adapter(
            profile,
            source_server_count=resolved_concurrency.source_server_count,
            pool_size=resolved_concurrency.connection_count,
            data_root=data_root,
            progress_callback=progress_callback,
        )
        if adapter_client is not None:
            request_adapter, managed_client = adapter_client
    connection_ms = _elapsed_ms(connection_started_perf)
    try:
        request_plan = build_request_plan(
            profile,
            params=params,
            fields=fields,
            concurrency=resolved_concurrency,
        )
        _emit_progress(progress_callback, 20, "请求源端数据")
        source_started_perf = perf_counter()
        result = _request_download_records(
            profile,
            params=request_plan.params,
            fields=request_plan.fields,
            options=request_plan.adapter_options if request_adapter is None else None,
            adapter=request_adapter,
            data_root=data_root,
            progress_callback=progress_callback,
        )
    finally:
        _close_client(managed_client)
    source_request_ms = _elapsed_ms(source_started_perf)
    _emit_progress(
        progress_callback,
        70,
        "整理返回数据",
        progress_current=None,
        progress_total=None,
        progress_unit=None,
        eta_ms=None,
    )
    transform_started_perf = perf_counter()
    records = _apply_local_row_limit(result.records, request_plan.local_params, profile)
    frame = pd.DataFrame.from_records(records)
    snapshot_date, snapshot_date_source = _snapshot_date(profile, result.meta, started_at, records=records)
    collection_time = _collection_time(started_at)
    file_stem = _file_stem(profile, snapshot_date=snapshot_date, collection_time=collection_time)
    transform_ms = _elapsed_ms(transform_started_perf)

    output_directory = _resolve_output_directory(
        profile,
        data_root=data_root,
        output_root=output_root,
        output_dir=output_dir,
    )
    write_started_perf = perf_counter()
    try:
        write_result = _write_outputs_with_metadata(
            profile,
            frame,
            output_dir=output_directory,
            file_stem=file_stem,
            formats=selected_formats,
            progress_callback=progress_callback,
        )
    except WriteStrategyError as exc:
        raise DownloaderError(str(exc)) from exc
    output_paths = dict(write_result["output_paths"])
    write_metadata = dict(write_result["metadata"])
    write_ms = _elapsed_ms(write_started_perf)
    _emit_progress(progress_callback, 96, "检查写出质量")
    quality_started_perf = perf_counter()
    quality = _QUALITY_CHECKER.evaluate(
        frame,
        primary_key=profile.primary_key,
        rules=profile.quality_rules,
        required_columns=profile.required_columns,
        expected_columns=profile.expected_columns,
        date_field=profile.date_field,
        datetime_field=profile.datetime_field,
        numeric_positive_columns=profile.numeric_positive_columns,
        field_mappings=profile.field_mappings,
        calendar_check=_calendar_check_enabled(profile),
        trade_calendar_dates=_load_local_trade_calendar_dates(
            profile,
            data_root=data_root,
            output_root=output_root,
        ),
    )
    quality.update(_quality_write_metadata(write_metadata))
    quality_ms = _elapsed_ms(quality_started_perf)
    _emit_progress(progress_callback, 100, "采集完成")

    finished_at = _utc_now()
    total_ms = _elapsed_ms(started_perf)
    response = {
        "job_id": run_id,
        "interface_name": interface_name,
        "status": "success",
        "downloader_type": profile.downloader_type,
        "collect_mode": resolved_collect_mode,
        "request_plan": request_plan.to_dict(),
        "params": _display_params(profile, request_plan.params),
        "fields": request_plan.fields,
        "row_count": len(records),
        "snapshot_date": snapshot_date,
        "snapshot_date_source": snapshot_date_source,
        "collection_time": collection_time,
        "file_stem": file_stem,
        "connection_mode": resolved_connection_mode,
        "connection_count": resolved_concurrency.connection_count,
        "concurrency": resolved_concurrency.to_dict(),
        "output_formats": selected_formats,
        "output_paths": {key: str(value) for key, value in output_paths.items()},
        "output_path": str(output_paths[selected_formats[0]]),
        **_top_level_write_metadata(write_metadata),
        "write_metadata": write_metadata,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_ms": total_ms,
        "duration_breakdown_ms": {
            "connection": connection_ms,
            "source_request": source_request_ms,
            "transform": transform_ms,
            "write": write_ms,
            "quality": quality_ms,
            "total": total_ms,
        },
        "source_meta": dict(result.meta),
        "quality": quality,
    }
    log_path = _METADATA_WRITER.write_run_log(output_directory, file_stem=file_stem, result=response)
    response["log_path"] = str(log_path)
    return response


def _elapsed_ms(started_perf: float) -> int:
    return max(0, int((perf_counter() - started_perf) * 1000))


def _display_params(profile: DownloaderProfile, params: dict[str, Any]) -> dict[str, Any]:
    return params


def _request_download_records(
    profile: DownloaderProfile,
    *,
    params: dict[str, Any],
    fields: list[str] | None,
    options: dict[str, Any] | None = None,
    adapter: SourceRequestAdapter | None,
    data_root: str | Path | None,
    progress_callback: Callable[..., None] | None = None,
) -> SourceRequestResult:
    return request_interface(
        profile.interface_name,
        params=params,
        fields=fields,
        persist=False,
        adapter=adapter,
        options=options,
        data_root=data_root,
    )


def _sample_size(value: Any) -> int | None:
    """Positive row limit, or None when absent/non-numeric (e.g. ``"all"``)."""

    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _apply_local_row_limit(
    records: list[dict[str, Any]],
    local_params: dict[str, Any],
    profile: DownloaderProfile,
) -> list[dict[str, Any]]:
    """Keep only the most recent ``count`` rows per instrument.

    Downloader profiles expose ``count`` as a sample size ("返回最近 K 线数量"),
    but paging is private to the source gateway, which rejects the param and
    always returns the full history. The limit is therefore applied here, after
    the request, so the profile's documented behaviour still holds.
    """

    limit = _sample_size(local_params.get("count"))
    if limit is None or not records:
        return records

    time_field = profile.datetime_field or profile.date_field
    key_field = "instrument_id" if "instrument_id" in records[0] else None

    groups: dict[Any, list[int]] = {}
    for index, row in enumerate(records):
        groups.setdefault(row.get(key_field) if key_field else None, []).append(index)

    keep: set[int] = set()
    for indices in groups.values():
        if time_field and all(time_field in records[index] for index in indices):
            indices = sorted(indices, key=lambda index: str(records[index].get(time_field) or ""))
        keep.update(indices[-limit:])
    return [row for index, row in enumerate(records) if index in keep]


def _resolve_output_directory(
    profile: DownloaderProfile,
    *,
    data_root: str | Path | None,
    output_root: str | Path | None,
    output_dir: str | Path | None,
) -> Path:
    if output_dir is not None:
        return Path(output_dir).expanduser().resolve()
    if output_root is not None:
        root_path = Path(output_root).expanduser().resolve()
    elif data_root is not None:
        root_path = Path(data_root).expanduser().resolve()
    else:
        root_path = (Path.cwd() / "data").resolve()
    return _default_output_dir(profile, root_path=root_path)


def _request_params_for_download(
    profile: DownloaderProfile,
    params: dict[str, Any],
    *,
    adapter: SourceRequestAdapter | None,
) -> dict[str, Any]:
    return _REQUEST_PLANNER._request_params_for_download(profile, params)


def _downloader_param_names(profile: DownloaderProfile) -> set[str] | None:
    return _REQUEST_PLANNER._downloader_param_names(profile)


def _write_outputs(
    profile: DownloaderProfile,
    frame: pd.DataFrame,
    *,
    output_dir: Path,
    file_stem: str,
    formats: list[str],
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Path]:
    return _DOWNLOAD_WRITER.write_outputs(
        profile,
        frame,
        output_dir=output_dir,
        file_stem=file_stem,
        formats=formats,
        progress_callback=progress_callback,
    )


def _write_outputs_with_metadata(
    profile: DownloaderProfile,
    frame: pd.DataFrame,
    *,
    output_dir: Path,
    file_stem: str,
    formats: list[str],
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    write_outputs_with_metadata = getattr(_DOWNLOAD_WRITER, "write_outputs_with_metadata", None)
    if callable(write_outputs_with_metadata):
        result = write_outputs_with_metadata(
            profile,
            frame,
            output_dir=output_dir,
            file_stem=file_stem,
            formats=formats,
            progress_callback=progress_callback,
        )
        output_paths = {
            str(format_name): Path(path)
            for format_name, path in dict(result.output_paths).items()
        }
        return {
            "output_paths": output_paths,
            "metadata": dict(result.metadata.to_dict()),
        }
    output_paths = _DOWNLOAD_WRITER.write_outputs(
        profile,
        frame,
        output_dir=output_dir,
        file_stem=file_stem,
        formats=formats,
        progress_callback=progress_callback,
    )
    rows_written = int(len(frame))
    return {
        "output_paths": output_paths,
        "metadata": {
            "write_mode": profile.write_mode,
            "partition_by": list(profile.partition_by),
            "primary_key": _primary_key_list(profile.primary_key),
            "date_field": profile.date_field or profile.datetime_field,
            "replace_range_start": None,
            "replace_range_end": None,
            "rows_before": None,
            "rows_written": rows_written,
            "rows_after": rows_written,
            "duplicate_rows_dropped": 0,
            "partitions_touched": [],
            "formats": {},
        },
    }


def _top_level_write_metadata(write_metadata: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "write_mode",
        "partition_by",
        "primary_key",
        "date_field",
        "replace_range_start",
        "replace_range_end",
        "rows_before",
        "rows_written",
        "rows_after",
        "duplicate_rows_dropped",
        "partitions_touched",
    )
    return {key: write_metadata.get(key) for key in keys}


def _quality_write_metadata(write_metadata: dict[str, Any]) -> dict[str, Any]:
    return {
        "write_mode": write_metadata.get("write_mode"),
        "partition_by": list(write_metadata.get("partition_by") or []),
        "write_primary_key": list(write_metadata.get("primary_key") or []),
        "write_date_field": write_metadata.get("date_field"),
        "replace_range_start": write_metadata.get("replace_range_start"),
        "replace_range_end": write_metadata.get("replace_range_end"),
        "rows_before": write_metadata.get("rows_before"),
        "rows_written": write_metadata.get("rows_written"),
        "rows_after": write_metadata.get("rows_after"),
        "duplicate_rows_dropped": write_metadata.get("duplicate_rows_dropped"),
        "partitions_touched": list(write_metadata.get("partitions_touched") or []),
    }


def _primary_key_list(value: str | tuple[str, ...]) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    return [str(item) for item in value]


def _write_collection_log(output_dir: Path, *, file_stem: str, result: dict[str, Any]) -> Path:
    return _METADATA_WRITER.write_run_log(output_dir, file_stem=file_stem, result=result)


def _snapshot_date(
    profile: DownloaderProfile,
    meta: dict[str, Any],
    fallback_time: datetime,
    *,
    records: list[dict[str, Any]] | None = None,
) -> tuple[str, str]:
    for key in profile.snapshot_date_meta_keys:
        value = meta.get(key)
        normalized = _normalize_snapshot_date(value)
        if normalized is not None:
            return normalized, key
    record_trade_dates = {
        normalized
        for record in records or []
        if (normalized := _normalize_snapshot_date(record.get("trade_date"))) is not None
    }
    if len(record_trade_dates) == 1:
        return next(iter(record_trade_dates)), "record_trade_date"
    return fallback_time.astimezone(LOCAL_TIMEZONE).strftime("%Y%m%d"), "collected_at"


def _collection_time(value: datetime) -> str:
    return value.astimezone(LOCAL_TIMEZONE).strftime("%Y%m%d_%H%M")


def _file_stem(profile: DownloaderProfile, *, snapshot_date: str, collection_time: str) -> str:
    return profile.file_stem_template.format(
        interface_name=profile.interface_name,
        snapshot_date=snapshot_date,
        collection_time=collection_time,
    )


def _normalize_snapshot_date(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return None


def _emit_progress(progress_callback: Callable[..., None] | None, percent: int, message: str, **details: Any) -> None:
    emit_progress(progress_callback, percent, message, **details)


def _default_output_dir(profile: DownloaderProfile, *, root_path: Path) -> Path:
    if profile.default_output_path_parts:
        return root_path.joinpath(*profile.default_output_path_parts)
    return root_path / profile.interface_name


def _normalize_formats(
    formats: list[str] | tuple[str, ...] | str | None,
    profile: DownloaderProfile,
) -> list[str]:
    if formats is None:
        raw_formats: list[str] = [profile.output_format]
    elif isinstance(formats, str):
        raw_formats = [item.strip() for item in formats.split(",")]
    else:
        raw_formats = [str(item).strip() for item in formats]

    selected: list[str] = []
    for item in raw_formats:
        normalized = item.lower()
        if not normalized:
            continue
        if normalized not in profile.supported_formats:
            supported = ", ".join(profile.supported_formats)
            raise DownloaderError(f"Unsupported output format {item!r}; supported formats: {supported}.")
        if normalized not in selected:
            selected.append(normalized)
    if not selected:
        raise DownloaderError("At least one output format is required.")
    return selected


def _normalize_collect_mode(collect_mode: str | None) -> str:
    normalized = (collect_mode or "manual_fill").strip().lower()
    if normalized not in {"manual_fill", "incremental"}:
        raise DownloaderError("collect_mode must be one of: manual_fill, incremental.")
    return normalized


def _normalize_connection_mode(connection_mode: str | None, profile: DownloaderProfile) -> str:
    normalized = (connection_mode or profile.default_connection_mode).strip().lower()
    if normalized not in {"long_connection", "connection_pool"}:
        raise DownloaderError("connection_mode must be one of: long_connection, connection_pool.")
    return normalized


def _normalize_connection_count(connection_count: int | None, profile: DownloaderProfile) -> int:
    return _normalize_concurrency(profile, legacy_connection_count=connection_count).connection_count


def _normalize_concurrency(
    profile: DownloaderProfile,
    *,
    data_root: str | Path | None = None,
    mode: str | None = None,
    source_server_count: int | None = None,
    connections_per_server: int | None = None,
    max_concurrent_tasks: int | None = None,
    batch_size: int | None = None,
    request_interval_ms: int | None = None,
    retry_count: int | None = None,
    timeout_ms: int | None = None,
    legacy_connection_count: int | None = None,
) -> ResolvedConcurrency:
    concurrency = profile.concurrency
    if legacy_connection_count is not None and (
        source_server_count is None
        and connections_per_server is None
        and max_concurrent_tasks is None
    ):
        max_concurrent_tasks = legacy_connection_count

    resolved_mode = _normalize_concurrency_mode(mode, concurrency)
    source_server_max = _runtime_source_server_max(profile, concurrency, data_root=data_root)
    preset = _concurrency_mode_preset(resolved_mode, concurrency, source_server_max)
    if preset:
        source_server_count = preset["source_server_count"]
        connections_per_server = preset["connections_per_server"]
        max_concurrent_tasks = preset["max_concurrent_tasks"]
    resolved_source_server_count = _normalize_limited_int(
        source_server_count,
        default=concurrency.default_source_server_count,
        minimum=1,
        maximum=source_server_max,
        editable=concurrency.source_server_count_editable,
        field_name="source_server_count",
    )
    resolved_connections_per_server = _normalize_limited_int(
        connections_per_server,
        default=concurrency.default_connections_per_server,
        minimum=1,
        maximum=concurrency.max_connections_per_server,
        editable=concurrency.connections_per_server_editable,
        field_name="connections_per_server",
    )
    default_max_tasks = min(
        concurrency.default_max_concurrent_tasks,
        resolved_source_server_count * resolved_connections_per_server,
    )
    max_tasks = min(
        concurrency.max_max_concurrent_tasks,
        resolved_source_server_count * resolved_connections_per_server,
    )
    if not concurrency.max_concurrent_tasks_editable:
        default_max_tasks = max_tasks
    resolved_max_concurrent_tasks = _normalize_limited_int(
        max_concurrent_tasks,
        default=default_max_tasks,
        minimum=1,
        maximum=max_tasks,
        editable=concurrency.max_concurrent_tasks_editable,
        field_name="max_concurrent_tasks",
    )
    resolved_batch_size = _normalize_limited_int(
        batch_size,
        default=concurrency.default_batch_size,
        minimum=1,
        maximum=concurrency.max_batch_size,
        editable=concurrency.batch_size_editable,
        field_name="batch_size",
    )
    resolved_request_interval_ms = _normalize_limited_int(
        request_interval_ms,
        default=concurrency.default_request_interval_ms,
        minimum=concurrency.min_request_interval_ms,
        maximum=concurrency.max_request_interval_ms,
        editable=concurrency.request_interval_ms_editable,
        field_name="request_interval_ms",
    )
    resolved_retry_count = _normalize_limited_int(
        retry_count,
        default=concurrency.default_retry_count,
        minimum=0,
        maximum=concurrency.max_retry_count,
        editable=concurrency.retry_count_editable,
        field_name="retry_count",
    )
    resolved_timeout_ms = _normalize_limited_int(
        timeout_ms,
        default=concurrency.default_timeout_ms,
        minimum=concurrency.min_timeout_ms,
        maximum=concurrency.max_timeout_ms,
        editable=concurrency.timeout_ms_editable,
        field_name="timeout_ms",
    )
    return ResolvedConcurrency(
        mode=resolved_mode,
        source_server_count=resolved_source_server_count,
        connections_per_server=resolved_connections_per_server,
        max_concurrent_tasks=resolved_max_concurrent_tasks,
        batch_size=resolved_batch_size,
        request_interval_ms=resolved_request_interval_ms,
        retry_count=resolved_retry_count,
        timeout_ms=resolved_timeout_ms,
    )


def _normalize_concurrency_mode(value: str | None, concurrency: ConcurrencyProfile) -> str:
    if not concurrency.mode_editable:
        return concurrency.mode
    raw = str(value or concurrency.mode).strip().lower()
    aliases = {
        "configurable": "custom",
        "auto": "medium",
        "conservative": "low",
        "aggressive": "high",
    }
    normalized = aliases.get(raw, raw)
    if normalized not in {"low", "medium", "high", "custom"}:
        raise DownloaderError("concurrency_mode must be low, medium, high, or custom.")
    return normalized


def _runtime_source_server_max(
    profile: DownloaderProfile,
    concurrency: ConcurrencyProfile,
    *,
    data_root: str | Path | None = None,
) -> int:
    if profile.runtime_source_server_max_factory is None:
        return max(1, int(concurrency.max_source_server_count))
    factories = load_builtin_runtime_source_server_max_factories()
    factory = factories.get(profile.runtime_source_server_max_factory)
    if factory is None:
        raise DownloaderError(
            f"Unknown runtime source-server max factory {profile.runtime_source_server_max_factory!r}."
        )
    return factory(
        profile.interface_name,
        configured_max=concurrency.max_source_server_count,
        source_server_count_editable=concurrency.source_server_count_editable,
        data_root=data_root,
    )


def _concurrency_mode_preset(
    mode: str,
    concurrency: ConcurrencyProfile,
    source_server_max: int,
) -> dict[str, int] | None:
    if mode not in {"low", "medium", "high"}:
        return None
    profile_max = {
        "source_server_count": source_server_max,
        "connections_per_server": max(1, int(concurrency.max_connections_per_server)),
        "max_concurrent_tasks": max(1, int(concurrency.max_max_concurrent_tasks)),
    }
    presets = {
        "low": {"source_server_count": 1, "connections_per_server": 2, "max_concurrent_tasks": 2},
        "medium": {"source_server_count": 2, "connections_per_server": 2, "max_concurrent_tasks": 4},
        "high": {"source_server_count": 4, "connections_per_server": 2, "max_concurrent_tasks": 8},
    }
    preset = presets[mode]
    source_count = min(profile_max["source_server_count"], preset["source_server_count"])
    connections_per_server = min(profile_max["connections_per_server"], preset["connections_per_server"])
    max_tasks = min(
        profile_max["max_concurrent_tasks"],
        preset["max_concurrent_tasks"],
        source_count * connections_per_server,
    )
    return {
        "source_server_count": source_count,
        "connections_per_server": connections_per_server,
        "max_concurrent_tasks": max(1, max_tasks),
    }


def _normalize_limited_int(
    value: int | None,
    *,
    default: int,
    minimum: int,
    maximum: int,
    editable: bool,
    field_name: str,
) -> int:
    if not editable:
        return default
    resolved = default if value is None else int(value)
    if resolved < minimum or resolved > maximum:
        raise DownloaderError(f"{field_name} must be between {minimum} and {maximum}.")
    return resolved


def _download_adapter(
    profile: DownloaderProfile,
    *,
    source_server_count: int,
    pool_size: int,
    data_root: str | Path | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> tuple[SourceRequestAdapter, Any] | None:
    if profile.adapter_factory is None:
        return None
    factories = load_builtin_downloader_adapter_factories()
    factory = factories.get(profile.adapter_factory)
    if factory is None:
        raise DownloaderError(f"Unknown downloader adapter factory {profile.adapter_factory!r}.")
    return factory(
        interface_name=profile.interface_name,
        source_server_count=source_server_count,
        pool_size=pool_size,
        data_root=data_root,
        progress_callback=progress_callback,
    )


def _close_client(client: Any | None) -> None:
    if client is None:
        return
    close = getattr(client, "close", None)
    if callable(close):
        close()


def _calendar_check_enabled(profile: DownloaderProfile) -> bool:
    return profile.interface_name in {
        "daily",
        "adj_factor",
        "stock_kline_daily_tdx",
        "stock_adj_factor_tdx",
    }


def _load_local_trade_calendar_dates(
    profile: DownloaderProfile,
    *,
    data_root: str | Path | None,
    output_root: str | Path | None,
) -> dict[str, list[str]] | None:
    if not _calendar_check_enabled(profile):
        return None
    for root in _candidate_calendar_roots(data_root=data_root, output_root=output_root):
        dates = _read_trade_calendar_dates(root)
        if dates:
            return dates
    return None


def _candidate_calendar_roots(
    *,
    data_root: str | Path | None,
    output_root: str | Path | None,
) -> list[Path]:
    # Only fall back to the working directory when no root was supplied at all;
    # an explicit root must not silently borrow another tree's calendar.
    explicit = (data_root, output_root)
    candidates = explicit if any(root is not None for root in explicit) else (Path.cwd() / "data",)
    roots: list[Path] = []
    for candidate in candidates:
        if candidate is None:
            continue
        try:
            root = Path(candidate).expanduser().resolve()
        except OSError:
            continue
        if root not in roots:
            roots.append(root)
    return roots


def _read_trade_calendar_dates(root: Path) -> dict[str, list[str]]:
    try:
        from .storage import core_table_partition_path, core_table_path
    except Exception:
        return {}

    parquet_paths: list[Path] = []
    file_path = core_table_path("trade_cal", root)
    partition_path = core_table_partition_path("trade_cal", root)
    if file_path.is_file():
        parquet_paths.append(file_path)
    if partition_path.exists():
        parquet_paths.extend(sorted(path for path in partition_path.rglob("*.parquet") if path.is_file()))
    if not parquet_paths:
        return {}

    frames: list[pd.DataFrame] = []
    for path in parquet_paths:
        try:
            frame = pd.read_parquet(path, engine="pyarrow")
        except Exception:
            continue
        if "exchange" not in frame.columns:
            exchange = _partition_value(path, partition_path, "exchange")
            if exchange:
                frame["exchange"] = exchange
        frames.append(frame)
    if not frames:
        return {}
    frame = pd.concat(frames, ignore_index=True)
    if "cal_date" not in frame.columns:
        return {}
    if "is_open" in frame.columns:
        frame = frame.loc[frame["is_open"].map(_is_open_calendar_value)]
    exchange_values = frame["exchange"] if "exchange" in frame.columns else pd.Series(["ALL"] * len(frame))
    grouped: dict[str, set[str]] = {}
    for exchange, value in zip(exchange_values, frame["cal_date"], strict=False):
        date_text = _normalize_calendar_date(value)
        if date_text is None:
            continue
        key = str(exchange or "ALL").upper()
        grouped.setdefault(key, set()).add(date_text)
    return {exchange: sorted(values) for exchange, values in grouped.items()}


def _is_open_calendar_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y"}


def _normalize_calendar_date(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return None


def _partition_value(file_path: Path, partition_root: Path, key: str) -> str | None:
    try:
        parts = file_path.relative_to(partition_root).parts[:-1]
    except ValueError:
        return None
    prefix = f"{key}="
    for part in parts:
        if part.startswith(prefix):
            return part[len(prefix) :]
    return None


def _primary_key_quality(frame: pd.DataFrame, primary_key: str | tuple[str, ...]) -> str:
    return _QUALITY_CHECKER.primary_key_status(frame, primary_key)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _write_frame_atomic(frame: pd.DataFrame, output_path: Path, output_format: str) -> None:
    try:
        _DOWNLOAD_WRITER.write_frame_atomic(frame, output_path, output_format)
    except ValueError as exc:
        raise DownloaderError(str(exc)) from exc


def _write_text_atomic(path: Path, text: str) -> None:
    _METADATA_WRITER.write_text_atomic(path, text)
