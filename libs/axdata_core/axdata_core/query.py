"""DuckDB-backed query helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from .schema import get_schema, require_fields
from .storage import core_table_duckdb_path, core_table_parquet_roots, core_table_path


_DUCKDB_TYPE_CASTS = {
    "boolean": "BOOLEAN",
    "date": "VARCHAR",
    "double": "DOUBLE",
    "float64": "DOUBLE",
    "int64": "BIGINT",
    "string": "VARCHAR",
    "timestamp": "TIMESTAMP",
}
_NO_MATCHING_PARTITIONS = object()


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _duckdb_type_for_field(table: str, field: str) -> str:
    schema = get_schema(table)
    field_meta = next(item for item in schema.fields if item.name == field)
    return _DUCKDB_TYPE_CASTS.get(field_meta.dtype, "VARCHAR")


def _source_column_for_field(table: str, field: str, available_fields: set[str]) -> str | None:
    """Resolve the parquet column backing a canonical field, honouring aliases.

    Provider-native datasets may store a field under one of its aliases (e.g. the
    TDX daily collector writes ``instrument_id``/``trade_time``/``volume`` for the
    canonical ``ts_code``/``trade_date``/``vol``). Prefer the canonical column,
    then fall back to the first available alias.
    """

    if field in available_fields:
        return field
    schema = get_schema(table)
    field_meta = next((item for item in schema.fields if item.name == field), None)
    if field_meta is not None:
        for alias in field_meta.aliases:
            if alias in available_fields:
                return alias
    return None


def _source_field_expression(table: str, field: str, available_fields: set[str]) -> str:
    duckdb_type = _duckdb_type_for_field(table, field)
    quoted = _quote_identifier(field)

    source_column = _source_column_for_field(table, field, available_fields)
    if source_column is None:
        return f"CAST(NULL AS {duckdb_type}) AS {quoted}"
    source_quoted = _quote_identifier(source_column)

    schema = get_schema(table)
    if field == schema.date_field:
        # Normalise to YYYYMMDD, tolerating both plain dates and ISO datetimes
        # (e.g. "2024-01-02" or "2024-01-02T15:00:00+08:00").
        return f"REPLACE(SUBSTR(CAST({source_quoted} AS VARCHAR), 1, 10), '-', '') AS {quoted}"

    return f"CAST({source_quoted} AS {duckdb_type}) AS {quoted}"


def _source_projection(table: str, fields: Sequence[str], available_fields: set[str]) -> str:
    return ", ".join(_source_field_expression(table, field, available_fields) for field in fields)


def _required_source_fields(
    table: str,
    selected_fields: Sequence[str],
    filters: Mapping[str, Any] | None,
    start_date: str | None,
    end_date: str | None,
) -> tuple[str, ...]:
    schema = get_schema(table)
    fields: list[str] = []

    for field in selected_fields:
        if field not in fields:
            fields.append(field)

    if filters:
        for field in filters:
            if field not in fields:
                fields.append(field)

    if (start_date or end_date) and schema.date_field and schema.date_field not in fields:
        fields.append(schema.date_field)

    return tuple(fields)


def _available_source_fields(conn: Any, path: str) -> set[str]:
    rows = conn.execute(
        "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = true, union_by_name = true)",
        [path],
    ).fetchall()
    return {str(row[0]) for row in rows}


def _source_read_path(table: str, root: str | Path, start_date: str | None, end_date: str | None) -> str | list[str] | object:
    file_path = core_table_path(table, root)
    if file_path.exists():
        return str(file_path)

    partition_roots = tuple(path for path in core_table_parquet_roots(table, root) if path.exists())
    if not partition_roots:
        return core_table_duckdb_path(table, root)

    schema = get_schema(table)
    if schema.date_field:
        matched_any = False
        date_partition_globs: list[str] = []
        for partition_path in partition_roots:
            root_globs = _date_partition_globs(
                partition_path,
                date_field=schema.date_field,
                start_date=start_date,
                end_date=end_date,
            )
            if root_globs is None:
                continue
            matched_any = True
            date_partition_globs.extend(root_globs)
        if matched_any:
            return date_partition_globs or _NO_MATCHING_PARTITIONS
    return str(partition_roots[0] / "**" / "*.parquet")


def _date_partition_globs(
    partition_path: Path,
    *,
    date_field: str,
    start_date: str | None,
    end_date: str | None,
) -> list[str] | None:
    if not start_date and not end_date:
        return None
    start = str(start_date).replace("-", "") if start_date else None
    end = str(end_date).replace("-", "") if end_date else None
    if (start is not None and (len(start) != 8 or not start.isdigit())) or (
        end is not None and (len(end) != 8 or not end.isdigit())
    ):
        return None
    date_dirs = [path for path in partition_path.glob(f"{date_field}=*") if path.is_dir()]
    date_files = [path for path in partition_path.glob("*.parquet") if _date_file_value(path) is not None]
    if not date_dirs and not date_files:
        return None
    globs: list[str] = []
    for path in sorted(date_dirs):
        value = path.name.split("=", 1)[1].replace("-", "")
        if _date_value_in_bounds(value, start, end):
            globs.append(str(path / "*.parquet"))
    for path in sorted(date_files):
        value = _date_file_value(path)
        if value is not None and _date_value_in_bounds(value, start, end):
            globs.append(str(path))
    return globs


def _date_file_value(path: Path) -> str | None:
    value = path.stem.replace("-", "")
    return value if len(value) == 8 and value.isdigit() else None


def _date_value_in_bounds(value: str, start: str | None, end: str | None) -> bool:
    return (start is None or value >= start) and (end is None or value <= end)


def _build_filter_clause(
    table: str,
    filters: Mapping[str, Any] | None,
    start_date: str | None,
    end_date: str | None,
) -> tuple[str, list[Any]]:
    schema = get_schema(table)
    clauses: list[str] = []
    params: list[Any] = []

    if filters:
        require_fields(table, filters.keys())
        for field, value in filters.items():
            quoted = _quote_identifier(field)
            if isinstance(value, (list, tuple, set, frozenset)):
                values = list(value)
                if not values:
                    clauses.append("1 = 0")
                    continue
                placeholders = ", ".join("?" for _ in values)
                clauses.append(f"{quoted} IN ({placeholders})")
                params.extend(values)
            elif value is None:
                clauses.append(f"{quoted} IS NULL")
            else:
                clauses.append(f"{quoted} = ?")
                params.append(value)

    if start_date or end_date:
        if schema.date_field is None:
            raise ValueError(f"Table {table!r} does not define a date field.")
        quoted_date = _quote_identifier(schema.date_field)
        if start_date:
            clauses.append(f"{quoted_date} >= ?")
            params.append(start_date)
        if end_date:
            clauses.append(f"{quoted_date} <= ?")
            params.append(end_date)

    if not clauses:
        return "", params
    return " WHERE " + " AND ".join(clauses), params


def query_table(
    table: str,
    *,
    root: str | Path,
    filters: Mapping[str, Any] | None = None,
    fields: Sequence[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None,
):
    """Query a core parquet table with DuckDB and return a pandas DataFrame."""

    try:
        import duckdb
    except ImportError as exc:
        raise ImportError("DuckDB query support requires duckdb. Install it with `pip install duckdb`.") from exc

    schema = get_schema(table)
    selected_fields = tuple(fields) if fields else schema.field_names
    require_fields(table, selected_fields)
    if filters:
        require_fields(table, filters.keys())

    path = _source_read_path(table, root, start_date, end_date)
    source_fields = _required_source_fields(table, selected_fields, filters, start_date, end_date)
    where_sql, params = _build_filter_clause(table, filters, start_date, end_date)
    limit_sql = " LIMIT ?" if limit is not None else ""
    limit_params = [limit] if limit is not None else []

    if path is _NO_MATCHING_PARTITIONS:
        import pandas as pd

        return pd.DataFrame(columns=list(selected_fields))

    with duckdb.connect(database=":memory:") as conn:
        available_fields = _available_source_fields(conn, path)
        select_sql = ", ".join(_quote_identifier(field) for field in selected_fields)
        source_sql = _source_projection(table, source_fields, available_fields)
        sql = (
            f"WITH axdata_source AS ("
            f"SELECT {source_sql} "
            f"FROM read_parquet(?, hive_partitioning = true, union_by_name = true)"
            f") SELECT {select_sql} FROM axdata_source{where_sql}{limit_sql}"
        )
        return conn.execute(sql, [path, *params, *limit_params]).fetchdf()
