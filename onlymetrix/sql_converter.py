"""SQL-to-Semantic-Layer converter.

Converts raw SQL queries into OnlyMetrix metric definitions. Parses SQL
to extract aggregations, source tables, filters, dimensions, and time
columns, then produces metric YAML or JSON ready for import.

Usage (Python):
    from onlymetrix.sql_converter import convert_sql, convert_sql_batch

    metric = convert_sql(
        "SELECT SUM(amount) FROM orders WHERE status = 'paid'",
        name="total_revenue",
        description="Total paid revenue",
    )
    om.setup.import_metrics([metric])

Usage (CLI):
    omx sql convert query.sql --name total_revenue
    omx sql convert-batch queries/ --output metrics.yaml
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Aggregation functions we recognize
_AGG_FUNCTIONS = {
    "SUM", "COUNT", "AVG", "MIN", "MAX",
    "COUNT_DISTINCT", "MEDIAN", "PERCENTILE_CONT",
    "STDDEV", "VARIANCE",
}

_AGG_PATTERN = re.compile(
    r"\b(SUM|COUNT|AVG|MIN|MAX|STDDEV|VARIANCE|MEDIAN)\s*\(\s*(DISTINCT\s+)?(.+?)\s*\)",
    re.IGNORECASE,
)

_FROM_PATTERN = re.compile(
    r"\bFROM\s+([a-zA-Z_][\w]*(?:\s*\.\s*[a-zA-Z_][\w]*){0,2})",
    re.IGNORECASE,
)

_JOIN_PATTERN = re.compile(
    r"\bJOIN\s+([a-zA-Z_][\w]*(?:\s*\.\s*[a-zA-Z_][\w]*){0,2})",
    re.IGNORECASE,
)

_WHERE_PATTERN = re.compile(
    r"\bWHERE\s+(.*?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b|$)",
    re.IGNORECASE | re.DOTALL,
)

_GROUP_BY_PATTERN = re.compile(
    r"\bGROUP\s+BY\s+(.*?)(?:\bORDER\b|\bLIMIT\b|\bHAVING\b|$)",
    re.IGNORECASE | re.DOTALL,
)

_ALIAS_PATTERN = re.compile(
    r"\bAS\s+([a-zA-Z_]\w*)\s*$",
    re.IGNORECASE,
)

# Common time column names
_TIME_COLUMN_HINTS = {
    "created_at", "updated_at", "order_date", "event_date", "timestamp",
    "date", "created_date", "event_time", "transaction_date", "ts",
    "occurred_at", "recorded_at", "invoice_date",
}


@dataclass
class ExtractedMetric:
    """A metric definition extracted from SQL."""
    name: str
    description: str
    sql: str
    aggregations: list[dict[str, str]] = field(default_factory=list)
    source_tables: list[str] = field(default_factory=list)
    filters: list[dict[str, str]] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)
    time_column: Optional[str] = None
    tags: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_metric_dict(self) -> dict[str, Any]:
        """Convert to the dict format accepted by setup.import_metrics()."""
        result: dict[str, Any] = {
            "name": self.name,
            "sql": self.sql,
            "description": self.description,
        }
        if self.source_tables:
            result["source_tables"] = self.source_tables
        if self.tags:
            result["tags"] = self.tags
        if self.time_column:
            result["time_column"] = self.time_column
        if self.dimensions:
            result["dimensions"] = True
        if self.filters:
            result["filters"] = self.filters
        return result

    def to_yaml(self) -> str:
        """Convert to YAML metric definition."""
        lines = [
            f"- name: {self.name}",
            f"  description: {self.description}",
            f"  sql: |",
        ]
        for sql_line in self.sql.strip().splitlines():
            lines.append(f"    {sql_line}")
        if self.source_tables:
            lines.append(f"  source_tables: [{', '.join(self.source_tables)}]")
        if self.tags:
            lines.append(f"  tags: [{', '.join(self.tags)}]")
        if self.time_column:
            lines.append(f"  time_column: {self.time_column}")
        if self.dimensions:
            lines.append("  dimensions: true")
        if self.filters:
            lines.append("  filters:")
            for f in self.filters:
                lines.append(f"    - name: {f['name']}")
                lines.append(f"      type: {f.get('type', 'string')}")
        return "\n".join(lines)


def _normalize_sql(sql: str) -> str:
    """Strip comments and normalize whitespace."""
    # Remove single-line comments
    sql = re.sub(r"--[^\n]*", "", sql)
    # Remove multi-line comments
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    # Normalize whitespace
    sql = re.sub(r"\s+", " ", sql).strip()
    return sql


def _extract_tables(sql: str) -> list[str]:
    """Extract table names from FROM and JOIN clauses."""
    tables = []
    for match in _FROM_PATTERN.finditer(sql):
        table = match.group(1).strip().replace(" ", "")
        tables.append(table)
    for match in _JOIN_PATTERN.finditer(sql):
        table = match.group(1).strip().replace(" ", "")
        tables.append(table)
    # Deduplicate preserving order
    seen = set()
    result = []
    for t in tables:
        t_lower = t.lower()
        if t_lower not in seen:
            seen.add(t_lower)
            result.append(t)
    return result


def _extract_aggregations(sql: str) -> list[dict[str, str]]:
    """Extract aggregation functions and their expressions."""
    aggs = []
    for match in _AGG_PATTERN.finditer(sql):
        func = match.group(1).upper()
        distinct = bool(match.group(2))
        expr = match.group(3).strip()
        if distinct:
            func = f"COUNT_DISTINCT" if func == "COUNT" else func

        # Try to find alias
        after_agg = sql[match.end():]
        alias_match = _ALIAS_PATTERN.match(after_agg.split(",")[0].split("\n")[0])
        alias = alias_match.group(1) if alias_match else expr.replace(".", "_").replace("*", "all")

        aggs.append({
            "function": func,
            "expression": expr,
            "alias": alias,
        })
    return aggs


def _extract_where_filters(sql: str) -> list[dict[str, str]]:
    """Extract filter columns from WHERE clause."""
    match = _WHERE_PATTERN.search(sql)
    if not match:
        return []

    where_clause = match.group(1).strip()
    # Extract column names used in conditions
    filters = []
    # Match patterns like: column = 'value', column IN (...), column > value
    condition_pattern = re.compile(
        r"([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)?)\s*"
        r"(=|!=|<>|>=?|<=?|IN|NOT\s+IN|LIKE|NOT\s+LIKE|IS|BETWEEN)\s*",
        re.IGNORECASE,
    )
    seen = set()
    for cond_match in condition_pattern.finditer(where_clause):
        col = cond_match.group(1)
        col_lower = col.lower()
        if col_lower not in seen and col_lower not in ("and", "or", "not"):
            seen.add(col_lower)
            # Infer type from operator and context
            op = cond_match.group(2).upper().strip()
            col_type = "string"
            if op in (">=", "<=", ">", "<", "BETWEEN"):
                col_type = "number"
            filters.append({"name": col, "type": col_type})
    return filters


def _extract_dimensions(sql: str) -> list[str]:
    """Extract dimension columns from GROUP BY clause."""
    match = _GROUP_BY_PATTERN.search(sql)
    if not match:
        return []

    group_clause = match.group(1).strip()
    # Split by comma, clean up
    dims = []
    for part in group_clause.split(","):
        part = part.strip()
        # Skip numeric references (GROUP BY 1, 2)
        if part.isdigit():
            continue
        # Clean alias references
        col = part.split(".")[-1].strip()
        if col and re.match(r"^[a-zA-Z_]\w*$", col):
            dims.append(col)
    return dims


def _detect_time_column(sql: str, tables: list[str]) -> Optional[str]:
    """Try to detect the time/date column from SQL."""
    sql_lower = sql.lower()
    for hint in _TIME_COLUMN_HINTS:
        if hint in sql_lower:
            # Find the actual cased version
            pattern = re.compile(rf"\b({re.escape(hint)})\b", re.IGNORECASE)
            match = pattern.search(sql)
            if match:
                return match.group(1)
    return None


def _infer_name(sql: str, aggs: list[dict[str, str]], tables: list[str]) -> str:
    """Generate a metric name from SQL content when none is provided."""
    if aggs:
        func = aggs[0]["function"].lower()
        expr = aggs[0]["expression"].replace("*", "all").replace(".", "_")
        expr = re.sub(r"[^a-zA-Z0-9_]", "", expr)
        name = f"{func}_{expr}"
    elif tables:
        name = f"query_{tables[0].split('.')[-1]}"
    else:
        name = f"metric_{hashlib.md5(sql.encode()).hexdigest()[:8]}"
    return name.lower()


def _infer_tags(tables: list[str], aggs: list[dict[str, str]]) -> list[str]:
    """Infer tags from table names and aggregation types."""
    tags = set()
    for table in tables:
        base = table.split(".")[-1].lower()
        # Common domain mappings
        if any(kw in base for kw in ("order", "invoice", "payment", "revenue", "transaction")):
            tags.add("finance")
        if any(kw in base for kw in ("customer", "user", "account")):
            tags.add("customers")
        if any(kw in base for kw in ("product", "item", "sku")):
            tags.add("product")
        if any(kw in base for kw in ("event", "click", "session", "pageview")):
            tags.add("engagement")
    for agg in aggs:
        func = agg["function"]
        if func in ("SUM", "AVG"):
            tags.add("aggregate")
        if func == "COUNT_DISTINCT":
            tags.add("cardinality")
    return sorted(tags)


def convert_sql(
    sql: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    tags: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Convert a single SQL query into an OnlyMetrix metric definition.

    Args:
        sql: Raw SQL query.
        name: Metric name (auto-inferred if not provided).
        description: Metric description (auto-generated if not provided).
        tags: Explicit tags (auto-inferred if not provided).

    Returns:
        Dict ready for om.setup.import_metrics([result]).
    """
    extracted = extract_sql(sql, name=name, description=description, tags=tags)
    return extracted.to_metric_dict()


def extract_sql(
    sql: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    tags: Optional[list[str]] = None,
) -> ExtractedMetric:
    """Parse SQL and extract a full ExtractedMetric with all metadata.

    This is the detailed version of convert_sql() — returns the full
    ExtractedMetric object including aggregations, dimensions, warnings.
    """
    normalized = _normalize_sql(sql)
    warnings = []

    tables = _extract_tables(normalized)
    aggs = _extract_aggregations(normalized)
    filters = _extract_where_filters(normalized)
    dimensions = _extract_dimensions(normalized)
    time_column = _detect_time_column(normalized, tables)

    if not aggs:
        warnings.append("No aggregation function detected — this may be a raw query, not a metric")
    if not tables:
        warnings.append("No source table detected in SQL")

    inferred_name = name or _infer_name(normalized, aggs, tables)
    inferred_tags = tags if tags is not None else _infer_tags(tables, aggs)

    if not description:
        # Auto-generate description from components
        parts = []
        if aggs:
            agg_desc = ", ".join(f"{a['function']}({a['expression']})" for a in aggs)
            parts.append(agg_desc)
        if tables:
            parts.append(f"from {', '.join(tables)}")
        if filters:
            filter_desc = ", ".join(f["name"] for f in filters)
            parts.append(f"filtered by {filter_desc}")
        description = " ".join(parts) if parts else f"Metric from SQL query"

    return ExtractedMetric(
        name=inferred_name,
        description=description,
        sql=sql.strip(),
        aggregations=aggs,
        source_tables=tables,
        filters=filters,
        dimensions=dimensions,
        time_column=time_column,
        tags=inferred_tags,
        warnings=warnings,
    )


def convert_sql_batch(
    sql_sources: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Convert multiple SQL queries into metric definitions.

    Args:
        sql_sources: List of dicts with keys:
            - sql: The SQL query (required)
            - name: Metric name (optional)
            - description: Description (optional)

    Returns:
        List of metric dicts ready for om.setup.import_metrics().
    """
    results = []
    for source in sql_sources:
        sql = source["sql"]
        metric = convert_sql(
            sql,
            name=source.get("name"),
            description=source.get("description"),
            tags=source.get("tags"),
        )
        results.append(metric)
    return results


def convert_sql_file(
    path: str | Path,
    name: Optional[str] = None,
    description: Optional[str] = None,
) -> dict[str, Any]:
    """Read a .sql file and convert it to a metric definition.

    If the file contains multiple statements separated by semicolons,
    only the first statement is used.
    """
    path = Path(path)
    sql = path.read_text(encoding="utf-8")

    # Use filename as default name
    if name is None:
        name = path.stem.replace("-", "_").replace(" ", "_").lower()

    # Split on semicolons, take the first non-empty statement
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    if not statements:
        raise ValueError(f"No SQL statements found in {path}")

    return convert_sql(statements[0], name=name, description=description)


def convert_sql_directory(
    directory: str | Path,
    pattern: str = "*.sql",
) -> list[dict[str, Any]]:
    """Convert all .sql files in a directory to metric definitions.

    Each file becomes one metric, named after the file.
    """
    directory = Path(directory)
    if not directory.is_dir():
        raise ValueError(f"Not a directory: {directory}")

    results = []
    for sql_file in sorted(directory.glob(pattern)):
        try:
            metric = convert_sql_file(sql_file)
            results.append(metric)
            logger.info("Converted %s -> %s", sql_file.name, metric["name"])
        except Exception as e:
            logger.warning("Skipping %s: %s", sql_file.name, e)
    return results


def metrics_to_yaml(metrics: list[dict[str, Any]]) -> str:
    """Convert a list of metric dicts to YAML format.

    Returns valid OnlyMetrix YAML ready for `omx init`.
    """
    lines = ["metrics:"]
    for m in metrics:
        lines.append(f"  - name: {m['name']}")
        lines.append(f"    description: \"{m.get('description', '')}\"")
        lines.append(f"    sql: |")
        for sql_line in m["sql"].strip().splitlines():
            lines.append(f"      {sql_line}")
        if m.get("source_tables"):
            lines.append(f"    source_tables: [{', '.join(m['source_tables'])}]")
        if m.get("tags"):
            lines.append(f"    tags: [{', '.join(m['tags'])}]")
        if m.get("time_column"):
            lines.append(f"    time_column: {m['time_column']}")
        if m.get("dimensions"):
            lines.append("    dimensions: true")
        if m.get("filters"):
            lines.append("    filters:")
            for f in m["filters"]:
                lines.append(f"      - name: {f['name']}")
                lines.append(f"        type: {f.get('type', 'string')}")
        lines.append("")
    return "\n".join(lines)
