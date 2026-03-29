"""Data models for OnlyMetrix API responses."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Metric:
    """A curated metric definition."""
    name: str
    description: str
    filters: list[dict[str, str]] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    open_filters: bool = False
    source_tables: list[str] = field(default_factory=list)
    dimensions: bool = False
    depends_on: list[str] = field(default_factory=list)
    time_column: Optional[str] = None
    time_filters: list[str] = field(default_factory=list)
    version: Optional[str] = None
    deprecated: Optional[str] = None
    datasource: Optional[str] = None
    relevance_score: Optional[int] = None
    ground_truth_sql: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> Metric:
        return cls(
            name=data["name"],
            description=data["description"],
            filters=data.get("filters", []),
            tags=data.get("tags", []),
            open_filters=data.get("open_filters", False),
            source_tables=data.get("source_tables", []),
            dimensions=data.get("dimensions", False),
            depends_on=data.get("depends_on", []),
            time_column=data.get("time_column"),
            time_filters=data.get("time_filters", []),
            version=data.get("version"),
            deprecated=data.get("deprecated"),
            datasource=data.get("datasource"),
            relevance_score=data.get("relevance_score"),
            ground_truth_sql=data.get("ground_truth_sql"),
        )


@dataclass
class MetricResult:
    """Result of executing a metric query."""
    metric: str
    columns: list[dict[str, str]]
    rows: list[dict[str, Any]]
    row_count: int
    execution_time_ms: int
    filters_applied: list[str] = field(default_factory=list)
    warning: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> MetricResult:
        return cls(
            metric=data["metric"],
            columns=data["columns"],
            rows=data["rows"],
            row_count=data["row_count"],
            execution_time_ms=data["execution_time_ms"],
            filters_applied=data.get("filters_applied", []),
            warning=data.get("warning"),
        )


@dataclass
class QueryResult:
    """Result of a raw SQL query."""
    columns: list[dict[str, str]]
    rows: list[dict[str, Any]]
    row_count: int
    execution_time_ms: int
    executed_sql: str

    @classmethod
    def from_dict(cls, data: dict) -> QueryResult:
        return cls(
            columns=data["columns"],
            rows=data["rows"],
            row_count=data["row_count"],
            execution_time_ms=data["execution_time_ms"],
            executed_sql=data["executed_sql"],
        )


@dataclass
class Table:
    """A database table."""
    schema: str
    table: str
    estimated_rows: Optional[int] = None
    description: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> Table:
        return cls(
            schema=data["schema"],
            table=data["table"],
            estimated_rows=data.get("estimated_rows"),
            description=data.get("description"),
        )


@dataclass
class Column:
    """A table column."""
    name: str
    type: str
    nullable: bool = True
    is_pii: bool = False
    description: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> Column:
        return cls(
            name=data["name"],
            type=data["type"],
            nullable=data.get("nullable", True),
            is_pii=data.get("is_pii", False),
            description=data.get("description"),
        )


@dataclass
class TableDescription:
    """Full table description with columns."""
    schema: str
    table: str
    description: Optional[str]
    columns: list[Column]

    @classmethod
    def from_dict(cls, data: dict) -> TableDescription:
        return cls(
            schema=data["schema"],
            table=data["table"],
            description=data.get("description"),
            columns=[Column.from_dict(c) for c in data["columns"]],
        )


@dataclass
class MetricRequest:
    """A metric request from an agent."""
    id: int
    description: str
    request_count: int
    status: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    example_query: Optional[str] = None
    requested_by: Optional[str] = None
    resolution_note: Optional[str] = None
    fulfilled_by: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> MetricRequest:
        return cls(
            id=data["id"],
            description=data["description"],
            request_count=data["request_count"],
            status=data["status"],
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            example_query=data.get("example_query"),
            requested_by=data.get("requested_by"),
            resolution_note=data.get("resolution_note"),
            fulfilled_by=data.get("fulfilled_by"),
        )
