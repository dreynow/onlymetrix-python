"""Tests for OnlyMetrix data models."""

import pytest

from onlymetrix.models import (
    Metric,
    MetricResult,
    MetricRequest,
    QueryResult,
    Table,
    Column,
    TableDescription,
)


class TestMetric:
    def test_from_dict_minimal(self):
        m = Metric.from_dict({"name": "revenue", "description": "Total revenue"})
        assert m.name == "revenue"
        assert m.description == "Total revenue"
        assert m.filters == []
        assert m.tags == []
        assert m.dimensions is False
        assert m.time_column is None
        assert m.version is None
        assert m.deprecated is None

    def test_from_dict_full(self):
        m = Metric.from_dict({
            "name": "revenue_by_region",
            "description": "Revenue by region",
            "filters": [{"column": "month", "type": "date"}],
            "tags": ["finance", "revenue"],
            "open_filters": True,
            "source_tables": ["orders"],
            "dimensions": True,
            "depends_on": ["base_revenue"],
            "time_column": "created_at",
            "time_filters": ["time_start", "time_end"],
            "version": "2.0",
            "deprecated": "Use v3",
            "datasource": "warehouse",
            "relevance_score": 6,
        })
        assert m.open_filters is True
        assert m.source_tables == ["orders"]
        assert m.dimensions is True
        assert m.depends_on == ["base_revenue"]
        assert m.time_column == "created_at"
        assert m.version == "2.0"
        assert m.deprecated == "Use v3"
        assert m.datasource == "warehouse"
        assert m.relevance_score == 6


class TestMetricResult:
    def test_from_dict(self):
        r = MetricResult.from_dict({
            "metric": "total_revenue",
            "columns": [{"name": "revenue", "type": "FLOAT8"}],
            "rows": [{"revenue": 42.5}],
            "row_count": 1,
            "execution_time_ms": 12,
            "filters_applied": ["created_at >= '2025-01-01'"],
        })
        assert r.metric == "total_revenue"
        assert r.row_count == 1
        assert r.rows[0]["revenue"] == 42.5
        assert r.warning is None

    def test_from_dict_with_warning(self):
        r = MetricResult.from_dict({
            "metric": "old_metric",
            "columns": [],
            "rows": [],
            "row_count": 0,
            "execution_time_ms": 5,
            "warning": "DEPRECATED: Use new_metric",
        })
        assert r.warning == "DEPRECATED: Use new_metric"


class TestQueryResult:
    def test_from_dict(self):
        r = QueryResult.from_dict({
            "columns": [{"name": "count", "type": "INT8"}],
            "rows": [{"count": 42}],
            "row_count": 1,
            "execution_time_ms": 8,
            "executed_sql": "SELECT COUNT(*) FROM users LIMIT 100",
        })
        assert r.executed_sql == "SELECT COUNT(*) FROM users LIMIT 100"
        assert r.rows[0]["count"] == 42


class TestTable:
    def test_from_dict(self):
        t = Table.from_dict({
            "schema": "public",
            "table": "orders",
            "estimated_rows": 5000,
            "description": "Customer orders",
        })
        assert t.schema == "public"
        assert t.table == "orders"
        assert t.estimated_rows == 5000

    def test_from_dict_minimal(self):
        t = Table.from_dict({"schema": "public", "table": "users"})
        assert t.estimated_rows is None
        assert t.description is None


class TestColumn:
    def test_from_dict(self):
        c = Column.from_dict({
            "name": "email",
            "type": "text",
            "nullable": True,
            "is_pii": True,
            "description": "User email address",
        })
        assert c.name == "email"
        assert c.is_pii is True
        assert c.description == "User email address"


class TestTableDescription:
    def test_from_dict(self):
        td = TableDescription.from_dict({
            "schema": "public",
            "table": "users",
            "description": "User accounts",
            "columns": [
                {"name": "id", "type": "integer", "nullable": False, "is_pii": False},
                {"name": "email", "type": "text", "nullable": True, "is_pii": True},
            ],
        })
        assert td.table == "users"
        assert len(td.columns) == 2
        assert td.columns[1].is_pii is True


class TestMetricRequest:
    def test_from_dict(self):
        mr = MetricRequest.from_dict({
            "id": 1,
            "description": "Need churn rate",
            "request_count": 3,
            "status": "pending",
            "created_at": "2025-01-01T00:00:00Z",
        })
        assert mr.id == 1
        assert mr.request_count == 3
        assert mr.status == "pending"
        assert mr.fulfilled_by is None


class TestFromDictErrors:
    """Verify from_dict gives clear errors for missing required fields."""

    def test_metric_missing_name(self):
        with pytest.raises(ValueError, match="Metric.from_dict: missing required field 'name'"):
            Metric.from_dict({"description": "test"})

    def test_metric_missing_description(self):
        with pytest.raises(ValueError, match="Metric.from_dict: missing required field 'description'"):
            Metric.from_dict({"name": "test"})

    def test_query_result_missing_columns(self):
        with pytest.raises(ValueError, match="QueryResult.from_dict: missing required field 'columns'"):
            QueryResult.from_dict({"rows": [], "row_count": 0, "execution_time_ms": 0, "executed_sql": ""})

    def test_table_missing_schema(self):
        with pytest.raises(ValueError, match="Table.from_dict: missing required field 'schema'"):
            Table.from_dict({"table": "orders"})

    def test_column_missing_type(self):
        with pytest.raises(ValueError, match="Column.from_dict: missing required field 'type'"):
            Column.from_dict({"name": "id"})

    def test_metric_request_missing_id(self):
        with pytest.raises(ValueError, match="MetricRequest.from_dict: missing required field 'id'"):
            MetricRequest.from_dict({"description": "x", "request_count": 1, "status": "pending"})
