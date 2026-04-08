"""Tests for SQL-to-Semantic-Layer converter."""

import json
import tempfile
from pathlib import Path

import pytest

from onlymetrix.sql_converter import (
    convert_sql,
    convert_sql_batch,
    convert_sql_file,
    convert_sql_directory,
    extract_sql,
    metrics_to_yaml,
    ExtractedMetric,
)


class TestConvertSql:
    """Test basic SQL-to-metric conversion."""

    def test_simple_sum(self):
        metric = convert_sql(
            "SELECT SUM(amount) FROM orders WHERE status = 'paid'",
            name="total_revenue",
            description="Total paid revenue",
        )
        assert metric["name"] == "total_revenue"
        assert metric["description"] == "Total paid revenue"
        assert metric["sql"] == "SELECT SUM(amount) FROM orders WHERE status = 'paid'"
        assert "orders" in metric["source_tables"]

    def test_count_distinct(self):
        metric = convert_sql(
            "SELECT COUNT(DISTINCT customer_id) FROM orders",
            name="unique_customers",
        )
        assert metric["name"] == "unique_customers"
        assert "orders" in metric["source_tables"]

    def test_multiple_aggregations(self):
        extracted = extract_sql(
            "SELECT SUM(amount) AS total, AVG(amount) AS avg_amount, COUNT(*) AS cnt FROM orders"
        )
        assert len(extracted.aggregations) == 3
        assert extracted.aggregations[0]["function"] == "SUM"
        assert extracted.aggregations[1]["function"] == "AVG"
        assert extracted.aggregations[2]["function"] == "COUNT"

    def test_group_by_extracts_dimensions(self):
        extracted = extract_sql(
            "SELECT country, SUM(amount) FROM orders GROUP BY country"
        )
        assert "country" in extracted.dimensions
        assert extracted.to_metric_dict().get("dimensions") is True

    def test_where_extracts_filters(self):
        extracted = extract_sql(
            "SELECT SUM(amount) FROM orders WHERE status = 'paid' AND amount > 100"
        )
        filter_names = [f["name"] for f in extracted.filters]
        assert "status" in filter_names
        assert "amount" in filter_names
        # amount uses > operator, should be inferred as number
        amount_filter = next(f for f in extracted.filters if f["name"] == "amount")
        assert amount_filter["type"] == "number"

    def test_join_extracts_multiple_tables(self):
        metric = convert_sql(
            "SELECT SUM(o.amount) FROM orders o JOIN customers c ON o.customer_id = c.id"
        )
        assert len(metric["source_tables"]) == 2
        assert "orders" in [t.split(".")[-1] for t in metric["source_tables"]]
        assert "customers" in [t.split(".")[-1] for t in metric["source_tables"]]

    def test_schema_qualified_table(self):
        metric = convert_sql(
            "SELECT SUM(amount) FROM analytics.public.orders",
            name="revenue",
        )
        assert "analytics.public.orders" in metric["source_tables"]

    def test_time_column_detection(self):
        extracted = extract_sql(
            "SELECT SUM(amount) FROM orders WHERE created_at >= '2025-01-01'"
        )
        assert extracted.time_column == "created_at"

    def test_time_column_order_date(self):
        extracted = extract_sql(
            "SELECT SUM(amount) FROM orders WHERE order_date BETWEEN '2025-01-01' AND '2025-12-31'"
        )
        assert extracted.time_column == "order_date"

    def test_no_aggregation_warning(self):
        extracted = extract_sql("SELECT * FROM users WHERE active = true")
        assert any("No aggregation" in w for w in extracted.warnings)

    def test_no_table_warning(self):
        extracted = extract_sql("SELECT 1 + 1")
        assert any("No source table" in w for w in extracted.warnings)

    def test_auto_infer_name(self):
        metric = convert_sql("SELECT SUM(amount) FROM orders")
        assert metric["name"] == "sum_amount"

    def test_auto_infer_tags(self):
        metric = convert_sql(
            "SELECT SUM(amount) FROM orders JOIN customers ON orders.customer_id = customers.id"
        )
        assert "finance" in metric.get("tags", [])
        assert "customers" in metric.get("tags", [])

    def test_comment_stripping(self):
        sql = """
        -- Total revenue metric
        SELECT SUM(amount) /* in cents */
        FROM orders
        WHERE status = 'paid'
        """
        extracted = extract_sql(sql, name="revenue")
        assert "orders" in extracted.source_tables
        assert len(extracted.aggregations) == 1

    def test_explicit_tags_override_auto(self):
        metric = convert_sql(
            "SELECT SUM(amount) FROM orders",
            name="revenue",
            tags=["kpi", "board-report"],
        )
        assert metric["tags"] == ["kpi", "board-report"]


class TestExtractedMetric:
    """Test the ExtractedMetric dataclass methods."""

    def test_to_metric_dict(self):
        em = ExtractedMetric(
            name="test",
            description="Test metric",
            sql="SELECT 1",
            source_tables=["orders"],
            tags=["finance"],
            time_column="created_at",
            dimensions=["country"],
        )
        d = em.to_metric_dict()
        assert d["name"] == "test"
        assert d["source_tables"] == ["orders"]
        assert d["time_column"] == "created_at"
        assert d["dimensions"] is True

    def test_to_yaml(self):
        em = ExtractedMetric(
            name="revenue",
            description="Total revenue",
            sql="SELECT SUM(amount) FROM orders",
            source_tables=["orders"],
            tags=["finance"],
        )
        yaml_str = em.to_yaml()
        assert "name: revenue" in yaml_str
        assert "description: Total revenue" in yaml_str
        assert "SELECT SUM(amount) FROM orders" in yaml_str
        assert "source_tables: [orders]" in yaml_str


class TestConvertSqlBatch:
    """Test batch conversion."""

    def test_batch_conversion(self):
        sources = [
            {"sql": "SELECT SUM(amount) FROM orders", "name": "revenue"},
            {"sql": "SELECT COUNT(*) FROM users", "name": "user_count"},
        ]
        metrics = convert_sql_batch(sources)
        assert len(metrics) == 2
        assert metrics[0]["name"] == "revenue"
        assert metrics[1]["name"] == "user_count"


class TestConvertSqlFile:
    """Test file-based conversion."""

    def test_convert_single_file(self):
        with tempfile.NamedTemporaryFile(suffix=".sql", mode="w", delete=False) as f:
            f.write("SELECT SUM(amount) FROM orders WHERE status = 'paid'")
            f.flush()
            metric = convert_sql_file(f.name)
            assert "orders" in metric["source_tables"]
            # Name comes from filename
            assert metric["name"] is not None

    def test_convert_file_with_name_override(self):
        with tempfile.NamedTemporaryFile(suffix=".sql", mode="w", delete=False) as f:
            f.write("SELECT COUNT(*) FROM users")
            f.flush()
            metric = convert_sql_file(f.name, name="active_users")
            assert metric["name"] == "active_users"

    def test_convert_file_semicolon_separated(self):
        with tempfile.NamedTemporaryFile(suffix=".sql", mode="w", delete=False) as f:
            f.write("SELECT SUM(a) FROM t1; SELECT AVG(b) FROM t2;")
            f.flush()
            metric = convert_sql_file(f.name)
            # Should use first statement only
            assert "t1" in metric["source_tables"]

    def test_empty_file_raises(self):
        with tempfile.NamedTemporaryFile(suffix=".sql", mode="w", delete=False) as f:
            f.write("   ")
            f.flush()
            with pytest.raises(ValueError, match="No SQL statements"):
                convert_sql_file(f.name)


class TestConvertSqlDirectory:
    """Test directory-based batch conversion."""

    def test_convert_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            p = Path(tmpdir)
            (p / "revenue.sql").write_text("SELECT SUM(amount) FROM orders")
            (p / "users.sql").write_text("SELECT COUNT(*) FROM users")
            (p / "notes.txt").write_text("not a sql file")

            metrics = convert_sql_directory(tmpdir)
            assert len(metrics) == 2
            names = [m["name"] for m in metrics]
            assert "revenue" in names
            assert "users" in names

    def test_invalid_directory_raises(self):
        with pytest.raises(ValueError, match="Not a directory"):
            convert_sql_directory("/nonexistent/path")

    def test_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            metrics = convert_sql_directory(tmpdir)
            assert metrics == []


class TestMetricsToYaml:
    """Test YAML output generation."""

    def test_yaml_output(self):
        metrics = [
            {
                "name": "revenue",
                "description": "Total revenue",
                "sql": "SELECT SUM(amount) FROM orders",
                "source_tables": ["orders"],
                "tags": ["finance"],
                "time_column": "created_at",
            }
        ]
        yaml_str = metrics_to_yaml(metrics)
        assert "metrics:" in yaml_str
        assert "name: revenue" in yaml_str
        assert "time_column: created_at" in yaml_str
        assert "tags: [finance]" in yaml_str

    def test_yaml_multiline_sql(self):
        metrics = [
            {
                "name": "test",
                "description": "Test",
                "sql": "SELECT\n  SUM(amount)\nFROM orders",
            }
        ]
        yaml_str = metrics_to_yaml(metrics)
        assert "sql: |" in yaml_str
        assert "      SELECT" in yaml_str


class TestEdgeCases:
    """Test edge cases and complex SQL patterns."""

    def test_subquery(self):
        sql = """
        SELECT SUM(total) FROM (
            SELECT customer_id, SUM(amount) AS total
            FROM orders
            GROUP BY customer_id
        ) sub
        """
        extracted = extract_sql(sql, name="customer_totals")
        assert len(extracted.aggregations) >= 1
        assert "orders" in extracted.source_tables

    def test_cte(self):
        sql = """
        WITH monthly AS (
            SELECT DATE_TRUNC('month', created_at) AS month, SUM(amount) AS total
            FROM orders
            GROUP BY 1
        )
        SELECT AVG(total) FROM monthly
        """
        extracted = extract_sql(sql, name="avg_monthly_revenue")
        assert len(extracted.aggregations) >= 1

    def test_case_insensitive_keywords(self):
        sql = "select sum(amount) from orders where status = 'paid' group by region"
        extracted = extract_sql(sql)
        assert len(extracted.aggregations) == 1
        assert "orders" in extracted.source_tables
        assert "region" in extracted.dimensions

    def test_multiple_joins(self):
        sql = """
        SELECT SUM(oi.quantity * oi.price) AS total
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.id
        JOIN products p ON oi.product_id = p.id
        """
        extracted = extract_sql(sql, name="total_item_revenue")
        assert len(extracted.source_tables) == 3

    def test_preserves_original_sql(self):
        original = "  SELECT SUM(amount)\n  FROM orders  "
        metric = convert_sql(original, name="test")
        assert metric["sql"] == original.strip()
