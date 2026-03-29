"""Live integration tests for OnlyMetrix Python SDK.

Requires a running OnlyMetrix server with demo data.
Default: http://localhost:8080

Run:
    pytest tests/test_live.py -v
    ONLYMETRIX_URL=http://host:port pytest tests/test_live.py -v
"""

import os
import pytest

from onlymetrix import OnlyMetrix, AsyncOnlyMetrix
from onlymetrix.client import OnlyMetrixError

URL = os.environ.get("ONLYMETRIX_URL", "http://localhost:8080")


def _server_available():
    try:
        om = OnlyMetrix(url=URL, timeout=2.0)
        om.health()
        om.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _server_available(),
    reason=f"OnlyMetrix server not available at {URL}",
)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class TestHealth:
    def test_health(self):
        with OnlyMetrix(url=URL) as om:
            result = om.health()
            assert result["status"] == "ok"
            assert result["service"] == "onlymetrix"


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------


class TestTables:
    def test_list_tables(self):
        with OnlyMetrix(url=URL) as om:
            tables = om.tables.list()
            assert len(tables) == 5
            names = [t.table for t in tables]
            assert "customers" in names
            assert "orders" in names
            assert "products" in names
            assert "order_items" in names
            assert "support_tickets" in names

    def test_list_tables_have_descriptions(self):
        with OnlyMetrix(url=URL) as om:
            tables = om.tables.list()
            customers = next(t for t in tables if t.table == "customers")
            assert customers.description is not None
            assert "Customer" in customers.description

    def test_describe_table(self):
        with OnlyMetrix(url=URL) as om:
            desc = om.tables.describe("customers")
            assert desc.table == "customers"
            assert desc.schema == "public"
            col_names = [c.name for c in desc.columns]
            assert "id" in col_names
            assert "email" in col_names
            assert "tier" in col_names

    def test_describe_pii_flags(self):
        with OnlyMetrix(url=URL) as om:
            desc = om.tables.describe("customers")
            email = next(c for c in desc.columns if c.name == "email")
            assert email.is_pii is True
            first = next(c for c in desc.columns if c.name == "first_name")
            assert first.is_pii is False

    def test_describe_context_annotations(self):
        with OnlyMetrix(url=URL) as om:
            desc = om.tables.describe("customers")
            tier = next(c for c in desc.columns if c.name == "tier")
            assert tier.description is not None
            assert "free" in tier.description

    def test_describe_nonexistent_table(self):
        with OnlyMetrix(url=URL) as om:
            with pytest.raises(OnlyMetrixError) as exc_info:
                om.tables.describe("nonexistent")
            assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


class TestMetrics:
    def test_list_metrics(self):
        with OnlyMetrix(url=URL) as om:
            metrics = om.metrics.list()
            assert len(metrics) >= 6
            names = [m.name for m in metrics]
            assert "total_revenue" in names
            assert "customer_count" in names

    def test_list_metrics_by_tag(self):
        with OnlyMetrix(url=URL) as om:
            metrics = om.metrics.list(tag="customers")
            assert all("customers" in m.tags for m in metrics)

    def test_search_metrics(self):
        with OnlyMetrix(url=URL) as om:
            metrics = om.metrics.list(search="revenue")
            assert len(metrics) > 0
            assert metrics[0].relevance_score is not None
            assert metrics[0].relevance_score > 0

    def test_get_metric(self):
        with OnlyMetrix(url=URL) as om:
            m = om.metrics.get("total_revenue")
            assert m is not None
            assert m.name == "total_revenue"
            assert "finance" in m.tags

    def test_get_nonexistent_metric(self):
        with OnlyMetrix(url=URL) as om:
            m = om.metrics.get("nonexistent_metric_xyz")
            assert m is None


# ---------------------------------------------------------------------------
# Query metrics
# ---------------------------------------------------------------------------


class TestQueryMetrics:
    def test_total_revenue(self):
        with OnlyMetrix(url=URL) as om:
            result = om.metrics.query("total_revenue")
            assert result.metric == "total_revenue"
            assert result.row_count == 1
            assert result.rows[0]["revenue_usd"] == 5885.0
            assert result.execution_time_ms >= 0

    def test_customer_count(self):
        with OnlyMetrix(url=URL) as om:
            result = om.metrics.query("customer_count")
            assert result.row_count == 1
            assert result.rows[0]["total"] == 10

    def test_top_products(self):
        with OnlyMetrix(url=URL) as om:
            result = om.metrics.query("top_products")
            assert result.row_count > 0
            # Sorted by revenue DESC
            revenues = [r["revenue_usd"] for r in result.rows]
            assert revenues == sorted(revenues, reverse=True)

    def test_with_filter(self):
        with OnlyMetrix(url=URL) as om:
            result = om.metrics.query("revenue_by_tier")
            assert result.row_count == 3
            enterprise = next(r for r in result.rows if r["tier"] == "enterprise")
            assert enterprise["revenue_usd"] == 4892.0

    def test_revenue_by_tier(self):
        with OnlyMetrix(url=URL) as om:
            result = om.metrics.query("revenue_by_tier")
            assert result.row_count == 3
            tiers = {r["tier"] for r in result.rows}
            assert tiers == {"enterprise", "pro", "free"}
            for row in result.rows:
                assert isinstance(row["revenue_usd"], (int, float))

    def test_nonexistent_metric(self):
        with OnlyMetrix(url=URL) as om:
            with pytest.raises(OnlyMetrixError) as exc_info:
                om.metrics.query("nonexistent")
            assert exc_info.value.status_code == 404

    def test_numeric_not_null(self):
        """Regression: NUMERIC columns were returning null."""
        with OnlyMetrix(url=URL) as om:
            result = om.metrics.query("total_revenue")
            assert result.rows[0]["revenue_usd"] is not None
            assert isinstance(result.rows[0]["revenue_usd"], (int, float))

    def test_avg_deal_size_numeric(self):
        """All NUMERIC columns should be numbers, not null."""
        with OnlyMetrix(url=URL) as om:
            result = om.metrics.query("avg_deal_size")
            assert result.row_count == 1
            assert isinstance(result.rows[0]["avg_order_usd"], (int, float))


# ---------------------------------------------------------------------------
# Raw SQL
# ---------------------------------------------------------------------------


class TestRawQuery:
    def test_basic_query(self):
        with OnlyMetrix(url=URL) as om:
            result = om.query("SELECT tier, COUNT(*) AS cnt FROM customers GROUP BY tier")
            assert result.row_count == 3
            total = sum(r["cnt"] for r in result.rows)
            assert total == 10

    def test_pii_masking(self):
        with OnlyMetrix(url=URL) as om:
            result = om.query("SELECT first_name, email, phone FROM customers LIMIT 3")
            for row in result.rows:
                assert "*" in row["email"], f"email should be masked: {row['email']}"
                if row["phone"] is not None:
                    assert "*" in row["phone"], f"phone should be masked: {row['phone']}"
                assert "*" not in row["first_name"], "first_name should not be masked"

    def test_limit_injected(self):
        with OnlyMetrix(url=URL) as om:
            result = om.query("SELECT * FROM customers")
            assert "LIMIT" in result.executed_sql.upper()

    def test_write_blocked(self):
        with OnlyMetrix(url=URL) as om:
            with pytest.raises(OnlyMetrixError) as exc_info:
                om.query("DELETE FROM customers WHERE id = 1")
            assert exc_info.value.status_code == 400


# ---------------------------------------------------------------------------
# Metric requests
# ---------------------------------------------------------------------------


class TestMetricRequests:
    def test_lifecycle(self):
        import time
        with OnlyMetrix(url=URL) as om:
            # Create with unique description to avoid dedup
            desc = f"SDK test: metric request lifecycle {int(time.time())}"
            req = om.metric_requests.create(desc, requested_by="sdk-test")
            assert req.status == "pending"
            assert req.request_count >= 1

            # List
            requests = om.metric_requests.list()
            assert any(r.id == req.id for r in requests)

            # Resolve
            resolved = om.metric_requests.resolve(
                req.id,
                status="fulfilled",
                fulfilled_by="test",
            )
            assert resolved.status == "fulfilled"

    def test_empty_description_rejected(self):
        with OnlyMetrix(url=URL) as om:
            with pytest.raises(OnlyMetrixError) as exc_info:
                om.metric_requests.create("  ")
            assert exc_info.value.status_code == 400


# ---------------------------------------------------------------------------
# Async client
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestAsyncClient:
    async def test_health(self):
        async with AsyncOnlyMetrix(url=URL) as om:
            result = await om.health()
            assert result["status"] == "ok"

    async def test_list_metrics(self):
        async with AsyncOnlyMetrix(url=URL) as om:
            metrics = await om.metrics.list()
            assert len(metrics) >= 6

    async def test_query_metric(self):
        async with AsyncOnlyMetrix(url=URL) as om:
            result = await om.metrics.query("total_revenue")
            assert result.rows[0]["revenue_usd"] == 5885.0

    async def test_list_tables(self):
        async with AsyncOnlyMetrix(url=URL) as om:
            tables = await om.tables.list()
            assert len(tables) == 5

    async def test_describe_table(self):
        async with AsyncOnlyMetrix(url=URL) as om:
            desc = await om.tables.describe("customers")
            assert desc.table == "customers"
            assert len(desc.columns) > 0

    async def test_query(self):
        async with AsyncOnlyMetrix(url=URL) as om:
            result = await om.query("SELECT COUNT(*) AS cnt FROM orders")
            assert result.rows[0]["cnt"] == 18
