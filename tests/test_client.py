"""Tests for OnlyMetrix client using respx to mock HTTP responses."""

import pytest
import httpx
import respx

from onlymetrix import OnlyMetrix, AsyncOnlyMetrix
from onlymetrix.client import OnlyMetrixError


BASE = "http://test:8080"


class TestOnlyMetrixSync:
    def test_health(self):
        with respx.mock:
            respx.get(f"{BASE}/healthz").respond(json={"status": "ok", "service": "onlymetrix"})
            om = OnlyMetrix(url=BASE)
            result = om.health()
            assert result["status"] == "ok"
            om.close()

    def test_list_metrics(self):
        with respx.mock:
            respx.get(f"{BASE}/v1/metrics").respond(json={
                "metrics": [
                    {"name": "revenue", "description": "Total revenue", "filters": [], "tags": ["finance"]},
                    {"name": "users", "description": "Active users", "filters": [], "tags": ["product"]},
                ]
            })
            om = OnlyMetrix(url=BASE)
            metrics = om.metrics.list()
            assert len(metrics) == 2
            assert metrics[0].name == "revenue"
            assert metrics[1].tags == ["product"]
            om.close()

    def test_list_metrics_with_search(self):
        with respx.mock:
            respx.get(f"{BASE}/v1/metrics", params={"search": "revenue"}).respond(json={
                "metrics": [
                    {"name": "revenue", "description": "Revenue", "filters": [], "tags": [], "relevance_score": 6},
                ]
            })
            om = OnlyMetrix(url=BASE)
            metrics = om.metrics.list(search="revenue")
            assert len(metrics) == 1
            assert metrics[0].relevance_score == 6
            om.close()

    def test_query_metric(self):
        with respx.mock:
            respx.post(f"{BASE}/v1/metrics/total_revenue").respond(json={
                "metric": "total_revenue",
                "columns": [{"name": "revenue", "type": "FLOAT8"}],
                "rows": [{"revenue": 1234.56}],
                "row_count": 1,
                "execution_time_ms": 45,
                "filters_applied": ["created_at >= '2025-01-01'"],
            })
            om = OnlyMetrix(url=BASE)
            result = om.metrics.query(
                "total_revenue",
                filters={"time_start": "2025-01-01"},
            )
            assert result.metric == "total_revenue"
            assert result.row_count == 1
            assert result.rows[0]["revenue"] == 1234.56
            om.close()

    def test_query_metric_not_found(self):
        with respx.mock:
            respx.post(f"{BASE}/v1/metrics/nonexistent").respond(
                status_code=404,
                json={"error": "Metric 'nonexistent' not found"},
            )
            om = OnlyMetrix(url=BASE)
            with pytest.raises(OnlyMetrixError) as exc_info:
                om.metrics.query("nonexistent")
            assert exc_info.value.status_code == 404
            assert "not found" in exc_info.value.message
            om.close()

    def test_raw_query(self):
        with respx.mock:
            respx.post(f"{BASE}/v1/query").respond(json={
                "columns": [{"name": "count", "type": "INT8"}],
                "rows": [{"count": 42}],
                "row_count": 1,
                "execution_time_ms": 8,
                "executed_sql": "SELECT COUNT(*) FROM users LIMIT 100",
            })
            om = OnlyMetrix(url=BASE)
            result = om.query("SELECT COUNT(*) FROM users")
            assert result.row_count == 1
            assert result.executed_sql.endswith("LIMIT 100")
            om.close()

    def test_raw_query_forbidden(self):
        with respx.mock:
            respx.post(f"{BASE}/v1/query").respond(
                status_code=403,
                json={"error": "Raw SQL is disabled by policy."},
            )
            om = OnlyMetrix(url=BASE)
            with pytest.raises(OnlyMetrixError) as exc_info:
                om.query("SELECT 1")
            assert exc_info.value.status_code == 403
            om.close()

    def test_list_tables(self):
        with respx.mock:
            respx.get(f"{BASE}/v1/tables").respond(json={
                "tables": [
                    {"schema": "public", "table": "orders", "estimated_rows": 5000},
                ]
            })
            om = OnlyMetrix(url=BASE)
            tables = om.tables.list()
            assert len(tables) == 1
            assert tables[0].table == "orders"
            om.close()

    def test_describe_table(self):
        with respx.mock:
            respx.get(f"{BASE}/v1/tables/orders").respond(json={
                "schema": "public",
                "table": "orders",
                "description": "Customer orders",
                "columns": [
                    {"name": "id", "type": "integer", "nullable": False, "is_pii": False},
                ],
            })
            om = OnlyMetrix(url=BASE)
            desc = om.tables.describe("orders")
            assert desc.table == "orders"
            assert len(desc.columns) == 1
            om.close()

    def test_request_metric(self):
        with respx.mock:
            respx.post(f"{BASE}/v1/metric-requests").respond(
                status_code=201,
                json={
                    "id": 1,
                    "description": "Need churn rate",
                    "request_count": 1,
                    "status": "pending",
                },
            )
            om = OnlyMetrix(url=BASE)
            req = om.metric_requests.create("Need churn rate")
            assert req.id == 1
            assert req.status == "pending"
            om.close()

    def test_context_manager(self):
        with respx.mock:
            respx.get(f"{BASE}/healthz").respond(json={"status": "ok"})
            with OnlyMetrix(url=BASE) as om:
                result = om.health()
                assert result["status"] == "ok"

    def test_api_key_header(self):
        with respx.mock:
            route = respx.get(f"{BASE}/healthz").respond(json={"status": "ok"})
            om = OnlyMetrix(url=BASE, api_key="test-key-123")
            om.health()
            assert route.calls[0].request.headers["authorization"] == "Bearer test-key-123"
            om.close()


@pytest.mark.asyncio
class TestOnlyMetrixAsync:
    async def test_health(self):
        async with respx.mock:
            respx.get(f"{BASE}/healthz").respond(json={"status": "ok", "service": "onlymetrix"})
            async with AsyncOnlyMetrix(url=BASE) as om:
                result = await om.health()
                assert result["status"] == "ok"

    async def test_list_metrics(self):
        async with respx.mock:
            respx.get(f"{BASE}/v1/metrics").respond(json={
                "metrics": [
                    {"name": "revenue", "description": "Revenue", "filters": [], "tags": []},
                ]
            })
            async with AsyncOnlyMetrix(url=BASE) as om:
                metrics = await om.metrics.list()
                assert len(metrics) == 1

    async def test_query_metric(self):
        async with respx.mock:
            respx.post(f"{BASE}/v1/metrics/revenue").respond(json={
                "metric": "revenue",
                "columns": [{"name": "total", "type": "FLOAT8"}],
                "rows": [{"total": 99.9}],
                "row_count": 1,
                "execution_time_ms": 10,
            })
            async with AsyncOnlyMetrix(url=BASE) as om:
                result = await om.metrics.query("revenue")
                assert result.rows[0]["total"] == 99.9
