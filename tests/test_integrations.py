"""E2E tests for LangChain and CrewAI integrations.

Requires a running OnlyMetrix server with demo data.
Default: http://localhost:8080

Run:
    ONLYMETRIX_URL=http://localhost:8222 pytest tests/test_integrations.py -v
"""

import json
import os
import time

import pytest

from onlymetrix import OnlyMetrix

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
# LangChain integration
# ---------------------------------------------------------------------------


class TestLangChainTools:
    @pytest.fixture(autouse=True)
    def setup_tools(self):
        from onlymetrix.integrations.langchain import onlymetrix_tools

        self.tools = onlymetrix_tools(url=URL)
        self.tool_map = {t.name: t for t in self.tools}

    def test_creates_three_tools(self):
        assert len(self.tools) == 3
        assert set(self.tool_map.keys()) == {
            "search_metrics",
            "query_metric",
            "request_metric",
        }

    def test_tools_have_descriptions(self):
        for tool in self.tools:
            assert tool.description
            assert len(tool.description) > 10

    def test_search_metrics(self):
        result = self.tool_map["search_metrics"].invoke({"query": "revenue"})
        data = json.loads(result)
        assert isinstance(data, list)
        assert len(data) > 0
        names = [m["name"] for m in data]
        assert "total_revenue" in names

    def test_search_metrics_returns_json(self):
        result = self.tool_map["search_metrics"].invoke(
            {"query": "zzz_nonexistent_metric_xyz"}
        )
        # Semantic search may still return low-relevance matches
        data = json.loads(result)
        assert isinstance(data, list)

    def test_query_metric(self):
        result = self.tool_map["query_metric"].invoke({"name": "total_revenue"})
        data = json.loads(result)
        assert data["row_count"] == 1
        assert data["rows"][0]["revenue_usd"] == 5885.0
        assert "execution_time_ms" in data

    def test_query_metric_with_limit(self):
        result = self.tool_map["query_metric"].invoke(
            {"name": "top_products", "limit": 2}
        )
        data = json.loads(result)
        assert data["row_count"] <= 2

    def test_request_metric(self):
        desc = f"LangChain e2e test: need conversion rate {int(time.time())}"
        result = self.tool_map["request_metric"].invoke({"description": desc})
        data = json.loads(result)
        assert data["status"] == "pending"
        assert data["request_count"] >= 1
        assert "id" in data

    def test_request_metric_with_example(self):
        desc = f"LangChain e2e test: need DAU {int(time.time())}"
        result = self.tool_map["request_metric"].invoke(
            {
                "description": desc,
                "example_query": "SELECT COUNT(DISTINCT user_id) FROM events WHERE date = CURRENT_DATE",
            }
        )
        data = json.loads(result)
        assert data["status"] == "pending"


# ---------------------------------------------------------------------------
# CrewAI integration
#
# CrewAI >= 0.80 requires Python < 3.14, so we mock BaseTool to test the
# integration logic against the live server.
# ---------------------------------------------------------------------------


class _MockBaseTool:
    """Minimal stand-in for crewai.tools.BaseTool."""

    model_config = {"arbitrary_types_allowed": True}

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def run(self, **kwargs):
        return self._run(**kwargs)


@pytest.fixture(autouse=False)
def mock_crewai(monkeypatch):
    """Patch crewai.tools.BaseTool so the integration module can import."""
    import types

    crewai_mod = types.ModuleType("crewai")
    crewai_tools_mod = types.ModuleType("crewai.tools")
    crewai_tools_mod.BaseTool = _MockBaseTool
    crewai_mod.tools = crewai_tools_mod

    import sys

    monkeypatch.setitem(sys.modules, "crewai", crewai_mod)
    monkeypatch.setitem(sys.modules, "crewai.tools", crewai_tools_mod)

    # Force re-import of the integration module with the mock
    if "onlymetrix.integrations.crewai" in sys.modules:
        del sys.modules["onlymetrix.integrations.crewai"]


class TestCrewAITools:
    @pytest.fixture(autouse=True)
    def setup_tools(self, mock_crewai):
        from onlymetrix.integrations.crewai import onlymetrix_tools

        self.tools = onlymetrix_tools(url=URL)
        self.tool_map = {t.name: t for t in self.tools}

    def test_creates_three_tools(self):
        assert len(self.tools) == 3
        assert set(self.tool_map.keys()) == {
            "search_metrics",
            "query_metric",
            "request_metric",
        }

    def test_tools_have_descriptions(self):
        for tool in self.tools:
            assert tool.description
            assert len(tool.description) > 10

    def test_search_metrics(self):
        result = self.tool_map["search_metrics"]._run(query="revenue")
        data = json.loads(result)
        assert isinstance(data, list)
        assert len(data) > 0
        names = [m["name"] for m in data]
        assert "total_revenue" in names

    def test_search_metrics_returns_json(self):
        result = self.tool_map["search_metrics"]._run(
            query="zzz_nonexistent_metric_xyz"
        )
        # Semantic search may still return low-relevance matches
        data = json.loads(result)
        assert isinstance(data, list)

    def test_query_metric(self):
        result = self.tool_map["query_metric"]._run(name="total_revenue")
        data = json.loads(result)
        assert data["row_count"] == 1
        assert data["rows"][0]["revenue_usd"] == 5885.0
        assert "execution_time_ms" in data

    def test_query_metric_with_limit(self):
        result = self.tool_map["query_metric"]._run(name="top_products", limit=2)
        data = json.loads(result)
        assert data["row_count"] <= 2

    def test_request_metric(self):
        desc = f"CrewAI e2e test: need retention rate {int(time.time())}"
        result = self.tool_map["request_metric"]._run(description=desc)
        data = json.loads(result)
        assert data["status"] == "pending"
        assert data["request_count"] >= 1
        assert "id" in data

    def test_request_metric_with_example(self):
        desc = f"CrewAI e2e test: need WAU {int(time.time())}"
        result = self.tool_map["request_metric"]._run(
            description=desc,
            example_query="SELECT COUNT(DISTINCT user_id) FROM events WHERE date >= CURRENT_DATE - 7",
        )
        data = json.loads(result)
        assert data["status"] == "pending"
