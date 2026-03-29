"""CrewAI tool integration for OnlyMetrix.

Usage:
    from onlymetrix.integrations.crewai import onlymetrix_tools

    tools = onlymetrix_tools("http://localhost:8080")
    agent = Agent(role="analyst", tools=tools, ...)
"""

from __future__ import annotations

import json
from typing import Optional

from crewai.tools import BaseTool
from pydantic import BaseModel, Field

from onlymetrix import OnlyMetrix


class SearchMetricsInput(BaseModel):
    query: str = Field(description="Natural language search query (e.g. 'revenue', 'how many customers')")


class QueryMetricInput(BaseModel):
    name: str = Field(description="Metric name from search_metrics")
    filters: Optional[dict[str, str]] = Field(default=None, description="Optional filters as column:value pairs. Use time_start/time_end for time ranges.")
    dimension: Optional[str] = Field(default=None, description="Column name to GROUP BY (for metrics with dimensions=true)")
    limit: Optional[int] = Field(default=None, description="Maximum rows to return")


class RequestMetricInput(BaseModel):
    description: str = Field(description="What metric you need. Be specific about the business question.")
    example_query: Optional[str] = Field(default=None, description="Optional SQL that approximates the metric")


class SearchMetricsTool(BaseTool):
    name: str = "search_metrics"
    description: str = "Search for available data metrics by intent. Use this to find what data is available before querying."
    args_schema: type[BaseModel] = SearchMetricsInput
    client: OnlyMetrix = None

    class Config:
        arbitrary_types_allowed = True

    def _run(self, query: str) -> str:
        metrics = self.client.metrics.list(search=query)
        if not metrics:
            return "No metrics found. Use request_metric to ask the data team."
        return json.dumps([{
            "name": m.name,
            "description": m.description,
            "filters": m.filters,
            "tags": m.tags,
            "dimensions": m.dimensions,
            "time_column": m.time_column,
        } for m in metrics], indent=2)


class QueryMetricTool(BaseTool):
    name: str = "query_metric"
    description: str = "Execute a data metric by name. Use search_metrics first to find the right metric. Supports filters, time ranges, and dimensions."
    args_schema: type[BaseModel] = QueryMetricInput
    client: OnlyMetrix = None

    class Config:
        arbitrary_types_allowed = True

    def _run(self, name: str, filters: Optional[dict] = None, dimension: Optional[str] = None, limit: Optional[int] = None) -> str:
        result = self.client.metrics.query(name, filters=filters, dimension=dimension, limit=limit)
        output = {"rows": result.rows, "row_count": result.row_count, "execution_time_ms": result.execution_time_ms}
        if result.warning:
            output["warning"] = result.warning
        return json.dumps(output, indent=2)


class RequestMetricTool(BaseTool):
    name: str = "request_metric"
    description: str = "Request a new metric when you cannot find what you need. The data team will see the request."
    args_schema: type[BaseModel] = RequestMetricInput
    client: OnlyMetrix = None

    class Config:
        arbitrary_types_allowed = True

    def _run(self, description: str, example_query: Optional[str] = None) -> str:
        req = self.client.metric_requests.create(description, example_query=example_query)
        return json.dumps({"id": req.id, "status": req.status, "request_count": req.request_count})


def onlymetrix_tools(
    url: str = "http://localhost:8080",
    api_key: Optional[str] = None,
) -> list[BaseTool]:
    """Create CrewAI tools for OnlyMetrix.

    Returns a list of tools that can be passed to a CrewAI Agent.
    """
    client = OnlyMetrix(url=url, api_key=api_key)

    return [
        SearchMetricsTool(client=client),
        QueryMetricTool(client=client),
        RequestMetricTool(client=client),
    ]
