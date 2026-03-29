"""LangChain tool integration for OnlyMetrix.

Usage:
    from onlymetrix.integrations.langchain import onlymetrix_tools

    tools = onlymetrix_tools("http://localhost:8080")
    agent = create_react_agent(llm, tools)
"""

from __future__ import annotations

import json
from typing import Optional

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from onlymetrix import OnlyMetrix


class QueryMetricInput(BaseModel):
    name: str = Field(description="Metric name from list_metrics")
    filters: Optional[dict[str, str]] = Field(default=None, description="Optional filters as column:value pairs. Use time_start/time_end for time ranges.")
    dimension: Optional[str] = Field(default=None, description="Column name to GROUP BY (for metrics with dimensions=true)")
    limit: Optional[int] = Field(default=None, description="Maximum rows to return")


class SearchMetricsInput(BaseModel):
    query: str = Field(description="Natural language search query (e.g. 'revenue', 'how many customers')")


class RequestMetricInput(BaseModel):
    description: str = Field(description="What metric you need. Be specific about the business question.")
    example_query: Optional[str] = Field(default=None, description="Optional SQL that approximates the metric")


def onlymetrix_tools(
    url: str = "http://localhost:8080",
    api_key: Optional[str] = None,
) -> list[StructuredTool]:
    """Create LangChain tools for OnlyMetrix.

    Returns a list of tools that can be passed directly to create_react_agent or similar.
    """
    client = OnlyMetrix(url=url, api_key=api_key)

    def search_metrics(query: str) -> str:
        metrics = client.metrics.list(search=query)
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

    def query_metric(name: str, filters: Optional[dict] = None, dimension: Optional[str] = None, limit: Optional[int] = None) -> str:
        result = client.metrics.query(name, filters=filters, dimension=dimension, limit=limit)
        output = {"rows": result.rows, "row_count": result.row_count, "execution_time_ms": result.execution_time_ms}
        if result.warning:
            output["warning"] = result.warning
        return json.dumps(output, indent=2)

    def request_metric(description: str, example_query: Optional[str] = None) -> str:
        req = client.metric_requests.create(description, example_query=example_query)
        return json.dumps({"id": req.id, "status": req.status, "request_count": req.request_count})

    return [
        StructuredTool.from_function(
            func=search_metrics,
            name="search_metrics",
            description="Search for available data metrics by intent. Use this to find what data is available before querying.",
            args_schema=SearchMetricsInput,
        ),
        StructuredTool.from_function(
            func=query_metric,
            name="query_metric",
            description="Execute a data metric by name. Use search_metrics first to find the right metric. Supports filters, time ranges (time_start/time_end), and dimensions.",
            args_schema=QueryMetricInput,
        ),
        StructuredTool.from_function(
            func=request_metric,
            name="request_metric",
            description="Request a new metric when you cannot find what you need. The data team will see the request.",
            args_schema=RequestMetricInput,
        ),
    ]
