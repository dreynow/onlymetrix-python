"""OnlyMetrix Python SDK - governed data access for AI agents."""

from __future__ import annotations
from typing import Any, Optional

import httpx

from onlymetrix.models import (
    Metric,
    MetricResult,
    MetricRequest,
    QueryResult,
    Table,
    TableDescription,
)


class OnlyMetrixError(Exception):
    """Raised when the OnlyMetrix API returns an error."""

    def __init__(self, message: str, status_code: int = 0):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class _MetricsResource:
    """Metric operations."""

    def __init__(self, client: httpx.Client, base_url: str):
        self._client = client
        self._base = base_url

    def list(
        self,
        tag: Optional[str] = None,
        search: Optional[str] = None,
    ) -> list[Metric]:
        """List available metrics, optionally filtered by tag or search query."""
        params = {}
        if tag:
            params["tag"] = tag
        if search:
            params["search"] = search

        resp = self._client.get(f"{self._base}/v1/metrics", params=params)
        data = _handle_response(resp)
        return [Metric.from_dict(m) for m in data["metrics"]]

    def query(
        self,
        name: str,
        filters: Optional[dict[str, str]] = None,
        dimension: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> MetricResult:
        """Execute a metric by name with optional filters."""
        body: dict[str, Any] = {}
        if filters:
            body["filters"] = filters
        if dimension:
            body["dimension"] = dimension
        if limit is not None:
            body["limit"] = limit

        resp = self._client.post(f"{self._base}/v1/metrics/{name}", json=body)
        data = _handle_response(resp)
        return MetricResult.from_dict(data)

    def get(self, name: str) -> Optional[Metric]:
        """Get a single metric by name. Returns None if not found."""
        metrics = self.list()
        return next((m for m in metrics if m.name == name), None)


class _MetricRequestsResource:
    """Metric request operations."""

    def __init__(self, client: httpx.Client, base_url: str):
        self._client = client
        self._base = base_url

    def list(
        self,
        status: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[MetricRequest]:
        """List metric requests."""
        params = {}
        if status:
            params["status"] = status
        if limit is not None:
            params["limit"] = str(limit)

        resp = self._client.get(f"{self._base}/v1/metric-requests", params=params)
        data = _handle_response(resp)
        return [MetricRequest.from_dict(r) for r in data["requests"]]

    def create(
        self,
        description: str,
        example_query: Optional[str] = None,
        requested_by: Optional[str] = None,
    ) -> MetricRequest:
        """Request a new metric."""
        body: dict[str, Any] = {"description": description}
        if example_query:
            body["example_query"] = example_query
        if requested_by:
            body["requested_by"] = requested_by

        resp = self._client.post(f"{self._base}/v1/metric-requests", json=body)
        data = _handle_response(resp)
        return MetricRequest.from_dict(data)

    def resolve(
        self,
        id: int,
        status: str,
        resolution_note: Optional[str] = None,
        fulfilled_by: Optional[str] = None,
    ) -> MetricRequest:
        """Resolve a metric request as fulfilled or rejected."""
        body: dict[str, Any] = {"status": status}
        if resolution_note:
            body["resolution_note"] = resolution_note
        if fulfilled_by:
            body["fulfilled_by"] = fulfilled_by

        resp = self._client.post(
            f"{self._base}/v1/metric-requests/{id}/resolve", json=body
        )
        data = _handle_response(resp)
        return MetricRequest.from_dict(data)


class _TablesResource:
    """Table schema operations."""

    def __init__(self, client: httpx.Client, base_url: str):
        self._client = client
        self._base = base_url

    def list(self) -> list[Table]:
        """List available tables."""
        resp = self._client.get(f"{self._base}/v1/tables")
        data = _handle_response(resp)
        return [Table.from_dict(t) for t in data["tables"]]

    def describe(self, table: str) -> TableDescription:
        """Get table schema with columns."""
        resp = self._client.get(f"{self._base}/v1/tables/{table}")
        data = _handle_response(resp)
        return TableDescription.from_dict(data)


class _SetupResource:
    """Setup / admin operations."""

    def __init__(self, client: httpx.Client, base_url: str):
        self._client = client
        self._base = base_url

    def connect_warehouse(self, type: str, **kwargs: Any) -> dict:
        """Connect a data warehouse."""
        body: dict[str, Any] = {"type": type, **kwargs}
        resp = self._client.post(f"{self._base}/v1/setup/connect-warehouse", json=body)
        return _handle_response(resp)

    def configure_access(
        self,
        allowed_schemas: Optional[list[str]] = None,
        pii_columns: Optional[dict[str, str]] = None,
        policies: Optional[list[dict[str, Any]]] = None,
    ) -> dict:
        """Configure data access policies."""
        body: dict[str, Any] = {}
        if allowed_schemas is not None:
            body["allowed_schemas"] = allowed_schemas
        if pii_columns is not None:
            body["pii_columns"] = pii_columns
        if policies is not None:
            body["policies"] = policies
        resp = self._client.post(f"{self._base}/v1/setup/configure-access", json=body)
        return _handle_response(resp)

    def status(self) -> dict:
        """Get setup status."""
        resp = self._client.get(f"{self._base}/v1/setup/status")
        return _handle_response(resp)

    def list_datasources(self) -> list[dict]:
        """List configured datasources."""
        resp = self._client.get(f"{self._base}/v1/setup/datasources")
        return _handle_response(resp)

    def delete_datasource(self, name: str) -> dict:
        """Delete a datasource by name."""
        resp = self._client.delete(f"{self._base}/v1/setup/datasources/{name}")
        return _handle_response(resp)

    def list_metrics(self) -> list[dict]:
        """List setup metrics."""
        resp = self._client.get(f"{self._base}/v1/setup/metrics")
        return _handle_response(resp)

    def create_metric(
        self, name: str, sql: str, description: str, **kwargs: Any
    ) -> dict:
        """Create a new metric."""
        body: dict[str, Any] = {
            "name": name,
            "sql": sql,
            "description": description,
            **kwargs,
        }
        resp = self._client.post(f"{self._base}/v1/setup/metrics", json=body)
        return _handle_response(resp)

    def delete_metric(self, name: str) -> dict:
        """Delete a metric by name."""
        resp = self._client.delete(f"{self._base}/v1/setup/metrics/{name}")
        return _handle_response(resp)

    def import_metrics(self, metrics: list[dict[str, Any]]) -> dict:
        """Import metrics from a list."""
        resp = self._client.post(
            f"{self._base}/v1/setup/metrics/import", json={"metrics": metrics}
        )
        return _handle_response(resp)

    def dbt_sync(self, manifest: dict[str, Any]) -> dict:
        """Sync metrics from a dbt manifest."""
        resp = self._client.post(
            f"{self._base}/v1/setup/dbt-sync", json={"manifest": manifest}
        )
        return _handle_response(resp)

    def generate_key(
        self,
        name: Optional[str] = None,
        scopes: Optional[list[str]] = None,
    ) -> dict:
        """Generate an API key."""
        body: dict[str, Any] = {}
        if name is not None:
            body["name"] = name
        if scopes is not None:
            body["scopes"] = scopes
        resp = self._client.post(f"{self._base}/v1/setup/generate-key", json=body)
        return _handle_response(resp)

    def list_keys(self) -> list[dict]:
        """List API keys."""
        resp = self._client.get(f"{self._base}/v1/setup/keys")
        return _handle_response(resp)

    def revoke_key(self, id: str) -> dict:
        """Revoke an API key."""
        resp = self._client.delete(f"{self._base}/v1/setup/keys/{id}")
        return _handle_response(resp)


class _AuthResource:
    """Authentication operations."""

    def __init__(self, client: httpx.Client, base_url: str):
        self._client = client
        self._base = base_url

    def signup(
        self, email: str, password: str, name: Optional[str] = None
    ) -> dict:
        """Sign up a new user."""
        body: dict[str, Any] = {"email": email, "password": password}
        if name is not None:
            body["name"] = name
        resp = self._client.post(f"{self._base}/v1/auth/signup", json=body)
        return _handle_response(resp)

    def login(self, email: str, password: str) -> dict:
        """Log in and get a token."""
        body: dict[str, Any] = {"email": email, "password": password}
        resp = self._client.post(f"{self._base}/v1/auth/login", json=body)
        return _handle_response(resp)

    def demo(self) -> dict:
        """Get demo credentials."""
        resp = self._client.get(f"{self._base}/v1/auth/demo")
        return _handle_response(resp)

    def me(self) -> dict:
        """Get current user info."""
        resp = self._client.get(f"{self._base}/v1/auth/me")
        return _handle_response(resp)

    def change_password(self, old_password: str, new_password: str) -> dict:
        """Change the current user's password."""
        body: dict[str, Any] = {
            "old_password": old_password,
            "new_password": new_password,
        }
        resp = self._client.post(f"{self._base}/v1/auth/change-password", json=body)
        return _handle_response(resp)


class _CompilerResource:
    """Compiler operations."""

    def __init__(self, client: httpx.Client, base_url: str):
        self._client = client
        self._base = base_url

    def status(self) -> dict:
        """Get compiler status."""
        resp = self._client.get(f"{self._base}/v1/compiler/status")
        return _handle_response(resp)

    def import_format(
        self, format: str, content: Any, apply: bool = False
    ) -> dict:
        """Import metrics from a format (dbt, lookml, etc)."""
        body: dict[str, Any] = {"format": format, "content": content, "apply": apply}
        resp = self._client.post(f"{self._base}/v1/compiler/import", json=body)
        return _handle_response(resp)


class _CustomAnalysisApiResource:
    """Server-side custom analysis DAG operations."""

    def __init__(self, client: httpx.Client, base_url: str):
        self._client = client
        self._base = base_url

    def register(self, name: str, definition: dict, description: str = "", author: Optional[str] = None) -> dict:
        """Register a DAG on the server."""
        body: dict[str, Any] = {"name": name, "definition": definition, "description": description}
        if author:
            body["author"] = author
        resp = self._client.post(f"{self._base}/v1/analysis/custom", json=body)
        return _handle_response(resp)

    def list(self) -> list[dict]:
        """List all server-side custom analyses."""
        resp = self._client.get(f"{self._base}/v1/analysis/custom")
        data = _handle_response(resp)
        return data.get("analyses", [])

    def get(self, name: str) -> dict:
        """Get a DAG definition by name."""
        resp = self._client.get(f"{self._base}/v1/analysis/custom/{name}")
        return _handle_response(resp)

    def delete(self, name: str) -> dict:
        """Delete a custom analysis."""
        resp = self._client.delete(f"{self._base}/v1/analysis/custom/{name}")
        return _handle_response(resp)


class _AutoresearchResource:
    """Autoresearch operations."""

    def __init__(self, client: httpx.Client, base_url: str):
        self._client = client
        self._base = base_url

    def run(
        self,
        metric_name: str,
        ground_truth_sql: Optional[str] = None,
        max_variations: Optional[int] = None,
        filters: Optional[dict[str, str]] = None,
    ) -> dict:
        """Run autoresearch on a metric.

        Args:
            metric_name: Metric to analyze.
            ground_truth_sql: If None, uses the metric's stored ground_truth_sql.
            max_variations: Max variants to test.
            filters: Segment filters — narrows both seed SQL and ground truth
                to a specific segment (e.g., {"tier": "enterprise"}).
        """
        body: dict[str, Any] = {"metric_name": metric_name}
        if ground_truth_sql:
            body["ground_truth_sql"] = ground_truth_sql
        if max_variations is not None:
            body["max_variations"] = max_variations
        if filters:
            body["filters"] = filters
        resp = self._client.post(f"{self._base}/v1/autoresearch/run", json=body)
        return _handle_response(resp)


class _AdminResource:
    """Admin operations (cache, catalog)."""

    def __init__(self, client: httpx.Client, base_url: str):
        self._client = client
        self._base = base_url

    def invalidate_cache(self, metric: Optional[str] = None) -> dict:
        """Invalidate query cache."""
        body: dict[str, Any] = {}
        if metric is not None:
            body["metric"] = metric
        resp = self._client.post(f"{self._base}/v1/cache/invalidate", json=body)
        return _handle_response(resp)

    def sync_catalog(self) -> dict:
        """Sync the data catalog."""
        resp = self._client.post(f"{self._base}/v1/catalog/sync")
        return _handle_response(resp)


class OnlyMetrix:
    """Synchronous OnlyMetrix client.

    Usage:
        om = OnlyMetrix("http://localhost:8080")
        metrics = om.metrics.list(search="revenue")
        result = om.metrics.query("total_revenue", filters={"time_start": "2025-01-01"})
    """

    def __init__(
        self,
        url: str = "http://localhost:8080",
        api_key: Optional[str] = None,
        timeout: float = 120.0,
    ):
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        self._client = httpx.Client(
            base_url=url,
            headers=headers,
            timeout=timeout,
        )
        self._url = url

        self.metrics = _MetricsResource(self._client, "")
        self.metric_requests = _MetricRequestsResource(self._client, "")
        self.tables = _TablesResource(self._client, "")
        self.setup = _SetupResource(self._client, "")
        self.auth = _AuthResource(self._client, "")
        self.compiler = _CompilerResource(self._client, "")
        self.autoresearch = _AutoresearchResource(self._client, "")
        self.admin = _AdminResource(self._client, "")
        self.custom_analyses = _CustomAnalysisApiResource(self._client, "")

        # Lazy-init: analysis is a property so it doesn't import until used
        self._analysis = None

    @property
    def analysis(self):
        """Analysis primitives: pareto, segment, contributions, trends, compare, top_movers."""
        if self._analysis is None:
            from onlymetrix.analysis import Analysis
            self._analysis = Analysis(self)
        return self._analysis

    def query(self, sql: str, limit: Optional[int] = None) -> QueryResult:
        """Execute a raw SQL query (only when raw SQL is allowed by policy)."""
        body: dict[str, Any] = {"sql": sql}
        if limit is not None:
            body["limit"] = limit

        resp = self._client.post("/v1/query", json=body)
        data = _handle_response(resp)
        return QueryResult.from_dict(data)

    def health(self) -> dict:
        """Check server health."""
        resp = self._client.get("/healthz")
        return _handle_response(resp)

    def close(self):
        """Close the HTTP client."""
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class AsyncOnlyMetrix:
    """Async OnlyMetrix client.

    Usage:
        async with AsyncOnlyMetrix("http://localhost:8080") as om:
            metrics = await om.metrics.list(search="revenue")
            result = await om.metrics.query("total_revenue")
    """

    def __init__(
        self,
        url: str = "http://localhost:8080",
        api_key: Optional[str] = None,
        timeout: float = 120.0,
    ):
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        self._client = httpx.AsyncClient(
            base_url=url,
            headers=headers,
            timeout=timeout,
        )

        self.metrics = _AsyncMetricsResource(self._client, "")
        self.metric_requests = _AsyncMetricRequestsResource(self._client, "")
        self.tables = _AsyncTablesResource(self._client, "")
        self.setup = _AsyncSetupResource(self._client, "")
        self.auth = _AsyncAuthResource(self._client, "")
        self.compiler = _AsyncCompilerResource(self._client, "")
        self.autoresearch = _AsyncAutoresearchResource(self._client, "")
        self.admin = _AsyncAdminResource(self._client, "")

    async def query(self, sql: str, limit: Optional[int] = None) -> QueryResult:
        """Execute a raw SQL query."""
        body: dict[str, Any] = {"sql": sql}
        if limit is not None:
            body["limit"] = limit

        resp = await self._client.post("/v1/query", json=body)
        data = _handle_response(resp)
        return QueryResult.from_dict(data)

    async def health(self) -> dict:
        """Check server health."""
        resp = await self._client.get("/healthz")
        return _handle_response(resp)

    async def close(self):
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()


# -- Async resource classes --


class _AsyncMetricsResource:
    def __init__(self, client: httpx.AsyncClient, base_url: str):
        self._client = client
        self._base = base_url

    async def list(
        self, tag: Optional[str] = None, search: Optional[str] = None
    ) -> list[Metric]:
        params = {}
        if tag:
            params["tag"] = tag
        if search:
            params["search"] = search
        resp = await self._client.get(f"{self._base}/v1/metrics", params=params)
        data = _handle_response(resp)
        return [Metric.from_dict(m) for m in data["metrics"]]

    async def query(
        self,
        name: str,
        filters: Optional[dict[str, str]] = None,
        dimension: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> MetricResult:
        body: dict[str, Any] = {}
        if filters:
            body["filters"] = filters
        if dimension:
            body["dimension"] = dimension
        if limit is not None:
            body["limit"] = limit
        resp = await self._client.post(f"{self._base}/v1/metrics/{name}", json=body)
        data = _handle_response(resp)
        return MetricResult.from_dict(data)

    async def get(self, name: str) -> Optional[Metric]:
        metrics = await self.list()
        return next((m for m in metrics if m.name == name), None)


class _AsyncMetricRequestsResource:
    def __init__(self, client: httpx.AsyncClient, base_url: str):
        self._client = client
        self._base = base_url

    async def list(
        self, status: Optional[str] = None, limit: Optional[int] = None
    ) -> list[MetricRequest]:
        params = {}
        if status:
            params["status"] = status
        if limit is not None:
            params["limit"] = str(limit)
        resp = await self._client.get(f"{self._base}/v1/metric-requests", params=params)
        data = _handle_response(resp)
        return [MetricRequest.from_dict(r) for r in data["requests"]]

    async def create(
        self,
        description: str,
        example_query: Optional[str] = None,
        requested_by: Optional[str] = None,
    ) -> MetricRequest:
        body: dict[str, Any] = {"description": description}
        if example_query:
            body["example_query"] = example_query
        if requested_by:
            body["requested_by"] = requested_by
        resp = await self._client.post(f"{self._base}/v1/metric-requests", json=body)
        data = _handle_response(resp)
        return MetricRequest.from_dict(data)

    async def resolve(
        self,
        id: int,
        status: str,
        resolution_note: Optional[str] = None,
        fulfilled_by: Optional[str] = None,
    ) -> MetricRequest:
        body: dict[str, Any] = {"status": status}
        if resolution_note:
            body["resolution_note"] = resolution_note
        if fulfilled_by:
            body["fulfilled_by"] = fulfilled_by
        resp = await self._client.post(
            f"{self._base}/v1/metric-requests/{id}/resolve", json=body
        )
        data = _handle_response(resp)
        return MetricRequest.from_dict(data)


class _AsyncTablesResource:
    def __init__(self, client: httpx.AsyncClient, base_url: str):
        self._client = client
        self._base = base_url

    async def list(self) -> list[Table]:
        resp = await self._client.get(f"{self._base}/v1/tables")
        data = _handle_response(resp)
        return [Table.from_dict(t) for t in data["tables"]]

    async def describe(self, table: str) -> TableDescription:
        resp = await self._client.get(f"{self._base}/v1/tables/{table}")
        data = _handle_response(resp)
        return TableDescription.from_dict(data)


class _AsyncSetupResource:
    """Async setup / admin operations."""

    def __init__(self, client: httpx.AsyncClient, base_url: str):
        self._client = client
        self._base = base_url

    async def connect_warehouse(self, type: str, **kwargs: Any) -> dict:
        body: dict[str, Any] = {"type": type, **kwargs}
        resp = await self._client.post(
            f"{self._base}/v1/setup/connect-warehouse", json=body
        )
        return _handle_response(resp)

    async def configure_access(
        self,
        allowed_schemas: Optional[list[str]] = None,
        pii_columns: Optional[dict[str, str]] = None,
        policies: Optional[list[dict[str, Any]]] = None,
    ) -> dict:
        body: dict[str, Any] = {}
        if allowed_schemas is not None:
            body["allowed_schemas"] = allowed_schemas
        if pii_columns is not None:
            body["pii_columns"] = pii_columns
        if policies is not None:
            body["policies"] = policies
        resp = await self._client.post(
            f"{self._base}/v1/setup/configure-access", json=body
        )
        return _handle_response(resp)

    async def status(self) -> dict:
        resp = await self._client.get(f"{self._base}/v1/setup/status")
        return _handle_response(resp)

    async def list_datasources(self) -> list[dict]:
        resp = await self._client.get(f"{self._base}/v1/setup/datasources")
        return _handle_response(resp)

    async def delete_datasource(self, name: str) -> dict:
        resp = await self._client.delete(f"{self._base}/v1/setup/datasources/{name}")
        return _handle_response(resp)

    async def list_metrics(self) -> list[dict]:
        resp = await self._client.get(f"{self._base}/v1/setup/metrics")
        return _handle_response(resp)

    async def create_metric(
        self, name: str, sql: str, description: str, **kwargs: Any
    ) -> dict:
        body: dict[str, Any] = {
            "name": name,
            "sql": sql,
            "description": description,
            **kwargs,
        }
        resp = await self._client.post(f"{self._base}/v1/setup/metrics", json=body)
        return _handle_response(resp)

    async def delete_metric(self, name: str) -> dict:
        resp = await self._client.delete(f"{self._base}/v1/setup/metrics/{name}")
        return _handle_response(resp)

    async def import_metrics(self, metrics: list[dict[str, Any]]) -> dict:
        resp = await self._client.post(
            f"{self._base}/v1/setup/metrics/import", json={"metrics": metrics}
        )
        return _handle_response(resp)

    async def dbt_sync(self, manifest: dict[str, Any]) -> dict:
        resp = await self._client.post(
            f"{self._base}/v1/setup/dbt-sync", json={"manifest": manifest}
        )
        return _handle_response(resp)

    async def generate_key(
        self,
        name: Optional[str] = None,
        scopes: Optional[list[str]] = None,
    ) -> dict:
        body: dict[str, Any] = {}
        if name is not None:
            body["name"] = name
        if scopes is not None:
            body["scopes"] = scopes
        resp = await self._client.post(
            f"{self._base}/v1/setup/generate-key", json=body
        )
        return _handle_response(resp)

    async def list_keys(self) -> list[dict]:
        resp = await self._client.get(f"{self._base}/v1/setup/keys")
        return _handle_response(resp)

    async def revoke_key(self, id: str) -> dict:
        resp = await self._client.delete(f"{self._base}/v1/setup/keys/{id}")
        return _handle_response(resp)


class _AsyncAuthResource:
    """Async authentication operations."""

    def __init__(self, client: httpx.AsyncClient, base_url: str):
        self._client = client
        self._base = base_url

    async def signup(
        self, email: str, password: str, name: Optional[str] = None
    ) -> dict:
        body: dict[str, Any] = {"email": email, "password": password}
        if name is not None:
            body["name"] = name
        resp = await self._client.post(f"{self._base}/v1/auth/signup", json=body)
        return _handle_response(resp)

    async def login(self, email: str, password: str) -> dict:
        body: dict[str, Any] = {"email": email, "password": password}
        resp = await self._client.post(f"{self._base}/v1/auth/login", json=body)
        return _handle_response(resp)

    async def demo(self) -> dict:
        resp = await self._client.get(f"{self._base}/v1/auth/demo")
        return _handle_response(resp)

    async def me(self) -> dict:
        resp = await self._client.get(f"{self._base}/v1/auth/me")
        return _handle_response(resp)

    async def change_password(self, old_password: str, new_password: str) -> dict:
        body: dict[str, Any] = {
            "old_password": old_password,
            "new_password": new_password,
        }
        resp = await self._client.post(
            f"{self._base}/v1/auth/change-password", json=body
        )
        return _handle_response(resp)


class _AsyncCompilerResource:
    """Async compiler operations."""

    def __init__(self, client: httpx.AsyncClient, base_url: str):
        self._client = client
        self._base = base_url

    async def status(self) -> dict:
        resp = await self._client.get(f"{self._base}/v1/compiler/status")
        return _handle_response(resp)

    async def import_format(
        self, format: str, content: Any, apply: bool = False
    ) -> dict:
        body: dict[str, Any] = {"format": format, "content": content, "apply": apply}
        resp = await self._client.post(f"{self._base}/v1/compiler/import", json=body)
        return _handle_response(resp)


class _AsyncAutoresearchResource:
    """Async autoresearch operations."""

    def __init__(self, client: httpx.AsyncClient, base_url: str):
        self._client = client
        self._base = base_url

    async def run(
        self,
        metric_name: str,
        ground_truth_sql: Optional[str] = None,
        max_variations: Optional[int] = None,
        filters: Optional[dict[str, str]] = None,
    ) -> dict:
        body: dict[str, Any] = {"metric_name": metric_name}
        if ground_truth_sql:
            body["ground_truth_sql"] = ground_truth_sql
        if max_variations is not None:
            body["max_variations"] = max_variations
        if filters:
            body["filters"] = filters
        resp = await self._client.post(f"{self._base}/v1/autoresearch/run", json=body)
        return _handle_response(resp)


class _AsyncAdminResource:
    """Async admin operations (cache, catalog)."""

    def __init__(self, client: httpx.AsyncClient, base_url: str):
        self._client = client
        self._base = base_url

    async def invalidate_cache(self, metric: Optional[str] = None) -> dict:
        body: dict[str, Any] = {}
        if metric is not None:
            body["metric"] = metric
        resp = await self._client.post(f"{self._base}/v1/cache/invalidate", json=body)
        return _handle_response(resp)

    async def sync_catalog(self) -> dict:
        resp = await self._client.post(f"{self._base}/v1/catalog/sync")
        return _handle_response(resp)


def _handle_response(resp: httpx.Response) -> dict:
    """Handle API response, raising OnlyMetrixError on failure."""
    if resp.status_code >= 400:
        try:
            data = resp.json()
            msg = data.get("error", resp.text)
        except Exception:
            msg = resp.text
        raise OnlyMetrixError(msg, status_code=resp.status_code)
    return resp.json()
