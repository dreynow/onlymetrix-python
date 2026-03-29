"""OnlyMetrix CLI — omx command-line interface."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict
from typing import Any

import click

from onlymetrix.client import OnlyMetrix, OnlyMetrixError


def _get_client() -> OnlyMetrix:
    url = os.environ.get("OMX_API_URL", "http://localhost:8080")
    api_key = os.environ.get("OMX_API_KEY")
    return OnlyMetrix(url=url, api_key=api_key)


def _output(data: Any, pretty: bool = False) -> None:
    """Print data as JSON."""
    if hasattr(data, "__dataclass_fields__"):
        data = asdict(data)
    elif isinstance(data, list) and data and hasattr(data[0], "__dataclass_fields__"):
        data = [asdict(item) for item in data]

    if pretty:
        click.echo(json.dumps(data, indent=2, default=str))
    else:
        click.echo(json.dumps(data, default=str))


def _handle_error(e: OnlyMetrixError) -> None:
    """Print error and exit."""
    msg = {"error": e.message}
    if e.status_code:
        msg["status_code"] = e.status_code
    click.echo(json.dumps(msg), err=True)
    sys.exit(1)


# ── Root ──────────────────────────────────────────────────────────────


@click.group()
@click.option("--pretty", is_flag=True, default=False, help="Pretty-print JSON output")
@click.pass_context
def cli(ctx: click.Context, pretty: bool) -> None:
    """OnlyMetrix CLI — governed data access for AI agents."""
    ctx.ensure_object(dict)
    ctx.obj["pretty"] = pretty


# ── Health ────────────────────────────────────────────────────────────


@cli.command()
@click.pass_context
def health(ctx: click.Context) -> None:
    """Check server health."""
    try:
        with _get_client() as om:
            _output(om.health(), ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


# ── Metrics ───────────────────────────────────────────────────────────


@cli.group()
def metrics() -> None:
    """Metric operations."""


@metrics.command("list")
@click.option("--search", default=None, help="Search query")
@click.option("--tag", default=None, help="Filter by tag")
@click.pass_context
def metrics_list(ctx: click.Context, search: str | None, tag: str | None) -> None:
    """List available metrics."""
    try:
        with _get_client() as om:
            result = om.metrics.list(search=search, tag=tag)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@metrics.command("query")
@click.argument("name")
@click.option("--filter", "filters", multiple=True, help="Filters as key=value pairs")
@click.option("--dimension", default=None, help="Dimension to group by")
@click.option("--limit", default=None, type=int, help="Row limit")
@click.pass_context
def metrics_query(
    ctx: click.Context,
    name: str,
    filters: tuple[str, ...],
    dimension: str | None,
    limit: int | None,
) -> None:
    """Query a metric by name."""
    try:
        parsed_filters: dict[str, str] | None = None
        if filters:
            parsed_filters = {}
            for f in filters:
                if "=" not in f:
                    click.echo(
                        json.dumps({"error": f"Invalid filter format: {f}. Use key=value"}),
                        err=True,
                    )
                    sys.exit(1)
                key, value = f.split("=", 1)
                parsed_filters[key] = value

        with _get_client() as om:
            result = om.metrics.query(
                name, filters=parsed_filters, dimension=dimension, limit=limit
            )
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@metrics.command("create")
@click.option("--name", required=True, help="Metric name")
@click.option("--sql", required=True, help="Metric SQL")
@click.option("--description", required=True, help="Metric description")
@click.pass_context
def metrics_create(ctx: click.Context, name: str, sql: str, description: str) -> None:
    """Create a new metric via setup API."""
    try:
        with _get_client() as om:
            result = om.setup.create_metric(name=name, sql=sql, description=description)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@metrics.command("delete")
@click.argument("name")
@click.pass_context
def metrics_delete(ctx: click.Context, name: str) -> None:
    """Delete a metric by name."""
    try:
        with _get_client() as om:
            result = om.setup.delete_metric(name)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@metrics.command("import")
@click.argument("file", type=click.Path(exists=True))
@click.pass_context
def metrics_import(ctx: click.Context, file: str) -> None:
    """Import metrics from a JSON file."""
    try:
        with open(file) as f:
            data = json.load(f)

        # Accept either a list or {"metrics": [...]}
        if isinstance(data, dict) and "metrics" in data:
            metric_list = data["metrics"]
        elif isinstance(data, list):
            metric_list = data
        else:
            click.echo(
                json.dumps({"error": "File must contain a JSON array or {\"metrics\": [...]}"}),
                err=True,
            )
            sys.exit(1)

        with _get_client() as om:
            result = om.setup.import_metrics(metric_list)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


# ── Tables ────────────────────────────────────────────────────────────


@cli.group()
def tables() -> None:
    """Table operations."""


@tables.command("list")
@click.pass_context
def tables_list(ctx: click.Context) -> None:
    """List available tables."""
    try:
        with _get_client() as om:
            result = om.tables.list()
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@tables.command("describe")
@click.argument("table")
@click.pass_context
def tables_describe(ctx: click.Context, table: str) -> None:
    """Describe a table's schema."""
    try:
        with _get_client() as om:
            result = om.tables.describe(table)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


# ── Query ─────────────────────────────────────────────────────────────


@cli.command()
@click.argument("sql")
@click.pass_context
def query(ctx: click.Context, sql: str) -> None:
    """Execute a raw SQL query."""
    try:
        with _get_client() as om:
            result = om.query(sql)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


# ── Setup ─────────────────────────────────────────────────────────────


@cli.group()
def setup() -> None:
    """Setup and configuration."""


@setup.command("status")
@click.pass_context
def setup_status(ctx: click.Context) -> None:
    """Get setup status."""
    try:
        with _get_client() as om:
            _output(om.setup.status(), ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@setup.command("connect-warehouse")
@click.option("--type", "wh_type", required=True, help="Warehouse type (postgres, snowflake, clickhouse)")
@click.option("--host", default=None, help="Host")
@click.option("--port", default=None, type=int, help="Port")
@click.option("--database", default=None, help="Database name")
@click.option("--user", default=None, help="Username")
@click.option("--password", default=None, help="Password")
@click.option("--account", default=None, help="Snowflake account")
@click.option("--warehouse", default=None, help="Snowflake warehouse")
@click.option("--schema", default=None, help="Default schema")
@click.option("--connection-string", default=None, help="Full connection string")
@click.pass_context
def setup_connect_warehouse(
    ctx: click.Context,
    wh_type: str,
    host: str | None,
    port: int | None,
    database: str | None,
    user: str | None,
    password: str | None,
    account: str | None,
    warehouse: str | None,
    schema: str | None,
    connection_string: str | None,
) -> None:
    """Connect a data warehouse."""
    try:
        kwargs: dict[str, Any] = {}
        if host is not None:
            kwargs["host"] = host
        if port is not None:
            kwargs["port"] = port
        if database is not None:
            kwargs["database"] = database
        if user is not None:
            kwargs["user"] = user
        if password is not None:
            kwargs["password"] = password
        if account is not None:
            kwargs["account"] = account
        if warehouse is not None:
            kwargs["warehouse"] = warehouse
        if schema is not None:
            kwargs["schema"] = schema
        if connection_string is not None:
            kwargs["connection_string"] = connection_string

        with _get_client() as om:
            result = om.setup.connect_warehouse(type=wh_type, **kwargs)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@setup.command("configure")
@click.option("--schemas", default=None, help="Comma-separated allowed schemas")
@click.option("--pii", default=None, help="PII columns as col=type,col=type")
@click.pass_context
def setup_configure(ctx: click.Context, schemas: str | None, pii: str | None) -> None:
    """Configure data access policies."""
    try:
        allowed_schemas = schemas.split(",") if schemas else None
        pii_columns: dict[str, str] | None = None
        if pii:
            pii_columns = {}
            for pair in pii.split(","):
                if "=" not in pair:
                    click.echo(
                        json.dumps({"error": f"Invalid PII format: {pair}. Use col=type"}),
                        err=True,
                    )
                    sys.exit(1)
                col, pii_type = pair.split("=", 1)
                pii_columns[col] = pii_type

        with _get_client() as om:
            result = om.setup.configure_access(
                allowed_schemas=allowed_schemas, pii_columns=pii_columns
            )
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


# ── Compiler ──────────────────────────────────────────────────────────


@cli.group()
def compiler() -> None:
    """Compiler operations."""


@compiler.command("status")
@click.pass_context
def compiler_status(ctx: click.Context) -> None:
    """Get compiler status."""
    try:
        with _get_client() as om:
            _output(om.compiler.status(), ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@compiler.command("import")
@click.option("--format", "fmt", required=True, help="Import format (dbt, lookml)")
@click.argument("file", type=click.Path(exists=True))
@click.option("--apply", "apply_flag", is_flag=True, default=False, help="Apply changes immediately")
@click.pass_context
def compiler_import(ctx: click.Context, fmt: str, file: str, apply_flag: bool) -> None:
    """Import metrics from a file (dbt YAML, LookML, etc)."""
    try:
        with open(file) as f:
            content = f.read()

        # Try parsing as JSON/YAML, fall back to raw string
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            # For YAML or other formats, send as raw string
            parsed = content

        with _get_client() as om:
            result = om.compiler.import_format(format=fmt, content=parsed, apply=apply_flag)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


# ── Autoresearch ──────────────────────────────────────────────────────


@cli.group()
def autoresearch() -> None:
    """Autoresearch operations."""


@autoresearch.command("run")
@click.option("--metric", required=True, help="Metric name")
@click.option("--ground-truth", required=True, help="Ground truth SQL")
@click.option("--max-variations", default=None, type=int, help="Max variations to generate")
@click.pass_context
def autoresearch_run(
    ctx: click.Context,
    metric: str,
    ground_truth: str,
    max_variations: int | None,
) -> None:
    """Run autoresearch on a metric."""
    try:
        with _get_client() as om:
            result = om.autoresearch.run(
                metric_name=metric,
                ground_truth_sql=ground_truth,
                max_variations=max_variations,
            )
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


# ── Auth ──────────────────────────────────────────────────────────────


@cli.group()
def auth() -> None:
    """Authentication operations."""


@auth.command("login")
@click.option("--email", required=True, help="Email address")
@click.option("--password", required=True, help="Password")
@click.pass_context
def auth_login(ctx: click.Context, email: str, password: str) -> None:
    """Log in and get a token."""
    try:
        with _get_client() as om:
            result = om.auth.login(email=email, password=password)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@auth.command("signup")
@click.option("--email", required=True, help="Email address")
@click.option("--password", required=True, help="Password")
@click.option("--name", default=None, help="Display name")
@click.pass_context
def auth_signup(ctx: click.Context, email: str, password: str, name: str | None) -> None:
    """Sign up a new account."""
    try:
        with _get_client() as om:
            result = om.auth.signup(email=email, password=password, name=name)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@auth.command("demo")
@click.pass_context
def auth_demo(ctx: click.Context) -> None:
    """Get a demo session key (no account required)."""
    try:
        with _get_client() as om:
            _output(om.auth.demo(), ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@auth.command("me")
@click.pass_context
def auth_me(ctx: click.Context) -> None:
    """Get current user info."""
    try:
        with _get_client() as om:
            _output(om.auth.me(), ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


# ── Keys ──────────────────────────────────────────────────────────────


@cli.group()
def keys() -> None:
    """API key management."""


@keys.command("generate")
@click.option("--name", default=None, help="Key name")
@click.option("--scopes", default=None, help="Comma-separated scopes (e.g. read,write)")
@click.pass_context
def keys_generate(ctx: click.Context, name: str | None, scopes: str | None) -> None:
    """Generate a new API key."""
    try:
        scope_list = scopes.split(",") if scopes else None
        with _get_client() as om:
            result = om.setup.generate_key(name=name, scopes=scope_list)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@keys.command("list")
@click.pass_context
def keys_list(ctx: click.Context) -> None:
    """List API keys."""
    try:
        with _get_client() as om:
            _output(om.setup.list_keys(), ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@keys.command("revoke")
@click.argument("id")
@click.pass_context
def keys_revoke(ctx: click.Context, id: str) -> None:
    """Revoke an API key."""
    try:
        with _get_client() as om:
            result = om.setup.revoke_key(id)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


# ── Cache ─────────────────────────────────────────────────────────────


@cli.group()
def cache() -> None:
    """Cache operations."""


@cache.command("invalidate")
@click.option("--metric", default=None, help="Invalidate only this metric's cache")
@click.pass_context
def cache_invalidate(ctx: click.Context, metric: str | None) -> None:
    """Invalidate query cache."""
    try:
        with _get_client() as om:
            result = om.admin.invalidate_cache(metric=metric)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


# ── Catalog ───────────────────────────────────────────────────────────


@cli.group()
def catalog() -> None:
    """Catalog operations."""


@catalog.command("sync")
@click.pass_context
def catalog_sync(ctx: click.Context) -> None:
    """Sync the data catalog."""
    try:
        with _get_client() as om:
            result = om.admin.sync_catalog()
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


# ── Analysis ──────────────────────────────────────────────────────────


@cli.group()
def analysis() -> None:
    """Reasoning substrate: pareto, segment-performance, contribution, drivers, anomalies, trends, compare."""


@analysis.command("pareto")
@click.argument("metric")
@click.option("--ground-truth", "-gt", default=None, help="Ground truth SQL for scoring")
@click.option("--max-variations", "-n", default=30, type=int, help="Max variants to test")
@click.pass_context
def analysis_pareto(ctx: click.Context, metric: str, ground_truth: str | None, max_variations: int) -> None:
    """Pareto frontier: precision-recall tradeoff across metric variants."""
    try:
        with _get_client() as om:
            result = om.analysis.pareto(metric, ground_truth_sql=ground_truth, max_variations=max_variations)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@analysis.command("segment-performance")
@click.argument("metric")
@click.option("--segments", "-s", required=True, help="Dimensions to analyze (comma-separated)")
@click.option("--target", "-t", default=None, help="Measure column (auto-detected)")
@click.pass_context
def analysis_segment_perf(ctx: click.Context, metric: str, segments: str, target: str | None) -> None:
    """Cross-dimensional segment performance analysis."""
    try:
        with _get_client() as om:
            result = om.analysis.segment_performance(metric, segments=segments.split(","), target=target)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@analysis.command("contribution")
@click.argument("metric")
@click.option("--current", required=True, help="Current period (e.g. 2025-02)")
@click.option("--previous", required=True, help="Previous period (e.g. 2025-01)")
@click.option("--dimension", "-d", default=None, help="Dimension to decompose by")
@click.option("--target", "-t", default=None, help="Measure column (auto-detected)")
@click.pass_context
def analysis_contribution(ctx: click.Context, metric: str, current: str, previous: str,
                          dimension: str | None, target: str | None) -> None:
    """Period-over-period change decomposition — what drove the change."""
    try:
        with _get_client() as om:
            result = om.analysis.contribution(
                metric, compare={"current": current, "previous": previous},
                dimension=dimension, target=target,
            )
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@analysis.command("drivers")
@click.argument("metric")
@click.option("--dimensions", "-d", required=True, help="Dimensions to rank (comma-separated)")
@click.option("--target", "-t", default=None, help="Measure column (auto-detected)")
@click.pass_context
def analysis_drivers(ctx: click.Context, metric: str, dimensions: str, target: str | None) -> None:
    """Rank dimensions by explanatory power (which drives variance most)."""
    try:
        with _get_client() as om:
            result = om.analysis.drivers(metric, dimensions=dimensions.split(","), target=target)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@analysis.command("anomalies")
@click.argument("metric")
@click.option("--dimension", "-d", required=True, help="Dimension to check")
@click.option("--target", "-t", default=None, help="Measure column (auto-detected)")
@click.option("--threshold", "-z", default=2.0, type=float, help="Z-score threshold (default 2.0)")
@click.pass_context
def analysis_anomalies(ctx: click.Context, metric: str, dimension: str, target: str | None, threshold: float) -> None:
    """Find segments behaving abnormally compared to peers."""
    try:
        with _get_client() as om:
            result = om.analysis.anomalies(metric, dimension, target=target, z_threshold=threshold)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@analysis.command("trends")
@click.argument("metric")
@click.option("--granularity", "-g", default="month", help="Time grain: day/week/month/quarter/year")
@click.option("--target", "-t", default=None, help="Measure column (auto-detected)")
@click.pass_context
def analysis_trends(ctx: click.Context, metric: str, granularity: str, target: str | None) -> None:
    """Time series with momentum analysis."""
    try:
        with _get_client() as om:
            result = om.analysis.trends(metric, granularity=granularity, target=target)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@analysis.command("compare")
@click.argument("metric")
@click.option("--filter-a", required=True, help="Filter for group A (key=value,key=value)")
@click.option("--filter-b", required=True, help="Filter for group B (key=value,key=value)")
@click.option("--label-a", default="Group A", help="Label for group A")
@click.option("--label-b", default="Group B", help="Label for group B")
@click.option("--target", "-t", default=None, help="Measure column (auto-detected)")
@click.pass_context
def analysis_compare(ctx: click.Context, metric: str, filter_a: str, filter_b: str,
                     label_a: str, label_b: str, target: str | None) -> None:
    """A/B comparison of two filter sets on the same metric."""
    try:
        fa = dict(kv.split("=", 1) for kv in filter_a.split(","))
        fb = dict(kv.split("=", 1) for kv in filter_b.split(","))
        with _get_client() as om:
            result = om.analysis.compare(metric, fa, fb, label_a=label_a, label_b=label_b, target=target)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@analysis.command("at-risk-profile")
@click.argument("metric")
@click.option("--basket-dimension", "-b", default="product_id", help="Dimension for basket analysis")
@click.option("--compare", "-c", default=None, help="Comparison entity metric")
@click.option("--top", "-n", default=5, type=int, help="Top N segments")
@click.pass_context
def analysis_at_risk(ctx: click.Context, metric: str, basket_dimension: str,
                     compare: str | None, top: int) -> None:
    """Profile at-risk entities: who they are, what distinguishes them."""
    try:
        with _get_client() as om:
            result = om.analysis.run_custom(
                "at_risk_profile", metric=metric,
                basket_dimension=basket_dimension,
                compare_metric=compare, top_n=top,
            )
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@analysis.command("run")
@click.argument("name")
@click.argument("metric")
@click.option("--param", "-p", multiple=True, help="key=value parameters")
@click.pass_context
def analysis_run_custom(ctx: click.Context, name: str, metric: str, param: tuple) -> None:
    """Run a custom analysis by name."""
    try:
        params = dict(kv.split("=", 1) for kv in param)
        with _get_client() as om:
            result = om.analysis.run_custom(name, metric=metric, **params)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@analysis.command("list-custom")
@click.pass_context
def analysis_list_custom(ctx: click.Context) -> None:
    """List registered custom analyses."""
    try:
        with _get_client() as om:
            result = om.analysis.list_custom()
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@analysis.command("save")
@click.argument("name")
@click.argument("file", type=click.Path(exists=True))
@click.pass_context
def analysis_save(ctx: click.Context, name: str, file: str) -> None:
    """Save a DAG JSON file to the server."""
    import json as _json
    try:
        with open(file) as f:
            dag = _json.load(f)
        dag["name"] = name
        with _get_client() as om:
            result = om.custom_analyses.register(
                name=name, definition=dag,
                description=dag.get("description", ""),
                author=dag.get("author"),
            )
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@analysis.command("export")
@click.argument("name")
@click.pass_context
def analysis_export(ctx: click.Context, name: str) -> None:
    """Export a function as a DAG (introspect and print)."""
    try:
        with _get_client() as om:
            dag = om.analysis.export_dag(name)
            _output(dag, ctx.obj["pretty"])
    except (OnlyMetrixError, ValueError) as e:
        _handle_error(e)


@analysis.command("get")
@click.argument("name")
@click.pass_context
def analysis_get(ctx: click.Context, name: str) -> None:
    """Get a DAG definition from the server."""
    try:
        with _get_client() as om:
            result = om.custom_analyses.get(name)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@analysis.command("delete")
@click.argument("name")
@click.pass_context
def analysis_delete(ctx: click.Context, name: str) -> None:
    """Delete a custom analysis from the server."""
    try:
        with _get_client() as om:
            result = om.custom_analyses.delete(name)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@analysis.command("load")
@click.argument("name")
@click.pass_context
def analysis_load(ctx: click.Context, name: str) -> None:
    """Load a DAG from the server into the local runtime."""
    try:
        with _get_client() as om:
            result = om.analysis.load_from_server(name)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


if __name__ == "__main__":
    cli()
