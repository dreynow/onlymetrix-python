"""OnlyMetrix CLI — omx command-line interface."""

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

import click

from onlymetrix.client import OnlyMetrix, OnlyMetrixError


def _get_client(url: str | None = None, api_key: str | None = None) -> OnlyMetrix:
    url = url or os.environ.get("OMX_API_URL", "http://localhost:8080")
    api_key = api_key or os.environ.get("OMX_API_KEY")
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
@click.option(
    "--period", default=None,
    help="Semantic period: mtd, ytd, wtd, wow, mom, yoy, last_7d, last_30d, range:start,end",
)
@click.pass_context
def metrics_query(
    ctx: click.Context,
    name: str,
    filters: tuple[str, ...],
    dimension: str | None,
    limit: int | None,
    period: str | None,
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
                name, filters=parsed_filters, dimension=dimension, limit=limit,
                period=period,
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


@compiler.command("inspect")
@click.argument("metric_name")
@click.pass_context
def compiler_inspect(ctx: click.Context, metric_name: str) -> None:
    """Show full compiled IR for a specific metric."""
    try:
        with _get_client() as om:
            result = om.compiler.inspect(metric_name)
            if result is None:
                click.echo(json.dumps({"error": f"Metric '{metric_name}' not found in compiled IR"}), err=True)
                sys.exit(1)
            _output(result, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@compiler.command("validate")
@click.pass_context
def compiler_validate(ctx: click.Context) -> None:
    """Validate all compiled metrics — check for fan-out risk, missing joins, PII exposure."""
    try:
        with _get_client() as om:
            data = om.compiler.status()
            metrics = data.get("metrics", [])
            issues: list[dict] = []
            for m in metrics:
                name = m.get("name", "")
                for j in m.get("joins", []):
                    if j.get("fanout_risk"):
                        issues.append({"metric": name, "type": "fanout_risk", "detail": f"Join {j['from']} → {j['to']} may cause row multiplication"})
                    if not j.get("cardinality"):
                        issues.append({"metric": name, "type": "unknown_cardinality", "detail": f"Join {j['from']} → {j['to']} has no cardinality info"})
                if not m.get("measures"):
                    issues.append({"metric": name, "type": "no_measures", "detail": "No measures detected — metric may be opaque"})
            summary = {
                "total_metrics": len(metrics),
                "structured": sum(1 for m in metrics if m.get("kind") == "structured"),
                "opaque": sum(1 for m in metrics if m.get("kind") != "structured"),
                "issues": len(issues),
                "details": issues,
            }
            _output(summary, ctx.obj["pretty"])
    except OnlyMetrixError as e:
        _handle_error(e)


@compiler.command("agent-context")
@click.option("--query", "-q", default=None, help="Query to rank metrics by relevance")
@click.option("--top-k", "-k", default=10, help="Max metrics to include (default 10)")
@click.pass_context
def compiler_agent_context(ctx: click.Context, query: str | None, top_k: int) -> None:
    """Emit prompt-ready IR for LLM agent injection."""
    try:
        with _get_client() as om:
            context = om.compiler.agent_context(query=query, top_k=top_k)
            click.echo(context)
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


# ── Export ────────────────────────────────────────────────────────────


@cli.command("export")
@click.option(
    "--format", "fmt",
    required=True,
    type=click.Choice(["metricflow"]),
    help="Output format.",
)
@click.option(
    "--output", "output_path",
    default=None,
    help="Output file path. Default: models/marts/om_generated_metrics.yml",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Preview output without writing to disk.",
)
@click.option(
    "--all-sources",
    is_flag=True,
    default=False,
    help="Include metrics from all sources (config, dashboard, dbt). Default: dbt-sourced only.",
)
@click.pass_context
def export_cmd(ctx: click.Context, fmt: str, output_path: str | None, dry_run: bool, all_sources: bool) -> None:
    """Export compiled IR to an external metric format.

    \b
    Examples:
      omx export --format metricflow
      omx export --format metricflow --output path/to/metrics.yml
      omx export --format metricflow --dry-run
      omx export --format metricflow --all-sources
    """
    from onlymetrix.export import run_export
    try:
        with _get_client() as om:
            exit_code = run_export(fmt=fmt, output_path=output_path, dry_run=dry_run, client=om, all_sources=all_sources)
            sys.exit(exit_code)
    except OnlyMetrixError as e:
        _handle_error(e)


@cli.command("validate")
@click.option(
    "--format", "fmt",
    required=True,
    type=click.Choice(["metricflow"]),
    help="Format to validate against.",
)
@click.option(
    "--strict",
    is_flag=True,
    default=False,
    help="Treat warnings as errors (for CI gates).",
)
@click.option(
    "--output", "output_fmt",
    default="text",
    type=click.Choice(["text", "json"]),
    help="Output format. Default: text",
)
@click.pass_context
def validate_cmd(ctx: click.Context, fmt: str, strict: bool, output_fmt: str) -> None:
    """Validate compiled IR against a MetricFlow schema.

    \b
    Exit codes:
      0 — all checks passed
      1 — one or more errors (invalid references, missing fields)
      2 — warnings only (opaque metrics, missing descriptions)
          With --strict, warnings become errors and exit code is 1.

    \b
    Examples:
      omx validate --format metricflow
      omx validate --format metricflow --strict
      omx validate --format metricflow --strict --output json
    """
    from onlymetrix.validate import run_validate
    try:
        with _get_client() as om:
            exit_code = run_validate(fmt=fmt, strict=strict, output_fmt=output_fmt, client=om)
            sys.exit(exit_code)
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


# ── dbt ──────────────────────────────────────────────────────────────


@cli.group()
def dbt() -> None:
    """dbt integration — sync metrics from dbt to OnlyMetrix."""


@dbt.command("connect")
@click.option("--profiles-dir", default=None, type=click.Path(), help="Directory containing profiles.yml")
@click.option("--project-dir", default=None, type=click.Path(), help="dbt project directory")
@click.option("--profile", default=None, help="Profile name (default: from dbt_project.yml)")
@click.option("--target", default=None, help="Target name (default: profile's default)")
@click.option("--name", "ds_name", default=None, help="Datasource name in OM (default: 'default')")
@click.option("--dry-run", is_flag=True, default=False, help="Show what would connect without calling API")
@click.option("--yes", "-y", is_flag=True, default=False, help="Skip confirmation prompt")
@click.option("--url", default=None, help="OnlyMetrix API URL (overrides OMX_API_URL)")
@click.option("--api-key", default=None, help="OnlyMetrix API key (overrides OMX_API_KEY)")
@click.pass_context
def dbt_connect(
    ctx: click.Context,
    profiles_dir: str | None,
    project_dir: str | None,
    profile: str | None,
    target: str | None,
    ds_name: str | None,
    dry_run: bool,
    yes: bool,
    url: str | None,
    api_key: str | None,
) -> None:
    """Connect your dbt warehouse to OnlyMetrix.

    Reads profiles.yml (same credentials dbt uses) and registers
    the datasource with OnlyMetrix.

    \b
    Usage:
      omx dbt connect              # reads ~/.dbt/profiles.yml
      omx dbt connect --dry-run    # preview without connecting
      omx dbt connect -y           # skip confirmation
    """
    from onlymetrix.dbt import find_profiles, parse_profiles

    # 1. Find profiles.yml
    try:
        profiles_path = find_profiles(profiles_dir, project_dir)
    except FileNotFoundError as e:
        click.echo(str(e), err=True)
        sys.exit(1)

    click.echo(f"Reading {profiles_path}")

    # 2. Parse profile + target
    try:
        db_profile = parse_profiles(profiles_path, profile, target, project_dir)
    except (ValueError, EnvironmentError, ImportError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    # 2b. Apply name override
    if ds_name:
        db_profile.name_override = ds_name

    # 3. Show what was found
    click.echo("")
    click.echo(db_profile.display_summary())
    click.echo("")

    if dry_run:
        click.echo("--dry-run: would connect this datasource to OnlyMetrix")
        click.echo(f"Datasource name: {db_profile.datasource_name}")
        return

    # 4. Confirmation
    if not yes:
        click.echo("These credentials will be sent to OnlyMetrix to")
        click.echo("execute metric queries against your warehouse.")
        click.echo("")
        if not click.confirm("Connect?"):
            click.echo("Cancelled.")
            return

    # 5. Connect
    if url:
        os.environ["OMX_API_URL"] = url
    if api_key:
        os.environ["OMX_API_KEY"] = api_key

    try:
        with _get_client() as om:
            result = om.setup.connect_warehouse(**db_profile.to_connect_payload())
            status = result.get("status", "unknown")
            if status == "healthy":
                click.echo(f"\nConnected: {db_profile.datasource_name} ({db_profile.ds_type})")
                click.echo("Run `omx dbt sync` to register your metrics.")
            elif status == "configured":
                click.echo(f"\nConfigured: {db_profile.datasource_name} ({db_profile.ds_type})")
                click.echo("Connection saved but could not verify. Check credentials.")
            else:
                click.echo(f"\nResult: {json.dumps(result)}")
    except OnlyMetrixError as e:
        click.echo(f"Connection failed: {e.message}", err=True)
        sys.exit(1)


@dbt.command("sync")
@click.option("--manifest", default=None, type=click.Path(), help="Path to manifest.json")
@click.option("--project-dir", default=None, type=click.Path(), help="dbt project directory")
@click.option("--out", "out_path", default=".omx/ir.json", type=click.Path(), help="Where to write the compiled IR")
@click.option("--strict", is_flag=True, default=False, help="Exit non-zero if any metric is opaque")
@click.option("--url", default=None, help="OnlyMetrix API URL (overrides OMX_API_URL)")
@click.option("--api-key", default=None, help="OnlyMetrix API key (overrides OMX_API_KEY)")
@click.pass_context
def dbt_sync(
    ctx: click.Context,
    manifest: str | None,
    project_dir: str | None,
    out_path: str,
    strict: bool,
    url: str | None,
    api_key: str | None,
) -> None:
    """Compile dbt metrics into an OM IR (local, no account needed).

    Reads target/manifest.json, extracts any MetricFlow / legacy dbt metrics
    directly in Python, and shells out to the bundled Rust `omx dbt compile`
    for inferred metrics on non-metric models. Writes the merged IR to
    .omx/ir.json. If OMX_API_KEY is set, additionally pushes to the cloud
    for reliability scoring, canvas and team features.

    \b
    Usage:
      dbt parse                        # produce target/manifest.json
      omx dbt sync --project-dir .     # compile IR locally (free)
      OMX_API_KEY=... omx dbt sync ... # also sync to cloud
    """
    from onlymetrix.dbt import find_manifest, parse_manifest
    from onlymetrix.rust_bridge import resolve_binary

    # 1. Locate manifest
    try:
        manifest_path = find_manifest(manifest, project_dir)
    except FileNotFoundError as e:
        click.echo(str(e), err=True)
        sys.exit(1)

    click.echo(f"Reading {manifest_path}")

    # 2. Python-side parse: MetricFlow + legacy dbt metric nodes.
    # These are user-authored metrics, so they take precedence over any
    # inferred candidate with the same name.
    try:
        mf_metrics = parse_manifest(manifest_path)
    except Exception as e:
        click.echo(f"Failed to parse manifest: {e}", err=True)
        sys.exit(1)

    # 3. Rust-side compile: heuristic inference from non-metric models.
    # The binary ships with the package via rust_bridge; it runs entirely
    # on the user's machine with no HTTP or DB dependency.
    import subprocess
    import tempfile

    try:
        binary = resolve_binary()
    except RuntimeError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
        tmp_out = Path(tf.name)
    try:
        result = subprocess.run(
            [
                str(binary), "dbt", "compile",
                "--manifest", str(manifest_path),
                "--out", str(tmp_out),
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            click.echo(
                f"Local compile failed (omx dbt compile): {result.stderr.strip()}",
                err=True,
            )
            sys.exit(1)
        inferred_ir = json.loads(tmp_out.read_text())
    finally:
        tmp_out.unlink(missing_ok=True)

    # 4. Merge. MetricFlow wins on name collision — the user wrote those
    # deliberately, the inference engine is guessing.
    merged: dict[str, dict] = {}
    for item in inferred_ir.get("metrics", []):
        merged[item["name"]] = item
    for m in mf_metrics:
        merged[m.name] = _metricflow_to_ir_entry(m)

    metrics_list = list(merged.values())

    # 5. Write .omx/ir.json
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    ir_doc = {
        "total": len(metrics_list),
        "metrics": metrics_list,
        "source": {
            "manifest": str(manifest_path),
            "metricflow_count": len(mf_metrics),
            "inferred_count": len(inferred_ir.get("metrics", [])),
        },
    }
    out.write_text(json.dumps(ir_doc, indent=2))

    # 6. Optional cloud sync. Presence of a key (env or flag) is the
    # free/paid toggle — no feature flag, no separate command. Delegates
    # to the Rust binary's existing dbt-sync POST to avoid re-implementing
    # HTTP semantics in Python.
    resolved_key = api_key or os.environ.get("OMX_API_KEY")
    cloud_synced = False
    cloud_workspace_url: str | None = None
    if resolved_key:
        rust_env = os.environ.copy()
        if url:
            rust_env["OMX_API_URL"] = url
        if api_key:
            rust_env["OMX_API_KEY"] = api_key
        cloud = subprocess.run(
            [str(binary), "dbt", "sync", str(manifest_path)],
            capture_output=True, text=True, env=rust_env,
        )
        if cloud.returncode != 0:
            click.echo(
                f"Cloud sync failed: {cloud.stderr.strip() or cloud.stdout.strip()}",
                err=True,
            )
        else:
            cloud_synced = True
            cloud_workspace_url = rust_env.get("OMX_API_URL")

    # 7. Pretty output
    _print_compile_summary(
        metrics_list,
        out_path=str(out),
        cloud_synced=cloud_synced,
        cloud_workspace_url=cloud_workspace_url,
    )

    if strict:
        opaque = [m for m in metrics_list if m.get("tier") == "opaque" or m.get("compile_hint") == "opaque"]
        if opaque:
            click.echo(f"\n--strict: {len(opaque)} opaque metric(s)", err=True)
            sys.exit(1)


def _metricflow_to_ir_entry(m) -> dict:
    """Shape a ParsedMetric (MetricFlow/legacy) to match Rust's IR entry format."""
    # MetricFlow metrics are user-authored, so they default to "core" tier
    # unless schema.yml meta.onlymetrix.tier overrides.
    tier = (m.omx_meta.tier if m.omx_meta and m.omx_meta.tier else "core")
    return {
        "name": m.name,
        "label": m.name.replace("_", " ").title(),
        "sql": m.sql_template,
        "type": m.metric_type,
        "dimensions": [],
        "time_column": m.time_column,
        "tier": tier,
        "tier_source": "metricflow",
        "confidence": "high",
        "notes": [],
        "source": "metricflow",
        "base_table": m.source_tables[0] if m.source_tables else None,
    }


_SQL_SUMMARY_PATTERN = re.compile(
    r"SELECT\s+((?:COUNT|SUM|AVG|MIN|MAX)\s*\(\s*(?:DISTINCT\s+)?[^)]*\))",
    re.IGNORECASE,
)


def _summarize_sql(sql: str) -> str:
    """Extract aggregate summary like 'SUM(revenue_gbp)' or fall back to '(complex SQL)'."""
    if not sql:
        return "(no SQL)"
    m = _SQL_SUMMARY_PATTERN.search(sql)
    if m:
        return m.group(1).upper().replace("  ", " ")
    return "(complex SQL)"


def _print_compile_summary(
    metrics_list: list[dict],
    *,
    out_path: str,
    cloud_synced: bool,
    cloud_workspace_url: str | None,
) -> None:
    total = len(metrics_list)
    opaque = sum(1 for m in metrics_list if m.get("tier") == "opaque")

    # Cloud vs local summary line
    if cloud_synced:
        click.echo(f"\n[ok] IR compiled — {total} metrics synced to cloud")
        click.echo("Reliability scores updating...")
        if cloud_workspace_url:
            click.echo(f"View at: {cloud_workspace_url}")
        click.echo()
    else:
        suffix = f", {opaque} opaque" if opaque else ""
        click.echo(f"\n[ok] IR compiled locally — {total} metrics{suffix}")
        click.echo(f"Written to: {out_path}\n")

    # Metric table: name / tier / sql summary
    name_w = max((len(m.get("name", "")) for m in metrics_list), default=4) + 2
    tier_w = 10
    for m in metrics_list:
        name = m.get("name", "")
        tier = m.get("tier", "standard")
        summary = _summarize_sql(m.get("sql", ""))
        click.echo(f"  {name.ljust(name_w)}{tier.ljust(tier_w)}{summary}")

    click.echo()
    click.echo("Query interface ready:")
    click.echo("  omx metrics list")
    if metrics_list:
        first = metrics_list[0]["name"]
        click.echo(f"  omx query --metric {first}")
    if cloud_synced:
        click.echo("\nReliability scores -> app.onlymetrix.com")
    else:
        click.echo("\nFor reliability scores and team features:")
        click.echo("  Sign up free at app.onlymetrix.com")


# ── Reliability ──────────────────────────────────────────────────────


# Colour helpers — degrade gracefully when piped
def _red(s: str) -> str: return click.style(s, fg="red")
def _yellow(s: str) -> str: return click.style(s, fg="yellow")
def _green(s: str) -> str: return click.style(s, fg="green")
def _dim(s: str) -> str: return click.style(s, dim=True)
def _bold(s: str) -> str: return click.style(s, bold=True)


def _status_icon(status: str) -> str:
    icons = {
        "healthy": _green("OK"),
        "degraded": _yellow("!!"),
        "unreliable": _red("XX"),
    }
    return icons.get(status, _dim("??"))


def _severity_icon(severity: str) -> str:
    icons = {
        "critical": _red("XX"),
        "high": _red("XX"),
        "medium": _yellow("!!"),
        "low": _dim("--"),
    }
    return icons.get(severity, _dim("  "))


def _pad(s: str, width: int) -> str:
    """Pad a string to width based on visible length (ignoring ANSI codes)."""
    import re
    visible_len = len(re.sub(r"\x1b\[[0-9;]*m", "", s))
    return s + " " * max(0, width - visible_len)


@cli.group()
def reliability() -> None:
    """Metric reliability — trace broken data to affected decisions."""


@reliability.command("check")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable JSON output")
@click.option("--quiet", is_flag=True, help="Exit code only (0 = all healthy, 1 = issues)")
@click.pass_context
def reliability_check(ctx: click.Context, as_json: bool, quiet: bool) -> None:
    """Run all reliability checks. Show which metrics you can trust."""
    try:
        with _get_client() as om:
            data = om.reliability.status(detail=True)
    except OnlyMetrixError as e:
        if quiet:
            sys.exit(1)
        _handle_error(e)
        return

    if as_json:
        _output(data, ctx.obj.get("pretty", False))
        return

    summary = data.get("summary", {})
    metrics = data.get("metrics", [])
    total = summary.get("total", 0)
    healthy = summary.get("healthy", 0)
    degraded = summary.get("degraded", 0)
    unreliable = summary.get("unreliable", 0)

    if quiet:
        sys.exit(0 if unreliable == 0 and degraded == 0 else 1)

    # Header
    click.echo()
    click.echo(_bold("METRIC RELIABILITY STATUS"))
    click.echo()

    # Summary
    click.echo(f"  {_green(str(healthy))} healthy    {_yellow(str(degraded))} degraded    {_red(str(unreliable))} unreliable    {_dim(str(total) + ' total')}")
    click.echo()

    if unreliable == 0 and degraded == 0:
        click.echo(f"  {_green('All metrics are reliable.')} No action needed.")
        click.echo()
        return

    # Show issues
    for m in metrics:
        status = m.get("status", "unknown")
        if status == "healthy":
            continue
        name = m.get("metric_name", "?")
        violations = m.get("violations", [])

        click.echo(f"  {_status_icon(status)} {_bold(name)}  —  {status.upper()}")
        for v in violations:
            sev = v.get("severity", "")
            desc = v.get("description", "")
            click.echo(f"      {_severity_icon(sev)} {desc}")
        click.echo()

    # Suggest next command
    first_unreliable = next((m for m in metrics if m.get("status") == "unreliable"), None)
    if first_unreliable:
        name = first_unreliable.get("metric_name", "?")
        click.echo(_dim(f"  → omx reliability trace --metric {name}"))
        click.echo(_dim(f"  → omx reliability watch --metric {name}"))
    click.echo()


@reliability.command("trace")
@click.option("--metric", required=True, help="Metric name to trace")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable JSON output")
@click.pass_context
def reliability_trace(ctx: click.Context, metric: str, as_json: bool) -> None:
    """Trace the full dependency chain for one metric."""
    try:
        with _get_client() as om:
            data = om.reliability.status_metric(metric, detail=True)
    except OnlyMetrixError as e:
        _handle_error(e)
        return

    if as_json:
        _output(data, ctx.obj.get("pretty", False))
        return

    status = data.get("status", "unknown")
    safe = data.get("safe_to_use", True)
    violations = data.get("violations", [])
    warning = data.get("warning")
    confidence = data.get("confidence", "unknown")

    click.echo()
    click.echo(f"  {_status_icon(status)} {_bold(metric)}  —  {status.upper()}")
    click.echo()

    if violations:
        click.echo("  Issues:")
        for v in violations:
            sev = v.get("severity", "")
            desc = v.get("description", "")
            table = v.get("source_table", "")
            col = v.get("source_column", "")
            source = f"{table}" + (f".{col}" if col else "")
            click.echo(f"    {_severity_icon(sev)} {desc}")
            if source and source != ".":
                click.echo(f"      {_dim('Source: ' + source)}")
        click.echo()

    click.echo(f"  Safe to use:  {_green('YES') if safe else _red('NO')}")
    click.echo(f"  Confidence:   {confidence}")
    if warning:
        click.echo(f"  Warning:      {warning}")
    click.echo()

    if not safe:
        click.echo(_dim(f"  → omx reliability watch --metric {metric}"))
        click.echo(_dim("    (waits until metric is reliable again)"))
    click.echo()


@reliability.command("watch")
@click.option("--metric", required=True, help="Metric name to watch")
@click.option("--interval", default=60, type=int, help="Poll interval in seconds (default: 60)")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable JSON output")
def reliability_watch(metric: str, interval: int, as_json: bool) -> None:
    """Poll until a metric becomes reliable. Ctrl+C to cancel."""
    import time
    from datetime import datetime

    # Gate: try to subscribe (Team-gated endpoint) to verify plan access.
    # If 402, show upgrade prompt before starting the poll loop.
    om = _get_client()
    try:
        om.reliability.subscribe(metric, "cli_watch", "poll")
    except OnlyMetrixError as e:
        if e.status_code == 402:
            click.echo()
            click.echo(f"  {_yellow('omx reliability watch')} requires the {_bold('Team')} plan.")
            click.echo(f"  Get notified automatically when metrics recover.")
            click.echo()
            click.echo(f"  {_dim('onlymetrix.com/pricing')}")
            click.echo()
            sys.exit(1)
        # Other errors (404, 500) are fine — we'll still poll

    click.echo()
    click.echo(f"  Watching {_bold(metric)} — polling every {interval}s")
    click.echo(f"  {_dim('Ctrl+C to cancel')}")
    click.echo()

    try:
        while True:
            try:
                data = om.reliability.status_metric(metric, detail=True)
            except OnlyMetrixError:
                ts = datetime.now().strftime("%H:%M:%S")
                click.echo(f"  {_dim(ts)}  {_red('ERROR')}  Could not reach API")
                time.sleep(interval)
                continue

            status = data.get("status", "unknown")
            safe = data.get("safe_to_use", False)
            ts = datetime.now().strftime("%H:%M:%S")

            if as_json:
                click.echo(json.dumps({"timestamp": ts, "metric": metric, "status": status, "safe_to_use": safe}))
            else:
                click.echo(f"  {_dim(ts)}  {_status_icon(status)}  {metric}  —  {status}")

            if status == "healthy":
                click.echo()
                click.echo(f"  {_green('OK')} {_bold(metric)} is now reliable.")
                click.echo()
                om.close()
                sys.exit(0)

            time.sleep(interval)
    except KeyboardInterrupt:
        click.echo()
        click.echo(_dim("  Stopped watching."))
        click.echo()
    finally:
        om.close()


@reliability.command("affected-by")
@click.option("--table", required=True, help="Table name to check impact for")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable JSON output")
@click.option("--quiet", is_flag=True, help="Exit code only (0 = no impact, 1 = metrics affected)")
@click.pass_context
def reliability_affected_by(ctx: click.Context, table: str, as_json: bool, quiet: bool) -> None:
    """Trace a broken table to every affected metric and decision.

    This is the command every data analyst wishes existed.
    """
    try:
        with _get_client() as om:
            data = om.reliability.affected_by(table)
    except OnlyMetrixError as e:
        if quiet:
            sys.exit(1)
        _handle_error(e)
        return

    if as_json:
        _output(data, ctx.obj.get("pretty", False))
        return

    affected = data.get("affected_metrics", [])
    table_status = data.get("table_status", "unknown")
    table_violations = data.get("table_violations", [])
    safe = data.get("safe_to_use", True)

    if quiet:
        sys.exit(0 if safe else 1)

    # ── The output that gets screenshotted ──
    click.echo()

    # Table status line
    if table_violations:
        worst = table_violations[0]
        vtype = worst.get("violation_type", "issue").upper().replace("_", " ")
        desc = worst.get("description", "")
        click.echo(f"  Table: {_bold(table)} — {_red(vtype)}")
        click.echo(f"  {desc}")
    else:
        status_str = table_status.upper()
        color_fn = _red if table_status in ("unreliable", "at_risk") else _yellow if table_status == "degraded" else _green
        click.echo(f"  Table: {_bold(table)} — {color_fn(status_str)}")
    click.echo()

    if not affected:
        click.echo(f"  {_green('No affected metrics.')} This table is not in any metric dependency chain.")
        click.echo()
        return

    # Affected metrics
    click.echo("  Affected metrics:")
    for m in affected:
        name = m.get("metric_name", "?")
        dep_type = m.get("dependency_type", "unknown")
        m_safe = m.get("safe_to_use", True)
        icon = _red("XX") if not m_safe else _yellow("!!")
        dep_label = f"({dep_type} dependency)" if dep_type == "direct" else f"(transitive — via dependency chain)"
        click.echo(f"    {icon} {_pad(_bold(name), 30)} {_dim(dep_label)}")
    click.echo()

    # Safe to use verdict
    click.echo(f"  Safe to use: {_red('NO') if not safe else _green('YES')}")
    click.echo()

    # Upsell: suggest watch (Team feature)
    if affected:
        first = affected[0].get("metric_name", "?")
        click.echo(f"  Run: {_dim('omx reliability watch --metric ' + first)}")
        click.echo(f"       {_dim('to be notified when this clears')} {_yellow('[Team]')}")
    click.echo()


# ── SQL Converter ────────────────────────────────────────────────────


@cli.group("sql")
def sql_group():
    """Convert raw SQL into semantic layer metric definitions."""
    pass


@sql_group.command("convert")
@click.argument("sql_input")
@click.option("--name", "-n", default=None, help="Metric name (auto-inferred from SQL if not provided)")
@click.option("--description", "-d", default=None, help="Metric description")
@click.option("--tags", "-t", default=None, help="Comma-separated tags")
@click.option("--format", "fmt", type=click.Choice(["json", "yaml"]), default="json", help="Output format")
@click.option("--import", "do_import", is_flag=True, default=False, help="Import the metric into OnlyMetrix")
@click.option("--url", default=None, help="OnlyMetrix server URL")
@click.option("--api-key", default=None, help="API key")
@click.pass_context
def sql_convert(ctx, sql_input, name, description, tags, fmt, do_import, url, api_key):
    """Convert a SQL query or .sql file into a metric definition.

    SQL_INPUT can be a raw SQL string or a path to a .sql file.
    """
    from pathlib import Path
    from onlymetrix.sql_converter import convert_sql, convert_sql_file, extract_sql, metrics_to_yaml

    tag_list = [t.strip() for t in tags.split(",")] if tags else None

    # Detect if input is a file path
    input_path = Path(sql_input)
    if input_path.exists() and input_path.suffix == ".sql":
        metric = convert_sql_file(input_path, name=name, description=description)
        extracted = extract_sql(input_path.read_text(), name=name, description=description, tags=tag_list)
    else:
        metric = convert_sql(sql_input, name=name, description=description, tags=tag_list)
        extracted = extract_sql(sql_input, name=name, description=description, tags=tag_list)

    # Show warnings
    if extracted.warnings:
        for w in extracted.warnings:
            click.echo(f"  Warning: {w}", err=True)

    # Output
    if fmt == "yaml":
        click.echo(metrics_to_yaml([metric]))
    else:
        pretty = ctx.obj.get("pretty", False)
        _output(metric, pretty=pretty)

    # Optionally import
    if do_import:
        try:
            with _get_client(url=url, api_key=api_key) as om:
                result = om.setup.import_metrics([metric])
                click.echo(f"\nImported: {metric['name']}")
                click.echo(json.dumps(result, default=str))
        except OnlyMetrixError as e:
            _handle_error(e)


@sql_group.command("convert-batch")
@click.argument("directory")
@click.option("--pattern", "-p", default="*.sql", help="File glob pattern (default: *.sql)")
@click.option("--output", "-o", default=None, help="Output file path (.json or .yaml)")
@click.option("--format", "fmt", type=click.Choice(["json", "yaml"]), default="json", help="Output format")
@click.option("--import", "do_import", is_flag=True, default=False, help="Import all metrics into OnlyMetrix")
@click.option("--url", default=None, help="OnlyMetrix server URL")
@click.option("--api-key", default=None, help="API key")
@click.pass_context
def sql_convert_batch(ctx, directory, pattern, output, fmt, do_import, url, api_key):
    """Convert all .sql files in a directory into metric definitions.

    Each SQL file becomes one metric, named after the filename.
    """
    from pathlib import Path
    from onlymetrix.sql_converter import convert_sql_directory, metrics_to_yaml

    try:
        metrics = convert_sql_directory(directory, pattern=pattern)
    except ValueError as e:
        click.echo(str(e), err=True)
        sys.exit(1)

    if not metrics:
        click.echo("No SQL files found.", err=True)
        sys.exit(1)

    click.echo(f"Converted {len(metrics)} SQL files into metric definitions")

    # Format output
    if fmt == "yaml":
        content = metrics_to_yaml(metrics)
    else:
        content = json.dumps(metrics, indent=2, default=str)

    # Write or print
    if output:
        out_path = Path(output)
        out_path.write_text(content, encoding="utf-8")
        click.echo(f"Written: {out_path}")
    else:
        click.echo(content)

    # Optionally import
    if do_import:
        try:
            with _get_client(url=url, api_key=api_key) as om:
                result = om.setup.import_metrics(metrics)
                click.echo(f"\nImported {len(metrics)} metrics")
                click.echo(json.dumps(result, default=str))
        except OnlyMetrixError as e:
            _handle_error(e)


@sql_group.command("inspect")
@click.argument("sql_input")
@click.option("--name", "-n", default=None, help="Metric name")
@click.pass_context
def sql_inspect(ctx, sql_input, name):
    """Inspect what would be extracted from a SQL query.

    Shows aggregations, tables, filters, dimensions, and time columns
    detected in the SQL — useful for debugging before import.
    """
    from pathlib import Path
    from onlymetrix.sql_converter import extract_sql

    input_path = Path(sql_input)
    if input_path.exists() and input_path.suffix == ".sql":
        sql = input_path.read_text()
        name = name or input_path.stem
    else:
        sql = sql_input

    extracted = extract_sql(sql, name=name)

    click.echo(f"  Name:         {extracted.name}")
    click.echo(f"  Description:  {extracted.description}")
    click.echo(f"  Tables:       {', '.join(extracted.source_tables) or '(none)'}")
    click.echo(f"  Aggregations: {len(extracted.aggregations)}")
    for agg in extracted.aggregations:
        click.echo(f"    - {agg['function']}({agg['expression']}) AS {agg['alias']}")
    click.echo(f"  Dimensions:   {', '.join(extracted.dimensions) or '(none)'}")
    click.echo(f"  Time column:  {extracted.time_column or '(not detected)'}")
    click.echo(f"  Filters:      {len(extracted.filters)}")
    for f in extracted.filters:
        click.echo(f"    - {f['name']} ({f['type']})")
    click.echo(f"  Tags:         {', '.join(extracted.tags) or '(none)'}")
    if extracted.warnings:
        click.echo(f"  Warnings:")
        for w in extracted.warnings:
            click.echo(f"    ! {w}")


def main() -> None:
    """Entry point shim.

    Rust-owned subcommands (ci/dbt/discover/scaffold) are dispatched to the
    `omx` binary via rust_bridge before click enters. Everything else
    passes through to the click group below.
    """
    from onlymetrix.rust_bridge import maybe_dispatch_to_rust

    maybe_dispatch_to_rust(sys.argv)
    cli()


if __name__ == "__main__":
    main()
