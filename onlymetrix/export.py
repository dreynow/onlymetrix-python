"""omx export — compile IR to external formats.

Currently supported:
  --format metricflow   → dbt MetricFlow semantic_models + metrics YAML
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import click

from onlymetrix.metricflow import export_metricflow, DEFAULT_OUTPUT_PATH

logger = logging.getLogger(__name__)


def run_export(
    fmt: str,
    output_path: str | None,
    dry_run: bool,
    client: Any,
    all_sources: bool = False,
) -> int:
    """Execute export. Returns exit code (0 = success, 1 = error, 2 = warnings)."""
    if fmt != "metricflow":
        click.echo(f"  Unknown format: {fmt}. Supported: metricflow", err=True)
        return 1

    # Fetch IR from compiler
    click.echo()
    click.echo("  Fetching compiled IR…")
    try:
        data = client.compiler.status()
    except Exception as e:
        click.echo(f"  Error fetching IR: {e}", err=True)
        return 1

    ir_metrics = data.get("metrics", [])
    if not ir_metrics:
        click.echo("  No compiled metrics found. Run: omx sync --source dbt", err=True)
        return 1

    # Fetch catalog metadata (source + source_tables) — used for:
    #   Fix 1: correct model: ref() pointing at actual dbt model
    #   Fix 3: filter to dbt-sourced metrics only (unless --all-sources)
    catalog_meta: dict[str, dict] = {}
    try:
        data = client.setup.list_metrics()
        for m in data.get("metrics", []):
            catalog_meta[m["name"]] = {
                "source": m.get("source", ""),
                "source_tables": m.get("source_tables", []),
                "time_column": m.get("time_column") or "",
                "primary_key_column": m.get("primary_key_column") or "",
            }
    except Exception:
        logger.debug("Could not fetch catalog metadata; export continues without it")

    # Fix 3: scope to dbt-synced metrics unless --all-sources requested
    if catalog_meta and not all_sources:
        dbt_names = {n for n, m in catalog_meta.items() if m.get("source") == "dbt"}
        filtered = [m for m in ir_metrics if m["name"] in dbt_names]
        if filtered:
            ir_metrics = filtered
        # If no dbt metrics found (e.g. all metrics are config-loaded), fall through
        # and export everything rather than producing an empty file.

    click.echo(f"  Found {len(ir_metrics)} compiled metrics")

    # Export
    try:
        out_path, content, structured, opaque = export_metricflow(
            ir_metrics,
            output_path=output_path,
            dry_run=dry_run,
            source="omx export --format metricflow",
            catalog_meta=catalog_meta,
        )
    except Exception as e:
        click.echo(f"  Export failed: {e}", err=True)
        return 1

    # Output
    click.echo()

    if dry_run:
        click.echo("  ─── DRY RUN — no file written ───")
        click.echo()
        click.echo(content)
        click.echo()
    else:
        click.echo(f"  Written: {out_path}")
        click.echo()

    # Summary
    _print_summary(ir_metrics, structured, opaque)

    if dry_run:
        click.echo()
        click.echo("  Run without --dry-run to write the file.")

    click.echo()
    return 0 if opaque == 0 else 2


def _print_summary(metrics: list[dict], structured: int, opaque: int) -> None:
    """Print per-metric status table."""
    name_w = max((len(m["name"]) for m in metrics), default=20)

    for m in metrics:
        name = m["name"]
        kind = m.get("kind", "unknown")
        sem = m.get("semantic", {})
        importance = sem.get("importance", "—")
        taxonomy = " › ".join(sem.get("taxonomy_path", [])) or "—"
        tags = ", ".join(sem.get("tags", [])) or "—"

        if kind == "structured":
            measures = m.get("measures", [])
            agg_summary = ", ".join(
                f"{me['alias']}({me['function'].lower()})" for me in measures
            ) or "—"
            icon = "✓"
            label = f"structured  · importance {importance}  · {agg_summary}"
        else:
            icon = "~"
            label = f"opaque      · manual refinement needed"

        click.echo(f"  {icon} {name.ljust(name_w)}  {label}")

    click.echo()
    click.echo("  " + "─" * 60)

    structured_str = f"{structured} structured"
    opaque_str = f"{opaque} opaque" if opaque else ""
    parts = [p for p in [structured_str, opaque_str] if p]
    click.echo(f"  {' · '.join(parts)}")
    click.echo()

    if opaque > 0:
        click.echo(
            f"  ~ {opaque} opaque metric{'s' if opaque > 1 else ''} exported with placeholder measures."
        )
        click.echo("    Review and replace expr: fields before running dbt compile.")
    else:
        click.echo("  ✓ All metrics structured — safe to run dbt compile.")
