"""IR → MetricFlow YAML mapper.

Converts the OM compiler IR (from /v1/compiler/status) into a valid
dbt MetricFlow semantic_models + metrics YAML file.

Design decisions:
  - SQL is source of truth. YAML is a compile artifact. DO NOT EDIT directly.
  - Opaque metrics get a stub semantic model with om_opaque: true — valid YAML,
    clearly marked for manual refinement.
  - model ref defaults to ref('metric_name') — the mart model convention.
  - Entity expressions inferred from join table names ('{table}_id' convention).
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VERSION = "0.4.1"

_AGG_MAP: dict[str, str] = {
    "Sum":            "sum",
    "Count":          "count",
    "Avg":            "average",
    "Average":        "average",
    "Min":            "min",
    "Max":            "max",
    "CountDistinct":  "count_distinct",
    "Median":         "median",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_dim_type(type_str: str) -> str:
    """Parse IR dimension type string → MetricFlow type.

    IR format examples:
      "Categorical { values: [] }"
      "Time { grain: Day }"
      "categorical"
      "time"
    """
    t = type_str.strip().lower()
    if t.startswith("time"):
        return "time"
    return "categorical"


def _to_label(name: str) -> str:
    """Convert snake_case → Title Case label."""
    return name.replace("_", " ").title()


def _snake(name: str) -> str:
    """Ensure name is snake_case (lowercase)."""
    return re.sub(r"[^a-z0-9_]", "_", name.lower()).strip("_")


# ---------------------------------------------------------------------------
# Semantic model builder
# ---------------------------------------------------------------------------


def _get_model_ref(name: str, catalog_meta: dict[str, Any] | None) -> str:
    """Resolve the dbt model ref for a metric.

    Uses source_tables from catalog metadata (populated at dbt sync time).
    Strips schema prefix: "default.stg_orders" → ref('stg_orders').
    Falls back to a loud placeholder that fails dbt compile visibly.
    """
    if catalog_meta:
        meta = catalog_meta.get(name, {})
        source_tables = meta.get("source_tables", [])
        if source_tables:
            # Strip schema/database prefix: "default.stg_orders" → "stg_orders"
            model_name = source_tables[0].split(".")[-1]
            return f"ref('{model_name}')"
    # No resolution — emit placeholder that fails dbt compile loudly
    # (silent wrong ref is worse than a clear error)
    return f"ref('__UNKNOWN__{name}__')"


def build_semantic_model(metric: dict[str, Any], catalog_meta: dict[str, Any] | None = None) -> dict[str, Any]:
    """Map one IR metric → MetricFlow semantic_model block."""
    name = metric["name"]

    sm: dict[str, Any] = {
        "name": name,
        "description": metric.get("semantic", {}).get("description", ""),
        "model": _get_model_ref(name, catalog_meta),
    }

    # Entities — inferred from join graph
    entities: list[dict[str, Any]] = []
    seen_entities: set[str] = set()

    joins = metric.get("joins", [])
    if joins:
        # First join source is treated as the primary table
        first_from = joins[0]["from"]
        primary_entity = _snake(first_from)
        if primary_entity not in seen_entities:
            entities.append({
                "name": primary_entity,
                "type": "primary",
                "expr": f"{primary_entity}_id",
            })
            seen_entities.add(primary_entity)

        for join in joins:
            to_entity = _snake(join["to"])
            if to_entity not in seen_entities:
                entities.append({
                    "name": to_entity,
                    "type": "foreign",
                    "expr": f"{to_entity}_id",
                })
                seen_entities.add(to_entity)

    # Time column from catalog — used for agg_time_dimension on all measures
    time_col: str = ""
    if catalog_meta:
        time_col = catalog_meta.get(name, {}).get("time_column", "") or ""

    # Measures
    measures_out: list[dict[str, Any]] = []
    for m in metric.get("measures", []):
        agg = _AGG_MAP.get(m["function"], m["function"].lower())
        measure: dict[str, Any] = {
            "name": m["alias"],
            "agg": agg,
            "expr": m.get("source_expr") or m["alias"],
        }
        if time_col:
            measure["agg_time_dimension"] = time_col
        measures_out.append(measure)

    if not measures_out and metric.get("kind") == "opaque":
        # Opaque fallback — generate a placeholder count measure
        measures_out.append({
            "name": f"{_snake(name)}_count",
            "agg": "count",
            "expr": "1",                  # user must replace with real expr
            "meta": {"om_opaque_placeholder": True},
        })

    sm["measures"] = measures_out

    # Dimensions — from IR, plus time dimension from catalog if present
    dims_out: list[dict[str, Any]] = []
    # Add time dimension first if we have one (required by MetricFlow for agg_time_dimension)
    if time_col:
        # Only add if not already present from the IR dimensions
        ir_dim_names = {d.get("name") for d in metric.get("dimensions", [])}
        if time_col not in ir_dim_names:
            dims_out.append({
                "name": time_col,
                "type": "time",
                "type_params": {"time_granularity": "day"},
            })
    for d in metric.get("dimensions", []):
        dim_type = _parse_dim_type(d["type"])
        dim: dict[str, Any] = {
            "name": d["name"],
            "type": dim_type,
        }
        if dim_type == "time":
            dim["type_params"] = {"time_granularity": "day"}
        dims_out.append(dim)

    sm["dimensions"] = dims_out

    # MetricFlow requires a primary entity when dimensions are defined.
    # If there were no joins (entities list is still empty) but we have dims,
    # synthesise one. We use the metric name as the entity name so that each
    # generated semantic model has a globally-unique (entity, dimension) pair —
    # a MetricFlow invariant that breaks if multiple SMs share the same entity
    # name (e.g. "order") and dimension (e.g. "created_at"). Using the metric
    # name sidesteps the collision. The expr falls back to 'id'; override via
    # catalog_meta['primary_key_column'].
    if dims_out and not entities:
        pk_col = ""
        if catalog_meta:
            pk_col = catalog_meta.get(name, {}).get("primary_key_column", "") or ""
        sm["entities"] = [{
            "name": _snake(name) + "_id",
            "type": "primary",
            "expr": pk_col or "id",
        }]
    elif entities:
        sm["entities"] = entities

    return sm


# ---------------------------------------------------------------------------
# Metric builder
# ---------------------------------------------------------------------------


def build_metric(metric: dict[str, Any], sm: dict[str, Any]) -> dict[str, Any]:
    """Map one IR metric → MetricFlow metric block."""
    name = metric["name"]
    semantic = metric.get("semantic", {})
    measures = sm.get("measures", [])
    joins = metric.get("joins", [])
    is_opaque = metric.get("kind") == "opaque"

    m: dict[str, Any] = {
        "name": name,
        "label": _to_label(name),
        "description": semantic.get("description", ""),
    }

    # Type + type_params
    if is_opaque:
        # Opaque → derived with placeholder expr
        m["type"] = "derived"
        m["type_params"] = {
            "expr": f"{{{{ metric('{name}') }}}}",  # placeholder — user must fill
            "metrics": [{"name": name}],
        }
    elif len(measures) == 1:
        fanout = any(j.get("fanout_risk") for j in joins)
        if fanout:
            # Ratio pattern when fan-out is detected — numerator/denominator split
            m["type"] = "ratio"
            m["type_params"] = {
                "numerator": {"name": measures[0]["name"]},
                "denominator": {"name": measures[0]["name"]},  # user must refine
            }
        else:
            m["type"] = "simple"
            m["type_params"] = {"measure": measures[0]["name"]}
    else:
        # Multiple measures → derived expression referencing each measure by name.
        # measure() is the correct MetricFlow function here — metric() would be a
        # self-referential loop. The additive default is a reasonable starting point;
        # non-additive combinations (ratio, weighted avg, etc.) need human review.
        m["type"] = "derived"
        expr = " + ".join(f"measure('{me['name']}')" for me in measures)
        # NOTE: the "metrics" key in type_params is for cross-metric dependencies only
        # (e.g. derived metric referencing metric('revenue') + metric('cost')).
        # For same-model multi-measure combinations using measure(), omit it entirely —
        # MetricFlow resolves measure() calls against the parent semantic model.
        m["type_params"] = {
            "expr": expr,
        }
        if len(measures) > 2:
            m["meta"]["om_needs_review"] = True
            m["meta"]["om_review_reason"] = (
                f"Derived metric with {len(measures)} measures — "
                "verify the expr combines them correctly before dbt compile"
            )

    # OM meta — full IR round-tripped into the YAML meta block
    taxonomy = semantic.get("taxonomy_path", [])
    m["meta"] = {
        "om_taxonomy":          " › ".join(taxonomy) if taxonomy else "",
        "om_importance":        semantic.get("importance", 5),
        "om_is_primary":        semantic.get("is_primary", False),
        "om_tags":              semantic.get("tags", []),
        "om_compiler_version":  metric.get("provenance", {}).get("compiler_version", VERSION),
        "om_source_format":     metric.get("provenance", {}).get("source_format", ""),
        "om_generated":         True,
        # om_generated_at intentionally omitted — timestamp belongs in file header
        # comment only. Including it in metric meta causes git churn on every export
        # run even when nothing changed.
    }
    if is_opaque:
        m["meta"]["om_opaque"] = True

    return m


# ---------------------------------------------------------------------------
# YAML serialiser (no PyYAML dep)
# ---------------------------------------------------------------------------

_DBT_EXPR_PREFIXES = ("ref(", "source(", "metric(", "measure(")


def _yaml_str(s: str, indent: int) -> str:
    """Serialise a string. Use block scalar for multi-line, quotes if needed.

    dbt Jinja expressions (ref(), source(), metric(), measure()) are emitted
    bare — never wrapped in quotes — so dbt can evaluate them at compile time.
    """
    if not s:
        return '""'
    if "\n" in s:
        pad = " " * (indent + 2)
        lines = s.replace("\r\n", "\n").split("\n")
        body = ("\n" + pad).join(lines)
        return f"|\n{pad}{body}"
    # Passthrough for dbt Jinja expressions — must not be quoted
    if any(s.startswith(prefix) for prefix in _DBT_EXPR_PREFIXES):
        return s
    # Quote if contains special chars
    needs_quote = any(c in s for c in (": ", "# ", "{", "}", "[", "]", ",", "&", "*", "?", "|", "-", "<", ">", "=", "!", "'", '"', "`", "@", "\\"))
    if needs_quote or s[0] in ('"', "'", " ") or s.strip() != s:
        escaped = s.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return s


def _yaml_val(v: Any, indent: int) -> str:
    """Serialise a scalar value."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        return _yaml_str(v, indent)
    return repr(v)


def _yaml_block(obj: Any, indent: int = 0) -> str:
    """Recursively serialise a dict/list/scalar to YAML block style."""
    pad = " " * indent

    if isinstance(obj, dict):
        if not obj:
            return "{}"
        lines = []
        for k, v in obj.items():
            key = str(k)
            if isinstance(v, dict):
                if v:
                    lines.append(f"{pad}{key}:")
                    lines.append(_yaml_block(v, indent + 2))
                else:
                    lines.append(f"{pad}{key}: {{}}")
            elif isinstance(v, list):
                if not v:
                    lines.append(f"{pad}{key}: []")
                elif all(not isinstance(i, (dict, list)) for i in v):
                    # Inline list for simple values
                    items = ", ".join(_yaml_val(i, indent) for i in v)
                    lines.append(f"{pad}{key}: [{items}]")
                else:
                    lines.append(f"{pad}{key}:")
                    lines.append(_yaml_block(v, indent + 2))
            else:
                lines.append(f"{pad}{key}: {_yaml_val(v, indent)}")
        return "\n".join(lines)

    if isinstance(obj, list):
        if not obj:
            return f"{pad}[]"
        lines = []
        for item in obj:
            if isinstance(item, dict):
                # First key on the dash line, rest indented
                keys = list(item.items())
                first_k, first_v = keys[0]
                rest = dict(keys[1:])
                if isinstance(first_v, (dict, list)):
                    lines.append(f"{pad}- {first_k}:")
                    lines.append(_yaml_block(first_v, indent + 4))
                else:
                    lines.append(f"{pad}- {first_k}: {_yaml_val(first_v, indent + 2)}")
                if rest:
                    lines.append(_yaml_block(rest, indent + 2))
            else:
                lines.append(f"{pad}- {_yaml_val(item, indent)}")
        return "\n".join(lines)

    return f"{pad}{_yaml_val(obj, indent)}"


# ---------------------------------------------------------------------------
# Main export function
# ---------------------------------------------------------------------------

HEADER_TEMPLATE = """\
# ============================================================
# Generated by OM Compiler v{version}
# Generated at: {timestamp}
# Source: {source}
#
# DO NOT EDIT THIS FILE MANUALLY
# Changes will be overwritten on next: omx export --format metricflow
#
# To change metric computation: edit the mart SQL, then run:
#   omx sync && omx export --format metricflow
#
# To add metadata (descriptions, owners): run:
#   omx metric edit <metric_name>
# ============================================================

"""

DEFAULT_OUTPUT_PATH = "models/marts/om_generated_metrics.yml"


def export_metricflow(
    ir_metrics: list[dict[str, Any]],
    output_path: str | None = None,
    dry_run: bool = False,
    source: str = "omx sync",
    catalog_meta: dict[str, Any] | None = None,
) -> tuple[str, str, int, int]:
    """Convert IR metrics list → MetricFlow YAML.

    Returns (output_path_used, yaml_content, structured_count, opaque_count).
    If dry_run=True, nothing is written to disk.
    catalog_meta: dict of name → {source, source_tables} from /v1/setup/metrics.
    """
    output_path = output_path or DEFAULT_OUTPUT_PATH

    semantic_models: list[dict] = []
    metrics_out: list[dict] = []

    structured = 0
    opaque = 0

    for metric in ir_metrics:
        sm = build_semantic_model(metric, catalog_meta)
        m = build_metric(metric, sm)
        semantic_models.append(sm)
        metrics_out.append(m)
        if metric.get("kind") == "opaque":
            opaque += 1
        else:
            structured += 1

    header = HEADER_TEMPLATE.format(
        version=VERSION,
        timestamp=datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        source=source,
    )

    body = "semantic_models:\n"
    for sm in semantic_models:
        body += _yaml_block([sm], indent=2) + "\n\n"

    body += "metrics:\n"
    for m in metrics_out:
        body += _yaml_block([m], indent=2) + "\n\n"

    content = header + body.rstrip() + "\n"

    if not dry_run:
        import os
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write(content)

    return output_path, content, structured, opaque
