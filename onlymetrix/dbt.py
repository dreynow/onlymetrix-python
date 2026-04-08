"""dbt manifest parser, profiles.yml reader, and sync logic for OnlyMetrix.

Reads a dbt manifest.json, extracts metric definitions, translates
MetricFlow types to SQL templates, and syncs to the OM API.

Also reads profiles.yml to auto-connect the warehouse.

Usage:
    from onlymetrix.dbt import parse_manifest, parse_profiles
    plan = parse_manifest("target/manifest.json")
    plan.dry_run()  # preview what would sync
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


# ---------------------------------------------------------------------------
# profiles.yml parsing (for omx dbt connect)
# ---------------------------------------------------------------------------

@dataclass
class DbtProfile:
    """Warehouse connection extracted from profiles.yml."""
    profile_name: str
    target_name: str
    ds_type: str  # postgres, snowflake, clickhouse, bigquery, redshift, databricks
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    # Snowflake-specific
    account: Optional[str] = None
    warehouse: Optional[str] = None
    role: Optional[str] = None
    # ClickHouse-specific
    secure: Optional[bool] = None
    # Connection string (for direct specification)
    connection_string: Optional[str] = None

    # Override name — set to 'default' for first datasource, or user-specified
    name_override: Optional[str] = None

    @property
    def datasource_name(self) -> str:
        return self.name_override or "default"

    def to_connect_payload(self) -> dict:
        """Convert to the JSON body for POST /v1/setup/connect-warehouse."""
        payload: dict[str, Any] = {
            "type": self.ds_type,
            "name": self.datasource_name,
        }
        for key in ("host", "port", "user", "password", "database", "schema",
                     "account", "warehouse", "role", "secure"):
            val = getattr(self, key)
            if val is not None:
                payload[key] = val
        return payload

    def display_summary(self) -> str:
        """Human-readable summary for confirmation prompt."""
        lines = [f"Profile: {self.profile_name} | Target: {self.target_name} | Type: {self.ds_type}"]
        if self.host:
            port_str = f":{self.port}" if self.port else ""
            lines.append(f"  Host: {self.host}{port_str}")
        if self.account:
            lines.append(f"  Account: {self.account}")
        if self.database:
            lines.append(f"  Database: {self.database}")
        if self.schema:
            lines.append(f"  Schema: {self.schema}")
        if self.warehouse:
            lines.append(f"  Warehouse: {self.warehouse}")
        if self.user:
            lines.append(f"  User: {self.user}")
        if self.password:
            lines.append(f"  Password: {'*' * min(len(self.password), 8)}")
        return "\n".join(lines)


def find_profiles(profiles_dir: Optional[str] = None, project_dir: Optional[str] = None) -> Path:
    """Locate the dbt profiles.yml file."""
    search = []
    if profiles_dir:
        search.append(Path(profiles_dir) / "profiles.yml")
    if project_dir:
        search.append(Path(project_dir) / "profiles.yml")
    search.append(Path("profiles.yml"))
    search.append(Path.home() / ".dbt" / "profiles.yml")

    for p in search:
        if p.exists():
            return p

    raise FileNotFoundError(
        "Could not find profiles.yml. "
        "Checked: ./profiles.yml, ~/.dbt/profiles.yml. "
        "Specify --profiles-dir PATH."
    )


def find_dbt_project(project_dir: Optional[str] = None) -> Optional[dict]:
    """Read dbt_project.yml for default profile name."""
    search = []
    if project_dir:
        search.append(Path(project_dir) / "dbt_project.yml")
    search.append(Path("dbt_project.yml"))

    for p in search:
        if p.exists():
            try:
                import yaml
            except ImportError:
                # Fall back to basic parsing if PyYAML not installed
                return _parse_yaml_basic(p)
            with open(p) as f:
                return yaml.safe_load(f)
    return None


def _parse_yaml_basic(path: Path) -> dict:
    """Minimal YAML parser for simple key: value files (no PyYAML dependency)."""
    result: dict[str, Any] = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if ":" in line and not line.startswith("#"):
                key, _, val = line.partition(":")
                val = val.strip().strip("'\"")
                if val:
                    result[key.strip()] = val
    return result


def _resolve_env_vars(value: Any) -> Any:
    """Resolve {{ env_var('NAME') }} and {{ env_var('NAME', 'default') }} in values."""
    if not isinstance(value, str):
        return value

    pattern = r"\{\{\s*env_var\(['\"](\w+)['\"](?:\s*,\s*['\"]([^'\"]*)['\"])?\)\s*\}\}"

    def replacer(match: re.Match) -> str:
        var_name = match.group(1)
        default = match.group(2)
        env_val = os.environ.get(var_name)
        if env_val is not None:
            return env_val
        if default is not None:
            return default
        raise EnvironmentError(
            f"Environment variable '{var_name}' not set "
            f"(referenced in profiles.yml). "
            f"Set it with: export {var_name}=..."
        )

    return re.sub(pattern, replacer, value)


def parse_profiles(
    profiles_path: str | Path,
    profile_name: Optional[str] = None,
    target_name: Optional[str] = None,
    project_dir: Optional[str] = None,
) -> DbtProfile:
    """Parse profiles.yml and extract warehouse connection details.

    Args:
        profiles_path: Path to profiles.yml
        profile_name: Override profile (default: read from dbt_project.yml)
        target_name: Override target (default: profile's default target)
        project_dir: dbt project directory (for finding dbt_project.yml)
    """
    try:
        import yaml
    except ImportError:
        raise ImportError(
            "PyYAML is required for profiles.yml parsing. "
            "Install it with: pip install pyyaml"
        )

    with open(profiles_path) as f:
        profiles = yaml.safe_load(f)

    if not profiles or not isinstance(profiles, dict):
        raise ValueError(f"Invalid profiles.yml: {profiles_path}")

    # Determine profile name
    if not profile_name:
        project = find_dbt_project(project_dir)
        if project:
            profile_name = project.get("profile")
        if not profile_name:
            # Use first profile
            profile_name = next(iter(profiles))

    if profile_name not in profiles:
        available = ", ".join(profiles.keys())
        raise ValueError(f"Profile '{profile_name}' not found in profiles.yml. Available: {available}")

    profile = profiles[profile_name]

    # Determine target
    if not target_name:
        target_name = profile.get("target", "dev")

    outputs = profile.get("outputs", {})
    if target_name not in outputs:
        available = ", ".join(outputs.keys())
        raise ValueError(f"Target '{target_name}' not found in profile '{profile_name}'. Available: {available}")

    target = outputs[target_name]

    # Resolve env vars in all string values
    resolved: dict[str, Any] = {}
    for k, v in target.items():
        resolved[k] = _resolve_env_vars(v)

    # Map dbt type to OM type
    dbt_type = resolved.get("type", "")
    type_map = {
        "postgres": "postgres",
        "snowflake": "snowflake",
        "clickhouse": "clickhouse",
        "bigquery": "bigquery",
        "redshift": "postgres",  # Redshift is Postgres-compatible
        "databricks": "databricks",
    }
    ds_type = type_map.get(dbt_type, dbt_type)

    return DbtProfile(
        profile_name=profile_name,
        target_name=target_name,
        ds_type=ds_type,
        host=resolved.get("host"),
        port=resolved.get("port"),
        user=resolved.get("user") or resolved.get("username"),
        password=resolved.get("password") or resolved.get("pass"),
        database=resolved.get("database") or resolved.get("dbname"),
        schema=resolved.get("schema"),
        account=resolved.get("account"),
        warehouse=resolved.get("warehouse"),
        role=resolved.get("role"),
        secure=resolved.get("secure"),
    )


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class OmxMeta:
    """OnlyMetrix-specific meta fields from dbt YAML."""
    tier: str = "standard"
    autoresearch: bool = False
    scorer: Optional[str] = None
    pii_columns: list[str] = field(default_factory=list)

    @staticmethod
    def from_dict(d: Optional[dict]) -> OmxMeta:
        if not d:
            return OmxMeta()
        return OmxMeta(
            tier=d.get("tier", "standard"),
            autoresearch=d.get("autoresearch", False),
            scorer=d.get("scorer"),
            pii_columns=d.get("pii_columns", []),
        )


@dataclass
class ParsedMetric:
    """A metric extracted from the dbt manifest."""
    name: str
    description: str
    sql_template: str
    tags: list[str] = field(default_factory=list)
    source_tables: list[str] = field(default_factory=list)
    time_column: Optional[str] = None
    metric_type: str = "simple"  # simple, ratio, derived
    omx_meta: OmxMeta = field(default_factory=OmxMeta)
    compile_hint: str = "structured"  # structured, opaque
    compile_note: Optional[str] = None
    # For ratio metrics: the component metric names
    component_metrics: list[ParsedMetric] = field(default_factory=list)

    def hash_key(self) -> str:
        """SHA256 hash for change detection."""
        payload = json.dumps({
            "name": self.name,
            "description": self.description,
            "sql_template": self.sql_template,
            "tags": sorted(self.tags),
            "source_tables": sorted(self.source_tables),
            "time_column": self.time_column,
            "tier": self.omx_meta.tier,
            "autoresearch": self.omx_meta.autoresearch,
        }, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()

    def to_api_payload(self) -> dict:
        """Convert to the JSON body for /v1/metrics/sync-dbt."""
        payload: dict[str, Any] = {
            "name": self.name,
            "description": self.description,
            "sql_template": self.sql_template,
            "tags": self.tags,
            "source_tables": self.source_tables,
        }
        if self.time_column:
            payload["time_column"] = self.time_column
        meta: dict[str, Any] = {}
        if self.omx_meta.tier != "standard":
            meta["tier"] = self.omx_meta.tier
        if self.omx_meta.autoresearch:
            meta["autoresearch"] = True
        if self.omx_meta.scorer:
            meta["scorer"] = self.omx_meta.scorer
        if self.omx_meta.pii_columns:
            meta["pii_columns"] = self.omx_meta.pii_columns
        if meta:
            payload["meta"] = meta
        return payload


# ---------------------------------------------------------------------------
# Manifest parsing
# ---------------------------------------------------------------------------

def find_manifest(manifest_path: Optional[str] = None, project_dir: Optional[str] = None) -> Path:
    """Locate the dbt manifest.json file."""
    if manifest_path:
        p = Path(manifest_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    search_dirs = []
    if project_dir:
        search_dirs.append(Path(project_dir) / "target" / "manifest.json")
    search_dirs.append(Path("target") / "manifest.json")

    for p in search_dirs:
        if p.exists():
            return p

    raise FileNotFoundError(
        "Could not find target/manifest.json. "
        "Run 'dbt run' first, or specify --manifest PATH."
    )


def parse_manifest(manifest_path: str | Path) -> list[ParsedMetric]:
    """Parse a dbt manifest.json and extract metrics.

    Handles both MetricFlow (dbt >= 1.6) and legacy metric formats.
    Returns a flat list of ParsedMetric objects, including component
    metrics split from ratio types.
    """
    with open(manifest_path) as f:
        manifest = json.load(f)

    metrics_block = manifest.get("metrics", {})
    nodes_block = manifest.get("nodes", {})
    sources_block = manifest.get("sources", {})
    semantic_models = manifest.get("semantic_models", {})

    # Build model → table name mapping from nodes
    model_tables = _build_model_table_map(nodes_block)
    # Build measure → (agg, expr, model) mapping from semantic models
    measure_map = _build_measure_map(semantic_models, model_tables)

    result: list[ParsedMetric] = []
    seen_names: set[str] = set()

    for key, metric_data in metrics_block.items():
        parsed = _parse_single_metric(metric_data, measure_map, model_tables)
        if parsed:
            # For ratio metrics, add component metrics first (deduplicate)
            for component in parsed.component_metrics:
                if component.name not in seen_names:
                    result.append(component)
                    seen_names.add(component.name)
            if parsed.name not in seen_names:
                result.append(parsed)
                seen_names.add(parsed.name)

    return result


def _build_model_table_map(nodes: dict) -> dict[str, str]:
    """Map dbt model unique_id and name → qualified table name."""
    mapping: dict[str, str] = {}
    for key, node in nodes.items():
        if node.get("resource_type") != "model":
            continue
        name = node.get("name", "")
        alias = node.get("alias") or name
        schema = node.get("schema", "public")
        table = f"{schema}.{alias}" if schema else alias
        mapping[key] = table
        mapping[name] = table
        # Also map ref-style: model.project.name
        mapping[f"ref('{name}')"] = table
    return mapping


def _build_measure_map(semantic_models: dict, model_tables: dict) -> dict[str, dict]:
    """Map measure name → {agg, expr, table, time_column, dimensions}."""
    mapping: dict[str, dict] = {}
    for key, sm in semantic_models.items():
        model_ref = sm.get("model", "")
        # Resolve model reference to table name
        table = _resolve_model_ref(model_ref, model_tables)

        # Extract time dimension
        time_col = None
        for dim in sm.get("dimensions", []):
            if dim.get("type") == "time":
                time_col = dim.get("name") or dim.get("expr")
                break

        for measure in sm.get("measures", []):
            measure_name = measure.get("name", "")
            mapping[measure_name] = {
                "agg": measure.get("agg", "sum"),
                "expr": measure.get("expr", measure_name),
                "table": table,
                "time_column": time_col,
                "description": measure.get("description", ""),
            }

    return mapping


def _resolve_model_ref(ref: str, model_tables: dict) -> str:
    """Resolve a dbt model reference to a table name."""
    # Handle ref('model_name') format
    if ref.startswith("ref("):
        inner = ref.strip("ref()'\"` ")
        return model_tables.get(inner, inner)
    # Direct table name or mapping key
    return model_tables.get(ref, ref)


def _parse_single_metric(
    data: dict,
    measure_map: dict,
    model_tables: dict,
) -> Optional[ParsedMetric]:
    """Parse a single metric entry from the manifest."""
    name = data.get("name", "")
    if not name:
        return None

    description = data.get("description", "") or data.get("label", "") or name
    tags = data.get("tags", [])
    meta = data.get("meta", {})
    omx_meta = OmxMeta.from_dict(meta.get("onlymetrix"))
    metric_type = data.get("type", "simple")
    type_params = data.get("type_params", {})
    filter_expr = data.get("filter")

    if metric_type == "simple":
        return _translate_simple(name, description, tags, type_params, measure_map, omx_meta, filter_expr)
    elif metric_type == "ratio":
        return _translate_ratio(name, description, tags, type_params, measure_map, omx_meta)
    elif metric_type == "derived":
        return _translate_derived(name, description, tags, type_params, omx_meta)
    else:
        # Legacy dbt metric format
        return _translate_legacy(data, model_tables, omx_meta)


def _translate_simple(
    name: str, description: str, tags: list[str],
    type_params: dict, measure_map: dict, omx_meta: OmxMeta,
    filter_expr: Optional[str],
) -> ParsedMetric:
    """Translate a simple MetricFlow metric to SQL."""
    measure_ref = type_params.get("measure", {})
    if isinstance(measure_ref, str):
        measure_name = measure_ref
    else:
        measure_name = measure_ref.get("name", "")

    measure_info = measure_map.get(measure_name, {})
    agg = measure_info.get("agg", "sum").upper()
    expr = measure_info.get("expr", measure_name)
    table = measure_info.get("table", "UNKNOWN_TABLE")
    time_col = measure_info.get("time_column")

    if agg == "COUNT_DISTINCT":
        sql = f"SELECT COUNT(DISTINCT {expr}) AS {name} FROM {table}"
    elif agg == "SUM_BOOLEAN":
        sql = f"SELECT SUM(CAST({expr} AS INT)) AS {name} FROM {table}"
    else:
        agg_map = {
            "SUM": "SUM", "COUNT": "COUNT", "AVG": "AVG",
            "AVERAGE": "AVG",   # dbt uses "average"; .upper() → "AVERAGE" needs mapping
            "MIN": "MIN", "MAX": "MAX",
        }
        sql_agg = agg_map.get(agg, agg)
        sql = f"SELECT {sql_agg}({expr}) AS {name} FROM {table}"

    if filter_expr:
        sql += f" WHERE {filter_expr}"

    source_tables = [table] if table != "UNKNOWN_TABLE" else []

    return ParsedMetric(
        name=name,
        description=description,
        sql_template=sql,
        tags=tags,
        source_tables=source_tables,
        time_column=time_col,
        metric_type="simple",
        omx_meta=omx_meta,
        compile_hint="structured",
    )


def _translate_ratio(
    name: str, description: str, tags: list[str],
    type_params: dict, measure_map: dict, omx_meta: OmxMeta,
) -> ParsedMetric:
    """Translate a ratio metric. Flags as Opaque, splits into components."""
    numerator = type_params.get("numerator", {})
    denominator = type_params.get("denominator", {})
    num_name = numerator.get("name", "") if isinstance(numerator, dict) else numerator
    den_name = denominator.get("name", "") if isinstance(denominator, dict) else denominator

    num_info = measure_map.get(num_name, {})
    den_info = measure_map.get(den_name, {})

    if not num_info or not den_info:
        missing = []
        if not num_info:
            missing.append(f"numerator '{num_name}'")
        if not den_info:
            missing.append(f"denominator '{den_name}'")
        return ParsedMetric(
            name=name,
            description=description,
            sql_template=f"-- ratio metric: missing measures ({', '.join(missing)})",
            tags=tags,
            metric_type="ratio",
            omx_meta=omx_meta,
            compile_hint="opaque",
            compile_note=f"ratio — missing measures: {', '.join(missing)}",
        )

    # Build component metrics
    components: list[ParsedMetric] = []
    for comp_name, comp_info in [(num_name, num_info), (den_name, den_info)]:
        if comp_info:
            agg = comp_info.get("agg", "sum").upper()
            expr = comp_info.get("expr", comp_name)
            table = comp_info.get("table", "UNKNOWN_TABLE")
            sql = f"SELECT {agg}({expr}) AS {comp_name} FROM {table}"
            components.append(ParsedMetric(
                name=comp_name,
                description=comp_info.get("description", f"Component of {name}"),
                sql_template=sql,
                tags=tags,
                source_tables=[table] if table != "UNKNOWN_TABLE" else [],
                time_column=comp_info.get("time_column"),
                metric_type="simple",
                omx_meta=OmxMeta(tier=omx_meta.tier),
                compile_hint="structured",
            ))

    # The ratio itself is Opaque
    num_table = num_info.get("table", "UNKNOWN_TABLE")
    den_table = den_info.get("table", num_table)
    num_expr = num_info.get("expr", num_name)
    den_expr = den_info.get("expr", den_name)
    num_agg = num_info.get("agg", "sum").upper()
    den_agg = den_info.get("agg", "sum").upper()

    if num_table == den_table:
        sql = (
            f"SELECT {num_agg}({num_expr})::FLOAT / NULLIF({den_agg}({den_expr}), 0) "
            f"AS {name} FROM {num_table}"
        )
    else:
        sql = (
            f"SELECT num.v::FLOAT / NULLIF(den.v, 0) AS {name} "
            f"FROM (SELECT {num_agg}({num_expr}) AS v FROM {num_table}) num, "
            f"(SELECT {den_agg}({den_expr}) AS v FROM {den_table}) den"
        )

    return ParsedMetric(
        name=name,
        description=description,
        sql_template=sql,
        tags=tags,
        source_tables=list({num_table, den_table} - {"UNKNOWN_TABLE"}),
        metric_type="ratio",
        omx_meta=omx_meta,
        compile_hint="opaque",
        compile_note=f"ratio -> splits into {num_name} + {den_name}",
        component_metrics=components,
    )


def _translate_derived(
    name: str, description: str, tags: list[str],
    type_params: dict, omx_meta: OmxMeta,
) -> ParsedMetric:
    """Translate a derived metric. Always Opaque."""
    expr = type_params.get("expr", name)
    # Derived metrics reference other metrics by name
    # We can't resolve them to SQL without the full metric graph
    sql = f"-- derived: {expr}"

    return ParsedMetric(
        name=name,
        description=description,
        sql_template=sql,
        tags=tags,
        metric_type="derived",
        omx_meta=omx_meta,
        compile_hint="opaque",
        compile_note="derived metric",
    )


def _translate_legacy(
    data: dict, model_tables: dict, omx_meta: OmxMeta,
) -> ParsedMetric:
    """Translate legacy dbt metric format (< 1.6)."""
    name = data.get("name", "")
    description = data.get("description", "") or name
    metric_type = data.get("type", "count")
    sql_expr = data.get("sql", "*")
    model_ref = data.get("model", "")
    table = _resolve_model_ref(model_ref, model_tables) if model_ref else "UNKNOWN_TABLE"
    tags = data.get("tags", [])
    time_col = data.get("timestamp")
    filter_data = data.get("filters", [])

    agg_map = {
        "count": "COUNT", "sum": "SUM", "average": "AVG", "avg": "AVG",
        "min": "MIN", "max": "MAX", "count_distinct": "COUNT(DISTINCT",
    }
    agg = agg_map.get(metric_type.lower(), "SUM")

    if metric_type.lower() == "count_distinct":
        sql = f"SELECT COUNT(DISTINCT {sql_expr}) AS {name} FROM {table}"
    elif metric_type.lower() == "count":
        sql = f"SELECT COUNT({sql_expr}) AS {name} FROM {table}"
    else:
        sql = f"SELECT {agg}({sql_expr}) AS {name} FROM {table}"

    # Apply filters
    if filter_data:
        where_clauses = []
        for f in filter_data:
            col = f.get("field", "")
            op = f.get("operator", "=")
            val = f.get("value", "")
            if isinstance(val, str):
                where_clauses.append(f"{col} {op} '{val}'")
            else:
                where_clauses.append(f"{col} {op} {val}")
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

    source_tables = [table] if table != "UNKNOWN_TABLE" else []

    return ParsedMetric(
        name=name,
        description=description,
        sql_template=sql,
        tags=tags,
        source_tables=source_tables,
        time_column=time_col,
        metric_type="simple",
        omx_meta=omx_meta,
        compile_hint="structured",
    )


# ---------------------------------------------------------------------------
# Sync plan — dry-run and change detection
# ---------------------------------------------------------------------------

SYNC_STATE_FILE = ".onlymetrix_sync_state.json"


def load_sync_state(target_dir: str | Path) -> dict[str, str]:
    """Load previous sync hashes from state file."""
    state_path = Path(target_dir) / SYNC_STATE_FILE
    if state_path.exists():
        with open(state_path) as f:
            return json.load(f)
    return {}


def save_sync_state(target_dir: str | Path, state: dict[str, str]) -> None:
    """Save sync hashes to state file."""
    state_path = Path(target_dir) / SYNC_STATE_FILE
    with open(state_path, "w") as f:
        json.dump(state, f, indent=2)


@dataclass
class SyncAction:
    """What to do with a single metric."""
    metric: ParsedMetric
    action: str  # create, update, unchanged, skip
    note: Optional[str] = None


def compute_sync_plan(
    metrics: list[ParsedMetric],
    prev_state: dict[str, str],
) -> list[SyncAction]:
    """Compare metrics against previous sync state to determine actions."""
    actions: list[SyncAction] = []
    current_names: set[str] = set()

    for m in metrics:
        current_names.add(m.name)
        current_hash = m.hash_key()
        prev_hash = prev_state.get(m.name)

        if prev_hash is None:
            action = "create"
        elif prev_hash != current_hash:
            action = "update"
        else:
            action = "unchanged"

        note = m.compile_note
        actions.append(SyncAction(metric=m, action=action, note=note))

    # Detect metrics deleted from manifest
    for name in prev_state:
        if name not in current_names:
            actions.append(SyncAction(
                metric=ParsedMetric(name=name, description="", sql_template=""),
                action="delete",
                note="removed from dbt manifest",
            ))

    return actions


def format_dry_run(actions: list[SyncAction]) -> str:
    """Format sync plan as human-readable dry-run output."""
    lines = []
    max_name = max((len(a.metric.name) for a in actions), default=20)

    creates = sum(1 for a in actions if a.action == "create")
    updates = sum(1 for a in actions if a.action == "update")
    unchanged = sum(1 for a in actions if a.action == "unchanged")
    deletes = sum(1 for a in actions if a.action == "delete")
    structured = sum(1 for a in actions if a.metric.compile_hint == "structured" and a.action in ("create", "update"))
    opaque = sum(1 for a in actions if a.metric.compile_hint == "opaque" and a.action in ("create", "update"))
    tier_updates = sum(1 for a in actions if a.metric.omx_meta.tier != "standard" and a.action in ("create", "update"))

    lines.append(f"Found {len(actions)} metrics in manifest\n")

    for a in actions:
        m = a.metric
        name_pad = m.name.ljust(max_name)
        hint = m.compile_hint
        tier_str = f"  (tier: {m.omx_meta.tier})" if m.omx_meta.tier != "standard" else ""

        action_parts = [a.action]
        if a.note:
            action_parts.append(a.note)
        action_str = ", ".join(action_parts)

        lines.append(f"  {name_pad} -> {hint:<12}{tier_str:20} [{action_str}]")

    lines.append("")
    to_sync = creates + updates
    lines.append(f"Would sync: {to_sync} metric{'s' if to_sync != 1 else ''} ({structured} structured, {opaque} opaque), skip {unchanged} unchanged")
    if deletes:
        lines.append(f"Would delete: {deletes} metric{'s' if deletes != 1 else ''} (removed from manifest)")
    if tier_updates:
        lines.append(f"Would update: {tier_updates} tier{'s' if tier_updates != 1 else ''}")

    return "\n".join(lines)
