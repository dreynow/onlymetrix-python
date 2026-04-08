# OnlyMetrix Python SDK

Python client and CLI for [OnlyMetrix](https://onlymetrix.com) — a governed metric layer for AI agents and data teams.

```bash
pip install onlymetrix
```

Requires Python 3.9+. See [CHANGELOG](CHANGELOG.md) for version history.

---

## What it does

OnlyMetrix sits between your warehouse and anything that queries it — agents, dashboards, notebooks. You define metrics once, and everything downstream queries through the governed layer: no raw SQL, PII masked, every query audited.

The SDK gives you:

- **Python client** — query metrics, run structured analysis, manage setup
- **CLI** (`omx`) — everything the client does, plus CI-friendly commands
- **dbt integration** — sync MetricFlow metrics from dbt into OnlyMetrix
- **MetricFlow export** — compile the OM IR back to dbt-compatible YAML
- **Agent integrations** — LangChain and CrewAI tool bindings

---

## Quick start

```python
from onlymetrix import OnlyMetrix

om = OnlyMetrix("https://api.onlymetrix.com", api_key="omx_sk_...")

# Query a metric
result = om.metrics.query("total_revenue", filters={"time_start": "2025-01-01"})
print(f"Revenue: ${result.rows[0]['revenue_usd']:,.2f}")

# Search metrics by name or intent
metrics = om.metrics.list(search="churn")

# Describe a table (PII columns flagged)
desc = om.tables.describe("customers")
for col in desc.columns:
    print(f"  {col.name} ({col.type}){' [PII]' if col.is_pii else ''}")
```

Environment variables: `OMX_API_URL` (default `http://localhost:8080`), `OMX_API_KEY`.

---

## SQL-to-Semantic-Layer converter

Convert raw SQL queries into governed metric definitions — no manual YAML writing.

```python
from onlymetrix import convert_sql

metric = convert_sql(
    "SELECT SUM(amount) FROM orders WHERE status = 'paid' AND created_at >= '2025-01-01'",
    name="total_revenue",
    description="Total paid revenue in USD",
)
# → {"name": "total_revenue", "sql": "...", "source_tables": ["orders"],
#    "tags": ["finance", "aggregate"], "time_column": "created_at",
#    "filters": [{"name": "status", "type": "string"}, ...]}

om.setup.import_metrics([metric])
```

Batch convert a directory of `.sql` files:

```bash
omx sql convert-batch ./queries/ --format yaml --output metrics.yaml
omx sql convert-batch ./queries/ --import   # convert + push to server
```

Inspect what gets extracted before importing:

```bash
omx sql inspect "SELECT country, SUM(amount) FROM orders GROUP BY country"
#   Name:         sum_amount
#   Tables:       orders
#   Aggregations: 1
#     - SUM(amount) AS amount
#   Dimensions:   country
#   Time column:  (not detected)
#   Tags:         aggregate, finance
```

Install with SQL support: `pip install onlymetrix[sql]`

---

## dbt integration

### 1. Connect your warehouse

OnlyMetrix reads your existing `profiles.yml` — no credentials to re-enter.

```bash
omx dbt connect                    # reads ~/.dbt/profiles.yml
omx dbt connect --profiles-dir .   # project-local profiles
omx dbt connect --dry-run          # preview without calling the API
```

### 2. Sync metrics

Reads `target/manifest.json` (produced by `dbt compile`), translates MetricFlow definitions to SQL, and pushes them to the OM compiler.

```bash
dbt compile
omx dbt sync
omx dbt sync --dry-run             # preview what would sync
omx dbt sync --strict              # exit non-zero if any metric is opaque or failed
```

What sync does:
- Parses MetricFlow `simple`, `ratio`, and `derived` metric types
- Translates aggregations (sum, count, average, min, max, count_distinct) to SQL
- Skips metrics unchanged since last sync (SHA256 hash)
- Triggers OM compiler after each batch

### 3. Validate

Check the compiled IR for MetricFlow structural correctness before exporting.

```bash
omx validate --format metricflow            # human output, exit 2 if warnings
omx validate --format metricflow --strict   # exit 2 on warnings (CI gate)
omx validate --format metricflow --strict --output json   # machine-readable
```

Exit codes: `0` = clean, `1` = hard errors, `2` = warnings (opaque metrics need refinement).

JSON output (for CI pipelines):
```json
{
  "passed": true,
  "errors": 0,
  "warnings": 0,
  "metrics_checked": 12,
  "issues": []
}
```

### 4. Export to MetricFlow YAML

Compile the OM IR back to a dbt-compatible `semantic_models` + `metrics` YAML file.

```bash
omx export --format metricflow
omx export --format metricflow --output models/marts/om_generated_metrics.yml
omx export --format metricflow --dry-run          # print YAML, write nothing
omx export --format metricflow --all-sources      # include non-dbt metrics
```

The generated file:
- Uses `ref('model_name')` — bare Jinja, not a string literal
- Sets `agg_time_dimension` on every measure (MetricFlow 1.11+ requirement)
- Adds a primary entity to each semantic model (required when dimensions are defined)
- Emits source columns as measure `expr` (e.g. `total_amount`), not output aliases
- Omits `om_generated_at` from metric meta so re-runs don't produce git noise
- Filters to dbt-sourced metrics by default; `--all-sources` to include all

Commit the output and run `dbt compile` to verify.

### Full pipeline

```bash
dbt compile
omx dbt sync
omx validate --format metricflow --strict
omx export --format metricflow --output models/marts/om_generated_metrics.yml
dbt compile   # verify the generated YAML is valid MetricFlow
```

See `tutorials/dbt-metricflow-export/` for a runnable end-to-end example against a live ClickHouse warehouse.

---

## Analysis

Structured reasoning primitives that return machine-parseable results — designed for agents to chain and explain.

```python
# Why did revenue change?
om.analysis.root_cause(
    "quarterly_revenue",
    compare={"current": "2025-02", "previous": "2025-01"},
    dimensions=["country", "tier", "product"],
)
# → {primary_dimension: "country", driver: "Germany", contribution: 0.72,
#    explanation: "Germany accounts for 72% of the decline",
#    suggested_actions: ["Investigate DACH expansion strategy"]}

# Concentration risk
om.analysis.sensitivity("revenue", "country", scenario="remove_top_3")
# → {impact_pct: 94, risk: "critical", herfindahl_index: 0.829}

# Anomaly detection
om.analysis.anomalies("order_count", "region")
# → {anomalous_segments: [{"region": "APAC", "z_score": 3.1}], ...}
```

Every method returns the same envelope:

```python
{
    "value": {...},              # structured finding
    "explanation": "...",        # plain English, one sentence
    "confidence": 0.85,
    "warnings": [...],           # data quality issues
    "suggested_actions": [...],
}
```

| Method | What it answers |
|--------|----------------|
| `root_cause(metric, compare, dimensions)` | Why did this metric change? |
| `correlate(metric_a, metric_b)` | Are these two populations related? |
| `threshold(metric)` | What's the optimal cutoff? |
| `sensitivity(metric, dimension, scenario)` | What's our concentration risk? |
| `segment_performance(metric, segments)` | How does this metric perform across segments? |
| `contribution(metric, compare, dimension)` | What drove the change between periods? |
| `drivers(metric, dimensions)` | Which dimension explains variance most? |
| `anomalies(metric, dimension)` | Which segments are behaving abnormally? |
| `pareto(metric)` | What's the precision-recall frontier? |
| `trends(metric)` | Is this accelerating or decelerating? |
| `forecast(metric, periods_ahead)` | Where is this heading? |
| `compare(metric, filter_a, filter_b)` | How do these two groups differ? |
| `health(metric)` | Can I trust this data? |

### Custom analysis

Compose primitives into reusable, governed workflows:

```python
@om.analysis.custom("store_risk")
def store_risk(ctx, dimension="region"):
    sensitivity = ctx.sensitivity(dimension=dimension, scenario="remove_top_3")
    drivers = ctx.drivers(dimensions=[dimension])
    return {
        "risk": sensitivity["value"]["risk"],
        "top_driver_cv": drivers["dimensions"][0]["coefficient_of_variation"],
    }

# Export as a JSON DAG (auditable, shareable)
om.analysis.export_dag("store_risk", save_to_server=True)

# Run from any session
result = om.analysis.run_custom("store_risk", metric="revenue")
```

Custom analyses can only call OM primitives — no raw SQL. Each execution runs a health check first.

---

## Agent integrations

### LangChain

```python
from onlymetrix.integrations.langchain import onlymetrix_tools

tools = onlymetrix_tools("https://api.onlymetrix.com", api_key="omx_sk_...")
# → [search_metrics, query_metric, request_metric]
```

### CrewAI

```python
from onlymetrix.integrations.crewai import onlymetrix_tools

tools = onlymetrix_tools("https://api.onlymetrix.com", api_key="omx_sk_...")
```

---

## Async client

```python
from onlymetrix import AsyncOnlyMetrix

async with AsyncOnlyMetrix("https://api.onlymetrix.com", api_key="...") as om:
    metrics = await om.metrics.list(search="revenue")
    result = await om.metrics.query("total_revenue")
```

---

## CLI reference

```bash
# Metrics
omx metrics list [--search revenue] [--tag finance]
omx metrics query total_revenue [--filter time_start=2025-01-01] [--dimension country]
omx metrics create --name churn_risk --sql "..." --description "..."
omx metrics delete churn_risk

# Tables
omx tables list
omx tables describe customers

# dbt integration
omx dbt connect [--profiles-dir .] [--dry-run]
omx dbt sync [--manifest path/to/manifest.json] [--dry-run] [--strict]

# Validation + export
omx validate --format metricflow [--strict] [--output json]
omx export --format metricflow [--output path/to/metrics.yml] [--dry-run] [--all-sources]

# Analysis
omx analysis root-cause quarterly_revenue --current 2025-02 --previous 2025-01 --dimension country
omx analysis sensitivity revenue --dimension country --scenario remove_top_3
omx analysis run-custom store_risk --metric revenue
omx analysis list-custom
omx analysis export store_risk
omx analysis load store_risk

# Reliability
omx reliability check [--json]
omx reliability trace --metric total_revenue [--json]
omx reliability watch --metric total_revenue [--interval 60]
omx reliability affected-by --table orders [--json]

# Setup
omx setup status
omx setup connect-warehouse --type postgres --host db.example.com --database analytics --user readonly --password ...
omx compiler status
omx health
```

---

## Python API reference

| Resource | Key methods |
|----------|------------|
| `om.metrics` | `list(tag, search)`, `query(name, filters, dimension, limit)`, `get(name)` |
| `om.tables` | `list()`, `describe(table)` |
| `om.analysis` | 13 primitives + `run_custom()`, `export_dag()`, `load_from_server()` |
| `om.setup` | `connect_warehouse()`, `configure_access()`, `status()`, `create_metric()`, `delete_metric()`, `import_metrics()`, `dbt_sync()` |
| `om.compiler` | `status()`, `import_format(format, content)` |
| `om.autoresearch` | `run(metric, ground_truth_sql, max_variations, filters)` |
| `om.metric_requests` | `list(status)`, `create(description)`, `resolve(id, status)` |
| `om.admin` | `invalidate_cache(metric)`, `sync_catalog()` |

### Error handling

```python
from onlymetrix import OnlyMetrix, OnlyMetrixError

try:
    result = om.metrics.query("nonexistent")
except OnlyMetrixError as e:
    print(f"Error {e.status_code}: {e.message}")
```

---

## Installation

```bash
pip install onlymetrix                  # core
pip install onlymetrix[langchain]       # + LangChain tools
pip install onlymetrix[crewai]          # + CrewAI tools
pip install onlymetrix[all]             # everything
```

---

## License

MIT
