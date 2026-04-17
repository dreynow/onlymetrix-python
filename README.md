# OnlyMetrix Python SDK

[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![Version](https://img.shields.io/badge/version-0.6.0-green.svg)](CHANGELOG.md)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Python client and CLI for [OnlyMetrix](https://onlymetrix.com) — a governed metric layer for AI agents and data teams.

---

## Installation

```bash
pip install onlymetrix
```

**From Google Colab / Jupyter:**

```python
!pip install "git+https://github.com/dreynow/onlymetrix-python.git"
```

**Optional extras:**

```bash
pip install onlymetrix[sql]             # SQL-to-Semantic-Layer converter
pip install onlymetrix[langchain]       # LangChain tool bindings
pip install onlymetrix[crewai]          # CrewAI tool bindings
pip install onlymetrix[all]             # everything
```

Requires Python 3.9+. See [CHANGELOG](CHANGELOG.md) for version history.

---

## What it does

OnlyMetrix sits between your warehouse and anything that queries it — agents, dashboards, notebooks. You define metrics once, and everything downstream queries through the governed layer: no raw SQL, PII masked, every query audited.

The SDK gives you:

- **Python client** — query metrics, run structured analysis, manage setup
- **CLI** (`omx`) — everything the client does, plus CI-friendly commands
- **SQL converter** — turn raw SQL into governed metric definitions
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

Convert raw SQL queries into governed metric definitions — no manual YAML writing. The converter parses SQL to extract aggregations, source tables, filters, dimensions, and time columns.

### Basic usage

```python
from onlymetrix.sql_converter import convert_sql, extract_sql
import json

metric = convert_sql(
    "SELECT SUM(amount) FROM orders WHERE status = 'paid'",
    name="total_revenue",
    description="Total paid revenue",
)

# Pretty-print the metric dict
print(json.dumps(metric, indent=2))
```

Output:

```json
{
  "name": "total_revenue",
  "description": "Total paid revenue",
  "sql": "SELECT SUM(amount) FROM orders WHERE status = 'paid'",
  "source_tables": ["orders"],
  "tags": ["aggregate", "finance"],
  "filters": [{"name": "status", "type": "string"}]
}
```

### YAML output with `extract_sql`

Use `extract_sql` for full metadata extraction — returns an `ExtractedMetric` dataclass with aggregations, dimensions, warnings, and a `.to_yaml()` method:

```python
from onlymetrix.sql_converter import extract_sql

metric = extract_sql(
    "SELECT SUM(amount) FROM orders WHERE status = 'paid'",
    name="total_revenue",
    description="Total paid revenue",
)

print(metric.to_yaml())
```

Output:

```yaml
- name: total_revenue
  description: Total paid revenue
  sql: |
    SELECT SUM(amount) FROM orders WHERE status = 'paid'
  source_tables: [orders]
  tags: [aggregate, finance]
  filters:
    - name: status
      type: string
```

### SQL with JOINs

The converter handles multi-table joins, extracting all source tables, dimensions, and time columns:

**Revenue by customer segment:**

```python
metric = extract_sql(
    """SELECT SUM(o.amount)
       FROM orders o
       JOIN customers c ON o.customer_id = c.id
       WHERE c.segment = 'enterprise'""",
    name="enterprise_revenue",
    description="Total revenue from enterprise customers",
)
print(metric.to_yaml())
```

```yaml
- name: enterprise_revenue
  description: Total revenue from enterprise customers
  sql: |
    SELECT SUM(o.amount)
       FROM orders o
       JOIN customers c ON o.customer_id = c.id
       WHERE c.segment = 'enterprise'
  source_tables: [orders, customers]
  tags: [aggregate, customers, finance]
  filters:
    - name: c.segment
      type: string
```

**Average order value by product category:**

```python
metric = extract_sql(
    """SELECT AVG(o.amount)
       FROM orders o
       JOIN order_items oi ON o.id = oi.order_id
       JOIN products p ON oi.product_id = p.id
       GROUP BY p.category""",
    name="avg_order_by_category",
    description="Average order value broken down by product category",
)
print(metric.to_yaml())
```

**Distinct active users with events:**

```python
metric = extract_sql(
    """SELECT COUNT(DISTINCT u.id)
       FROM users u
       JOIN events e ON u.id = e.user_id
       WHERE e.event_date >= '2024-01-01'
         AND u.status = 'active'""",
    name="active_users_with_events",
    description="Distinct active users who triggered at least one event",
)
print(metric.to_yaml())
```

```yaml
- name: active_users_with_events
  description: Distinct active users who triggered at least one event
  sql: |
    SELECT COUNT(DISTINCT u.id)
       FROM users u
       JOIN events e ON u.id = e.user_id
       WHERE e.event_date >= '2024-01-01'
         AND u.status = 'active'
  source_tables: [users, events]
  tags: [cardinality, customers, engagement]
  time_column: event_date
  filters:
    - name: e.event_date
      type: number
    - name: u.status
      type: string
```

**Net payments excluding refunds:**

```python
metric = extract_sql(
    """SELECT SUM(p.amount)
       FROM payments p
       JOIN invoices i ON p.invoice_id = i.id
       JOIN customers c ON i.customer_id = c.id
       WHERE p.status = 'completed'
         AND p.refunded = false""",
    name="net_payments",
    description="Total completed payments excluding refunds",
)
print(metric.to_yaml())
```

**Pro-tier session count:**

```python
metric = extract_sql(
    """SELECT COUNT(s.id)
       FROM sessions s
       JOIN accounts a ON s.account_id = a.id
       JOIN plans pl ON a.plan_id = pl.id
       WHERE pl.tier = 'pro'
       GROUP BY a.name, s.created_at""",
    name="pro_session_count",
    description="Session count for pro-tier accounts by month",
)
print(metric.to_yaml())
```

### Accessing extracted fields

```python
from dataclasses import asdict

metric = extract_sql(...)

# Direct field access
print(f"Name:        {metric.name}")
print(f"Tables:      {metric.source_tables}")
print(f"Aggregation: {metric.aggregations}")
print(f"Filters:     {metric.filters}")
print(f"Dimensions:  {metric.dimensions}")
print(f"Time column: {metric.time_column}")
print(f"Tags:        {metric.tags}")
print(f"Warnings:    {metric.warnings}")

# Full dict (all fields)
print(json.dumps(asdict(metric), indent=2))
```

### Batch conversion

```python
from onlymetrix.sql_converter import convert_sql_batch

metrics = convert_sql_batch([
    {"sql": "SELECT SUM(amount) FROM orders", "name": "total_orders"},
    {"sql": "SELECT COUNT(DISTINCT user_id) FROM sessions", "name": "unique_users"},
    {"sql": "SELECT AVG(score) FROM reviews WHERE rating >= 4", "name": "avg_positive_score"},
])

# Import all at once
om.setup.import_metrics(metrics)
```

### File and directory conversion

```python
from onlymetrix.sql_converter import convert_sql_file, convert_sql_directory

# Single file (metric name defaults to filename)
metric = convert_sql_file("queries/total_revenue.sql")

# All .sql files in a directory
metrics = convert_sql_directory("queries/")
```

### CLI

```bash
# Convert a single query
omx sql convert "SELECT SUM(amount) FROM orders" --name total_revenue

# Inspect extraction details before importing
omx sql inspect "SELECT country, SUM(amount) FROM orders GROUP BY country"
#   Name:         sum_amount
#   Tables:       orders
#   Aggregations: 1
#     - SUM(amount) AS amount
#   Dimensions:   country
#   Time column:  (not detected)
#   Tags:         aggregate, finance

# Batch convert a directory
omx sql convert-batch ./queries/ --format yaml --output metrics.yaml
omx sql convert-batch ./queries/ --import   # convert + push to server
```

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

### CI/CD for pull requests (v0.6.0+)

Catch breaking metric changes before they merge:

```bash
omx ci snapshot                                              # pin current IR baseline (once)
omx ci check --manifest ./target/manifest.json --strict      # runs in CI on every PR
```

Detects dropped columns, probable renames, and flags impact by metric tier
(`core` blocks the PR, `standard` warns, `foundation` is info-only). Posts a
PR comment showing affected dashboards and — on OnlyMetrix cloud — which
business decisions referenced the metric.

Full walkthrough with the GitHub Actions workflow: [dbt CI/CD docs][ci-docs].

[ci-docs]: https://onlymetrix.com/docs/integrations/dbt.html#ci-cd-for-pull-requests

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

# SQL converter
omx sql convert "SELECT SUM(amount) FROM orders" --name total_revenue
omx sql convert-batch ./queries/ [--format yaml] [--output metrics.yaml] [--import]
omx sql inspect "SELECT ..."

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

## Google Colab quickstart

```python
# Cell 1 — Install
!pip install "git+https://github.com/dreynow/onlymetrix-python.git"

# Cell 2 — Verify install
import onlymetrix
print(f"OnlyMetrix SDK v{onlymetrix.__version__}")

# Cell 3 — SQL converter (works without an API key)
from onlymetrix.sql_converter import extract_sql
import json

metric = extract_sql(
    """SELECT COUNT(DISTINCT u.id)
       FROM users u
       JOIN events e ON u.id = e.user_id
       WHERE e.event_date >= '2024-01-01'
         AND u.status = 'active'""",
    name="active_users_with_events",
    description="Distinct active users who triggered at least one event",
)

# Pretty JSON
print(json.dumps(json.loads(json.dumps(
    {k: v for k, v in metric.__dict__.items()}
)), indent=2))

# YAML output
print(metric.to_yaml())

# Cell 4 — Connect and query (requires API key)
from onlymetrix import OnlyMetrix

om = OnlyMetrix("https://api.onlymetrix.com", api_key="omx_sk_...")
result = om.metrics.query("total_revenue", filters={"time_start": "2025-01-01"})
print(result.rows)
```

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

MIT
