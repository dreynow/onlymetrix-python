# OnlyMetrix

Your AI agents shouldn't write SQL. Your data team defines the metrics once. The agent just picks which one to run.

```bash
pip install onlymetrix
```

```python
from onlymetrix import OnlyMetrix

om = OnlyMetrix("https://api.onlymetrix.com", api_key="omx_sk_...")

result = om.metrics.query("total_revenue")
print(f"Revenue: £{result.rows[0]['revenue_gbp']:,.2f}")
# → Revenue: £17,743,429.16
# Deterministic. Auditable. No SQL generated.
```

## What It Does

OnlyMetrix sits between your AI agents and your warehouse. Agents query governed metrics — not raw SQL. Every query is PII-masked, audit-logged, and rate-limited.

```python
# Search by intent — semantic, not keyword
metrics = om.metrics.list(search="customer churn risk")

# Query with filters and dimensions
result = om.metrics.query("churn_by_country", filters={"country": "United Kingdom"})

# Schema discovery
tables = om.tables.list()
desc = om.tables.describe("customers")
```

## Analysis — Not Just Queries, Reasoning

The analysis layer lets agents explain *why* a metric changed, not just report the number.

```python
# "Why did revenue drop?"
root = om.analysis.root_cause(
    "quarterly_revenue",
    compare={"current": "2025-02", "previous": "2025-01"},
    dimensions=["country", "tier"],
)
print(root["explanation"])
# → "Revenue decreased. Primary driver: country=Germany (72% of change)."
print(root["suggested_actions"])
# → ["Investigate DACH expansion strategy"]

# "What's our concentration risk?"
risk = om.analysis.sensitivity("revenue", "country", scenario="remove_top_3")
print(f"{risk['value']['impact_pct']}% of revenue in top 3 countries — {risk['value']['risk']}")
# → 94% of revenue in top 3 countries — critical

# "Do churned customers overlap with high spenders?"
corr = om.analysis.correlate("churned_customers", "high_spenders")
print(f"Jaccard: {corr['value']['jaccard']:.3f} — {corr['value']['interpretation']}")
# → Jaccard: 0.049 — independent (churn isn't a spending problem)
```

Every analysis returns structured output agents can parse:

```python
{
    "value": {...},              # the finding
    "explanation": "...",        # plain English
    "confidence": 0.85,          # 0.0-1.0
    "warnings": [...],           # data quality issues
    "suggested_actions": [...],  # what to do next
}
```

### Available Analyses

| Method | Question it answers |
|--------|-------------------|
| `root_cause` | Why did this metric change? |
| `sensitivity` | What's our concentration risk? |
| `correlate` | Are these populations related? |
| `threshold` | What's the optimal cutoff? |
| `segment_performance` | How does this metric vary across segments? |
| `contribution` | What drove the change between periods? |
| `drivers` | Which dimension explains variance most? |
| `anomalies` | Which segments are behaving abnormally? |
| `pareto` | What's the precision-recall frontier? |
| `trends` | Is this accelerating or decelerating? |
| `forecast` | Where is this heading? |
| `compare` | How do these two groups differ? |
| `health` | Can I trust this data? |

## Custom Analysis

Define your own analytical workflows by composing primitives. No raw SQL — just governed API calls.

```python
@om.analysis.custom("store_risk")
def store_risk(ctx, dimension="region"):
    s = ctx.sensitivity(dimension=dimension, scenario="remove_top_3")
    d = ctx.drivers(dimensions=[dimension])
    return {
        "risk": s["value"]["risk"],
        "top_driver": d["dimensions"][0]["dimension"],
    }

# Export as JSON DAG — storable, shareable, auditable
dag = om.analysis.export_dag("store_risk", save_to_server=True)

# Anyone on your team can load and run it
om.analysis.load_from_server("store_risk")
result = om.analysis.run_custom("store_risk", metric="revenue")
```

## CLI

```bash
omx health
omx metrics list --search revenue
omx metrics query total_revenue --filter time_start=2025-01-01
omx analysis sensitivity churn_by_country -d country
omx analysis at-risk-profile churn_risk --compare high_spenders
```

Set `OMX_API_URL` and `OMX_API_KEY` environment variables, or pass them to the client.

## Autoresearch

Automatically discover better metric definitions by testing variations against ground truth.

```python
result = om.autoresearch.run("churn_risk_entities", max_variations=30)
print(f"Baseline F1: {result['baseline']['f1']:.3f}")
print(f"Best variation: +{result['improvements']} improvements found")
# → Baseline F1: 0.660
# → Best variation: +3 improvements found
```

## Agent Integrations

### LangChain

```python
from onlymetrix.integrations.langchain import onlymetrix_tools
tools = onlymetrix_tools(url, api_key=key)
```

### CrewAI

```python
from onlymetrix.integrations.crewai import onlymetrix_tools
tools = onlymetrix_tools(url, api_key=key)
```

## Install

```bash
pip install onlymetrix              # core
pip install onlymetrix[langchain]   # + LangChain tools
pip install onlymetrix[crewai]      # + CrewAI tools
pip install onlymetrix[all]         # everything
```

Python 3.9+. Async client included (`AsyncOnlyMetrix`).

## Links

- [Documentation](https://docs.onlymetrix.com)
- [API Reference](https://api.onlymetrix.com)
- [Changelog](CHANGELOG.md)

## License

MIT
