"""OnlyMetrix Quickstart — first query in 10 lines."""

from onlymetrix import OnlyMetrix

om = OnlyMetrix("https://api.onlymetrix.com", api_key="omx_sk_...")

# List available metrics
metrics = om.metrics.list(search="revenue")
for m in metrics:
    print(f"  {m.name}: {m.description}")

# Query a metric
result = om.metrics.query("total_revenue")
print(f"\nRevenue: {result.rows[0]}")
print(f"Execution time: {result.execution_time_ms}ms")
