"""Custom analysis — define, export, share.

Shows the full lifecycle:
  1. Define a custom analysis from primitives
  2. Export as JSON DAG
  3. Save to server (persists across sessions)
  4. Load and run from another session
"""

from onlymetrix import OnlyMetrix

om = OnlyMetrix("http://localhost:8333", api_key="omx_sk_...")

# 1. Define a custom analysis
@om.analysis.custom("concentration_report")
def concentration_report(ctx, dimension="country", top_n=3):
    """Revenue concentration risk by dimension."""
    sensitivity = ctx.sensitivity(
        dimension=dimension,
        scenario=f"remove_top_{top_n}",
    )
    drivers = ctx.drivers(dimensions=[dimension])
    return {
        "risk": sensitivity["value"]["risk"],
        "impact_pct": sensitivity["value"]["impact_pct"],
        "hhi": sensitivity["value"]["herfindahl_index"],
        "top_driver": drivers["dimensions"][0] if drivers["dimensions"] else None,
        "explanation": (
            f"Top {top_n} {dimension}s account for "
            f"{sensitivity['value']['impact_pct']:.0f}% — "
            f"risk is {sensitivity['value']['risk']}"
        ),
        "confidence": 0.9,
    }

# 2. Run it
result = om.analysis.run_custom(
    "concentration_report",
    metric="churn_by_country",
    dimension="country",
    top_n=3,
)
print("Result:", result["explanation"])

# 3. Export as JSON DAG and save to server
dag = om.analysis.export_dag("concentration_report", save_to_server=True)
print(f"\nExported DAG: {len(dag['steps'])} steps")
print(f"Primitives used: {dag['permitted_primitives']}")

# 4. List all analyses (local + server)
analyses = om.analysis.list_custom()
for a in analyses:
    print(f"  [{a['source']}] {a['name']}: {a.get('description', '')[:50]}")

om.close()
