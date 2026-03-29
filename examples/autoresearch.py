"""Autoresearch — find better metric definitions automatically.

Tests filter variations against ground truth, identifies the
Pareto-optimal precision-recall frontier, and suggests improvements.
"""

from onlymetrix import OnlyMetrix

om = OnlyMetrix("http://localhost:8333", api_key="omx_sk_...")

# Run autoresearch with 30 variations
result = om.autoresearch.run("churn_risk_entities", max_variations=30)

baseline = result["baseline"]
print(f"Baseline: F1={baseline['f1']:.3f}, "
      f"precision={baseline['precision']:.3f}, "
      f"recall={baseline['recall']:.3f}")

print(f"\nTested: {result['total_tested']} variations")
print(f"Improvements found: {result['improvements']}")

# Pareto frontier
frontier = result.get("pareto_frontier", [])
print(f"\nPareto frontier ({len(frontier)} points):")
for v in frontier:
    print(f"  {v['name']}: F1={v['f1']:.3f} "
          f"(precision={v['precision']:.3f}, recall={v['recall']:.3f}) "
          f"profile={v['profile']}")

# Insights
for insight in result.get("insights", []):
    if isinstance(insight, dict):
        print(f"\n{insight.get('title', '')}")
        print(f"  {insight.get('detail', '')}")

# Pareto analysis via the analysis layer
pareto = om.analysis.pareto("churn_risk_entities", max_variations=10)
print(f"\nPareto analysis:")
print(f"  Frontier: {len(pareto['frontier'])} variants")
print(f"  Recommendations: {list(pareto['recommendations'].keys())}")

# Threshold discovery — find the optimal cutoff
threshold = om.analysis.threshold("churn_risk_entities", steps=10)
v = threshold["value"]
print(f"\nThreshold analysis:")
print(f"  Current F1: {v['current']['f1']:.3f}")
print(f"  Optimal F1: {v['optimal']['f1']:.3f}")
print(f"  Improvement: {v['improvement']:.3f}")

om.close()
