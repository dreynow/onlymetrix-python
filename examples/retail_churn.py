"""Retail churn analysis — the finding that closes deals.

Run against UCI Online Retail II dataset (5,942 customers, 41 countries).
Demonstrates: autoresearch scoring, at-risk profiling, concentration risk.
"""

from onlymetrix import OnlyMetrix

om = OnlyMetrix("http://localhost:8333", api_key="omx_sk_...")

# 1. How many customers are churning?
count = om.metrics.query("customer_count")
row = count.rows[0]
print(f"Customers: {row['total']} total, {row['churned']} churned ({row['churned']/row['total']*100:.0f}%)")

# 2. How good is our churn metric?
baseline = om.autoresearch.run("churn_risk_entities", max_variations=0)
b = baseline["baseline"]
print(f"\nChurn metric: F1={b['f1']:.3f}, precision={b['precision']:.3f}, recall={b['recall']:.3f}")
print(f"Flagged {b['flagged']} customers as at-risk")

# 3. Do churned customers overlap with high spenders?
corr = om.analysis.correlate("churned_customers", "high_spenders")
v = corr["value"]
print(f"\nChurned × High spenders:")
print(f"  Overlap: {v['overlap']} (Jaccard={v['jaccard']:.3f})")
print(f"  Interpretation: {v['interpretation']}")
# → "independent" — churn isn't a spending problem

# 4. What's our country concentration risk?
risk = om.analysis.sensitivity("churn_by_country", "country",
                                scenario="remove_top_3", target="customers")
rv = risk["value"]
print(f"\nConcentration risk:")
print(f"  Remove top 3: {rv['impact_pct']:.0f}% impact")
print(f"  Risk: {rv['risk']}")
print(f"  HHI: {rv['herfindahl_index']:.3f}")
for action in risk["suggested_actions"]:
    print(f"  → {action}")

om.close()
