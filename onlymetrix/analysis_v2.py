"""OnlyMetrix Analysis V2 — Reasoning tier.

Tier 1: correlate, root_cause, threshold
Tier 2: health (auto-check), sensitivity, forecast

Every method returns AnalysisResult with:
  - value (the structured finding)
  - explanation (one sentence, plain English)
  - confidence (0.0-1.0)
  - warnings (data quality issues)
  - suggested_actions (what to do about it)

Health auto-check runs before every analysis unless disabled.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Optional

from onlymetrix.client import OnlyMetrix, OnlyMetrixError


# ── Result contract ──────────────────────────────────────────────

def _result(
    metric: str,
    analysis_type: str,
    value: dict,
    explanation: str,
    confidence: float = 1.0,
    warnings: Optional[list[str]] = None,
    suggested_actions: Optional[list[str]] = None,
    insights: Optional[list[str]] = None,
    health: Optional[dict] = None,
) -> dict:
    """Uniform result wrapper for all analysis methods."""
    return {
        "metric": metric,
        "analysis_type": analysis_type,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "value": value,
        "explanation": explanation,
        "confidence": round(confidence, 3),
        "warnings": warnings or [],
        "suggested_actions": suggested_actions or [],
        "insights": insights or [],
        "metric_health": health,
    }


# ── Health check ─────────────────────────────────────────────────

def health(om: OnlyMetrix, metric: str) -> dict:
    """Check metric health: freshness, null rates, row count, stability.

    Runs automatically before every analysis unless disabled.
    Returns a dict with overall status and warnings.
    """
    warnings = []
    status = "healthy"

    try:
        result = om.metrics.query(metric, limit=1000)
    except OnlyMetrixError as e:
        return {
            "status": "error",
            "error": str(e),
            "warnings": [f"Metric query failed: {e}"],
        }

    row_count = result.row_count
    if row_count == 0:
        warnings.append("Metric returns zero rows — may be misconfigured or data is missing")
        status = "warning"

    # Check for high null rates across columns
    if result.rows:
        for col in result.columns:
            name = col["name"]
            null_count = sum(1 for r in result.rows if r.get(name) is None)
            null_rate = null_count / len(result.rows)
            if null_rate > 0.5:
                warnings.append(f"Column '{name}' has {null_rate:.0%} null rate")
                status = "warning"

    return {
        "status": status,
        "row_count": row_count,
        "columns": len(result.columns),
        "warnings": warnings,
    }


# ── Correlate ────────────────────────────────────────────────────

def correlate(
    om: OnlyMetrix,
    metric_a: str,
    metric_b: str,
    limit: int = 5000,
    auto_health: bool = True,
    server_side: bool = True,
) -> dict:
    """Correlate two entity metrics by shared entity keys.

    Both metrics must return rows with entity IDs (customer_ref, id).
    Computes overlap, Jaccard similarity, and directional relationship.

    When server_side=True (default), uses the server-side correlate endpoint
    which operates on unmasked data — solving PII masking issues.

    Agent use: "Do high-value customers churn more?"
    """
    health_status = None
    warnings = []

    if auto_health:
        h_a = health(om, metric_a)
        h_b = health(om, metric_b)
        health_status = {"metric_a": h_a, "metric_b": h_b}
        warnings.extend([f"{metric_a}: {w}" for w in h_a.get("warnings", [])])
        warnings.extend([f"{metric_b}: {w}" for w in h_b.get("warnings", [])])

    # Prefer server-side correlation (operates on unmasked data)
    if server_side and hasattr(om, "server_analysis"):
        try:
            result = om.server_analysis.correlate(metric_a, metric_b, limit=limit)
            # Merge in health status and any client-side warnings
            if health_status:
                result["metric_health"] = health_status
            if warnings:
                result.setdefault("warnings", []).extend(warnings)
            return result
        except Exception as e:
            warnings.append(f"Server-side correlation failed ({e}), falling back to client-side (PII masked)")
            # Fall through to client-side correlation

    # Client-side fallback: queries go through PII masking
    try:
        result_a = om.metrics.query(metric_a, limit=limit)
        result_b = om.metrics.query(metric_b, limit=limit)
    except OnlyMetrixError as e:
        return _result(
            metric=f"{metric_a} × {metric_b}",
            analysis_type="correlate",
            value={},
            explanation=f"Failed to query metrics: {e}",
            confidence=0.0,
            warnings=warnings + [str(e)],
            health=health_status,
        )

    # Extract entity IDs from both results
    ids_a = _extract_entity_ids(result_a)
    ids_b = _extract_entity_ids(result_b)

    if not ids_a or not ids_b:
        missing = []
        if not ids_a:
            missing.append(metric_a)
        if not ids_b:
            missing.append(metric_b)
        return _result(
            metric=f"{metric_a} × {metric_b}",
            analysis_type="correlate",
            value={"overlap": 0, "jaccard": 0},
            explanation=f"No entity IDs found in {', '.join(missing)}. "
                        f"Correlation requires entity-returning metrics (SELECT id/customer_ref).",
            confidence=0.0,
            warnings=warnings,
            suggested_actions=[f"Rewrite {m} to return entity IDs" for m in missing],
            health=health_status,
        )

    set_a = set(ids_a)
    set_b = set(ids_b)
    overlap = set_a & set_b
    union = set_a | set_b
    only_a = set_a - set_b
    only_b = set_b - set_a

    jaccard = len(overlap) / len(union) if union else 0
    overlap_pct_a = len(overlap) / len(set_a) * 100 if set_a else 0
    overlap_pct_b = len(overlap) / len(set_b) * 100 if set_b else 0

    # Interpret
    if jaccard > 0.7:
        interpretation = "strong_overlap"
        explanation = (f"{metric_a} and {metric_b} identify largely the same entities "
                       f"({len(overlap)} overlap, Jaccard={jaccard:.2f}).")
    elif jaccard > 0.3:
        interpretation = "moderate_overlap"
        explanation = (f"Moderate relationship: {len(overlap)} entities appear in both "
                       f"{metric_a} and {metric_b} (Jaccard={jaccard:.2f}).")
    elif jaccard > 0.05:
        interpretation = "weak_overlap"
        explanation = (f"Weak relationship between {metric_a} and {metric_b}: "
                       f"only {len(overlap)} shared entities (Jaccard={jaccard:.2f}).")
    else:
        interpretation = "independent"
        explanation = (f"{metric_a} and {metric_b} identify largely independent populations "
                       f"({len(overlap)} overlap out of {len(union)}).")

    confidence = min(1.0, (len(set_a) + len(set_b)) / 100)  # More data = more confident

    actions = []
    if jaccard > 0.5:
        actions.append(f"Investigate shared characteristics of {len(overlap)} overlapping entities")
    if len(only_a) > len(overlap):
        actions.append(f"{len(only_a)} entities in {metric_a} but not {metric_b} — potential segment to investigate")

    return _result(
        metric=f"{metric_a} × {metric_b}",
        analysis_type="correlate",
        value={
            "metric_a": {"name": metric_a, "count": len(set_a)},
            "metric_b": {"name": metric_b, "count": len(set_b)},
            "overlap": len(overlap),
            "only_a": len(only_a),
            "only_b": len(only_b),
            "union": len(union),
            "jaccard": round(jaccard, 3),
            "overlap_pct_of_a": round(overlap_pct_a, 1),
            "overlap_pct_of_b": round(overlap_pct_b, 1),
            "interpretation": interpretation,
        },
        explanation=explanation,
        confidence=confidence,
        warnings=warnings,
        suggested_actions=actions,
        health=health_status,
    )


# ── Root Cause Analysis ─────────────────────────────────────────

def root_cause(
    om: OnlyMetrix,
    metric: str,
    compare: dict[str, str],
    dimensions: list[str],
    target: Optional[str] = None,
    limit: Optional[int] = None,
    auto_health: bool = True,
) -> dict:
    """Automated root cause analysis: why did a metric change?

    Decomposes the change across every provided dimension, finds which
    dimension explains the most variance, drills into the top contributing
    segment, and builds a structured explanation.

    Agent use: "Why did revenue drop last month?"

    Args:
        metric: Metric name.
        compare: {"current": "2025-02", "previous": "2025-01"}.
        dimensions: Dimensions to investigate.
        target: Measure column. Auto-detected.
        limit: Max rows per query.
    """
    from onlymetrix.analysis import Analysis
    a = Analysis(om)

    health_status = None
    warnings = []

    if auto_health:
        health_status = health(om, metric)
        warnings = health_status.get("warnings", [])
        if health_status.get("status") == "error":
            return _result(
                metric=metric, analysis_type="root_cause",
                value={}, explanation=f"Health check failed: {health_status.get('error')}",
                confidence=0.0, warnings=warnings, health=health_status,
            )
        if health_status.get("status") == "warning":
            warnings.insert(0, "Metric health warnings detected — results may be unreliable")

    # Step 1: Run contribution analysis across each dimension
    dimension_contributions = []
    for dim in dimensions:
        try:
            contrib = a.contribution(metric, compare=compare, dimension=dim, target=target, limit=limit)
            total_change = contrib.get("total_change_absolute", 0)
            breakdown = contrib.get("breakdown", [])
            top_driver = contrib.get("top_driver")

            # Compute how much of the total change this dimension explains
            if breakdown and total_change != 0:
                max_contribution = max(abs(b.get("contribution", 0)) for b in breakdown) if breakdown else 0
            else:
                max_contribution = 0

            dimension_contributions.append({
                "dimension": dim,
                "total_change": total_change,
                "breakdown": breakdown[:5],  # top 5 segments
                "top_driver": top_driver,
                "max_segment_contribution": max_contribution,
                "insights": contrib.get("insights", []),
            })
        except Exception as e:
            warnings.append(f"Failed to analyze dimension '{dim}': {e}")

    if not dimension_contributions:
        return _result(
            metric=metric, analysis_type="root_cause",
            value={"dimensions_analyzed": dimensions},
            explanation="No dimensional data available for root cause analysis.",
            confidence=0.0, warnings=warnings, health=health_status,
        )

    # Step 2: Rank dimensions by explanatory power
    dimension_contributions.sort(key=lambda d: d["max_segment_contribution"], reverse=True)
    primary = dimension_contributions[0]
    primary_dim = primary["dimension"]
    primary_top = primary["top_driver"]

    # Step 3: Build explanation
    current = compare.get("current", "?")
    previous = compare.get("previous", "?")
    total_change = primary["total_change"]
    direction = "increased" if total_change > 0 else "decreased"

    if primary_top and "dimension" in (primary_top or {}):
        primary_segment = list(primary_top["dimension"].values())[0]
        primary_pct = primary["max_segment_contribution"]
        explanation = (
            f"{metric} {direction} from {previous} to {current}. "
            f"Primary driver: {primary_dim}={primary_segment} "
            f"(accounts for {abs(primary_pct):.0%} of the change)."
        )
    else:
        explanation = (
            f"{metric} {direction} from {previous} to {current}. "
            f"{primary_dim} is the most explanatory dimension."
        )

    # Step 4: Suggested actions
    actions = []
    if primary_top and "dimension" in (primary_top or {}):
        seg = list(primary_top["dimension"].values())[0]
        if total_change < 0:
            actions.append(f"Investigate {primary_dim}={seg} — largest contributor to decline")
            actions.append(f"Review {seg} performance vs prior periods")
        else:
            actions.append(f"Understand what drove {primary_dim}={seg} growth — replicate elsewhere")

    # Add secondary drivers
    if len(dimension_contributions) > 1:
        secondary = dimension_contributions[1]
        if secondary["max_segment_contribution"] > 0.1:
            actions.append(f"Also investigate {secondary['dimension']} (secondary driver)")

    confidence = min(1.0, primary["max_segment_contribution"] + 0.3)

    return _result(
        metric=metric, analysis_type="root_cause",
        value={
            "comparison": compare,
            "total_change": total_change,
            "direction": direction,
            "primary_dimension": primary_dim,
            "primary_driver": primary_top,
            "dimension_rankings": [
                {"dimension": d["dimension"],
                 "max_contribution": d["max_segment_contribution"],
                 "top_segments": d["breakdown"][:3]}
                for d in dimension_contributions
            ],
        },
        explanation=explanation,
        confidence=confidence,
        warnings=warnings,
        suggested_actions=actions,
        insights=[i for d in dimension_contributions for i in d.get("insights", [])],
        health=health_status,
    )


# ── Threshold Discovery ─────────────────────────────────────────

def threshold(
    om: OnlyMetrix,
    metric: str,
    ground_truth_sql: Optional[str] = None,
    steps: int = 10,
    auto_health: bool = True,
) -> dict:
    """Find the optimal threshold that maximizes F1 for an entity metric.

    Tests multiple autoresearch variations to find which filter values
    produce the best precision-recall balance. Returns the optimal
    configuration and the improvement over current.

    Agent use: "What's the optimal churn threshold — 90, 120, 180 days?"
    """
    health_status = None
    warnings = []

    if auto_health:
        health_status = health(om, metric)
        warnings = health_status.get("warnings", [])

    # Run autoresearch with enough variations to explore the space
    try:
        result = om.autoresearch.run(metric, ground_truth_sql=ground_truth_sql, max_variations=steps * 3)
    except OnlyMetrixError as e:
        return _result(
            metric=metric, analysis_type="threshold",
            value={},
            explanation=f"Autoresearch failed: {e}",
            confidence=0.0, warnings=warnings + [str(e)],
            health=health_status,
        )

    baseline = result.get("baseline", {})
    variations = result.get("variations", [])
    baseline_f1 = baseline.get("f1", 0)

    # Find best variation
    best = None
    best_f1 = baseline_f1
    for v in variations:
        f1 = v.get("f1", 0)
        if f1 > best_f1:
            best_f1 = f1
            best = v

    improvement = best_f1 - baseline_f1

    # Build explanation
    if best and improvement > 0.01:
        explanation = (
            f"Found better threshold: F1 improves from {baseline_f1:.3f} to {best_f1:.3f} "
            f"(+{improvement:.3f}). Variation: {best.get('name', 'unknown')}."
        )
        actions = [
            f"Apply variation '{best.get('name')}' to improve F1 by {improvement:.1%}",
            f"Review precision ({best.get('precision', 0):.3f}) vs recall ({best.get('recall', 0):.3f}) tradeoff",
        ]
        confidence = min(1.0, 0.5 + improvement * 5)
    else:
        explanation = (
            f"Current threshold is near-optimal (F1={baseline_f1:.3f}). "
            f"No variation improved F1 by more than 1%."
        )
        actions = []
        confidence = 0.8 if baseline_f1 > 0.5 else 0.4

    return _result(
        metric=metric, analysis_type="threshold",
        value={
            "current": {
                "f1": baseline_f1,
                "precision": baseline.get("precision", 0),
                "recall": baseline.get("recall", 0),
                "flagged": baseline.get("flagged", 0),
            },
            "optimal": {
                "f1": best_f1,
                "precision": best.get("precision", 0) if best else baseline.get("precision", 0),
                "recall": best.get("recall", 0) if best else baseline.get("recall", 0),
                "variation": best.get("name") if best else None,
                "flagged": best.get("flagged", 0) if best else baseline.get("flagged", 0),
            },
            "improvement": round(improvement, 4),
            "variations_tested": len(variations),
        },
        explanation=explanation,
        confidence=confidence,
        warnings=warnings,
        suggested_actions=actions,
        health=health_status,
    )


# ── Sensitivity ──────────────────────────────────────────────────

def sensitivity(
    om: OnlyMetrix,
    metric: str,
    dimension: str,
    scenario: str = "remove_top_3",
    target: Optional[str] = None,
    limit: Optional[int] = None,
    auto_health: bool = True,
) -> dict:
    """Concentration risk: what happens if top segments disappear?

    Scenarios:
      - "remove_top_1" / "remove_top_3" / "remove_top_5"
      - "remove_bottom_5"
      - "double_bottom_5" (what if underperformers improved)

    Agent use: "What's our revenue risk if top 3 countries leave?"
    """
    from onlymetrix.analysis import Analysis, _extract_values, _compute_stats
    a = Analysis(om)

    health_status = None
    warnings = []

    if auto_health:
        health_status = health(om, metric)
        warnings = health_status.get("warnings", [])

    # Query metric by dimension
    try:
        result = _query_with_dim(om, metric, dimension, limit=limit)
    except OnlyMetrixError as e:
        return _result(
            metric=metric, analysis_type="sensitivity",
            value={}, explanation=f"Query failed: {e}",
            confidence=0.0, warnings=warnings + [str(e)],
            health=health_status,
        )

    values = _extract_values(result, dimension, target)
    if not values:
        return _result(
            metric=metric, analysis_type="sensitivity",
            value={}, explanation=f"No dimensional data for {metric} by {dimension}.",
            confidence=0.0, warnings=warnings, health=health_status,
        )

    values.sort(key=lambda x: x[1], reverse=True)
    total = sum(v for _, v in values)

    # Parse scenario
    if scenario.startswith("remove_top_"):
        n = int(scenario.split("_")[-1])
        removed = values[:n]
        remaining = values[n:]
        scenario_label = f"remove top {n} {dimension}s"
    elif scenario.startswith("remove_bottom_"):
        n = int(scenario.split("_")[-1])
        removed = values[-n:]
        remaining = values[:-n]
        scenario_label = f"remove bottom {n} {dimension}s"
    elif scenario.startswith("double_bottom_"):
        n = int(scenario.split("_")[-1])
        bottom = values[-n:]
        doubled = sum(v for _, v in bottom)
        scenario_total = total + doubled
        impact_pct = (doubled / total) * 100 if total else 0
        return _result(
            metric=metric, analysis_type="sensitivity",
            value={
                "scenario": scenario,
                "dimension": dimension,
                "current_total": total,
                "scenario_total": scenario_total,
                "impact_absolute": doubled,
                "impact_pct": round(impact_pct, 1),
                "affected_segments": [str(l) for l, _ in bottom],
            },
            explanation=(f"If bottom {n} {dimension}s doubled performance, "
                         f"{metric} would increase by {impact_pct:.1f}% (+{doubled:,.0f})."),
            confidence=0.7,
            warnings=warnings,
            suggested_actions=[f"Invest in growing bottom {n}: {', '.join(str(l) for l, _ in bottom)}"],
            health=health_status,
        )
    else:
        return _result(
            metric=metric, analysis_type="sensitivity",
            value={}, explanation=f"Unknown scenario: {scenario}",
            confidence=0.0, warnings=warnings, health=health_status,
        )

    scenario_total = sum(v for _, v in remaining)
    lost = total - scenario_total
    impact_pct = (lost / total) * 100 if total else 0

    # Concentration ratio (Herfindahl)
    shares = [(v / total) for _, v in values] if total else []
    hhi = sum(s * s for s in shares) if shares else 0

    # Risk classification
    if impact_pct > 70:
        risk = "critical"
    elif impact_pct > 50:
        risk = "high"
    elif impact_pct > 30:
        risk = "moderate"
    else:
        risk = "low"

    removed_labels = [str(l) for l, _ in removed]
    explanation = (
        f"Removing top {len(removed)} {dimension}s ({', '.join(removed_labels[:3])}) "
        f"would reduce {metric} by {impact_pct:.0f}% ({lost:,.0f} of {total:,.0f}). "
        f"Concentration risk: {risk}."
    )

    actions = []
    if risk in ("critical", "high"):
        actions.append(f"Diversify — {impact_pct:.0f}% of {metric} concentrated in {len(removed)} {dimension}s")
        actions.append(f"Develop contingency plan for loss of {removed_labels[0]}")
    if hhi > 0.25:
        actions.append(f"Herfindahl index {hhi:.3f} indicates high market concentration")

    return _result(
        metric=metric, analysis_type="sensitivity",
        value={
            "scenario": scenario,
            "dimension": dimension,
            "current_total": total,
            "scenario_total": scenario_total,
            "impact_absolute": lost,
            "impact_pct": round(impact_pct, 1),
            "risk": risk,
            "herfindahl_index": round(hhi, 4),
            "removed_segments": removed_labels,
            "segments_remaining": len(remaining),
        },
        explanation=explanation,
        confidence=0.9,
        warnings=warnings,
        suggested_actions=actions,
        health=health_status,
    )


# ── Forecast ─────────────────────────────────────────────────────

def forecast(
    om: OnlyMetrix,
    metric: str,
    periods_ahead: int = 3,
    granularity: str = "month",
    target: Optional[str] = None,
    auto_health: bool = True,
) -> dict:
    """Simple trend extrapolation — linear regression on existing periods.

    Not ML. Not time series decomposition. Just least-squares fit on
    observed data, projected forward. Agents use this for "at this rate,
    when do we hit X?"
    """
    from onlymetrix.analysis import Analysis
    a = Analysis(om)

    health_status = None
    warnings = []

    if auto_health:
        health_status = health(om, metric)
        warnings = health_status.get("warnings", [])

    trend = a.trends(metric, granularity=granularity, target=target)
    periods = trend.get("periods", [])

    if len(periods) < 3:
        return _result(
            metric=metric, analysis_type="forecast",
            value={"periods": periods},
            explanation=f"Need at least 3 historical periods for forecast, have {len(periods)}.",
            confidence=0.0, warnings=warnings, health=health_status,
        )

    # Linear regression: y = mx + b
    n = len(periods)
    xs = list(range(n))
    ys = [p["value"] for p in periods]

    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    ss_xy = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    ss_xx = sum((x - x_mean) ** 2 for x in xs)

    if ss_xx == 0:
        return _result(
            metric=metric, analysis_type="forecast",
            value={"periods": periods},
            explanation="No variance in time periods — cannot forecast.",
            confidence=0.0, warnings=warnings, health=health_status,
        )

    slope = ss_xy / ss_xx
    intercept = y_mean - slope * x_mean

    # R-squared
    ss_res = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(xs, ys))
    ss_tot = sum((y - y_mean) ** 2 for y in ys)
    r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

    # Project forward
    forecasted = []
    for i in range(1, periods_ahead + 1):
        x_new = n - 1 + i
        y_new = slope * x_new + intercept
        forecasted.append({
            "period_offset": i,
            "value": round(y_new, 2),
        })

    # Direction
    if slope > 0:
        direction = "increasing"
        trend_desc = f"+{slope:,.1f} per period"
    elif slope < 0:
        direction = "decreasing"
        trend_desc = f"{slope:,.1f} per period"
    else:
        direction = "flat"
        trend_desc = "no change"

    last_value = ys[-1]
    forecast_end = forecasted[-1]["value"]
    pct_change = ((forecast_end - last_value) / abs(last_value) * 100) if last_value else 0

    explanation = (
        f"{metric} is {direction} ({trend_desc}, R²={r_squared:.2f}). "
        f"Forecast: {last_value:,.1f} → {forecast_end:,.1f} over {periods_ahead} periods "
        f"({pct_change:+.1f}%)."
    )

    confidence = max(0, min(1.0, r_squared))

    # Detect seasonality: check if residuals alternate sign (seasonal pattern)
    residuals = [y - (slope * x + intercept) for x, y in zip(xs, ys)]
    sign_changes = sum(1 for i in range(1, len(residuals)) if residuals[i] * residuals[i-1] < 0)
    has_seasonality = sign_changes >= len(residuals) * 0.5

    if r_squared < 0.3:
        warnings.append(
            f"Very low R²={r_squared:.2f} — linear model explains less than 30% of variance. "
            f"Forecast is unreliable. Do not use for planning."
        )
        confidence = min(confidence, 0.2)
    elif r_squared < 0.5:
        warnings.append(f"Low R²={r_squared:.2f} — trend is noisy, forecast should be treated as directional only")
        confidence = min(confidence, 0.4)

    if has_seasonality:
        warnings.append(
            f"Seasonal pattern detected ({sign_changes} sign changes in {len(residuals)} residuals). "
            f"Linear model cannot capture seasonality — consider quarterly or year-over-year comparison instead."
        )

    return _result(
        metric=metric, analysis_type="forecast",
        value={
            "model": "linear",
            "slope": round(slope, 4),
            "intercept": round(intercept, 4),
            "r_squared": round(r_squared, 4),
            "direction": direction,
            "historical_periods": len(periods),
            "forecast": forecasted,
            "last_observed": last_value,
            "forecast_end": forecast_end,
            "pct_change": round(pct_change, 1),
        },
        explanation=explanation,
        confidence=confidence,
        warnings=warnings,
        health=health_status,
    )


# ── Helpers ──────────────────────────────────────────────────────

def _extract_entity_ids(result: Any) -> list[str]:
    """Extract entity IDs from query results."""
    # Try common entity column names
    for col_name in ["customer_ref", "id", "entity_id", "user_id", "account_id"]:
        ids = []
        for row in result.rows:
            val = row.get(col_name)
            if val is not None:
                ids.append(str(val))
        if ids:
            return ids
    # Fallback: first column that looks like IDs
    if result.rows and result.columns:
        first_col = result.columns[0]["name"]
        return [str(row.get(first_col, "")) for row in result.rows if row.get(first_col) is not None]
    return []


def _query_with_dim(om: OnlyMetrix, metric: str, dimension: str,
                    filters: Optional[dict] = None, limit: Optional[int] = None) -> Any:
    """Query with dimension, fallback to without."""
    try:
        return om.metrics.query(metric, filters=filters, dimension=dimension, limit=limit)
    except Exception:
        return om.metrics.query(metric, filters=filters, limit=limit)
