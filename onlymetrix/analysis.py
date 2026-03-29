"""OnlyMetrix Analysis Layer — reasoning substrate for AI agents.

Every response is:
  - Deterministic structure → agents can parse
  - Self-explanatory → includes meaning, not just numbers
  - Composable → can be chained with other calls
  - Auditable → traceable back to metric + data

Agent flow for "Why is churn increasing?":
    1. om.metrics.query("churn_risk")                       → what's the number
    2. om.analysis.segment_performance("churn_risk",        → where is it broken
           segments=["country", "tier"])
    3. om.analysis.pareto(metric="churn_risk")              → what are the optimal variants
    4. om.analysis.contribution("revenue",                  → what drove the change
           compare={"current": "2025-02", "previous": "2025-01"})
    → agent merges responses → builds explanation
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Optional

from onlymetrix.client import OnlyMetrix


class Analysis:
    """Reasoning substrate for AI agents on top of OnlyMetrix metrics.

    Every method returns a dict with a consistent structure:
        metric, value, explanation, confidence, warnings, suggested_actions

    Tier 1 (reasoning):
        correlate, root_cause, threshold
    Tier 2 (planning):
        sensitivity, forecast, health
    Core (existing):
        pareto, segment_performance, contribution, drivers, anomalies, trends, compare
    """

    def __init__(self, client: OnlyMetrix):
        self._om = client
        self._custom_registry = None

    @property
    def _custom(self):
        if self._custom_registry is None:
            from onlymetrix.custom_analysis import CustomAnalysisRegistry, register_builtins
            self._custom_registry = CustomAnalysisRegistry()
            register_builtins(self._custom_registry)
        return self._custom_registry

    def custom(self, name: str):
        """Decorator to register a custom analysis function.

        Usage:
            @om.analysis.custom("my_analysis")
            def my_analysis(ctx, dimension="country"):
                drivers = ctx.drivers(dimensions=[dimension])
                return {"top": drivers["dimensions"][0]}

            result = om.analysis.run_custom("my_analysis", metric="revenue")
        """
        def decorator(fn):
            self._custom.register(name, fn, description=fn.__doc__ or "")
            return fn
        return decorator

    def run_custom(self, name: str, metric: str, **params) -> dict:
        """Run a registered custom analysis (function or DAG)."""
        return self._custom.run(name, self, metric, **params)

    def register_dag(self, dag: dict, save_to_server: bool = True):
        """Register a JSON DAG as a custom analysis.

        Stores locally and optionally pushes to the server for
        persistence across sessions and sharing with team.
        """
        self._custom.register_dag(dag)
        if save_to_server:
            try:
                self._om.custom_analyses.register(
                    name=dag["name"],
                    definition=dag,
                    description=dag.get("description", ""),
                    author=dag.get("author"),
                )
            except Exception:
                pass  # Server save failed — local registration still works

    def export_dag(self, name: str, save_to_server: bool = False, **params) -> dict:
        """Export a registered Python function as a JSON DAG.

        Runs the function in record mode to introspect the call graph.
        Optionally saves to the server.
        """
        dag = self._custom.export_dag(name, self, **params)
        if save_to_server:
            self.register_dag(dag, save_to_server=True)
        return dag

    def list_custom(self) -> list[dict]:
        """List all custom analyses — local (functions + DAGs) + server-side."""
        local = self._custom.list()
        try:
            server = self._om.custom_analyses.list()
            # Merge: server entries not already in local
            local_names = {c["name"] for c in local}
            for s in server:
                if s["name"] not in local_names:
                    s["source"] = "server"
                    local.append(s)
            for c in local:
                if "source" not in c:
                    c["source"] = "local"
        except Exception:
            for c in local:
                c["source"] = "local"
        return local

    def load_from_server(self, name: str) -> dict:
        """Load a DAG from the server and register it locally."""
        data = self._om.custom_analyses.get(name)
        definition = data.get("definition")
        if isinstance(definition, str):
            import json
            definition = json.loads(definition)
        if definition:
            self._custom.register_dag(definition)
        return data

    # ── Tier 1: Reasoning ────────────────────────────────────────

    def correlate(self, metric_a: str, metric_b: str, **kwargs) -> dict:
        """Correlate two entity metrics by shared entity keys."""
        from onlymetrix.analysis_v2 import correlate
        return correlate(self._om, metric_a, metric_b, **kwargs)

    def root_cause(self, metric: str, compare: dict, dimensions: list[str], **kwargs) -> dict:
        """Automated root cause analysis: why did a metric change?"""
        from onlymetrix.analysis_v2 import root_cause
        return root_cause(self._om, metric, compare, dimensions, **kwargs)

    def threshold(self, metric: str, **kwargs) -> dict:
        """Find the optimal threshold that maximizes F1."""
        from onlymetrix.analysis_v2 import threshold
        return threshold(self._om, metric, **kwargs)

    # ── Tier 2: Planning ─────────────────────────────────────────

    def sensitivity(self, metric: str, dimension: str, scenario: str = "remove_top_3", **kwargs) -> dict:
        """Concentration risk: what happens if top segments disappear?"""
        from onlymetrix.analysis_v2 import sensitivity
        return sensitivity(self._om, metric, dimension, scenario, **kwargs)

    def forecast(self, metric: str, periods_ahead: int = 3, **kwargs) -> dict:
        """Simple trend extrapolation."""
        from onlymetrix.analysis_v2 import forecast
        return forecast(self._om, metric, periods_ahead, **kwargs)

    def health(self, metric: str) -> dict:
        """Check metric health: freshness, null rates, stability."""
        from onlymetrix.analysis_v2 import health
        return health(self._om, metric)

    # ── Core (existing) ──────────────────────────────────────────

    # ── Pareto Frontier ──────────────────────────────────────────

    def pareto(
        self,
        metric: str,
        ground_truth_sql: Optional[str] = None,
        max_variations: int = 30,
        **kwargs: Any,
    ) -> dict:
        """Pareto frontier: precision-recall tradeoff across metric variants.

        Runs autoresearch to generate and score variants, then identifies
        the Pareto-optimal frontier — variants where improving precision
        requires sacrificing recall and vice versa.

        Args:
            metric: Metric name.
            ground_truth_sql: SQL that returns (id, outcome) for scoring.
                If None, uses stored ground truth from prior autoresearch runs.
            max_variations: Max variants to test.

        Returns:
            {metric, objective, frontier, recommendations, insights}
        """
        # ground_truth_sql is optional — if not provided, the server
        # falls back to the metric's stored ground_truth_sql field.
        # Cold start works: om.analysis.pareto("churn_risk") just works
        # if the metric has ground_truth_sql configured.
        raw = self._om.autoresearch.run(metric, ground_truth_sql, max_variations)

        # Extract Pareto frontier from autoresearch results
        frontier = []
        all_variants = raw.get("variations", [])
        pareto_variants = raw.get("pareto_frontier", [])
        baseline = raw.get("baseline", {})

        for i, v in enumerate(pareto_variants):
            profile = v.get("profile", "moderate")
            frontier.append({
                "id": f"variant_{i+1}",
                "definition": {
                    "name": v.get("name", ""),
                    "sql": v.get("sql", ""),
                },
                "scores": {
                    "precision": v.get("precision", 0),
                    "recall": v.get("recall", 0),
                    "f1": v.get("f1", 0),
                    "flagged": v.get("flagged", 0),
                    "tp": v.get("tp", 0),
                    "fp": v.get("fp", 0),
                    "fn": v.get("fn", 0),
                },
                "delta_f1": v.get("delta", 0),
                "profile": profile,
            })

        # Build recommendations
        best_f1 = max(frontier, key=lambda f: f["scores"]["f1"]) if frontier else None
        best_precision = max(frontier, key=lambda f: f["scores"]["precision"]) if frontier else None
        best_recall = max(frontier, key=lambda f: f["scores"]["recall"]) if frontier else None

        recommendations = {}
        if best_f1:
            recommendations["best_overall"] = best_f1["id"]
        if best_precision:
            recommendations["high_precision"] = best_precision["id"]
        if best_recall:
            recommendations["high_recall"] = best_recall["id"]

        return {
            "metric": metric,
            "objective": "precision_recall_tradeoff",
            "generated_at": _now(),
            "baseline": {
                "scores": {
                    "precision": baseline.get("precision", 0),
                    "recall": baseline.get("recall", 0),
                    "f1": baseline.get("f1", 0),
                },
                "profile": baseline.get("profile", "moderate"),
            },
            "frontier": frontier,
            "total_tested": raw.get("total_tested", 0),
            "improvements": raw.get("improvements", 0),
            "ground_truth_size": raw.get("ground_truth_size", 0),
            "recommendations": recommendations,
            "insights": raw.get("insights", []),
        }

    # ── Segment Performance ──────────────────────────────────────

    def segment_performance(
        self,
        metric: str,
        segments: list[str],
        ground_truth_sql: Optional[str] = None,
        target: Optional[str] = None,
        filters: Optional[dict[str, str]] = None,
        max_variations: int = 0,
        limit: Optional[int] = None,
    ) -> dict:
        """Cross-dimensional segment performance analysis.

        Two modes:
        1. With ground truth (precision/recall/F1 per segment) — runs
           autoresearch per segment value against filtered ground truth.
           This tells you "is this metric reliable for enterprise customers?"
        2. Without ground truth — uses value distribution, z-scores,
           and coefficient of variation. Still useful but less precise.

        Args:
            metric: Metric name.
            segments: Dimensions to analyze (e.g., ["country", "tier"]).
            ground_truth_sql: SQL returning (id, outcome). If None, tries
                stored ground_truth_sql on the metric, then falls back to
                value-based analysis.
            target: Measure column. Auto-detected if None.
            filters: Optional query filters.
            max_variations: Max autoresearch variants per segment (when scoring).
            limit: Max rows per dimension.

        Returns:
            {metric, segments_analyzed, results, insights, alerts}
        """
        # Check if we can do scoring (ground truth available)
        has_ground_truth = ground_truth_sql is not None
        if not has_ground_truth:
            # Check if metric has stored ground truth
            meta = self._om.metrics.get(metric)
            if meta and hasattr(meta, "ground_truth_sql") and meta.ground_truth_sql:
                ground_truth_sql = meta.ground_truth_sql
                has_ground_truth = True

        results = []
        alerts = []
        all_insights = []

        for dim in segments:
            result = _query_with_dimension(self._om, metric, dim, filters=filters, limit=limit)
            values = _extract_values(result, dim, target)

            if not values:
                continue

            values.sort(key=lambda x: x[1], reverse=True)
            total = sum(v for _, v in values)
            nums = [v for _, v in values]
            stats = _compute_stats(nums)

            segment_entries = []
            for label, value in values:
                z = (value - stats.mean) / stats.std_dev if stats.std_dev > 0 else 0
                pct = (value / total * 100) if total else 0
                population = int(value)

                entry: dict[str, Any] = {
                    "segment": {dim: str(label)},
                    "population": population,
                    "value": value,
                    "pct_of_total": round(pct, 2),
                    "z_score": round(z, 2),
                }

                # Per-segment scoring via autoresearch with segment filter.
                # The server narrows both the metric SQL and ground truth SQL
                # to this segment, so scores are segment-specific.
                if has_ground_truth and ground_truth_sql:
                    try:
                        seg_filters = dict(filters or {})
                        seg_filters[dim] = str(label)

                        seg_result = self._om.autoresearch.run(
                            metric, ground_truth_sql, max_variations,
                            filters=seg_filters,
                        )
                        baseline = seg_result.get("baseline", {})
                        entry["scores"] = {
                            "precision": baseline.get("precision", 0),
                            "recall": baseline.get("recall", 0),
                            "f1": baseline.get("f1", 0),
                        }

                        f1 = baseline.get("f1", 0)
                        if f1 >= 0.7:
                            entry["performance"] = "strong"
                        elif f1 >= 0.5:
                            entry["performance"] = "moderate"
                        elif f1 >= 0.3:
                            entry["performance"] = "weak"
                        else:
                            entry["performance"] = "unreliable"

                        if entry["performance"] in ("weak", "unreliable"):
                            alerts.append({
                                "segment": {dim: str(label)},
                                "issue": f"low_f1",
                                "f1": f1,
                                "message": f"Metric is {entry['performance']} for {dim}={label} (F1={f1:.3f})",
                            })
                    except Exception:
                        # Autoresearch failed for this segment — fall back to value-based
                        entry["performance"] = _classify_by_zscore(z)
                        entry["scores"] = None
                else:
                    # Value-based classification
                    entry["performance"] = _classify_by_zscore(z)

                segment_entries.append(entry)

            dim_result: dict[str, Any] = {
                "dimension": dim,
                "segment_count": len(segment_entries),
                "total": total,
                "scored": has_ground_truth,
                "stats": {
                    "mean": round(stats.mean, 2),
                    "median": round(stats.median, 2),
                    "std_dev": round(stats.std_dev, 2),
                    "cv": round(stats.cv, 3),
                    "min": round(stats.min, 2),
                    "max": round(stats.max, 2),
                },
                "segments": segment_entries,
                "explanatory_power": round(stats.cv, 3),
            }
            results.append(dim_result)

            # Insights per dimension
            if has_ground_truth:
                weak = [e for e in segment_entries if e.get("performance") in ("weak", "unreliable")]
                if weak:
                    labels = [list(e["segment"].values())[0] for e in weak]
                    all_insights.append(
                        f"Metric performs poorly on {dim}={', '.join(labels)} — "
                        f"consider segment-specific definitions"
                    )
            if stats.cv > 0.5:
                top = values[0][0]
                bot = values[-1][0]
                all_insights.append(
                    f"{dim} shows high variance (CV={stats.cv:.2f}): "
                    f"{top} ({values[0][1]:.0f}) vs {bot} ({values[-1][1]:.0f})"
                )
            elif stats.cv < 0.1:
                all_insights.append(f"{dim} shows little variation — {metric} is stable across {dim}s")

        # Rank dimensions by explanatory power
        results.sort(key=lambda r: r["explanatory_power"], reverse=True)
        most_explanatory = results[0]["dimension"] if results else None

        if most_explanatory:
            all_insights.insert(0,
                f"{most_explanatory} explains the most variance in {metric} "
                f"(CV={results[0]['explanatory_power']:.3f})"
            )

        if alerts:
            all_insights.append(
                f"{len(alerts)} segment(s) flagged — see alerts for details"
            )

        return {
            "metric": metric,
            "segments_analyzed": segments,
            "generated_at": _now(),
            "scoring_mode": "precision_recall" if has_ground_truth else "value_distribution",
            "results": results,
            "most_explanatory_dimension": most_explanatory,
            "insights": all_insights,
            "alerts": alerts,
        }

    # ── Contribution (Delta) Analysis ────────────────────────────

    def contribution(
        self,
        metric: str,
        compare: dict[str, str],
        dimension: Optional[str] = None,
        target: Optional[str] = None,
        filters: Optional[dict[str, str]] = None,
        limit: Optional[int] = None,
    ) -> dict:
        """Period-over-period change decomposition — what drove the change.

        Args:
            metric: Metric name.
            compare: {"current": "2025-02", "previous": "2025-01"} — time periods.
            dimension: Dimension to decompose by. Required for dimensional breakdown.
            target: Measure column. Auto-detected if None.
            filters: Additional filters (combined with time filters).
            limit: Max rows.

        Returns:
            {metric, comparison, total_change, breakdown, top_driver, insights}
        """
        current = compare.get("current", "")
        previous = compare.get("previous", "")

        # Strategy 1: Use time_start filters if metric supports them.
        # Strategy 2: Query all data, split by time column client-side.
        # Try strategy 1 first, fall back to strategy 2.
        try:
            filters_current = dict(filters or {})
            filters_previous = dict(filters or {})
            filters_current["time_start"] = current
            filters_previous["time_start"] = previous

            result_prev = _query_with_dimension(self._om, metric, dimension or "", filters=filters_previous, limit=limit) if dimension else self._om.metrics.query(metric, filters=filters_previous, limit=limit)
            result_curr = _query_with_dimension(self._om, metric, dimension or "", filters=filters_current, limit=limit) if dimension else self._om.metrics.query(metric, filters=filters_current, limit=limit)
        except Exception:
            # Strategy 2: query all data, split by time column client-side
            all_data = self._om.metrics.query(metric, filters=filters, limit=limit or 10000)
            time_col = _find_time_column(all_data)
            if not time_col:
                return {
                    "metric": metric, "comparison": {"current": current, "previous": previous},
                    "generated_at": _now(), "insights": ["Metric does not support time filters and has no time column"],
                    "breakdown": [], "total_change_absolute": 0, "direction": "unknown",
                }
            # Split rows by time period. Normalize time values to handle
            # various formats: "2011-07-01", "2011-07-01T00:00:00+00:00",
            # "2011-07-01 00:00:00", etc.
            def _matches_period(row_val: Any, period: str) -> bool:
                s = str(row_val).replace("T", " ").split(" ")[0]  # extract date part
                return s.startswith(period)

            prev_rows = [r for r in all_data.rows if _matches_period(r.get(time_col, ""), previous)]
            curr_rows = [r for r in all_data.rows if _matches_period(r.get(time_col, ""), current)]

            # Build fake result objects for downstream processing
            class _FakeResult:
                def __init__(self, rows, columns):
                    self.rows = rows
                    self.columns = columns
                    self.row_count = len(rows)
            result_prev = _FakeResult(prev_rows, all_data.columns)
            result_curr = _FakeResult(curr_rows, all_data.columns)

        val_col = target or _guess_measure(result_prev, exclude=dimension)

        if dimension:
            values_prev = dict(_extract_values(result_prev, dimension, val_col))
            values_curr = dict(_extract_values(result_curr, dimension, val_col))
        else:
            values_prev = {"_total": _aggregate(result_prev, val_col)}
            values_curr = {"_total": _aggregate(result_curr, val_col)}

        total_prev = sum(values_prev.values())
        total_curr = sum(values_curr.values())
        total_change_abs = total_curr - total_prev
        total_change_pct = (total_change_abs / abs(total_prev)) if total_prev != 0 else None

        # Build breakdown
        all_labels = sorted(set(values_prev.keys()) | set(values_curr.keys()))
        breakdown = []
        for label in all_labels:
            vp = values_prev.get(label, 0)
            vc = values_curr.get(label, 0)
            delta = vc - vp
            contribution = (delta / abs(total_change_abs)) if total_change_abs != 0 else 0

            entry = {
                "previous_value": vp,
                "current_value": vc,
                "delta": delta,
                "contribution": round(contribution, 3),
            }
            if dimension:
                entry["dimension"] = {dimension: str(label)}
            breakdown.append(entry)

        # Sort by absolute contribution
        breakdown.sort(key=lambda b: abs(b["delta"]), reverse=True)

        # Top driver
        top_driver = None
        if breakdown and total_change_abs != 0:
            top = breakdown[0]
            direction = "positive" if top["delta"] > 0 else "negative"
            top_driver = {
                "reason": f"largest {direction} contribution",
            }
            if dimension:
                top_driver["dimension"] = top["dimension"]

        # Insights
        insights = []
        direction_word = "increased" if total_change_abs > 0 else "decreased"
        if total_change_pct is not None:
            insights.append(
                f"{metric} {direction_word} by {abs(total_change_abs):.1f} "
                f"({total_change_pct:+.1%}) from {previous} to {current}"
            )
        if breakdown and dimension:
            top_b = breakdown[0]
            top_label = top_b["dimension"][dimension]
            insights.append(
                f"{top_label} accounts for {abs(top_b['contribution']):.0%} of the total change"
            )
            # Offsetting factors
            offsetting = [b for b in breakdown[1:4]
                          if (b["delta"] > 0) != (total_change_abs > 0)]
            if offsetting:
                labels = [b["dimension"][dimension] for b in offsetting]
                insights.append(f"Partially offset by: {', '.join(labels)}")

        return {
            "metric": metric,
            "comparison": {"current": current, "previous": previous},
            "generated_at": _now(),
            "total_previous": total_prev,
            "total_current": total_curr,
            "total_change": round(total_change_pct, 4) if total_change_pct is not None else None,
            "total_change_absolute": total_change_abs,
            "breakdown": breakdown,
            "top_driver": top_driver,
            "insights": insights,
        }

    # ── Drivers ──────────────────────────────────────────────────

    def drivers(
        self,
        metric: str,
        dimensions: list[str],
        target: Optional[str] = None,
        filters: Optional[dict[str, str]] = None,
        limit: Optional[int] = None,
    ) -> dict:
        """Rank dimensions by how much they explain a metric's variance.

        Higher coefficient of variation = more explanatory power.
        Agents use this to decide which dimension to investigate first.

        Args:
            metric: Metric name.
            dimensions: Dimensions to compare.
            target: Measure column. Auto-detected if None.
            filters: Optional filters.
            limit: Max rows per dimension.

        Returns:
            {metric, dimensions, insights}
        """
        explanations = []

        for dim in dimensions:
            result = _query_with_dimension(self._om, metric, dim, filters=filters, limit=limit)
            values = _extract_values(result, dim, target)

            if not values or len(values) < 2:
                continue

            values.sort(key=lambda x: x[1], reverse=True)
            nums = [v for _, v in values]
            stats = _compute_stats(nums)

            range_ratio = (stats.max / stats.min) if stats.min > 0 else 0

            explanations.append({
                "dimension": dim,
                "coefficient_of_variation": round(stats.cv, 3),
                "segment_count": len(values),
                "range_ratio": round(range_ratio, 2),
                "top_segment": str(values[0][0]),
                "top_value": values[0][1],
                "bottom_segment": str(values[-1][0]),
                "bottom_value": values[-1][1],
            })

        # Rank by CV
        explanations.sort(key=lambda e: e["coefficient_of_variation"], reverse=True)
        for i, e in enumerate(explanations):
            e["rank"] = i + 1

        insights = []
        if explanations:
            top = explanations[0]
            insights.append(
                f"{top['dimension']} explains the most variance in {metric} "
                f"(CV={top['coefficient_of_variation']:.3f}, "
                f"range: {top['bottom_segment']}={top['bottom_value']:.1f} "
                f"to {top['top_segment']}={top['top_value']:.1f})"
            )
            if top["coefficient_of_variation"] > 1.0:
                insights.append(f"High variance — {top['dimension']} is a strong segmentation axis")
            elif top["coefficient_of_variation"] < 0.2 and len(explanations) > 1:
                insights.append(f"Low variance across all dimensions — {metric} is stable")

        return {
            "metric": metric,
            "generated_at": _now(),
            "dimensions": explanations,
            "insights": insights,
        }

    # ── Anomalies ────────────────────────────────────────────────

    def anomalies(
        self,
        metric: str,
        dimension: str,
        target: Optional[str] = None,
        z_threshold: float = 2.0,
        filters: Optional[dict[str, str]] = None,
        limit: Optional[int] = None,
    ) -> dict:
        """Find segments behaving abnormally compared to peers.

        Args:
            metric: Metric name.
            dimension: Dimension to analyze.
            target: Measure column. Auto-detected if None.
            z_threshold: Z-score threshold (default 2.0).
            filters: Optional filters.
            limit: Max rows.

        Returns:
            {metric, dimension, anomalies, baseline, insights}
        """
        result = _query_with_dimension(self._om, metric, dimension, filters=filters, limit=limit)
        values = _extract_values(result, dimension, target)

        if len(values) < 3:
            return {
                "metric": metric, "dimension": dimension,
                "generated_at": _now(),
                "anomalies": [],
                "baseline": {"mean": 0, "std_dev": 0},
                "insights": [f"Too few segments ({len(values)}) for anomaly detection"],
            }

        nums = [v for _, v in values]
        stats = _compute_stats(nums)

        anomalies = []
        for label, value in values:
            z = (value - stats.mean) / stats.std_dev if stats.std_dev > 0 else 0
            if abs(z) >= z_threshold:
                dev_pct = ((value - stats.mean) / abs(stats.mean) * 100) if stats.mean != 0 else 0
                anomalies.append({
                    "segment": {dimension: str(label)},
                    "value": value,
                    "z_score": round(z, 2),
                    "deviation_pct": round(dev_pct, 1),
                    "direction": "above" if z > 0 else "below",
                })

        anomalies.sort(key=lambda a: abs(a["z_score"]), reverse=True)

        insights = []
        if anomalies:
            parts = [f"{a['segment'][dimension]} ({a['direction']} by {abs(a['deviation_pct']):.0f}%)"
                     for a in anomalies[:3]]
            insights.append(f"Found {len(anomalies)} anomalous {dimension}(s): {'; '.join(parts)}")
            insights.append(f"Baseline: mean={stats.mean:.1f}, std={stats.std_dev:.1f}")
        else:
            insights.append(
                f"No anomalies — all {dimension}s within {z_threshold} std devs of mean ({stats.mean:.1f})"
            )

        return {
            "metric": metric, "dimension": dimension,
            "generated_at": _now(),
            "anomalies": anomalies,
            "baseline": {"mean": round(stats.mean, 2), "std_dev": round(stats.std_dev, 2)},
            "insights": insights,
        }

    # ── Trends ───────────────────────────────────────────────────

    def trends(
        self,
        metric: str,
        granularity: str = "month",
        target: Optional[str] = None,
        filters: Optional[dict[str, str]] = None,
        limit: Optional[int] = None,
    ) -> dict:
        """Time series trend with momentum analysis.

        Args:
            metric: Metric name.
            granularity: day, week, month, quarter, year.
            target: Measure column. Auto-detected if None.
            filters: Optional filters.
            limit: Max periods.

        Returns:
            {metric, granularity, periods, momentum, insights}
        """
        meta = self._om.metrics.get(metric)
        if meta and meta.time_column:
            result = self._om.metrics.query(metric, filters=filters, dimension=meta.time_column, limit=limit)
        else:
            result = self._om.metrics.query(metric, filters=filters, limit=limit)

        time_col = _find_time_column(result)
        val_col = target or _guess_measure(result, exclude=time_col)

        periods = []
        prev_value = None
        changes = []

        for row in result.rows:
            period_label = str(row.get(time_col, "")) if time_col else str(len(periods))
            value = _to_float(row.get(val_col)) if val_col else 0.0

            change = None
            change_pct = None
            if prev_value is not None:
                change = value - prev_value
                if prev_value != 0:
                    change_pct = round((change / abs(prev_value)) * 100, 2)
                changes.append(change_pct or 0)

            periods.append({
                "period": period_label,
                "value": value,
                "change": round(change, 2) if change is not None else None,
                "change_pct": change_pct,
            })
            prev_value = value

        # Overall change
        overall = None
        if len(periods) >= 2 and periods[0]["value"] != 0:
            overall = round(((periods[-1]["value"] - periods[0]["value"]) / abs(periods[0]["value"])) * 100, 2)

        # Momentum
        momentum = "stable"
        if len(changes) >= 3:
            recent = changes[-3:]
            if all(c > 0 for c in recent):
                momentum = "accelerating" if recent[-1] > recent[0] else "decelerating"
            elif all(c < 0 for c in recent):
                momentum = "accelerating_decline" if recent[-1] < recent[0] else "recovering"
            elif max(abs(c) for c in recent) > 20:
                momentum = "volatile"

        # Insights
        insights = []
        if periods:
            last = periods[-1]
            if len(periods) >= 2:
                dir_str = "up" if (last["change"] or 0) > 0 else "down"
                insights.append(
                    f"{metric} is {dir_str} {abs(last['change_pct'] or 0):.1f}% "
                    f"in the latest period ({last['period']})"
                )
                insights.append(f"Momentum: {momentum}")
                if overall is not None:
                    insights.append(f"Overall change across {len(periods)} periods: {overall:+.1f}%")

        return {
            "metric": metric,
            "granularity": granularity,
            "generated_at": _now(),
            "measure_column": val_col or "",
            "periods": periods,
            "overall_change_pct": overall,
            "momentum": momentum,
            "insights": insights,
        }

    # ── Compare ──────────────────────────────────────────────────

    def compare(
        self,
        metric: str,
        filter_a: dict[str, str],
        filter_b: dict[str, str],
        label_a: str = "Group A",
        label_b: str = "Group B",
        target: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> dict:
        """A/B comparison of a metric under two filter sets.

        Args:
            metric: Metric name.
            filter_a / filter_b: Filters for each group.
            label_a / label_b: Labels.
            target: Measure column. Auto-detected.
            limit: Max rows per group.

        Returns:
            {metric, group_a, group_b, diff, insights}
        """
        result_a = self._om.metrics.query(metric, filters=filter_a, limit=limit)
        result_b = self._om.metrics.query(metric, filters=filter_b, limit=limit)

        val_col = target or _guess_measure(result_a)
        agg_a = _aggregate(result_a, val_col)
        agg_b = _aggregate(result_b, val_col)

        diff = agg_a - agg_b
        pct_diff = round((diff / abs(agg_b)) * 100, 2) if agg_b != 0 else None
        direction = "higher" if diff > 0 else "lower" if diff < 0 else "equal"

        insights = [
            f"{label_a} is {direction} than {label_b} on {metric}: "
            f"{agg_a:.1f} vs {agg_b:.1f} "
            f"(diff: {diff:+.1f}{f', {pct_diff:+.1f}%' if pct_diff is not None else ''})"
        ]

        return {
            "metric": metric,
            "generated_at": _now(),
            "group_a": {"label": label_a, "filters": filter_a, "aggregate": agg_a, "row_count": result_a.row_count},
            "group_b": {"label": label_b, "filters": filter_b, "aggregate": agg_b, "row_count": result_b.row_count},
            "absolute_diff": diff,
            "pct_diff": pct_diff,
            "direction": direction,
            "insights": insights,
        }


# ── Helpers ──────────────────────────────────────────────────────

def _classify_by_zscore(z: float) -> str:
    """Classify performance from z-score when ground truth is unavailable."""
    if z > 1.0:
        return "strong"
    elif z < -1.0:
        return "weak"
    elif z > 0.3:
        return "above_average"
    elif z < -0.3:
        return "below_average"
    return "average"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _query_with_dimension(om: OnlyMetrix, metric: str, dimension: str,
                          filters: Optional[dict] = None, limit: Optional[int] = None) -> Any:
    """Query a metric with a dimension. If the metric doesn't support the
    dimension parameter, query without it — the metric may already produce
    dimensional output via its own GROUP BY."""
    try:
        return om.metrics.query(metric, filters=filters, dimension=dimension, limit=limit)
    except Exception:
        # Fall back to querying without dimension — the column may
        # already exist in the output from the metric's own GROUP BY.
        return om.metrics.query(metric, filters=filters, limit=limit)


def _extract_values(result: Any, dimension: str, target: Optional[str] = None) -> list[tuple[Any, float]]:
    if not result.rows:
        return []
    # Metrics using $dimension placeholder output column named "dimension",
    # not the actual dimension name. Try both.
    dim_col = dimension
    if result.rows and dimension not in result.rows[0] and "dimension" in result.rows[0]:
        dim_col = "dimension"
    val_col = target or _guess_measure(result, exclude=dim_col)
    values = []
    for row in result.rows:
        label = row.get(dim_col)
        raw = row.get(val_col)
        value = _to_float(raw)
        if label is not None and value is not None:
            values.append((label, value))
    return values


def _guess_measure(result: Any, exclude: Optional[str] = None) -> Optional[str]:
    if not result.rows:
        return None
    first_row = result.rows[0]
    for col in result.columns:
        name = col["name"]
        if name == exclude:
            continue
        if isinstance(first_row.get(name), (int, float)):
            return name
    for col in result.columns:
        if col["name"] != exclude:
            return col["name"]
    return None


def _find_time_column(result: Any) -> Optional[str]:
    for col in result.columns:
        name = col["name"]
        col_type = col.get("type", "").lower()
        if "date" in col_type or "timestamp" in col_type or name in (
            "month", "week", "day", "quarter", "year", "period", "date",
        ):
            return name
    return result.columns[0]["name"] if result.columns else None


def _aggregate(result: Any, column: Optional[str]) -> float:
    if not column or not result.rows:
        return 0.0
    return sum(_to_float(row.get(column)) or 0.0 for row in result.rows)


def _to_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


class _Stats:
    __slots__ = ("count", "mean", "median", "min", "max", "std_dev", "cv")

    def __init__(self, count, mean, median, mn, mx, std_dev, cv):
        self.count = count
        self.mean = mean
        self.median = median
        self.min = mn
        self.max = mx
        self.std_dev = std_dev
        self.cv = cv


def _compute_stats(values: list[float]) -> _Stats:
    n = len(values)
    if n == 0:
        return _Stats(0, 0, 0, 0, 0, 0, 0)
    mean = sum(values) / n
    s = sorted(values)
    median = s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2
    variance = sum((v - mean) ** 2 for v in values) / n
    std_dev = math.sqrt(variance)
    cv = (std_dev / abs(mean)) if mean != 0 else 0
    return _Stats(n, mean, median, min(values), max(values), std_dev, cv)
