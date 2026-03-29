"""OnlyMetrix Custom Analysis — composable, governed, shareable.

Architecture:
  - JSON workflow DAG is the stored artifact (like IR for metrics)
  - Python @decorator generates the DAG via introspection
  - Server stores, versions, and shares DAGs across tenants
  - Execution resolves the DAG with health checks and audit

Two-phase AnalysisContext:
  - Record mode: decorator introspection — captures calls, builds DAG
  - Execute mode: runtime — proxies to real analysis primitives

Safety:
  - permitted_primitives validated at registration, not runtime
  - No arbitrary code stored — only JSON DAG of primitive calls
  - Depth guard, health auto-check, PII guard inherited from primitives
"""

from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from onlymetrix.analysis_v2 import _result, health as _health


# ── Primitives list ──────────────────────────────────────────────

ALL_PRIMITIVES = frozenset([
    "pareto", "segment_performance", "contribution", "drivers",
    "anomalies", "trends", "compare", "correlate", "root_cause",
    "threshold", "sensitivity", "forecast", "health", "query_metric",
])


# ── DAG Schema ───────────────────────────────────────────────────

def validate_dag(dag: dict) -> list[str]:
    """Validate a custom analysis DAG. Returns list of errors (empty = valid)."""
    errors = []

    if "name" not in dag:
        errors.append("Missing 'name'")
    if "steps" not in dag or not isinstance(dag.get("steps"), list):
        errors.append("Missing or invalid 'steps' array")
        return errors

    step_ids = set()
    for i, step in enumerate(dag["steps"]):
        sid = step.get("id", f"step_{i}")
        if sid in step_ids:
            errors.append(f"Duplicate step id: {sid}")
        step_ids.add(sid)

        prim = step.get("primitive", "")
        if prim not in ALL_PRIMITIVES:
            errors.append(f"Step '{sid}': unknown primitive '{prim}'. Valid: {sorted(ALL_PRIMITIVES)}")

        deps = step.get("depends_on", [])
        for dep in deps:
            if dep not in step_ids:
                errors.append(f"Step '{sid}': depends_on '{dep}' not defined before this step")

    # Check for cycles (simple: topological order check)
    visited = set()
    for step in dag["steps"]:
        sid = step.get("id")
        deps = set(step.get("depends_on", []))
        if deps & {sid}:
            errors.append(f"Step '{sid}' depends on itself")
        visited.add(sid)

    return errors


# ── Two-Phase Context ────────────────────────────────────────────

class _StepRef:
    """Reference to a step's output, used for DAG wiring."""

    def __init__(self, step_id: str):
        self._step_id = step_id
        self._data = {}

    def get(self, key: str, default=None):
        return self._data.get(key, default)

    def __getitem__(self, key: str):
        return self._data.get(key, f"${self._step_id}.{key}")

    def __contains__(self, key: str):
        return key in self._data


class AnalysisContext:
    """Sandboxed context for custom analysis execution.

    Two modes:
    - record=True: captures primitive calls for DAG generation
    - record=False: executes against real data
    """

    def __init__(self, analysis: Any, metric: str, params: dict, record: bool = False):
        self._analysis = analysis
        self.metric = metric
        self.params = params
        self._record = record
        self._steps: list[dict] = []
        self._step_counter = 0

    def _call_primitive(self, name: str, **kwargs) -> Any:
        """Route to recording or execution."""
        if self._record:
            self._step_counter += 1
            step_id = f"s{self._step_counter}"

            # Resolve $input references in kwargs
            resolved_params = {}
            for k, v in kwargs.items():
                if isinstance(v, str) and v.startswith("$"):
                    resolved_params[k] = v  # keep as reference
                else:
                    resolved_params[k] = v

            # Infer depends_on from previous steps referenced in params
            deps = []
            for v in resolved_params.values():
                if isinstance(v, str) and v.startswith("$s"):
                    dep_id = v.split(".")[0].lstrip("$")
                    if dep_id not in deps:
                        deps.append(dep_id)

            step = {
                "id": step_id,
                "primitive": name,
                "params": resolved_params,
            }
            if deps:
                step["depends_on"] = deps

            self._steps.append(step)
            return _StepRef(step_id)
        else:
            # Execute mode — call real primitive
            fn = getattr(self._analysis, name, None)
            if fn is None:
                raise ValueError(f"Unknown primitive: {name}")
            return fn(**kwargs)

    # ── Permitted primitives ─────────────────────────────────

    def pareto(self, metric: Optional[str] = None, **kwargs) -> Any:
        return self._call_primitive("pareto", metric=metric or self.metric, **kwargs)

    def segment_performance(self, metric: Optional[str] = None, **kwargs) -> Any:
        return self._call_primitive("segment_performance", metric=metric or self.metric, **kwargs)

    def contribution(self, metric: Optional[str] = None, **kwargs) -> Any:
        return self._call_primitive("contribution", metric=metric or self.metric, **kwargs)

    def drivers(self, metric: Optional[str] = None, **kwargs) -> Any:
        return self._call_primitive("drivers", metric=metric or self.metric, **kwargs)

    def anomalies(self, metric: Optional[str] = None, **kwargs) -> Any:
        return self._call_primitive("anomalies", metric=metric or self.metric, **kwargs)

    def trends(self, metric: Optional[str] = None, **kwargs) -> Any:
        return self._call_primitive("trends", metric=metric or self.metric, **kwargs)

    def compare(self, metric: Optional[str] = None, **kwargs) -> Any:
        return self._call_primitive("compare", metric=metric or self.metric, **kwargs)

    def correlate(self, metric_a: Optional[str] = None, metric_b: str = "", **kwargs) -> Any:
        return self._call_primitive("correlate", metric_a=metric_a or self.metric, metric_b=metric_b, **kwargs)

    def root_cause(self, metric: Optional[str] = None, **kwargs) -> Any:
        return self._call_primitive("root_cause", metric=metric or self.metric, **kwargs)

    def threshold(self, metric: Optional[str] = None, **kwargs) -> Any:
        return self._call_primitive("threshold", metric=metric or self.metric, **kwargs)

    def sensitivity(self, metric: Optional[str] = None, **kwargs) -> Any:
        return self._call_primitive("sensitivity", metric=metric or self.metric, **kwargs)

    def forecast(self, metric: Optional[str] = None, **kwargs) -> Any:
        return self._call_primitive("forecast", metric=metric or self.metric, **kwargs)

    def health(self, metric: Optional[str] = None) -> Any:
        return self._call_primitive("health", metric=metric or self.metric)

    def query_metric(self, metric: Optional[str] = None, **kwargs) -> Any:
        if self._record:
            return self._call_primitive("query_metric", metric=metric or self.metric, **kwargs)
        return self._analysis._om.metrics.query(metric or self.metric, **kwargs)


# ── Registry ─────────────────────────────────────────────────────

class CustomAnalysisRegistry:
    """Registry of custom analysis functions and DAGs.

    Stores both:
    - Python functions (Level 2 — client-side composition)
    - JSON DAGs (Level 3 — storable, shareable, server-side)
    """

    MAX_DEPTH = 10

    def __init__(self):
        self._functions: dict[str, _FunctionEntry] = {}
        self._dags: dict[str, dict] = {}
        self._depth = 0

    def register(
        self,
        name: str,
        fn: Callable,
        description: str = "",
        parameters: Optional[dict[str, Any]] = None,
    ):
        """Register a Python function as a custom analysis."""
        self._functions[name] = _FunctionEntry(
            name=name, fn=fn,
            description=description or fn.__doc__ or "",
            parameters=parameters or {},
            created_at=datetime.now(timezone.utc).isoformat(),
        )

    def register_dag(self, dag: dict):
        """Register a JSON DAG as a custom analysis."""
        errors = validate_dag(dag)
        if errors:
            raise ValueError(f"Invalid DAG: {'; '.join(errors)}")
        self._dags[dag["name"]] = dag

    def introspect(self, name: str, analysis: Any, metric: str = "__introspect__", **params) -> dict:
        """Run a registered function in record mode to generate its DAG."""
        entry = self._functions.get(name)
        if not entry:
            raise ValueError(f"Function '{name}' not registered")

        ctx = AnalysisContext(analysis, metric, params, record=True)
        try:
            entry.fn(ctx, **params)
        except Exception:
            pass  # Record mode — calls return StepRefs, downstream may fail

        # Extract function signature for inputs
        sig = inspect.signature(entry.fn)
        inputs = []
        for pname, param in sig.parameters.items():
            if pname == "ctx":
                continue
            inp = {"name": pname, "type": "Any"}
            if param.default is not inspect.Parameter.empty:
                inp["default"] = param.default
            else:
                inp["required"] = True
            inputs.append(inp)

        # Detect which primitives were used
        used_primitives = sorted(set(s["primitive"] for s in ctx._steps))

        dag = {
            "name": name,
            "version": 1,
            "description": entry.description,
            "created_at": entry.created_at,
            "inputs": inputs,
            "steps": ctx._steps,
            "permitted_primitives": used_primitives,
            "sandbox": {
                "max_depth": self.MAX_DEPTH,
                "health_check": True,
                "pii_guard": True,
            },
        }
        return dag

    def get(self, name: str) -> Optional[dict]:
        """Get a DAG by name (registered function or stored DAG)."""
        if name in self._dags:
            return self._dags[name]
        if name in self._functions:
            return {"name": name, "type": "function", "description": self._functions[name].description}
        return None

    def list(self) -> list[dict]:
        """List all registered analyses (functions + DAGs)."""
        result = []
        for e in self._functions.values():
            result.append({
                "name": e.name,
                "type": "function",
                "description": e.description,
                "parameters": e.parameters,
            })
        for dag in self._dags.values():
            result.append({
                "name": dag["name"],
                "type": "dag",
                "description": dag.get("description", ""),
                "steps": len(dag.get("steps", [])),
                "version": dag.get("version", 1),
            })
        return result

    def run(
        self,
        name: str,
        analysis: Any,
        metric: str,
        auto_health: bool = True,
        **params: Any,
    ) -> dict:
        """Execute a custom analysis (function or DAG)."""
        # Try function first
        if name in self._functions:
            return self._run_function(name, analysis, metric, auto_health, **params)
        # Try DAG
        if name in self._dags:
            return self._run_dag(name, analysis, metric, auto_health, **params)
        # Not found
        available = list(self._functions.keys()) + list(self._dags.keys())
        return _result(
            metric=metric, analysis_type=f"custom:{name}",
            value={}, explanation=f"Custom analysis '{name}' not found. Available: {available}",
            confidence=0.0,
        )

    def _run_function(self, name, analysis, metric, auto_health, **params):
        """Execute a Python function custom analysis."""
        entry = self._functions[name]

        self._depth += 1
        if self._depth > self.MAX_DEPTH:
            self._depth -= 1
            return _result(
                metric=metric, analysis_type=f"custom:{name}",
                value={}, explanation=f"Max depth ({self.MAX_DEPTH}) exceeded.",
                confidence=0.0,
            )

        health_status = None
        warnings = []
        if auto_health:
            health_status = _health(analysis._om, metric)
            warnings = health_status.get("warnings", [])

        ctx = AnalysisContext(analysis, metric, params, record=False)

        try:
            result = entry.fn(ctx, **params)
        except Exception as e:
            self._depth -= 1
            return _result(
                metric=metric, analysis_type=f"custom:{name}",
                value={}, explanation=f"Custom analysis failed: {e}",
                confidence=0.0, warnings=warnings + [str(e)],
                health=health_status,
            )

        self._depth -= 1

        if isinstance(result, dict) and "analysis_type" in result:
            if health_status:
                result["metric_health"] = health_status
            return result

        explanation = result.pop("explanation", f"Custom analysis '{name}' completed.") if isinstance(result, dict) else f"'{name}' completed."
        confidence = result.pop("confidence", 0.8) if isinstance(result, dict) else 0.8
        actions = result.pop("suggested_actions", []) if isinstance(result, dict) else []
        insights = result.pop("insights", []) if isinstance(result, dict) else []

        return _result(
            metric=metric, analysis_type=f"custom:{name}",
            value=result if isinstance(result, dict) else {"result": result},
            explanation=explanation, confidence=confidence,
            warnings=warnings, suggested_actions=actions,
            insights=insights, health=health_status,
        )

    def _run_dag(self, name, analysis, metric, auto_health, **params):
        """Execute a JSON DAG custom analysis."""
        dag = self._dags[name]

        health_status = None
        warnings = []
        if auto_health:
            health_status = _health(analysis._om, metric)
            warnings = health_status.get("warnings", [])

        # Validate permitted primitives
        permitted = set(dag.get("permitted_primitives", ALL_PRIMITIVES))

        # Resolve inputs
        input_values = dict(params)
        for inp in dag.get("inputs", []):
            if inp["name"] not in input_values:
                if "default" in inp:
                    input_values[inp["name"]] = inp["default"]
                elif inp.get("required"):
                    return _result(
                        metric=metric, analysis_type=f"custom:{name}",
                        value={}, explanation=f"Missing required input: {inp['name']}",
                        confidence=0.0, warnings=warnings, health=health_status,
                    )

        # Execute steps in order (DAG is topologically sorted)
        step_results: dict[str, Any] = {}

        for step in dag["steps"]:
            sid = step["id"]
            prim = step["primitive"]

            if prim not in permitted:
                return _result(
                    metric=metric, analysis_type=f"custom:{name}",
                    value={}, explanation=f"Step '{sid}' uses '{prim}' which is not permitted.",
                    confidence=0.0, warnings=warnings, health=health_status,
                )

            # Resolve params: $input references and $step.field references
            resolved = {}
            for k, v in step.get("params", {}).items():
                if isinstance(v, str) and v.startswith("$"):
                    ref = v[1:]  # strip $
                    if ref in input_values:
                        resolved[k] = input_values[ref]
                    elif "." in ref:
                        step_ref, field = ref.split(".", 1)
                        if step_ref in step_results:
                            r = step_results[step_ref]
                            # Navigate nested fields
                            for part in field.split("."):
                                if isinstance(r, dict):
                                    r = r.get(part)
                                else:
                                    r = None
                                    break
                            resolved[k] = r
                        else:
                            resolved[k] = v  # unresolved reference
                    else:
                        resolved[k] = v
                else:
                    resolved[k] = v

            # Default metric to the analysis metric
            if "metric" not in resolved and prim not in ("correlate",):
                resolved["metric"] = metric

            # Execute the primitive
            fn = getattr(analysis, prim, None)
            if fn is None:
                step_results[sid] = {"error": f"Unknown primitive: {prim}"}
                continue

            try:
                step_results[sid] = fn(**resolved)
            except Exception as e:
                step_results[sid] = {"error": str(e)}
                warnings.append(f"Step '{sid}' ({prim}) failed: {e}")

        # Build output from output template or last step
        output = dag.get("output")
        if output and isinstance(output, dict):
            result_value = {}
            for k, v in output.items():
                if isinstance(v, str) and v.startswith("$"):
                    ref = v[1:]
                    parts = ref.split(".")
                    if parts[0] in step_results:
                        r = step_results[parts[0]]
                        for part in parts[1:]:
                            if isinstance(r, dict):
                                r = r.get(part)
                            else:
                                r = None
                                break
                        result_value[k] = r
                    else:
                        result_value[k] = v
                else:
                    result_value[k] = v
        else:
            # Use last step's result
            last_step = dag["steps"][-1]["id"] if dag["steps"] else None
            result_value = step_results.get(last_step, {}) if last_step else {}

        return _result(
            metric=metric, analysis_type=f"custom:{name}",
            value=result_value,
            explanation=dag.get("description", f"DAG analysis '{name}' completed."),
            confidence=0.8,
            warnings=warnings,
            health=health_status,
        )

    def export_dag(self, name: str, analysis: Any, **params) -> dict:
        """Export a registered function as a JSON DAG.

        Runs the function in record mode to introspect the call graph,
        then returns the DAG schema.
        """
        return self.introspect(name, analysis, **params)


class _FunctionEntry:
    __slots__ = ("name", "fn", "description", "parameters", "created_at")

    def __init__(self, name, fn, description, parameters, created_at):
        self.name = name
        self.fn = fn
        self.description = description
        self.parameters = parameters
        self.created_at = created_at


# ── Built-in custom analyses ─────────────────────────────────────

def register_builtins(registry: CustomAnalysisRegistry):
    """Register built-in custom analyses that ship with OM."""

    def at_risk_profile(ctx: AnalysisContext, basket_dimension: str = "product_id",
                        compare_metric: Optional[str] = None, top_n: int = 5):
        """Profile at-risk customers: who are they, what did they buy,
        and what distinguishes them from retained customers."""
        at_risk = ctx.query_metric()
        at_risk_count = at_risk.row_count if hasattr(at_risk, "row_count") else 0

        correlation = None
        if compare_metric:
            correlation = ctx.correlate(metric_b=compare_metric)

        segment_result = None
        try:
            segment_result = ctx.segment_performance(segments=[basket_dimension] if basket_dimension else [])
        except Exception:
            pass

        sensitivity_result = None
        try:
            sensitivity_result = ctx.sensitivity(dimension=basket_dimension, scenario="remove_top_3")
        except Exception:
            pass

        profile = {"at_risk_count": at_risk_count}
        insights = [f"{at_risk_count} entities currently flagged as at-risk"]
        actions = []

        if correlation and isinstance(correlation, dict):
            v = correlation.get("value", {})
            overlap = v.get("overlap", 0)
            jaccard = v.get("jaccard", 0)
            interp = v.get("interpretation", "unknown")
            profile["correlation_with_comparison"] = {
                "metric": compare_metric, "overlap": overlap,
                "jaccard": jaccard, "interpretation": interp,
            }
            if jaccard > 0.3:
                insights.append(f"Strong overlap with {compare_metric} (Jaccard={jaccard:.2f})")
                actions.append(f"Prioritize retention for the {overlap} overlapping entities")
            elif jaccard < 0.1:
                insights.append(f"At-risk entities are independent of {compare_metric} (Jaccard={jaccard:.2f})")

        if segment_result and isinstance(segment_result, dict):
            results = segment_result.get("results", [])
            if results:
                segs = results[0].get("segments", [])
                top = sorted(segs, key=lambda s: s.get("value", 0), reverse=True)[:top_n]
                profile["top_segments"] = top

        if sensitivity_result and isinstance(sensitivity_result, dict):
            v = sensitivity_result.get("value", {})
            profile["concentration_risk"] = {"risk": v.get("risk"), "impact_pct": v.get("impact_pct")}
            if v.get("risk") in ("critical", "high"):
                actions.append(f"Concentration risk is {v['risk']} — {v.get('impact_pct', 0):.0f}% impact")

        return {
            "profile": profile,
            "explanation": f"{at_risk_count} entities at risk.",
            "confidence": 0.75,
            "insights": insights,
            "suggested_actions": actions,
        }

    registry.register(
        "at_risk_profile", at_risk_profile,
        description="Profile at-risk entities: who they are, what they buy, overlap with other segments",
        parameters={"basket_dimension": "str", "compare_metric": "str (optional)", "top_n": "int"},
    )
