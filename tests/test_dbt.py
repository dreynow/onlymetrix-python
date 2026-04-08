"""Tests for dbt manifest parser, profiles parser, and sync logic."""

import json
import os
import tempfile
from pathlib import Path

import pytest

from onlymetrix.dbt import (
    DbtProfile,
    ParsedMetric,
    OmxMeta,
    parse_manifest,
    parse_profiles,
    find_profiles,
    _resolve_env_vars,
    compute_sync_plan,
    format_dry_run,
    load_sync_state,
    save_sync_state,
)


# ---------------------------------------------------------------------------
# Test manifest fixtures
# ---------------------------------------------------------------------------

MANIFEST_METRICFLOW = {
    "semantic_models": {
        "semantic_model.my_project.orders": {
            "name": "orders",
            "model": "ref('stg_orders')",
            "dimensions": [
                {"name": "order_date", "type": "time"},
                {"name": "status", "type": "categorical"},
            ],
            "measures": [
                {"name": "order_total", "agg": "sum", "expr": "amount", "description": "Total order amount"},
                {"name": "order_count", "agg": "count", "expr": "id", "description": "Number of orders"},
            ],
        },
        "semantic_model.my_project.customers": {
            "name": "customers",
            "model": "ref('stg_customers')",
            "dimensions": [
                {"name": "signup_date", "type": "time"},
            ],
            "measures": [
                {"name": "customer_count", "agg": "count", "expr": "id", "description": "Total customers"},
                {"name": "churned_count", "agg": "sum", "expr": "is_churned", "description": "Churned customers"},
                {"name": "active_count", "agg": "count", "expr": "id", "description": "Active customers"},
            ],
        },
    },
    "metrics": {
        "metric.my_project.total_revenue": {
            "name": "total_revenue",
            "label": "Total Revenue",
            "description": "Total paid revenue in USD",
            "type": "simple",
            "type_params": {"measure": {"name": "order_total"}},
            "filter": "status = 'paid'",
            "tags": ["finance", "revenue"],
            "meta": {
                "onlymetrix": {"tier": "core", "autoresearch": True, "scorer": "revenue_correlation"}
            },
        },
        "metric.my_project.order_count": {
            "name": "order_count",
            "description": "Total number of orders",
            "type": "simple",
            "type_params": {"measure": "order_count"},
            "tags": ["orders"],
            "meta": {},
        },
        "metric.my_project.customer_count": {
            "name": "customer_count",
            "description": "Total unique customers",
            "type": "simple",
            "type_params": {"measure": {"name": "customer_count"}},
            "tags": ["customers"],
            "meta": {},
        },
        "metric.my_project.churn_rate": {
            "name": "churn_rate",
            "description": "Percentage of customers who churned",
            "type": "ratio",
            "type_params": {
                "numerator": {"name": "churned_count"},
                "denominator": {"name": "active_count"},
            },
            "tags": ["customers", "churn"],
            "meta": {},
        },
        "metric.my_project.ltv_estimate": {
            "name": "ltv_estimate",
            "description": "Estimated lifetime value",
            "type": "derived",
            "type_params": {"expr": "total_revenue / customer_count"},
            "tags": ["finance"],
            "meta": {},
        },
    },
    "nodes": {
        "model.my_project.stg_orders": {
            "resource_type": "model",
            "name": "stg_orders",
            "schema": "public",
            "alias": "orders",
        },
        "model.my_project.stg_customers": {
            "resource_type": "model",
            "name": "stg_customers",
            "schema": "public",
            "alias": "customers",
        },
    },
    "sources": {},
}


MANIFEST_LEGACY = {
    "metrics": {
        "metric.my_project.total_revenue": {
            "name": "total_revenue",
            "description": "Total revenue",
            "type": "sum",
            "sql": "amount",
            "model": "ref('orders')",
            "timestamp": "created_at",
            "tags": ["finance"],
            "filters": [
                {"field": "status", "operator": "=", "value": "paid"}
            ],
            "meta": {"onlymetrix": {"tier": "core"}},
        },
    },
    "nodes": {
        "model.my_project.orders": {
            "resource_type": "model",
            "name": "orders",
            "schema": "analytics",
        },
    },
    "sources": {},
    "semantic_models": {},
}


def _write_manifest(manifest: dict) -> str:
    """Write manifest to a temp file and return the path."""
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(manifest, tmp)
    tmp.close()
    return tmp.name


# ---------------------------------------------------------------------------
# Tests: MetricFlow parsing
# ---------------------------------------------------------------------------

class TestMetricFlowParsing:
    def test_simple_metric(self):
        path = _write_manifest(MANIFEST_METRICFLOW)
        metrics = parse_manifest(path)
        revenue = next(m for m in metrics if m.name == "total_revenue")

        assert revenue.metric_type == "simple"
        assert revenue.compile_hint == "structured"
        assert "SUM(amount)" in revenue.sql_template
        assert "orders" in revenue.sql_template.lower()
        assert "status = 'paid'" in revenue.sql_template
        assert revenue.tags == ["finance", "revenue"]
        assert revenue.omx_meta.tier == "core"
        assert revenue.omx_meta.autoresearch is True
        assert revenue.omx_meta.scorer == "revenue_correlation"

    def test_simple_metric_string_measure_ref(self):
        path = _write_manifest(MANIFEST_METRICFLOW)
        metrics = parse_manifest(path)
        orders = next(m for m in metrics if m.name == "order_count")

        assert orders.metric_type == "simple"
        assert "COUNT(id)" in orders.sql_template

    def test_ratio_metric_flagged_opaque(self):
        path = _write_manifest(MANIFEST_METRICFLOW)
        metrics = parse_manifest(path)
        churn = next(m for m in metrics if m.name == "churn_rate")

        assert churn.metric_type == "ratio"
        assert churn.compile_hint == "opaque"
        assert churn.compile_note is not None
        assert "churned_count" in churn.compile_note
        assert "active_count" in churn.compile_note

    def test_ratio_splits_into_components(self):
        path = _write_manifest(MANIFEST_METRICFLOW)
        metrics = parse_manifest(path)

        # Component metrics should be in the list
        names = [m.name for m in metrics]
        assert "churned_count" in names
        assert "active_count" in names

        churned = next(m for m in metrics if m.name == "churned_count")
        assert churned.compile_hint == "structured"
        assert "SUM(is_churned)" in churned.sql_template

    def test_derived_metric_flagged_opaque(self):
        path = _write_manifest(MANIFEST_METRICFLOW)
        metrics = parse_manifest(path)
        ltv = next(m for m in metrics if m.name == "ltv_estimate")

        assert ltv.metric_type == "derived"
        assert ltv.compile_hint == "opaque"
        assert ltv.compile_note == "derived metric"

    def test_total_metric_count(self):
        """5 metrics defined + 2 ratio components = 7 total."""
        path = _write_manifest(MANIFEST_METRICFLOW)
        metrics = parse_manifest(path)
        assert len(metrics) == 7

    def test_default_omx_meta(self):
        path = _write_manifest(MANIFEST_METRICFLOW)
        metrics = parse_manifest(path)
        orders = next(m for m in metrics if m.name == "order_count")

        assert orders.omx_meta.tier == "standard"
        assert orders.omx_meta.autoresearch is False


# ---------------------------------------------------------------------------
# Tests: Legacy format
# ---------------------------------------------------------------------------

class TestLegacyParsing:
    def test_legacy_sum(self):
        path = _write_manifest(MANIFEST_LEGACY)
        metrics = parse_manifest(path)
        assert len(metrics) == 1

        revenue = metrics[0]
        assert revenue.name == "total_revenue"
        assert "SUM(amount)" in revenue.sql_template
        assert "analytics.orders" in revenue.sql_template
        assert "status = 'paid'" in revenue.sql_template
        assert revenue.time_column == "created_at"
        assert revenue.omx_meta.tier == "core"


# ---------------------------------------------------------------------------
# Tests: Hash comparison / sync plan
# ---------------------------------------------------------------------------

class TestSyncPlan:
    def test_all_create_on_empty_state(self):
        path = _write_manifest(MANIFEST_METRICFLOW)
        metrics = parse_manifest(path)
        actions = compute_sync_plan(metrics, {})

        assert all(a.action == "create" for a in actions)

    def test_unchanged_on_same_hash(self):
        path = _write_manifest(MANIFEST_METRICFLOW)
        metrics = parse_manifest(path)

        # First run: build state
        state = {m.name: m.hash_key() for m in metrics}
        # Second run: should all be unchanged
        actions = compute_sync_plan(metrics, state)
        assert all(a.action == "unchanged" for a in actions)

    def test_update_on_changed_description(self):
        path = _write_manifest(MANIFEST_METRICFLOW)
        metrics = parse_manifest(path)

        state = {m.name: m.hash_key() for m in metrics}
        # Mutate one metric
        metrics[0].description = "Changed description"
        actions = compute_sync_plan(metrics, state)

        changed = [a for a in actions if a.action == "update"]
        unchanged = [a for a in actions if a.action == "unchanged"]
        assert len(changed) == 1
        assert len(unchanged) == len(metrics) - 1

    def test_dry_run_format(self):
        path = _write_manifest(MANIFEST_METRICFLOW)
        metrics = parse_manifest(path)
        actions = compute_sync_plan(metrics, {})
        output = format_dry_run(actions)

        assert "Found 7 metrics" in output
        assert "total_revenue" in output
        assert "churn_rate" in output
        assert "[create" in output
        assert "structured" in output
        assert "opaque" in output
        assert "tier: core" in output

    def test_sync_state_persistence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state = {"total_revenue": "abc123", "order_count": "def456"}
            save_sync_state(tmpdir, state)
            loaded = load_sync_state(tmpdir)
            assert loaded == state


# ---------------------------------------------------------------------------
# Tests: API payload
# ---------------------------------------------------------------------------

class TestApiPayload:
    def test_simple_metric_payload(self):
        path = _write_manifest(MANIFEST_METRICFLOW)
        metrics = parse_manifest(path)
        revenue = next(m for m in metrics if m.name == "total_revenue")

        payload = revenue.to_api_payload()
        assert payload["name"] == "total_revenue"
        assert "SUM(amount)" in payload["sql_template"]
        assert payload["tags"] == ["finance", "revenue"]
        assert payload["meta"]["tier"] == "core"
        assert payload["meta"]["autoresearch"] is True

    def test_default_meta_not_included(self):
        path = _write_manifest(MANIFEST_METRICFLOW)
        metrics = parse_manifest(path)
        orders = next(m for m in metrics if m.name == "order_count")

        payload = orders.to_api_payload()
        assert "meta" not in payload  # standard tier, no autoresearch


# ---------------------------------------------------------------------------
# Tests: profiles.yml parsing
# ---------------------------------------------------------------------------

PROFILES_POSTGRES = """
my_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: analytics
      password: secret123
      database: warehouse
      schema: public
    prod:
      type: postgres
      host: db.example.com
      port: 5432
      user: analytics_prod
      password: "{{ env_var('DBT_PROD_PASSWORD') }}"
      database: warehouse
      schema: analytics
"""

PROFILES_SNOWFLAKE = """
snow_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: xy12345.us-east-1
      user: dbt_user
      password: snowpass
      database: ANALYTICS
      schema: PUBLIC
      warehouse: COMPUTE_WH
      role: ANALYST
"""


def _write_profiles(content: str) -> str:
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False)
    tmp.write(content)
    tmp.close()
    return tmp.name


class TestProfilesParsing:
    def test_postgres_dev(self):
        path = _write_profiles(PROFILES_POSTGRES)
        profile = parse_profiles(path, profile_name="my_project", target_name="dev")

        assert profile.profile_name == "my_project"
        assert profile.target_name == "dev"
        assert profile.ds_type == "postgres"
        assert profile.host == "localhost"
        assert profile.port == 5432
        assert profile.user == "analytics"
        assert profile.password == "secret123"
        assert profile.database == "warehouse"
        assert profile.schema == "public"

    def test_postgres_prod_with_env_var(self):
        path = _write_profiles(PROFILES_POSTGRES)
        os.environ["DBT_PROD_PASSWORD"] = "prod_secret_pw"
        try:
            profile = parse_profiles(path, profile_name="my_project", target_name="prod")
            assert profile.password == "prod_secret_pw"
            assert profile.host == "db.example.com"
        finally:
            del os.environ["DBT_PROD_PASSWORD"]

    def test_env_var_missing_raises(self):
        path = _write_profiles(PROFILES_POSTGRES)
        os.environ.pop("DBT_PROD_PASSWORD", None)
        with pytest.raises(EnvironmentError, match="DBT_PROD_PASSWORD"):
            parse_profiles(path, profile_name="my_project", target_name="prod")

    def test_snowflake(self):
        path = _write_profiles(PROFILES_SNOWFLAKE)
        profile = parse_profiles(path, profile_name="snow_project")

        assert profile.ds_type == "snowflake"
        assert profile.account == "xy12345.us-east-1"
        assert profile.warehouse == "COMPUTE_WH"
        assert profile.role == "ANALYST"

    def test_default_target(self):
        path = _write_profiles(PROFILES_POSTGRES)
        profile = parse_profiles(path, profile_name="my_project")
        assert profile.target_name == "dev"  # default target from profile

    def test_datasource_name_default(self):
        path = _write_profiles(PROFILES_POSTGRES)
        profile = parse_profiles(path, profile_name="my_project", target_name="dev")
        assert profile.datasource_name == "default"

    def test_datasource_name_override(self):
        path = _write_profiles(PROFILES_POSTGRES)
        profile = parse_profiles(path, profile_name="my_project", target_name="dev")
        profile.name_override = "my_warehouse"
        assert profile.datasource_name == "my_warehouse"

    def test_connect_payload(self):
        path = _write_profiles(PROFILES_POSTGRES)
        profile = parse_profiles(path, profile_name="my_project", target_name="dev")
        payload = profile.to_connect_payload()

        assert payload["type"] == "postgres"
        assert payload["name"] == "default"
        assert payload["host"] == "localhost"
        assert payload["port"] == 5432
        assert payload["user"] == "analytics"
        assert payload["password"] == "secret123"
        assert payload["database"] == "warehouse"

    def test_display_summary_masks_password(self):
        path = _write_profiles(PROFILES_POSTGRES)
        profile = parse_profiles(path, profile_name="my_project", target_name="dev")
        summary = profile.display_summary()

        assert "my_project" in summary
        assert "postgres" in summary
        assert "secret123" not in summary
        assert "********" in summary

    def test_invalid_profile_name(self):
        path = _write_profiles(PROFILES_POSTGRES)
        with pytest.raises(ValueError, match="not found"):
            parse_profiles(path, profile_name="nonexistent")

    def test_invalid_target_name(self):
        path = _write_profiles(PROFILES_POSTGRES)
        with pytest.raises(ValueError, match="not found"):
            parse_profiles(path, profile_name="my_project", target_name="staging")


class TestResolveEnvVars:
    def test_simple_env_var(self):
        os.environ["TEST_VAR"] = "hello"
        try:
            assert _resolve_env_vars("{{ env_var('TEST_VAR') }}") == "hello"
        finally:
            del os.environ["TEST_VAR"]

    def test_env_var_with_default(self):
        os.environ.pop("MISSING_VAR", None)
        assert _resolve_env_vars("{{ env_var('MISSING_VAR', 'fallback') }}") == "fallback"

    def test_non_string_passthrough(self):
        assert _resolve_env_vars(5432) == 5432
        assert _resolve_env_vars(None) is None

    def test_no_env_var_passthrough(self):
        assert _resolve_env_vars("just a string") == "just a string"
