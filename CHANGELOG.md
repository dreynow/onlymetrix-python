# Changelog

## v0.4.2

### Bug fixes
- Fixed: `__version__` synced to match `pyproject.toml` (was `0.3.2`, now matches declared version)
- Fixed: `AsyncOnlyMetrix` missing `custom_analyses`, `server_analysis`, `reliability` resources (raised `AttributeError`)
- Fixed: Async `metrics.query()` missing `period` parameter — now matches sync API contract
- Fixed: Async autoresearch used blocking `time.sleep()` — replaced with `asyncio.sleep()`
- Fixed: Silent `except Exception: pass` replaced with `logging.warning()`/`debug()` in analysis, export
- Fixed: `_handle_response()` now gives clear `OnlyMetrixError` for non-JSON 200 responses (was `JSONDecodeError`)
- Fixed: Hardcoded DB credentials in `datasets/retail/import.py` replaced with `OMX_RETAIL_DB` env var
- Fixed: `from_dict()` methods now raise `ValueError` with field name instead of bare `KeyError`
- Fixed: Autoresearch `poll_interval` and `poll_timeout` are now configurable (default 1.5s / 300s)
- Fixed: CLI `_get_client()` accepts `url`/`api_key` params directly — no longer mutates `os.environ`
- Fixed: Added `logging.getLogger(__name__)` throughout SDK (client, analysis, export)
- Fixed: Dependency upper bounds added for `langchain-core` and `crewai` optional extras
- Fixed: Hardcoded output path in brand asset generator

### New features
- Added: SQL-to-Semantic-Layer converter (`onlymetrix.sql_converter`)
  - `convert_sql(sql, name, description, tags)` — single SQL → metric dict
  - `convert_sql_batch(sources)` — batch conversion
  - `extract_sql(sql)` — full extraction with aggregations, dimensions, warnings
  - `convert_sql_file(path)` / `convert_sql_directory(dir)` — file/directory conversion
  - `metrics_to_yaml(metrics)` — output as OnlyMetrix YAML
- Added: CLI commands `omx sql convert`, `omx sql convert-batch`, `omx sql inspect`
- Added: `sqlglot` as optional dependency (`pip install onlymetrix[sql]`)
- Added: Async docstrings for all resource classes
- Added: GitHub Actions workflow for syncing SDK to `onlymetrix-python` public repo

## v0.4.1

- Fixed: `agg_time_dimension` now set on every measure (MetricFlow 1.11.7 requirement)
- Fixed: `model: ref()` resolved from catalog source_tables instead of metric name
- Fixed: `measure.expr` uses source column (e.g. `total_amount`) not output alias
- Fixed: dbt-sourced metrics filtered from export by default (`--all-sources` to override)
- Fixed: `avg_order_value` classified as structured (was opaque due to AVERAGE agg mapping)
- Fixed: Primary entity auto-generated for single-table semantic models (required when dimensions present)
- Added: `omx export --all-sources` flag to include non-dbt metrics in export

## v0.3.0

- Added: `omx dbt sync` command — sync dbt metrics to OnlyMetrix
- Added: dbt manifest parser with MetricFlow and legacy format support
- Added: MetricFlow-to-SQL translator (simple, ratio, derived)
- Added: `--dry-run` flag with action column output (create/update/unchanged/delete)
- Added: `--strict` flag for CI — exits non-zero if any metric is opaque or failed
- Added: `--manifest` flag to specify manifest.json path
- Added: Hash-based skip-unchanged logic (SHA256 of metric definition)
- Added: Ratio metric splitting — components synced as Structured, ratio as Opaque
- Added: `meta.onlymetrix` support for tier, autoresearch, scorer, pii_columns

## v0.2.0

- Added: Python SDK with sync and async clients
- Added: `omx` CLI with metrics, tables, query, setup, compiler, autoresearch, analysis commands
- Added: LangChain and CrewAI integrations
- Added: Analysis reasoning primitives (pareto, segment-performance, contribution, drivers, anomalies, trends, compare)
- Added: Custom analysis DAG engine

## v0.1.0

- Initial release: OnlyMetrix Python SDK
- Sync client with metrics, tables, metric requests
- API key authentication
