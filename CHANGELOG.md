# Changelog

## v0.6.4

### New features
- Metric inference now walks the SQL AST of each dbt model and surfaces
  every aliased aggregate call (SUM, AVG, COUNT, COUNT DISTINCT, MIN,
  MAX) as a candidate metric. Previous inference depended on schema.yml
  `data_type` declarations and naming conventions (`id`, `is_*`) that
  most real dbt projects don't follow — jaffle_shop produced 0 metrics
  on 0.6.3, produces 4 here. Benchmark_dbt goes from 6 to 16 metrics
  with no regression.
- Works with `dbt parse` alone — no warehouse connection required. The
  compiler strips Jinja before parsing so pre-compile manifests still
  give usable output. When `dbt compile` has been run and
  `compiled_code` is present, it's preferred over `raw_code`.

## v0.6.3

### Changed
- `omx dbt sync` footer now only shows commands that actually work in
  the local free-tier flow: `cat .omx/ir.json` for inspection, plus the
  app.onlymetrix.com signup link for the cloud-only commands. The
  previous `omx metrics list` / `omx query --metric X` hints errored
  with `Connection refused` on a fresh install.

## v0.6.2

### New features
- `omx dbt sync --project-dir .` now works **without any OnlyMetrix
  account**. Parses `target/manifest.json`, infers metrics from dbt
  model SQL locally via the bundled Rust engine, and writes
  `.omx/ir.json`. Zero server dependency, works offline.
- Set `OMX_API_KEY` to additionally sync the compiled IR to the cloud
  for reliability scoring, canvas dashboards, and team features — same
  command, same compile, just an extra step when credentials are present.

### Changed
- `omx dbt sync` is now orchestrated by the Python CLI (click), not the
  Rust binary. Rust handles the inference pass via a new internal
  `omx dbt compile --manifest <path> --out <path>` subcommand that
  Python shells into.
- Free vs. paid signal: presence of `OMX_API_KEY`. No feature flags.
- Output now shows a tier table (name / tier / SQL summary) instead of
  a server-returned compile summary.

## v0.6.1

### New features
- Rust `omx` binary now ships for **linux-x64, linux-arm64, macos-arm64,
  windows-x64**. Apple Silicon Macs, arm64 Linux (Graviton, GH Actions
  arm runners, Raspberry Pi), and Windows laptops all work without
  setting `OMX_BINARY`.

### Internal
- `rust_bridge.py` learns Windows (`omx.exe` handling, no `chmod +x`),
  linux-arm64, and drops the never-shipped macos-x64 branch.
- Release pipeline builds all four targets on tag push via GitHub
  Actions matrix in the private `dreynow/onlymetrix` repo, which
  uploads tarballs to this repo's release.

## v0.6.0

### New features
- `omx ci check` — dbt manifest vs IR baseline diff for CI/CD. Detects breaking
  column changes (drops, possible renames), enforces metric tier severity
  (core/critical blocks, standard warns, foundation is info-only), and lists
  affected canvas dashboards plus a decisions-at-risk touchpoint.
- `omx ci snapshot` — write `.omx/ir.lock.json` for repos that commit their
  baseline instead of hitting the API at CI time.
- `omx dbt sync` — push a dbt manifest to the OM catalog (moved from Rust CLI
  into the unified `omx` surface).
- Rust binary ships as a GitHub Release asset and is fetched lazily on first
  use of a Rust-owned subcommand. Set `OMX_BINARY=/path/to/omx` to skip the
  download (dev loops, pre-seeded CI caches).
- Platforms supported at launch: linux-x64. macos-x64 / macos-arm64 /
  linux-arm64 coming in follow-up releases.

### Internal
- New `onlymetrix.rust_bridge` module handles platform detection, binary cache
  (`~/.cache/onlymetrix/omx-<version>`), and dispatch.
- CLI entry point changed from `onlymetrix.cli:cli` to `onlymetrix.cli:main`
  so Rust subcommands can intercept before click parses.

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
