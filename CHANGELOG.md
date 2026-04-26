# Changelog

## v0.6.8 — 2026-04-26

### New: `omx propose` — discover metrics from the warehouse schema

`omx dbt sync` only sees what your dbt project exposes. `omx propose` reads
the live warehouse `information_schema` and infers metrics from the actual
tables and columns.

```bash
omx propose                        # writes proposed.yml in CWD
omx propose --schemas public,sales # multi-schema scan
omx propose --no-dedupe            # don't filter against .omx/ir.json
```

Reuses the same heuristic engine the cloud auto-suggest uses (pattern
templates first — retail, subscription, etc. — then generic rules). Output
is `proposed.yml`: top-level `metrics:` array each marked
`confidence: high|medium|low`. Review, edit, then either copy entries into
your canonical metrics.yml or `omx metrics import proposed.yml` (cloud).

By default dedupes against `.omx/ir.json` so `omx dbt sync && omx propose`
gives you "what dbt didn't see."

On the demo Postgres (4 tables, 44 columns): dbt sync finds 7 metrics,
propose finds 15 more — `avg_order_value`, `customer_count`,
`revenue_by_country`, etc. Total surface 7 → 22.

## v0.6.7 — 2026-04-26

### Fix

- `__version__` in `onlymetrix/__init__.py` was stuck at `0.5.0` (since v0.5.0)
  and not bumped in step with `pyproject.toml`. `rust_bridge.py` uses
  `__version__` to pick which `omx` binary to download, so the stale value
  caused `pip install onlymetrix==0.6.6` to download the v0.6.5 binary
  (which doesn't know about `omx mcp serve`). v0.6.6 is yanked; install
  v0.6.7 instead.

## v0.6.6 — 2026-04-26 (yanked)

### New: local MCP server + runtime reliability gating

- **`omx mcp serve`** — inline stdio MCP server. Loads metrics from
  `.omx/ir.json`, connects to the warehouse via `OMX_WAREHOUSE_URL`/`DATABASE_URL`,
  serves the same tool surface as `dataquery-mcp` from the single `omx` binary
  (no separate install).
- **`omx mcp config`** — print the Claude Code MCP config snippet.
- **Runtime drift detection** — every `query_metric` call does a pre-flight
  reliability check against the live `information_schema`. Refuses (does not
  fabricate) below 50% reliability score. Detects column rename, table missing,
  table empty.
- **Three new MCP tools**: `check_reliability`, `trace_dependencies`,
  `affected_by` (table → metric blast-radius).
- **Refusal payload** structured with `reliability_score`, typed `issues`,
  `affected_metrics` cascade, `suggested_actions`. Writes a synthetic
  `omx-ci-output.json` so `omx scaffold --fix` can act on the detected rename
  immediately.
- **`omx scaffold --fix`** now also edits `.omx/ir.json` directly when a metric
  has no top-level `metrics:` YAML to rewrite (the dbt-sync flow's common case).
- `mcp` added to `RUST_SUBCOMMANDS` so `omx mcp serve|config` route directly to
  the Rust binary.

### Demo

- `scripts/demo/` (in the upstream monorepo): setup.sh, seed.sql (200
  customers, 50 products, 5000 invoices), simulate_drift.py (rename
  total_amount → amount; truncate products), restore.py, RUNBOOK.md.
  Reproducible end-to-end against an isolated Postgres container — no cloud,
  no API key.

### Tests

- 17 new `reliability::local` unit tests (drift classification, fuzzy match,
  cascade, synthetic CI output).
- 4 new `tools` tests covering the three new tool definitions.
- 5 new `local_mcp` tests (IR load, DSN resolution, config snippet).
- Full `cargo test --lib` green: 847 passed.

## v0.6.5

### New features
- `meta.onlymetrix.tier` on a dbt model now overrides the compiler's
  heuristic tier assignment for every metric inferred from that model:

  ```yaml
  models:
    - name: orders
      meta:
        onlymetrix:
          tier: core        # core | standard | foundation
          label: "Orders"   # optional display label
  ```

  Metrics inferred from `orders` — regardless of whether they come
  from MetricFlow or raw-SQL parsing — will be tagged with the chosen
  tier. The heuristic classifier still runs as a fallback when no
  override is present.

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
