# Changelog

## 0.2.0 (2026-03-29)

### Added
- **Analysis reasoning layer** — 13 analysis primitives: root_cause, correlate, threshold, sensitivity, segment_performance, contribution, drivers, anomalies, pareto, trends, forecast, compare, health
- **Custom analysis framework** — `@om.analysis.custom` decorator, JSON DAG export, server-side storage and sharing
- **Full API coverage** — 35/35 endpoints (setup, auth, compiler, autoresearch, admin)
- **CLI** — `omx` command with 30+ subcommands
- **MetricKind** — compiler infers Aggregate vs EntitySet from SQL
- **Stored ground truth** — `ground_truth_sql` on metrics for cold-start autoresearch
- **Segment filters** — autoresearch per-segment precision/recall scoring

### Fixed
- AVG post-aggregation in pre-agg cache
- Entity column auto-detection in autoresearch
- Aggregate metric rejection with actionable error messages

## 0.1.0 (2026-03-21)

### Added
- Initial release
- Sync and async clients (`OnlyMetrix`, `AsyncOnlyMetrix`)
- Metric query, search, and listing
- Table schema discovery
- Raw SQL execution
- Metric requests lifecycle
- LangChain and CrewAI integrations
