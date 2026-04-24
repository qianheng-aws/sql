# Archived plans

Superseded or discarded plans kept for historical context.

- **`2026-04-20-index-insight-ppl-integration.md` / `-context.md`**: the
  original design for consuming table statistics from the ml-commons Index
  Insight framework (rather than owning the subsystem inside the SQL plugin).
  Rejected on 2026-04-20 after live-cluster verification surfaced cross-plugin
  classloader and transitive Guava version conflicts — full rationale in
  [`docs/dev/table-statistics-design.md`](../../../dev/table-statistics-design.md) §3.1.
