
================
Table Statistics
================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Introduction
============

The table-statistics subsystem collects per-index and per-field data profiles — row count, approximate distinct count, min / max, null ratio — and feeds them to Calcite's cost-based optimizer so that join order, filter selectivity, and aggregate rowcount reflect real data instead of Calcite's built-in guesses. The subsystem is gated behind ``plugins.calcite.table_statistics.enabled`` (default ``false``) and only runs when the Calcite engine itself is also enabled.

When it is off, the optimizer's ``IS NULL``, ``=``, range, join-size, and aggregate-rowcount estimates all fall back to the default Calcite heuristics (``guessSelectivity``, ``input / 10``, ``maxResultWindow=10000``). Turning it on is a no-op until at least one index has been analyzed.

Enabling
========

::

    PUT /_cluster/settings
    {
      "persistent": {
        "plugins.calcite.enabled":                        "true",
        "plugins.calcite.table_statistics.enabled":       "true"
      }
    }

The subsystem stores its records in a dedicated system index ``.opensearch-statistics``. The index is created lazily on first write and its mapping is fixed; operators do not need to provision it manually.

REST API
========

Read the current statistic for an index::

    GET /_plugins/_sql/_statistics/{index}

Returns a JSON document with ``status``, ``last_updated_time``, ``doc_count``, and a per-field block. ``status=COMPLETED`` is the only status a consumer should trust — ``GENERATING`` and ``FAILED`` are transient collector states.

Synchronously trigger a refresh::

    POST /_plugins/_sql/_statistics/{index}/analyze

Returns ``202 Accepted`` immediately; the collection runs in the background. Poll the ``GET`` endpoint until ``status`` is ``COMPLETED``.

Both endpoints accept a single concrete index name. Wildcards (``logs-*``), comma-separated lists (``logs,events``), and date-math expressions (``<logs-{now/d}>``) are rejected with ``400 Bad Request``.

Automatic refresh
=================

Once the subsystem is enabled, the cluster-manager node runs a periodic sweep that re-collects any statistic older than ``plugins.calcite.table_statistics.ttl`` (default ``24h``). The sweep runs every ``plugins.calcite.table_statistics.refresh_interval`` seconds (default ``60s``) and is capped at ``plugins.calcite.table_statistics.refresh_max_in_flight`` concurrent refreshes (default ``4``).

For most clusters the defaults are sufficient — no manual ``analyze`` calls are needed after the first one. If you lower the TTL for testing, the sweep catches up on the next tick; if you raise it for load reduction, cached stats simply persist longer.

The optimizer consumes whatever record is currently in ``.opensearch-statistics`` at query time, even if the record is older than the TTL. A stale record is still better than the Calcite default, so the query path never blocks on a refresh.

Operational guidance
====================

Newly created indices
---------------------
The first query against a new index does not have statistics available. The optimizer uses the defaults (``maxResultWindow = 10000``) for planning, and the query-path miss schedules an asynchronous refresh. The *second* query typically sees statistics — not the first.

To have statistics ready before production traffic, call ``POST /_plugins/_sql/_statistics/{index}/analyze`` explicitly after bulk-loading an index.

Skewed data
-----------
Range selectivity (``>``, ``<``, ``BETWEEN``) uses linear interpolation over the stored ``[min, max]``. This is accurate on uniformly distributed fields and degrades on heavily skewed fields. Equality selectivity (``col = literal``) is based on cardinality (``1 / distinct_count``) and is unbiased. If range queries on a skewed column produce poor plans, leave the rest of the subsystem on and rewrite the query with equality predicates where possible; a histogram-backed estimator is tracked as future work.

Under cluster load
------------------
When the search thread pool is saturated, background refresh attempts are rejected by OpenSearch. The subsystem treats this as "cluster busy, try again later" — it does **not** mark the record as ``FAILED``. The next tick of the cron will retry automatically. If this persists, raise ``plugins.calcite.table_statistics.refresh_interval`` or lower ``plugins.calcite.table_statistics.refresh_max_in_flight``.

Disabling temporarily
---------------------
Setting ``plugins.calcite.table_statistics.enabled=false`` stops the background sweep and forces the optimizer to ignore stored statistics. Existing records are not deleted, so turning the flag back on resumes consumption without re-analyze.

Observability
=============

Three counters are exposed through the PPL stats endpoint::

    GET /_plugins/_ppl/stats

Returned as plain JSON fields:

- ``table_statistics_refresh_success_count`` — number of successful ``COMPLETED`` writes (cron-triggered or ``POST /analyze``-triggered).
- ``table_statistics_refresh_failure_count`` — number of ``FAILED`` markers written. Rejection (cluster busy) is **not** counted here; use cluster-level metrics for that.
- ``table_statistics_read_timeout_count`` — number of times the optimizer's 500 ms read of ``.opensearch-statistics`` timed out and fell back to ``UNKNOWN``.

These are node-local counters and do not reset across restarts until the node process cycles.

Limitations
===========

- Statistics cover single concrete indices only. Aliases and wildcards are not supported by the REST API; the Calcite optimizer operates on a single index per scan, so this matches the cost-model consumer surface.
- Range selectivity assumes uniform distribution — see "Skewed data" above.
- The subsystem does not propagate statistics across cluster restarts beyond what is persisted in ``.opensearch-statistics``. Data nodes have no local cache.
- Text fields without a ``.keyword`` sub-field produce doc-count-only statistics (no cardinality, no min/max). This is a mapping choice outside the subsystem's control.
