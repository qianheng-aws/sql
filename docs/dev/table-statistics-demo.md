# Table Statistics — Demo Script

Walk-through script for showing how stored statistics affect Calcite's plan on a live 2-index dataset. Every step has a "stats ON vs OFF" side-by-side so the effect is visible on the `explain cost` output.

**Audience:** Reviewers / maintainers who want to see the subsystem work without reading code.

**Prerequisite:** Local OpenSearch 3.6 with this branch's SQL plugin installed. Cluster must have `plugins.calcite.enabled = true`; the demo flips `plugins.calcite.table_statistics.enabled` to toggle stats on/off.

**Shell convention:** all commands hit `localhost:9200` and use `python3 -m json.tool` for pretty output. Replace host if remote.

---

## 0. Setup — two indices that exercise every hook

```bash
# Clean slate
for idx in demo-events demo-users .opensearch-statistics; do
  curl -s -X DELETE "http://localhost:9200/${idx}" >/dev/null
done

# demo-events: 2000 docs, 3 distinct statuses, latency 1..2000, user_id 1..50
curl -s -X PUT "http://localhost:9200/demo-events" \
  -H 'Content-Type: application/json' -d '{
    "mappings": {"properties":{
      "status":  {"type":"keyword"},
      "latency": {"type":"long"},
      "user_id": {"type":"keyword"},
      "region":  {"type":"keyword"}
    }}}' >/dev/null

python3 - <<'PY' | curl -s -X POST "http://localhost:9200/demo-events/_bulk?refresh=true" \
  -H 'Content-Type: application/json' --data-binary @- >/dev/null
import random, json
random.seed(42)
statuses = ['OK'] * 85 + ['WARN'] * 12 + ['ERROR'] * 3      # skewed distribution (85/12/3)
regions  = ['us-east', 'us-west', 'eu']
for i in range(1, 2001):
    print(json.dumps({"index": {"_id": str(i)}}))
    print(json.dumps({
        "status":  random.choice(statuses),
        "latency": random.randint(1, 2000),
        "user_id": f"u{random.randint(1, 50)}",
        "region":  random.choice(regions),
    }))
PY

# demo-users: 50 docs, one per user_id
curl -s -X PUT "http://localhost:9200/demo-users" \
  -H 'Content-Type: application/json' -d '{
    "mappings": {"properties":{
      "user_id": {"type":"keyword"},
      "name":    {"type":"keyword"},
      "tier":    {"type":"keyword"}
    }}}' >/dev/null

python3 - <<'PY' | curl -s -X POST "http://localhost:9200/demo-users/_bulk?refresh=true" \
  -H 'Content-Type: application/json' --data-binary @- >/dev/null
import json
tiers = ['free', 'pro', 'enterprise']
for i in range(1, 51):
    print(json.dumps({"index": {"_id": str(i)}}))
    print(json.dumps({
        "user_id": f"u{i}",
        "name":    f"user{i}",
        "tier":    tiers[i % 3],
    }))
PY

curl -s "http://localhost:9200/_cat/count/demo-events,demo-users?h=index,count"
# Expect: demo-events 2000, demo-users 50
```

## 0.1 — Enable the subsystem

```bash
curl -s -X PUT "http://localhost:9200/_cluster/settings" \
  -H 'Content-Type: application/json' -d '{
    "persistent": {
      "plugins.calcite.enabled":                 "true",
      "plugins.calcite.table_statistics.enabled":"true"
    }}' | python3 -m json.tool
```

## 0.2 — Trigger stats collection

```bash
curl -s -X POST "http://localhost:9200/_plugins/_sql/_statistics/demo-events/analyze"
curl -s -X POST "http://localhost:9200/_plugins/_sql/_statistics/demo-users/analyze"
sleep 3

# Verify both COMPLETED
for idx in demo-events demo-users; do
  echo "=== $idx ==="
  curl -s "http://localhost:9200/_plugins/_sql/_statistics/${idx}" | python3 -m json.tool
done
```

**What to point out in the output:**
- `doc_count`: real row count (2000 / 50).
- `status.unique_count = 3`, `region.unique_count = 3`, `user_id.unique_count = 50` (approx HLL, exact on small cardinality).
- `latency.min_value ≈ 1`, `max_value ≈ 2000` — read via top-level aggs (Lucene BKD short-circuit, exact).
- `null_ratio = 0.0` across fields (full coverage in this dataset).

---

## 1. TableScan row count — the foundation

**Stats OFF** → Calcite falls back to `max_result_window = 10000`. Every cost computation upstream uses this fake baseline.

**Stats ON** → `TableStatistic.getRowCount()` returns the real `doc_count = 2000`.

```bash
# --- OFF ---
curl -s -X PUT "http://localhost:9200/_cluster/settings" \
  -H 'Content-Type: application/json' \
  -d '{"persistent":{"plugins.calcite.table_statistics.enabled":"false"}}' >/dev/null

curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain" \
  -H 'Content-Type: application/json' -d '{
    "query": "source=demo-events",
    "mode":  "cost"
  }' | python3 -m json.tool

# --- ON ---
curl -s -X PUT "http://localhost:9200/_cluster/settings" \
  -H 'Content-Type: application/json' \
  -d '{"persistent":{"plugins.calcite.table_statistics.enabled":"true"}}' >/dev/null

curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain" \
  -H 'Content-Type: application/json' -d '{
    "query": "source=demo-events",
    "mode":  "cost"
  }' | python3 -m json.tool
```

**Compare:** `CalciteLogicalIndexScan(...: rowcount = X)`
- OFF: `rowcount = 10000` (maxResultWindow)
- ON: `rowcount = 2000` (stored doc_count)

**Why it matters:** every downstream estimator multiplies against this baseline. Getting it right is the precondition for all of the below.

---

## 2. Filter selectivity — equality (`status = 'OK'`)

**Stats OFF** → Calcite's `guessSelectivity` returns `0.15` for any equality predicate.

**Stats ON** → `TableStatisticSelectivityHandler` returns `1 / cardinality(status) = 1/3 ≈ 0.3333`.

```bash
# toggle to OFF, then:
curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain" \
  -H 'Content-Type: application/json' -d '{
    "query": "source=demo-events | where status = \"OK\"",
    "mode":  "cost"
  }' | python3 -c 'import json,sys; p=json.load(sys.stdin)["calcite"]["logical"]; print(p)'
```

Then flip to ON and re-run the same query.

**Compare:** `LogicalFilter(...: rowcount = X)`
- OFF: `10000 × 0.15 = 1500`
- ON: `2000 × 1/3 ≈ 666.67`

**Caveat — the skew:** real data has 85% `OK`, but our estimator says 33%. This is the known equality-selectivity limitation (uniform distribution assumption); the Calcite default is even more wrong, and a histogram is the long-term fix. The **plan quality** is still better because join reorder & aggregate downstream get better NDV.

---

## 3. Filter selectivity — range (`latency BETWEEN 500 AND 1500`)

**Stats OFF** → two arbitrary `0.5` defaults for range predicates → combined `0.25`.

**Stats ON** → `TableStatisticSelectivityHandler` sees `SEARCH(latency, Sarg[[500..1500]])`, calls `RexUtil.expandSearch` to flatten into two comparisons, then linear-interpolates over stored `[min=1, max=2000]`:

```
selectivity = (1500 - 500) / (2000 - 1) ≈ 0.5003
```

```bash
curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain" \
  -H 'Content-Type: application/json' -d '{
    "query": "source=demo-events | where latency >= 500 AND latency <= 1500",
    "mode":  "cost"
  }' | python3 -c 'import json,sys; p=json.load(sys.stdin)["calcite"]["logical"]; print(p)'
```

**Compare:** `LogicalFilter(...: rowcount = X)`
- OFF: `10000 × 0.25 = 2500`
- ON: `2000 × 0.5003 ≈ 1000.5`

**Point to mention:** without `expandSearch` the handler would see one opaque `SEARCH` conjunct and give up — this was caught during Phase 1b cluster verify.

---

## 4. IS NULL / IS NOT NULL — null_ratio consumption

The null_ratio metric only became meaningful in M2 Phase 3 (before, the collector wrote `0.0` and the handler treated IS NULL as "no info"). Our dataset has 100% coverage (null_ratio = 0.0), so:

**Stats OFF** → default guess `0.25` for IS NULL.

**Stats ON** → `IS NULL` → `null_ratio = 0`; `IS NOT NULL` → `1 - null_ratio = 1`.

```bash
curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain" \
  -H 'Content-Type: application/json' -d '{
    "query": "source=demo-events | where isnotnull(status)",
    "mode":  "cost"
  }' | python3 -c 'import json,sys; p=json.load(sys.stdin)["calcite"]["logical"]; print(p)'
```

**Compare:** `LogicalFilter(...: rowcount = X)`
- OFF: `10000 × 0.75 = 7500`
- ON: `2000 × 1.0 = 2000` (optimizer can prove the filter is a no-op for rowcount purposes)

---

## 5. Aggregate rowcount — `DistinctRowCount.Handler`

`stats count() by status` — the classic aggregate-by-single-column case.

**Stats OFF** → `RelMdRowCount.getRowCount(Aggregate)` falls back to `inputRowCount / 10`.

**Stats ON** → our `TableStatisticDistinctRowCountHandler` returns `min(cardinality(status), inputRowCount) = min(3, 2000) = 3`.

```bash
curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain" \
  -H 'Content-Type: application/json' -d '{
    "query": "source=demo-events | stats count() by status",
    "mode":  "cost"
  }' | python3 -c 'import json,sys; p=json.load(sys.stdin)["calcite"]["logical"]; print(p)'
```

**Compare:** `LogicalAggregate(...: rowcount = X)`
- OFF: `10000 / 10 = 1000`
- ON: `3.0`  ← **this is huge for downstream sort/limit costing**

**Multi-column variant:** `stats count() by status, region` → uses `RelMdUtil.numDistinctVals(3 × 3, 2000) ≈ 8.97`. Calcite's inclusion-exclusion: distinct pairs aren't simply 9, because the 2000 rows might not cover every pair.

```bash
curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain" \
  -H 'Content-Type: application/json' -d '{
    "query": "source=demo-events | stats count() by status, region",
    "mode":  "cost"
  }' | python3 -c 'import json,sys; p=json.load(sys.stdin)["calcite"]["logical"]; print(p)'
```

Expect aggregate rowcount ≈ 8.97.

---

## 6. `top N by col` — RareTop stat-aware estimation (M3 #6)

**Stats OFF** → fixed heuristic `N × rowCount × (1 - 0.5^G)`.

**Stats ON (PPL `top 2 status`, no by-columns)**:
- No by → `min(N, rowCount) = min(2, 2000) = 2`.

**Stats ON (PPL `top 5 status by region`, groupCount=1)**:
- `cardinality(region) = 3`, `ndv = numDistinctVals(3, 2000) ≈ 3.0`, emitted = `min(5 × 3, 2000) = 15`.

```bash
# No by-columns
curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain" \
  -H 'Content-Type: application/json' -d '{
    "query": "source=demo-events | top 2 status",
    "mode":  "cost"
  }' | python3 -c 'import json,sys; p=json.load(sys.stdin)["calcite"]["logical"]; print(p)'

# With 1 by-column
curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain" \
  -H 'Content-Type: application/json' -d '{
    "query": "source=demo-events | top 5 status by region",
    "mode":  "cost"
  }' | python3 -c 'import json,sys; p=json.load(sys.stdin)["calcite"]["logical"]; print(p)'
```

**Compare:**
- No by, OFF: `2 × 2000 = 4000` (no group collapse); ON: `2`.
- With by, OFF: `5 × 2000 × (1 - 0.5^1) = 5000`; ON: `15`.

**The contrast is large because the heuristic catastrophically overestimates `top N` output when N × rowCount exceeds the real group count.**

---

## 7. Join reorder — `CoreRules.JOIN_COMMUTE` + metadata rowcount

This is the showcase — **stats change both join order AND join algorithm**.

The query is identical in both runs. What changes is whether stats are available.

```bash
# Force left-side to be the large index in PPL syntax — Calcite should swap it when stats are on.
QUERY='{"query":"source=demo-events | join on demo-events.user_id=demo-users.user_id demo-users","mode":"cost"}'

# --- OFF ---
curl -s -X PUT "http://localhost:9200/_cluster/settings" \
  -H 'Content-Type: application/json' \
  -d '{"persistent":{"plugins.calcite.table_statistics.enabled":"false"}}' >/dev/null
echo "=== stats OFF ==="
curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain" \
  -H 'Content-Type: application/json' -d "$QUERY" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["calcite"]["physical"])'

# --- ON ---
curl -s -X PUT "http://localhost:9200/_cluster/settings" \
  -H 'Content-Type: application/json' \
  -d '{"persistent":{"plugins.calcite.table_statistics.enabled":"true"}}' >/dev/null
echo "=== stats ON ==="
curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain" \
  -H 'Content-Type: application/json' -d "$QUERY" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["calcite"]["physical"])'
```

**Compare:**
| | Physical plan shape | Meaning |
|---|---|---|
| OFF | `EnumerableMergeJoin(demo-events LEFT, demo-users RIGHT)` | Both sides look like 10 000 rows → optimizer picks sort-merge and keeps declared order. |
| ON  | `EnumerableHashJoin(demo-users LEFT, demo-events RIGHT)` + `EnumerableCalc` restores column order | 50 vs 2000 → optimizer swaps to put 50-row side on the build side of a hash join. |

**This is the headline demo.** Two things change at once:
1. **Join order** — small table becomes the build side regardless of PPL-declared order.
2. **Join algorithm** — HashJoin becomes cheaper than MergeJoin once sizes are known; MergeJoin requires sorting both sides which is wasteful when one side fits in memory.

---

## 8. End-to-end query — compound effect

Combine filter + aggregate + join in one query.

```bash
curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain" \
  -H 'Content-Type: application/json' -d '{
    "query": "source=demo-events | where status=\"OK\" AND latency < 500 | join on demo-events.user_id=demo-users.user_id demo-users | stats count() by tier",
    "mode":  "cost"
  }' | python3 -m json.tool
```

**What to point out in the plan:**
- Filter after `demo-events` scan: `2000 × 1/3 × 250/2000 ≈ 83` rows (equality × range compound).
- Join between 83 filtered rows and 50 users → HashJoin with filtered side as probe, users as build.
- Final aggregate by `tier` (cardinality=3) → `LogicalAggregate(...: rowcount ≈ 3)`.

With stats OFF this plan would show `~ 1500 × 0.5 / 10 ≈ 75` at the aggregate and MergeJoin both sides — compounding uncertainty through every step.

---

## 9. Observability — metrics + REST

While the demo runs, show the metrics counters incrementing.

```bash
curl -s "http://localhost:9200/_plugins/_ppl/stats" \
  | python3 -c 'import json,sys; s=json.loads(sys.stdin.read()); [print(f"{k}: {v}") for k,v in s.items() if "table_statistics" in k]'

# Expected after this demo run:
# table_statistics_refresh_success_count: 2   (one per /analyze)
# table_statistics_refresh_failure_count: 0
# table_statistics_read_timeout_count:    0
```

Also the raw stored doc:

```bash
curl -s "http://localhost:9200/_plugins/_sql/_statistics/demo-events" | python3 -m json.tool
```

---

## 10. Cleanup

```bash
for idx in demo-events demo-users; do
  curl -s -X DELETE "http://localhost:9200/${idx}" >/dev/null
done
curl -s -X PUT "http://localhost:9200/_cluster/settings" \
  -H 'Content-Type: application/json' \
  -d '{"persistent":{"plugins.calcite.table_statistics.enabled":null}}' >/dev/null
echo "cleaned"
```

---

## Appendix — mapping each demo section to the roadmap milestone

| Section | Milestone item | What it proves |
|---|---|---|
| §1 | M1 POC + M3 #1 | `doc_count` from `.opensearch-statistics`, no `maxResultWindow` fallback |
| §2 | M3 #3 equality | `Selectivity.Handler` unwrap → `1/cardinality` |
| §3 | M2 Phase 1b | range linear interpolation + `RexUtil.expandSearch` |
| §4 | M2 Phase 3 (null_ratio) + M3 #3 null | IS NULL / IS NOT NULL stat-aware |
| §5 | M3 #2 | `DistinctRowCount.Handler` unwrap |
| §6 | M3 #6 | RareTop stat-aware estimation |
| §7 | M3 #5 | Join reorder via `JOIN_COMMUTE` + `RelMetadataQuery.getRowCount` |
| §8 | compound | all of the above in one plan |
| §9 | M4 metrics | counter observability through `/stats` |
