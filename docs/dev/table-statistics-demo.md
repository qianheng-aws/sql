
# Table Statistics — Demo Script

Walk-through script for showing how stored statistics affect Calcite's plan on a live 2-index dataset. Every step has a "stats ON vs OFF" side-by-side so the effect is visible on the `explain cost` output.

**Audience:** Reviewers / maintainers who want to see the subsystem work without reading code.

**Prerequisite:** Local OpenSearch 3.6 with this branch's SQL plugin installed. Cluster must have `plugins.calcite.enabled = true`; the demo flips `plugins.calcite.table_statistics.enabled` to toggle stats on/off.

**Shell convention:** all commands hit `localhost:9200`. The explain endpoint takes `format` and `mode` as **query params** (not body): `?format=json&mode=cost`.

The real numbers below were produced on a live cluster 2026-04-24 — if your re-run differs by a fraction, it's likely because the HLL cardinality is approximate (e.g. `status.unique_count = 3` exactly, `latency.unique_count` near 1250 with slight sampling variance).

---

## 0. Setup — two indices that exercise every hook

```bash
# Clean slate
for idx in demo-events demo-users .opensearch-statistics; do
  curl -s -X DELETE "http://localhost:9200/${idx}" >/dev/null
done

# demo-events: 2000 docs, 3 distinct statuses, latency 1..2000, user_id 1..50, region in 3
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
statuses = ['OK'] * 85 + ['WARN'] * 12 + ['ERROR'] * 3     # skewed 85/12/3
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

curl -s "http://localhost:9200/demo-events/_count"; echo
curl -s "http://localhost:9200/demo-users/_count"
# Expect: demo-events 2000, demo-users 50
```

## 0.1 — Enable the subsystem

```bash
curl -s -X PUT "http://localhost:9200/_cluster/settings" \
  -H 'Content-Type: application/json' -d '{
    "persistent": {
      "plugins.calcite.enabled":                 "true",
      "plugins.calcite.table_statistics.enabled":"true"
    }}'
```

## 0.2 — Trigger stats collection (serial, not parallel)

**Important:** BUG-001 (see `docs/dev/table-statistics-design.md` §6) — calling `/analyze` on two indices in parallel against an empty `.opensearch-statistics` can leave both stuck in `GENERATING`. Run them serially.

```bash
curl -s -X POST "http://localhost:9200/_plugins/_sql/_statistics/demo-events/analyze"; echo
sleep 3
curl -s -X POST "http://localhost:9200/_plugins/_sql/_statistics/demo-users/analyze"; echo
sleep 3

# Verify both COMPLETED
for idx in demo-events demo-users; do
  echo "=== $idx ==="
  curl -s "http://localhost:9200/_plugins/_sql/_statistics/${idx}" | python3 -m json.tool
done
```

**Actual output points to call out:**

```
demo-events:
  doc_count: 2000
  status.unique_count  = 3       (exact)
  region.unique_count  = 3       (exact)
  user_id.unique_count = 50      (exact — HLL is exact on small cardinality)
  latency.unique_count = 1251    (approx HLL; ≈ 1250)
  latency.min_value    = 2.0     (exact, Lucene BKD)
  latency.max_value    = 1997.0  (exact, Lucene BKD — sample didn't hit 1 or 2000)
  all null_ratio       = 0.0
demo-users:
  doc_count: 50
  tier.unique_count    = 3
  user_id.unique_count = 50
  name.unique_count    = 50
```

## 0.3 — A reusable helper to extract rowcount from `mode=cost`

Every section below uses this. It parses the logical plan and prints `<node>: rowcount = <X>` one per line.

```bash
extract() {
  python3 -c '
import json, sys, re
d = json.load(sys.stdin)
for line in d["calcite"]["logical"].split("\n"):
    m = re.search(r"(\w+)\(.*?rowcount = ([0-9.E+-]+)", line)
    if m:
        print(f"  {m.group(1)}: rowcount = {m.group(2)}")'
}
```

And a tiny `toggle` helper:

```bash
toggle() {
  curl -s -X PUT "http://localhost:9200/_cluster/settings" \
    -H 'Content-Type: application/json' \
    -d "{\"persistent\":{\"plugins.calcite.table_statistics.enabled\":\"$1\"}}" >/dev/null
}
```

---

## 1. TableScan row count — the foundation

```bash
for flag in false true; do
  toggle "$flag"
  echo "=== stats=$flag — source=demo-events ==="
  curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain?format=json&mode=cost" \
    -H 'Content-Type: application/json' \
    -d '{"query":"source=demo-events"}' | extract
done
```

**Real output:**

| stats | Scan rowcount | What drives it |
|---|---|---|
| false | **10000** | `maxResultWindow` fallback (`OpenSearchIndex.getStatistic()` returns UNKNOWN) |
| true  | **2000**  | Real `doc_count` from `.opensearch-statistics` |

---

## 2. Filter selectivity — equality (`status = 'OK'`)

```bash
for flag in false true; do
  toggle "$flag"
  echo "=== stats=$flag — where status = 'OK' ==="
  curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain?format=json&mode=cost" \
    -H 'Content-Type: application/json' \
    -d "{\"query\":\"source=demo-events | where status = 'OK'\"}" | extract
done
```

**Real output:**

| stats | Scan | Filter | Math |
|---|---|---|---|
| false | 10000 | **1500** | `10000 × 0.15` (Calcite default EQUALS selectivity) |
| true  | 2000  | **666.67** | `2000 × 1/3` (using stored `cardinality(status) = 3`) |

**Caveat — the skew:** real data has 85% `OK`, so the optimizer is still 2.5× off. But the Calcite default (15%) is 5× off, and this demo's value being *low* is safer for plan choice than the default being low (filter downstream of a real 85%-selectivity predicate doesn't benefit much from knowing the exact number). Histogram-backed selectivity (M3 #4, deferred) is the long-term fix.

---

## 3. Filter selectivity — range (`latency BETWEEN 500 AND 1500`)

```bash
for flag in false true; do
  toggle "$flag"
  echo "=== stats=$flag — latency 500..1500 ==="
  curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain?format=json&mode=cost" \
    -H 'Content-Type: application/json' \
    -d "{\"query\":\"source=demo-events | where latency >= 500 AND latency <= 1500\"}" | extract
done
```

**Real output:**

| stats | Scan | Filter | Math |
|---|---|---|---|
| false | 10000 | **2500** | `10000 × 0.25` (Calcite default 0.5 × 0.5 for the two comparisons) |
| true  | 2000  | **1126.88** | `2000 × ((1500 - 500) / (1997 - 2))`  = `2000 × 0.5013` — linear interpolation over stored `[min=2, max=1997]` |

**Subtle point to mention:** with stats on, the handler still runs even though `min/max=[2, 1997]` don't match the user-supplied `[500, 1500]` exactly — linear interpolation gives a proportional estimate. With uniform distribution this is accurate.

---

## 4. IS NULL / IS NOT NULL — null_ratio consumption

```bash
for flag in false true; do
  toggle "$flag"
  echo "=== stats=$flag — isnotnull(status) ==="
  curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain?format=json&mode=cost" \
    -H 'Content-Type: application/json' \
    -d "{\"query\":\"source=demo-events | where isnotnull(status)\"}" | extract
done
```

**Real output:**

| stats | Scan | Filter | Math |
|---|---|---|---|
| false | 10000 | **9000** | `10000 × 0.9` (Calcite default IS NOT NULL selectivity) |
| true  | 2000  | **2000** | `2000 × (1 - 0)` — optimizer proves the filter is a no-op for rowcount because `null_ratio = 0` |

---

## 5. Aggregate rowcount — `DistinctRowCount.Handler`

### 5a. Single by-column: `by status`

```bash
for flag in false true; do
  toggle "$flag"
  echo "=== stats=$flag — stats count() by status ==="
  curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain?format=json&mode=cost" \
    -H 'Content-Type: application/json' \
    -d "{\"query\":\"source=demo-events | stats count() by status\"}" | extract
done
```

**Real output:**

| stats | Scan | Aggregate | Math |
|---|---|---|---|
| false | 10000 | **1000** | `inputRowCount / 10` (Calcite fallback) |
| true  | 2000  | **3**    | `min(cardinality(status), rowcount) = min(3, 2000)` |

### 5b. Multi by-column: `by status, region`

```bash
curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain?format=json&mode=cost" \
  -H 'Content-Type: application/json' \
  -d "{\"query\":\"source=demo-events | stats count() by status, region\"}" | extract
```

**Real output (with stats ON):**

| stats | Aggregate | Math |
|---|---|---|
| false | **1000** | same fallback as single-column |
| true  | **9**    | `numDistinctVals(3 × 3, 2000) = 9` (Calcite's inclusion-exclusion; since 2000 >> 9 no further collapse) |

---

## 6. `top N by col` — RareTop consumption through aggregate pipeline

PPL's `top N` expands to `Aggregate(count) + Filter(row_number ≤ N) + Project`, so the RARE_TOP scan-level stat-aware formula (see `estimateRareTopRowCount` in `AbstractCalciteIndexScan`) is consumed through the same `DistinctRowCount.Handler` as §5 when the top isn't pushed down as a scan op.

```bash
for flag in false true; do
  toggle "$flag"
  echo "=== stats=$flag — top 2 status ==="
  curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain?format=json&mode=cost" \
    -H 'Content-Type: application/json' \
    -d "{\"query\":\"source=demo-events | top 2 status\"}" | extract

  echo "=== stats=$flag — top 5 status by region ==="
  curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain?format=json&mode=cost" \
    -H 'Content-Type: application/json' \
    -d "{\"query\":\"source=demo-events | top 5 status by region\"}" | extract
done
```

**Real output — focus on the outer Filter rowcount (= top-N materialized output):**

| stats | `top 2 status` outer filter | `top 5 status by region` outer filter |
|---|---|---|
| false | **500** | **500** |
| true  | **1.5** | **4.5** |

With stats the optimizer can tell `top 2` against 3-value cardinality will emit at most 2 rows (here 1.5 is the post-filter rowcount estimate the outer scan observes after aggregate rowcount=3 and filter keeps top 2 / 3 = 0.5). Without stats the aggregate shows as 1000 and `top N` applies on top of that.

---

## 7. Join reorder — the showcase

**Gotcha:** the bare join query (without any downstream operator) does not trigger `CoreRules.JOIN_COMMUTE` in every build path. Adding `| head 5` makes the cost-based join commute kick in reliably. Use the trailing `| head N` form for a clean demo.

```bash
for flag in false true; do
  toggle "$flag"
  echo "=== stats=$flag — large-on-left PPL, with head 5 ==="
  curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain?format=json&mode=cost" \
    -H 'Content-Type: application/json' \
    -d '{"query":"source=demo-events | join on demo-events.user_id=demo-users.user_id demo-users | head 5"}' \
    | python3 -c '
import json, sys
d = json.load(sys.stdin)
for line in d["calcite"]["physical"].split("\n"):
    if "Join" in line:
        print("JOIN :", line.strip()[:140])
    elif "IndexScan(table=[[" in line:
        print("SCAN :", line.strip()[:120])'
done
```

**Real output:**

```
=== stats=false — large-on-left PPL, with head 5 ===
JOIN : EnumerableMergeJoin(condition=[=($1, $6)], joinType=[inner]): rowcount = 1.5E7, cumulative cost = {1.51009964E7 rows, ...
SCAN : CalciteEnumerableIndexScan(table=[[OpenSearch, demo-events]], ...
SCAN : CalciteEnumerableIndexScan(table=[[OpenSearch, demo-users]], ...

=== stats=true — large-on-left PPL, with head 5 ===
JOIN : EnumerableHashJoin(condition=[=($2, $4)], joinType=[inner]): rowcount = 15000.0, cumulative cost = {24528.9 rows, ...
SCAN : CalciteEnumerableIndexScan(table=[[OpenSearch, demo-users]], ...
SCAN : CalciteEnumerableIndexScan(table=[[OpenSearch, demo-events]], ...
```

**Two changes at once:**

1. **Join order swaps:** with stats, the 50-row `demo-users` becomes the LEFT (build) side; without stats, the PPL declared order (`demo-events` on left) is preserved.
2. **Join algorithm changes:** MergeJoin → HashJoin. MergeJoin needs to sort both sides, which is wasteful when one side fits in memory.
3. **Cumulative cost collapses:** `1.51E7` → `2.45E4` — a **~600× improvement** just from correct cardinality.

---

## 8. End-to-end compound query — the "why stats matter" finale

```bash
for flag in false true; do
  toggle "$flag"
  echo "=== stats=$flag — compound filter+join+aggregate ==="
  curl -s -X POST "http://localhost:9200/_plugins/_ppl/_explain?format=json&mode=cost" \
    -H 'Content-Type: application/json' \
    -d "{\"query\":\"source=demo-events | where status='OK' AND latency < 500 | join on demo-events.user_id=demo-users.user_id demo-users | stats count() by tier\"}" \
    | extract
done
```

**Real output:**

| Stage | OFF | ON |
|---|---|---|
| Scan `demo-events` | 10000 | **2000** |
| Filter (`status='OK' AND latency<500`) | 750 | **166.4** |
| Scan `demo-users` | 10000 | **50** |
| Join | 1125000 | **1248** |
| Aggregate `by tier` | 112500 | **3** |

**The join line is the headline — 1.12 M vs 1.2 K, a ~900× difference.** Downstream physical-plan choices propagate that error into actual runtime.

---

## 9. Observability — metrics counters

```bash
curl -s "http://localhost:9200/_plugins/_ppl/stats" | python3 -c '
import json, sys
d = json.load(sys.stdin)
for k, v in sorted(d.items()):
    if "table_statistics" in k:
        print(f"  {k}: {v}")'
```

**Real output after the demo run:**

```
  table_statistics_read_timeout_count: 0
  table_statistics_refresh_failure_count: 0
  table_statistics_refresh_success_count: 8
```

Each `/analyze` call (and every cron-triggered refresh on the same index) bumps `success_count`. `read_timeout_count` remains 0 because the dataset is small and local — the 500 ms storage-read budget is plenty.

---

## 10. Cleanup

```bash
for idx in demo-events demo-users; do
  curl -s -X DELETE "http://localhost:9200/${idx}" >/dev/null
done
# Leave plugins.calcite.* settings as the operator prefers; usually no reset needed.
```

---

## Appendix — real output summary

All numbers from a single live-cluster pass on 2026-04-24. Section 7 is the most visually striking (algorithm + order swap); sections 5a, 5b, and 8 are the most numerically striking.

| § | Feature | OFF | ON | Ratio |
|---|---|---|---|---|
| 1 | TableScan rowcount | 10000 | 2000 | 5× |
| 2 | `= 'OK'` | 1500 | 666.67 | 2.25× |
| 3 | `BETWEEN 500 AND 1500` | 2500 | 1126.88 | 2.2× |
| 4 | `IS NOT NULL` | 9000 | 2000 | 4.5× |
| 5a | `by status` | 1000 | **3** | 333× |
| 5b | `by status, region` | 1000 | **9** | 111× |
| 7 | join cumulative cost | 1.51E7 | 2.45E4 | **~600×** |
| 8 | join rowcount (compound) | 1 125 000 | **1248** | **~900×** |
| 8' | aggregate rowcount (compound) | 112500 | **3** | ~37 500× |

## Appendix — known issue uncovered during demo rehearsal

**BUG-001 — concurrent `/analyze` can leave status=GENERATING** — if `/analyze` is issued on two indices *in parallel* against a cluster where `.opensearch-statistics` hasn't been created yet, `putStatus(GENERATING)` and `put(COMPLETED)` are both fire-and-forget, and the GENERATING write can sometimes land after COMPLETED (overwriting it). The cron refresh path is unaffected (serialized through the semaphore). Run `/analyze` serially, pre-create the system index, or rely on cron. Recorded in design doc §6 with the deferred fix (seq-numbered writes).
