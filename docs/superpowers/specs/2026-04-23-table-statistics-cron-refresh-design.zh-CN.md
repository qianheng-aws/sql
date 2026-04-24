# Table Statistics — Cron 刷新（M2 Phase 2）

本 spec 描述一个后台定时刷新子系统，让已存储的 table statistics 在无用户干预的情况下保持在 TTL 内。是 M2 Phase 1（min/max 抽出 sampler、range selectivity）的后续。每项决策的详细取舍理由在 `docs/dev/table-statistics-design.md` §3.5 — 本文档聚焦于实现契约。

## 1. 目标

让 table statistics 能按集群范围的周期自动重新采集。今天触发 refresh 只有两条路：
- (a) `OpenSearchIndex.getStatistic()` 的 miss 分支（查询时触发）
- (b) 显式调用 `POST /_plugins/_sql/_statistics/{index}/analyze`

对开发 / demo 够用，但对生产化不够 —— 一个不再被查询的索引即使统计已经过时也没人刷。

**验收标准**：在真实集群上设置 `plugins.calcite.table_statistics.ttl=5s` 和 `plugins.calcite.table_statistics.refresh_interval=2s` 后，不发任何用户请求，stored record 的 `last_updated_time` 仍会前进。

## 2. 范围

**Spec 内：**
- 由选出的 cluster-manager 驱动的周期刷新。
- 三个新增 dynamic setting：`refresh_interval`、`ttl`、`refresh_max_in_flight`。
- 用 `Semaphore` 做并发节流。
- 区分 search 线程池 rejection 与其他硬失败（rejection 时**不**写 `FAILED` marker）。
- 单元测试 + 集成测试覆盖。

**Spec 外**（明确登记为后续里程碑的 TODO）：
- 孤儿 stat 文档清理（索引已删除但 `.opensearch-statistics` 记录还在）。
- `listStale` 的分页 / scroll（stat 文档数 > 1000 时）。
- Stat 文档 mapping 版本迁移（Phase 3）。
- `null_ratio` / `top_terms` 采集（Phase 3）。
- Sampler `shard_size` 调参并提升为 setting（Phase 3）。

## 3. 架构

```
 cluster-manager 节点                                 所有节点
 ┌──────────────────────────────────┐         ┌───────────────────────┐
 │  TableStatisticRefreshScheduler  │         │  SQLPlugin            │
 │   └─ LocalNodeClusterManager…    │         │   createComponents()  │
 │       ├─ onClusterManager()─┐    │         │         │             │
 │       └─ offClusterManager()│    │          └─────────┼─────────────┘
 │                             ▼    │                    │
 │   ThreadPool.scheduleWithFixedDelay (refresh_interval)
 │                             │                         ▼
 │                             ▼         （所有节点都注册 listener
 │        TableStatisticRefreshTask       但只有 CM 的 onClusterManager
 │          1) flag 关闭？ → noop          会回调；tick 只在那里跑）
 │          2) storage.listStale(ttl)
 │          3) 遍历 stale 索引：
 │               tryAcquire semaphore
 │                 ├─ 失败 → 跳过
 │                 └─ 成功 → resolveMapping (GENERIC)
 │                             │
 │                             ▼
 │                    collector.refreshAsync(name, fieldTypes,
 │                         completion → semaphore.release())
 └──────────────────────────────────┘
```

### 3.1 新增组件

全部位于 `opensearch/src/main/java/org/opensearch/sql/opensearch/storage/statistics/`。

| 类 | 职责 |
|---|---|
| `TableStatisticRefreshScheduler` | 向 `ClusterService` 注册 `LocalNodeClusterManagerListener`。`onClusterManager()` → 用 `ThreadPool.scheduleWithFixedDelay` 启动 tick；`offClusterManager()` → cancel。持有 `Cancellable` 句柄。同时订阅三个 dynamic setting 的更新。 |
| `TableStatisticRefreshTask` | tick 的 body，一个 `Runnable`。读 settings、调 `storage.listStale`、在 semaphore 保护下遍历结果、调用 `refreshAsync`。除注入的协作者外无状态。 |
| Mapping resolver helper | 把当前 inline 在 `RestTableStatisticsAction` 里的"索引名 → `Map<String, OpenSearchDataType>`"逻辑抽成可复用组件（scheduler + REST handler 共享）。 |

### 3.2 修改的组件

- `TableStatisticStorage`：新增 `void listStale(Duration ttl, int maxResults, ActionListener<List<String>> listener)`。在 `.opensearch-statistics` 上发起一个 `SearchRequest`，`size=maxResults`，过滤 `status IN (COMPLETED, FAILED) AND last_updated_time < now() - ttl`，从文档 `_source.index_name` 取索引名（见 §3.3）。`IndexNotFoundException` 返回空列表（冷启动正常态）。**从不调用 `onFailure`。**
- `TableStatisticStorage.INDEX_MAPPING`：增加 `index_name` 为 `keyword` 字段，让 `listStale` 能直接返回索引名。`put()` / `putStatus()` 都在 source 里写入 `index_name`。
- `TableStatistic` / 存储文档：`toStoredDocSource()` 写入 `"index_name": "<原始索引名>"`。读回时 `fromStoredDoc` 不需要处理（今天的消费者都从已知的 name 侧读），但加字段无副作用。
- `TableStatisticCollector.refreshAsync`：新增重载 `refreshAsync(String name, Map<String, OpenSearchDataType> fieldTypes, ActionListener<Void> completion)`。Completion 在所有终端路径都会触发（成功、写 FAILED marker、`EsRejectedExecutionException` 跳过）。原 2 参版本委托给新版本，传一个 noop listener。
- `TableStatisticCollector.proceedWithCollect` 的 `onFailure` 分支：若 `ExceptionsHelper.unwrap(e, EsRejectedExecutionException.class) != null`，调用 `completion.onResponse(null)` 并 log debug — **不写 FAILED marker**。其他异常仍走原逻辑。

### 3.3 存储文档结构变更

存储文档新增一个 keyword 字段：

```json
{
  "status": "COMPLETED",
  "last_updated_time": "2026-04-23T...",
  "index_name": "logs-2026-04-23",   // 新增
  "doc_count": 137,
  "fields": { ... }
}
```

确定性 `docId = sha256(indexName)` 保持不变 — `index_name` 只是给 sweep 查询用的附加元数据。已有的缺少此字段的旧文档仍可正常读取（消费者根本不碰这个字段），只是 `listStale` 查不到它们 —— 直到下次 refresh 重写。这点可以接受 —— 它们在索引里没有 `index_name` 的状态等价于"还没被新代码 refresh 过"，query-path 的 miss 终会覆盖它们。

**上线影响**：部署 M2 Phase 2 后，旧格式的 stat 文档对读取仍然有效，但对 sweeper 不可见。不做主动迁移 —— 文档会通过下次 refresh（query miss、显式 `/analyze` 或手动降低 TTL）自行修复。此条目登记到 Phase 3（系统索引生命周期 / mapping 迁移）。

### 3.4 Wiring（`SQLPlugin.createComponents`）

```java
TableStatisticRefreshScheduler scheduler = new TableStatisticRefreshScheduler(
    clusterService, threadPool, settings,
    tableStatisticStorage, tableStatisticCollector, mappingResolver);
scheduler.register();  // clusterService.addLocalNodeClusterManagerListener(this)
components.add(scheduler);  // 让插件生命周期持有引用
```

不需要 Guice 额外布线 —— scheduler 是自包含的，通过 `createComponents` 返回列表维持单例。

## 4. 数据流

### 4.1 启动

1. 每个节点调用 `createComponents` → 注册 `LocalNodeClusterManagerListener`。
2. 在 cluster-manager 节点上，OpenSearch 会立即回调 `onClusterManager()`。
3. Scheduler 读 `TABLE_STATISTICS_ENABLED`：
   - `false` → 不调度 tick。订阅 flag 的 update-consumer 以便之后翻开时启动。
   - `true` → `threadPool.scheduleWithFixedDelay(task, refresh_interval, ThreadPool.Names.GENERIC)`。

### 4.2 Tick

```
TableStatisticRefreshTask.run()
 ├─ if (!flag.enabled) return
 ├─ ttl = settings.get(TTL)        // 每 tick 重新读，不缓存
 ├─ semaphore = scheduler.currentSemaphore()   // 见 §4.4，volatile 引用
 ├─ storage.listStale(ttl, LIST_STALE_LIMIT /*=1000*/, ActionListener {
 │    onResponse(staleNames):
 │      for name in staleNames:
 │        if (!semaphore.tryAcquire()) continue   // 尝试下一个，不重试
 │        threadPool.executor(GENERIC).execute(() -> {
 │          try {
 │            fieldTypes = mappingResolver.resolve(name)  // 阻塞；GENERIC 允许阻塞
 │            collector.refreshAsync(name, fieldTypes,
 │                ActionListener.wrap(
 │                    v -> semaphore.release(),
 │                    e -> semaphore.release()));
 │          } catch (Exception ex) {
 │            semaphore.release();
 │            LOG.debug(...);
 │          }
 │        });
 │    onFailure: /* 永不会调用 */
 │  });
```

`LIST_STALE_LIMIT=1000` 是常量。如果某次扫描正好返回 1000 条，下一次 tick 会接着处理剩余的。

### 4.3 停止 / cluster-manager 失去

```
offClusterManager():
  cancellable.cancel();      // 停止后续 tick
  // 已启动的 in-flight refresh 各自跑完；它们会在
  // 自己的 callback 里 release semaphore
  // （这些 callback 跟 tick 生命周期解耦）
```

**不等待** in-flight 完成。它们会把最新的 `last_updated_time` 写回 `.opensearch-statistics`，接手的新 cluster-manager 第一次 tick 时不会重复刷这些索引。

### 4.4 Setting 动态变更

Scheduler 通过 `clusterService.getClusterSettings().addSettingsUpdateConsumer` 为每个 setting 注册 update-consumer：

- `refresh_interval` → `cancellable.cancel(); cancellable = threadPool.scheduleWithFixedDelay(task, newValue, GENERIC)`。
- `ttl` → 无动作。每 tick 从 `settings` 重读。
- `refresh_max_in_flight` → 替换 `Semaphore` 引用。in-flight 持有的是旧 semaphore 的 permit，干净地对旧 semaphore release（permit 就这么 lost 掉了也没关系 —— 旧 semaphore 会被 GC）。新请求用新 semaphore。
- `plugins.calcite.table_statistics.enabled` → false：取消 tick。→ true：如果仍是 cluster-manager 就调度 tick。

所有 update-consumer 都在 cluster-state applier 线程上跑 —— 严格限制为 "换引用、启 / 停一个任务"，**不做任何 I/O**。

## 5. 错误处理

| 来源 | 处理 |
|---|---|
| `listStale` 失败（索引不存在等） | `onResponse([])` —— tick 干净返回。冷启动正常。 |
| `mappingResolver.resolve` 抛出（tick 进行中索引被删） | `semaphore.release()` + `log.debug`；继续下一个索引。`.opensearch-statistics` 里的孤儿文档保留（清理在 spec 外）。 |
| `refreshAsync` GENERATING-dedup 命中 | Collector 原有逻辑 skip；completion listener 以 `onResponse(null)` 触发；permit release。 |
| `refreshAsync` 持久化成功 | `onResponse(null)` → release。 |
| `refreshAsync` 持久化失败（非 rejection） | 原逻辑写 FAILED marker → `onResponse(null)`（终端）→ release。 |
| `refreshAsync` 持久化失败（rejection） | `onResponse(null)`，**不写 FAILED marker** → release。Stat 记录不变；下 tick 会被再次选中。 |
| Listener 回调本身抛异常 | 每个 `refreshAsync` completion 的 wrap 都有 try/finally 保证 `semaphore.release()`。Permit 泄漏是所有症状里最难定位的 —— 这里的偏执是值得的。 |

## 6. Settings

三个 setting 都是 dynamic + `NodeScope`。需要添加到：

1. `common/src/main/java/org/opensearch/sql/common/setting/Settings.java` —— `Key` 枚举项。
2. `opensearch/src/main/java/org/opensearch/sql/opensearch/setting/OpenSearchSettings.java` —— `Setting<?>` 定义 + `register(...)` 调用 + `pluginSettings()` 列表。

| Key | 类型 | 默认 | 最小 | 最大 |
|---|---|---|---|---|
| `plugins.calcite.table_statistics.refresh_interval` | `TimeValue` | `60s` | `5s` | — |
| `plugins.calcite.table_statistics.ttl` | `TimeValue` | `24h` | `1m` | — |
| `plugins.calcite.table_statistics.refresh_max_in_flight` | `Integer` | `4` | `1` | `100` |

上下界是防御性的 —— 某个无关的用户把 `refresh_interval=0` 误设时不会 OOM 节点。`refresh_max_in_flight` 的 max 是建议值；运维把它调过 100 就是已接受后果。

## 7. 测试

### 7.1 单元测试

新增 `TableStatisticRefreshSchedulerTest`（mock `ClusterService`、`ThreadPool`、`Settings`）：
- `onClusterManager` + flag on → 调度出 `Cancellable`。
- `onClusterManager` + flag off → 不调度。
- `offClusterManager` 取消已有的 `Cancellable`。
- 运行中 flag true→false → 取消。
- 在 cluster-manager 状态下 flag false→true → 调度。
- `refresh_interval` 更新 → cancel + 重新调度。
- `refresh_max_in_flight` 更新 → 换 semaphore。
- 插件关闭时 listener 清理。

新增 `TableStatisticRefreshTaskTest`（mock collector、storage、mapping resolver）：
- 0 个 stale 索引 → 无 refresh 调用。
- N 个 stale、semaphore = 2 → 恰好 2 次 refresh；completion 触发 release 后允许后续 tick。
- Listener 回调抛异常 → semaphore 仍 release。
- `listStale` 返回空 → 无副作用。
- Mapping resolve 抛异常 → release + 下一个索引继续尝试。
- `refreshAsync` completion 在失败路径上也触发 → release 仍然发生。

扩展 `TableStatisticCollectorTest`：
- `EsRejectedExecutionException` → completion 被调用，无 FAILED marker 写入。
- 3 参 `refreshAsync` 在成功、硬失败、rejection、GENERATING-skip 所有路径上都触发 completion。

扩展 `TableStatisticStorageTest`：
- `listStale` 在索引缺失时 → `[]`。
- `listStale` 正确过滤 `last_updated_time` 和 `status`。
- `listStale` 遵守 `maxResults`。
- `put` 会写入 `index_name` 字段。

### 7.2 集成测试

扩展 `TableStatisticsIT`：
- **Stale refresh**：写一个 `last_updated_time` = 1 小时前的 stat 文档；设 `refresh_interval=2s, ttl=5s`；等 6 秒；断言 `last_updated_time` 已前进。
- **Disabled flag**：flag 为 false 时，即使 TTL 过了也不发生 refresh。
- **动态 interval**：起始 `refresh_interval=10m`；改成 `2s`；几秒内观察到 refresh。
- **Rejection 不 persist FAILED**：在 `IntegTestCase` 里确定性地触发 search-pool rejection 很脆；只在单元层面覆盖此场景，IT 里跳过。

现有单节点 IT harness 够用 —— OpenSearch test cluster 总会在 node 0 选出 cluster-manager。

## 8. 风险与缓解

| 风险 | 缓解 |
|---|---|
| Cron 的 `listStale` 每 tick 占一个 search slot | size=1000 on 一个系统索引 ≈ 零开销；同池争用封顶为每 `refresh_interval` 一次请求。 |
| Mapping 解析阻塞 `GENERIC` 线程池 | 和现有 REST `/analyze` 同模式，无新增风险面。 |
| 1k+ 索引时 cluster-manager 过载 | 当前 `listStale` 每 tick 最多 1k；1k 个 refresh × 4 in-flight × 60s tick ≈ 250 tick ≈ 4 小时，远在 24h TTL 内。真变瓶颈时加分页或调高 `refresh_max_in_flight`，无架构变更。 |
| Callback 上抛异常导致 permit 泄漏 | 每个 release 都包在 `ActionListener.wrap` 里，onResponse/onFailure 两分支逻辑相同；mapping 解析还有显式 try/catch。单元测试断言异常路径 permit 归还。 |
| 旧格式 stat 文档对 sweeper 不可见 | 已接受 —— 见 §3.3。它们读取仍有效；query miss 或显式 `/analyze` 会用新字段重写。 |

## 9. 开放问题

无。所有决策都在 `docs/dev/table-statistics-design.md` §3.5，或本 spec 的 §3 / §5 里。

## 10. Done 定义

- [ ] 新 scheduler 类 + listener 在 `SQLPlugin.createComponents` 接好。
- [ ] 三个 dynamic setting 注册、加边界、文档化。
- [ ] `refreshAsync` 3 参重载；rejection 路径从硬失败里分离出来。
- [ ] `listStale` + `index_name` 存储字段。
- [ ] §7.1 所列单元测试通过。
- [ ] 集成测试 "stale refresh" 场景通过。
- [ ] Living design 的 §3.5 + worklog 条目已随本 spec 前置工作合入。
- [ ] 实机验证：stale 文档的 `last_updated_time` 在无用户动作情况下前进。
