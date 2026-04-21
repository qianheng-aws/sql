/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.utils.SystemIndexUtils.isSystemIndex;

import lombok.Getter;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticCollector;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticStorage;
import org.opensearch.sql.opensearch.storage.system.OpenSearchSystemIndex;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

/** OpenSearch storage engine implementation. */
public class OpenSearchStorageEngine implements StorageEngine {

  /** OpenSearch client connection. */
  @Getter private final OpenSearchClient client;

  @Getter private final Settings settings;

  @Nullable private final TableStatisticStorage statisticStorage;

  @Nullable private final TableStatisticCollector statisticCollector;

  /** Primary constructor: accepts optional statistics services. Pass nulls to disable. */
  public OpenSearchStorageEngine(
      OpenSearchClient client,
      Settings settings,
      @Nullable TableStatisticStorage statisticStorage,
      @Nullable TableStatisticCollector statisticCollector) {
    this.client = client;
    this.settings = settings;
    this.statisticStorage = statisticStorage;
    this.statisticCollector = statisticCollector;
  }

  /** Backwards-compat: no statistics services. */
  public OpenSearchStorageEngine(OpenSearchClient client, Settings settings) {
    this(client, settings, null, null);
  }

  @Override
  public Table getTable(DataSourceSchemaName dataSourceSchemaName, String name) {
    if (isSystemIndex(name)) {
      return new OpenSearchSystemIndex(client, settings, name);
    } else {
      return new OpenSearchIndex(client, settings, name, statisticStorage, statisticCollector);
    }
  }
}
