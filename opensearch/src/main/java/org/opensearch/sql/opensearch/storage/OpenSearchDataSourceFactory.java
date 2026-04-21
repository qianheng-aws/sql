/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticCollector;
import org.opensearch.sql.opensearch.storage.statistics.TableStatisticStorage;
import org.opensearch.sql.storage.DataSourceFactory;

public class OpenSearchDataSourceFactory implements DataSourceFactory {

  /** OpenSearch client connection. */
  private final OpenSearchClient client;

  private final Settings settings;

  @Nullable private final TableStatisticStorage statisticStorage;

  @Nullable private final TableStatisticCollector statisticCollector;

  /** Primary constructor: accepts optional statistics services. */
  public OpenSearchDataSourceFactory(
      OpenSearchClient client,
      Settings settings,
      @Nullable TableStatisticStorage statisticStorage,
      @Nullable TableStatisticCollector statisticCollector) {
    this.client = client;
    this.settings = settings;
    this.statisticStorage = statisticStorage;
    this.statisticCollector = statisticCollector;
  }

  /** Backwards-compat: no statistics services wired. */
  public OpenSearchDataSourceFactory(OpenSearchClient client, Settings settings) {
    this(client, settings, null, null);
  }

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.OPENSEARCH;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    return new DataSource(
        metadata.getName(),
        DataSourceType.OPENSEARCH,
        new OpenSearchStorageEngine(client, settings, statisticStorage, statisticCollector));
  }
}
