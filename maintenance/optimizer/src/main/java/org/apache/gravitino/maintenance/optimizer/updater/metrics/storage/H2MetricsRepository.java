/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.maintenance.optimizer.updater.metrics.storage;

import com.google.common.base.Preconditions;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * H2-backed implementation of {@link MetricsRepository}.
 *
 * <p>All timestamps are epoch seconds. Read APIs use a half-open time window [fromSecs, toSecs).
 */
public class H2MetricsRepository implements MetricsRepository {

  private static final Logger LOG = LoggerFactory.getLogger(H2MetricsRepository.class);
  private static final int DEFAULT_PARTITION_COLUMN_LENGTH = 1024;
  private static final int MAX_METRIC_VALUE_LENGTH = 1024;
  private static final int MAX_IDENTIFIER_LENGTH = 1024;
  private static final int MAX_METRIC_NAME_LENGTH = 1024;
  private static final long MAX_REASONABLE_EPOCH_SECONDS = 9_999_999_999L;

  private static final String DEFAULT_USER = "sa";
  private static final String DEFAULT_PASSWORD = "";
  private static final String DEFAULT_JDBC_URL =
      "jdbc:h2:file:./metrics_db;DB_CLOSE_DELAY=-1;MODE=MYSQL;AUTO_SERVER=TRUE";
  private String jdbcUrl = DEFAULT_JDBC_URL;
  private String username = DEFAULT_USER;
  private String password = DEFAULT_PASSWORD;
  private int partitionColumnLength = DEFAULT_PARTITION_COLUMN_LENGTH;
  private volatile boolean initialized = false;

  public H2MetricsRepository() {}

  @Override
  public void initialize(Map<String, String> optimizerProperties) {
    Preconditions.checkState(!initialized, "H2MetricsRepository has already been initialized.");
    Map<String, String> h2Properties =
        MapUtils.getPrefixMap(
            optimizerProperties,
            OptimizerConfig.OPTIMIZER_PREFIX + H2MetricsRepositoryConfig.H2_METRICS_PREFIX);
    H2MetricsRepositoryConfig config = new H2MetricsRepositoryConfig(h2Properties);
    this.username = config.get(H2MetricsRepositoryConfig.USERNAME_CONFIG);
    this.password = config.get(H2MetricsRepositoryConfig.PASSWORD_CONFIG);
    this.partitionColumnLength =
        config.get(H2MetricsRepositoryConfig.PARTITION_COLUMN_LENGTH_CONFIG);
    String configuredJdbcUrl = config.get(H2MetricsRepositoryConfig.JDBC_URL_CONFIG);
    if (StringUtils.isNotBlank(configuredJdbcUrl)) {
      this.jdbcUrl = constructH2JdbcUrl(configuredJdbcUrl);
    } else {
      String path = resolveStoragePath(config.get(H2MetricsRepositoryConfig.STORAGE_PATH_CONFIG));
      this.jdbcUrl = constructH2JdbcUrl("jdbc:h2:file:" + path);
    }
    initializeDatabase();
    this.initialized = true;
  }

  private void initializeDatabase() {
    String createTableMetricsSql =
        "CREATE TABLE IF NOT EXISTS table_metrics ("
            + "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
            + "table_identifier VARCHAR(1024) NOT NULL, "
            + "metric_name VARCHAR(1024) NOT NULL, "
            + "table_partition VARCHAR("
            + partitionColumnLength
            + "), "
            + "metric_ts BIGINT NOT NULL, "
            + "metric_value VARCHAR(1024) NOT NULL"
            + ")";

    String createJobMetricsSql =
        "CREATE TABLE IF NOT EXISTS job_metrics ("
            + "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
            + "job_identifier VARCHAR(1024) NOT NULL, "
            + "metric_name VARCHAR(1024) NOT NULL, "
            + "metric_ts BIGINT NOT NULL, "
            + "metric_value VARCHAR(1024) NOT NULL"
            + ")";

    String createIndexSql1 =
        "CREATE INDEX IF NOT EXISTS idx_table_metrics_metric_ts ON table_metrics(metric_ts)";
    String createIndexSql2 =
        "CREATE INDEX IF NOT EXISTS idx_job_metrics_metric_ts ON job_metrics(metric_ts)";
    String createIndexSql3 =
        "CREATE INDEX IF NOT EXISTS idx_table_metrics_composite ON table_metrics(table_identifier, table_partition, metric_ts)";
    String createIndexSql4 =
        "CREATE INDEX IF NOT EXISTS idx_job_metrics_identifier_metric_ts ON job_metrics(job_identifier, metric_ts)";
    try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
        Statement stmt = conn.createStatement()) {
      stmt.execute(createTableMetricsSql);
      stmt.execute(createJobMetricsSql);
      stmt.execute(createIndexSql1);
      stmt.execute(createIndexSql2);
      stmt.execute(createIndexSql3);
      stmt.execute(createIndexSql4);
      int currentPartitionColumnLength = getCurrentPartitionColumnLength(conn);
      String alterTablePartitionSql =
          "ALTER TABLE table_metrics ALTER COLUMN table_partition VARCHAR("
              + partitionColumnLength
              + ")";
      if (currentPartitionColumnLength <= 0
          || partitionColumnLength > currentPartitionColumnLength) {
        stmt.execute(alterTablePartitionSql);
      } else if (partitionColumnLength < currentPartitionColumnLength) {
        LOG.warn(
            "Skip shrinking table_metrics.table_partition length from {} to {} to avoid migration "
                + "failure and data truncation risk.",
            currentPartitionColumnLength,
            partitionColumnLength);
      }
    } catch (SQLException e) {
      throw new MetricsStorageException(
          "Failed to initialize H2 metrics storage with URL: " + jdbcUrl, e);
    }
  }

  private int getCurrentPartitionColumnLength(Connection connection) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet columns = metaData.getColumns(null, null, "TABLE_METRICS", "TABLE_PARTITION")) {
      if (columns.next()) {
        return columns.getInt("COLUMN_SIZE");
      }
    }
    try (ResultSet columns = metaData.getColumns(null, null, "table_metrics", "table_partition")) {
      if (columns.next()) {
        return columns.getInt("COLUMN_SIZE");
      }
    }
    return -1;
  }

  @Override
  public void storeTableMetric(
      NameIdentifier nameIdentifier,
      String metricName,
      Optional<String> partition,
      MetricRecord metric) {
    validateWriteArguments(nameIdentifier, metricName, partition, metric);
    String insertSql =
        "INSERT INTO table_metrics (table_identifier, metric_name, table_partition, metric_ts, metric_value) VALUES (?, ?, ?, ?, ?)";

    String normalizedIdentifier = normalizeIdentifier(nameIdentifier);
    String normalizedMetricName = normalizeMetricName(metricName);
    Optional<String> normalizedPartition = normalizePartition(partition);
    String metricValue = metric.getValue();
    long metricTimestamp = metric.getTimestamp();

    try (Connection conn = getConnection();
        PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
      insertStmt.setString(1, normalizedIdentifier);
      insertStmt.setString(2, normalizedMetricName);
      insertStmt.setString(3, normalizedPartition.orElse(null));
      insertStmt.setLong(4, metricTimestamp);
      insertStmt.setString(5, metricValue);
      insertStmt.executeUpdate();
    } catch (SQLException e) {
      throw new MetricsStorageException(
          "Failed to store table metric: identifier="
              + nameIdentifier
              + ", metric="
              + metricName
              + ", partition="
              + partition.orElse("<table-level>"),
          e);
    }
  }

  @Override
  public Map<String, List<MetricRecord>> getTableMetrics(
      NameIdentifier nameIdentifier, long fromSecs, long toSecs) {
    Preconditions.checkArgument(nameIdentifier != null, "nameIdentifier must not be null");
    validateTimeWindow(fromSecs, toSecs);
    Map<String, List<MetricRecord>> resultMap = new HashMap<>();
    String sql =
        "SELECT metric_name, metric_ts, metric_value FROM table_metrics "
            + "WHERE table_identifier = ? AND metric_ts >= ? AND metric_ts < ? "
            + "AND table_partition IS NULL";

    try (Connection conn = getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, normalizeIdentifier(nameIdentifier));
      pstmt.setLong(2, fromSecs);
      pstmt.setLong(3, toSecs);

      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          String metricName = rs.getString("metric_name");
          long timestamp = rs.getLong("metric_ts");
          String value = rs.getString("metric_value");
          MetricRecord metric = new MetricRecordImpl(timestamp, value);
          resultMap.computeIfAbsent(metricName, k -> new ArrayList<>()).add(metric);
        }
      }
    } catch (SQLException e) {
      throw new MetricsStorageException(
          "Failed to retrieve table metrics: identifier="
              + nameIdentifier
              + ", from="
              + fromSecs
              + ", to="
              + toSecs,
          e);
    }
    return resultMap;
  }

  @Override
  public Map<String, List<MetricRecord>> getPartitionMetrics(
      NameIdentifier nameIdentifier, String partition, long fromSecs, long toSecs) {
    Preconditions.checkArgument(nameIdentifier != null, "nameIdentifier must not be null");
    Preconditions.checkArgument(StringUtils.isNotBlank(partition), "partition must not be blank");
    validateTimeWindow(fromSecs, toSecs);
    Map<String, List<MetricRecord>> resultMap = new HashMap<>();
    String normalizedPartition = normalizePartition(partition).orElse(null);
    String sql =
        "SELECT metric_name, metric_ts, metric_value FROM table_metrics "
            + "WHERE table_identifier = ? AND table_partition = ? "
            + "AND metric_ts >= ? AND metric_ts < ?";

    try (Connection conn = getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, normalizeIdentifier(nameIdentifier));
      pstmt.setString(2, normalizedPartition);
      pstmt.setLong(3, fromSecs);
      pstmt.setLong(4, toSecs);

      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          String metricName = rs.getString("metric_name");
          long timestamp = rs.getLong("metric_ts");
          String value = rs.getString("metric_value");
          MetricRecord metric = new MetricRecordImpl(timestamp, value);
          resultMap.computeIfAbsent(metricName, k -> new ArrayList<>()).add(metric);
        }
      }
    } catch (SQLException e) {
      throw new MetricsStorageException(
          "Failed to retrieve partition metrics: identifier="
              + nameIdentifier
              + ", partition="
              + partition
              + ", from="
              + fromSecs
              + ", to="
              + toSecs,
          e);
    }
    return resultMap;
  }

  @Override
  public void storeJobMetric(
      NameIdentifier nameIdentifier, String metricName, MetricRecord metric) {
    validateWriteArguments(nameIdentifier, metricName, Optional.empty(), metric);
    String insertSql =
        "INSERT INTO job_metrics (job_identifier, metric_name, metric_ts, metric_value) VALUES (?, ?, ?, ?)";

    String normalizedIdentifier = normalizeIdentifier(nameIdentifier);
    String normalizedMetricName = normalizeMetricName(metricName);
    String metricValue = metric.getValue();
    long metricTimestamp = metric.getTimestamp();

    try (Connection conn = getConnection();
        PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
      insertStmt.setString(1, normalizedIdentifier);
      insertStmt.setString(2, normalizedMetricName);
      insertStmt.setLong(3, metricTimestamp);
      insertStmt.setString(4, metricValue);
      insertStmt.executeUpdate();
    } catch (SQLException e) {
      throw new MetricsStorageException(
          "Failed to store job metric: identifier=" + nameIdentifier + ", metric=" + metricName, e);
    }
  }

  @Override
  public Map<String, List<MetricRecord>> getJobMetrics(
      NameIdentifier nameIdentifier, long fromSecs, long toSecs) {
    Preconditions.checkArgument(nameIdentifier != null, "nameIdentifier must not be null");
    validateTimeWindow(fromSecs, toSecs);
    Map<String, List<MetricRecord>> resultMap = new HashMap<>();
    String sql =
        "SELECT metric_name, metric_ts, metric_value FROM job_metrics "
            + "WHERE job_identifier = ? AND metric_ts >= ? AND metric_ts < ?";

    try (Connection conn = getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, normalizeIdentifier(nameIdentifier));
      pstmt.setLong(2, fromSecs);
      pstmt.setLong(3, toSecs);

      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          String metricName = rs.getString("metric_name");
          long timestamp = rs.getLong("metric_ts");
          String value = rs.getString("metric_value");
          MetricRecord metric = new MetricRecordImpl(timestamp, value);
          resultMap.computeIfAbsent(metricName, k -> new ArrayList<>()).add(metric);
        }
      }
    } catch (SQLException e) {
      throw new MetricsStorageException(
          "Failed to retrieve job metrics: identifier="
              + nameIdentifier
              + ", from="
              + fromSecs
              + ", to="
              + toSecs,
          e);
    }
    return resultMap;
  }

  private String normalizeIdentifier(NameIdentifier identifier) {
    return identifier == null ? null : identifier.toString().toLowerCase(Locale.ROOT);
  }

  private String normalizeMetricName(String metricName) {
    return metricName == null ? null : metricName.toLowerCase(Locale.ROOT);
  }

  private Optional<String> normalizePartition(String partition) {
    return normalizePartition(Optional.ofNullable(partition));
  }

  private Optional<String> normalizePartition(Optional<String> partition) {
    return partition.map(p -> p.toLowerCase(Locale.ROOT));
  }

  @Override
  public int cleanupTableMetricsBefore(long beforeTimestamp) {
    Preconditions.checkArgument(
        beforeTimestamp >= 0, "beforeTimestamp must be non-negative, but got %s", beforeTimestamp);
    Preconditions.checkArgument(
        beforeTimestamp <= MAX_REASONABLE_EPOCH_SECONDS,
        "beforeTimestamp must be epoch seconds, but got suspiciously large value %s",
        beforeTimestamp);
    String sql = "DELETE FROM table_metrics WHERE metric_ts < ?";

    try (Connection conn = getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setLong(1, beforeTimestamp);
      int deletedRows = pstmt.executeUpdate();
      LOG.info("Cleaned up {} rows from table_metrics before {}", deletedRows, beforeTimestamp);
      return deletedRows;
    } catch (SQLException e) {
      throw new MetricsStorageException(
          "Failed to cleanup table metrics before timestamp: " + beforeTimestamp, e);
    }
  }

  @Override
  public int cleanupJobMetricsBefore(long beforeTimestamp) {
    Preconditions.checkArgument(
        beforeTimestamp >= 0, "beforeTimestamp must be non-negative, but got %s", beforeTimestamp);
    Preconditions.checkArgument(
        beforeTimestamp <= MAX_REASONABLE_EPOCH_SECONDS,
        "beforeTimestamp must be epoch seconds, but got suspiciously large value %s",
        beforeTimestamp);
    String sql = "DELETE FROM job_metrics WHERE metric_ts < ?";

    try (Connection conn = getConnection();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setLong(1, beforeTimestamp);
      int deletedRows = pstmt.executeUpdate();
      LOG.info("Cleaned up {} rows from job_metrics before {}", deletedRows, beforeTimestamp);
      return deletedRows;
    } catch (SQLException e) {
      throw new MetricsStorageException(
          "Failed to cleanup job metrics before timestamp: " + beforeTimestamp, e);
    }
  }

  private Connection getConnection() throws SQLException {
    ensureInitialized();
    return DriverManager.getConnection(jdbcUrl, username, password);
  }

  private void ensureInitialized() {
    Preconditions.checkState(
        initialized,
        "H2MetricsRepository has not been initialized. Call initialize(properties) before use.");
  }

  @Override
  public void close() {
    // No-op by design. This repository is stateless and opens/closes a JDBC connection per
    // operation. Avoid issuing H2 SHUTDOWN here because monitor/updater may share the same DB.
  }

  private String resolveStoragePath(String configuredPath) {
    Path path = Paths.get(configuredPath);
    if (path.isAbsolute()) {
      return configuredPath;
    }

    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    if (StringUtils.isBlank(gravitinoHome)) {
      return configuredPath;
    }

    return Paths.get(gravitinoHome, configuredPath).toString();
  }

  private void validateTimeWindow(long fromSecs, long toSecs) {
    Preconditions.checkArgument(
        fromSecs < toSecs,
        "Invalid time window: fromSecs (%s) must be less than toSecs (%s)",
        fromSecs,
        toSecs);
  }

  private void validateWriteArguments(
      NameIdentifier nameIdentifier,
      String metricName,
      Optional<String> partition,
      MetricRecord metric) {
    Preconditions.checkArgument(nameIdentifier != null, "nameIdentifier must not be null");
    Preconditions.checkArgument(StringUtils.isNotBlank(metricName), "metricName must not be blank");
    Preconditions.checkArgument(
        nameIdentifier.toString().length() <= MAX_IDENTIFIER_LENGTH,
        "nameIdentifier length exceeds max %s: actual=%s",
        MAX_IDENTIFIER_LENGTH,
        nameIdentifier.toString().length());
    Preconditions.checkArgument(
        metricName.length() <= MAX_METRIC_NAME_LENGTH,
        "metricName length exceeds max %s: actual=%s",
        MAX_METRIC_NAME_LENGTH,
        metricName.length());
    Preconditions.checkArgument(metric != null, "metric record must not be null");
    Preconditions.checkArgument(
        metric.getTimestamp() >= 0,
        "metric timestamp must be non-negative, but got %s",
        metric.getTimestamp());
    Preconditions.checkArgument(
        metric.getTimestamp() <= MAX_REASONABLE_EPOCH_SECONDS,
        "metric timestamp must be epoch seconds, but got suspiciously large value %s",
        metric.getTimestamp());
    Preconditions.checkArgument(metric.getValue() != null, "metric value must not be null");
    Preconditions.checkArgument(
        metric.getValue().length() <= MAX_METRIC_VALUE_LENGTH,
        "metric value length exceeds max %s: actual=%s",
        MAX_METRIC_VALUE_LENGTH,
        metric.getValue().length());
    if (partition.isPresent()) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(partition.get()), "partition must not be blank");
      Preconditions.checkArgument(
          partition.get().length() <= partitionColumnLength,
          "partition length exceeds max %s: actual=%s",
          partitionColumnLength,
          partition.get().length());
    }
  }

  private String constructH2JdbcUrl(String originUrl) {
    String resolvedUrl = originUrl;
    if (!containsJdbcParam(resolvedUrl, "DB_CLOSE_DELAY")) {
      resolvedUrl = resolvedUrl + ";DB_CLOSE_DELAY=-1";
    }
    if (!containsJdbcParam(resolvedUrl, "MODE")) {
      resolvedUrl = resolvedUrl + ";MODE=MYSQL";
    }
    if (!containsJdbcParam(resolvedUrl, "AUTO_SERVER")) {
      resolvedUrl = resolvedUrl + ";AUTO_SERVER=TRUE";
    }
    return resolvedUrl;
  }

  private boolean containsJdbcParam(String jdbcUrl, String paramName) {
    String upperUrl = jdbcUrl.toUpperCase(Locale.ROOT);
    String target = (paramName + "=").toUpperCase(Locale.ROOT);
    return upperUrl.contains(target);
  }

  /** Configuration wrapper for H2 metrics storage options. */
  public static class H2MetricsRepositoryConfig extends Config {
    static final String H2_METRICS_PREFIX = "h2Metrics.";

    public static final String STORAGE_PATH = "storagePath";
    public static final String JDBC_URL = "jdbcUrl";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String PARTITION_COLUMN_LENGTH = "partitionColumnLength";

    public static final ConfigEntry<String> STORAGE_PATH_CONFIG =
        new ConfigBuilder(STORAGE_PATH)
            .doc("The path for H2 metrics storage.")
            .version(ConfigConstants.VERSION_1_2_0)
            .stringConf()
            .createWithDefault("./data/metrics.db");

    public static final ConfigEntry<String> JDBC_URL_CONFIG =
        new ConfigBuilder(JDBC_URL)
            .doc(
                "Optional H2 JDBC URL. If provided, it takes precedence over storage path and "
                    + "missing parameters DB_CLOSE_DELAY/MODE/AUTO_SERVER are auto-appended.")
            .version(ConfigConstants.VERSION_1_2_0)
            .stringConf()
            .createWithDefault("");

    public static final ConfigEntry<String> USERNAME_CONFIG =
        new ConfigBuilder(USERNAME)
            .doc("H2 username for metrics repository.")
            .version(ConfigConstants.VERSION_1_2_0)
            .stringConf()
            .createWithDefault(DEFAULT_USER);

    public static final ConfigEntry<String> PASSWORD_CONFIG =
        new ConfigBuilder(PASSWORD)
            .doc("H2 password for metrics repository.")
            .version(ConfigConstants.VERSION_1_2_0)
            .stringConf()
            .createWithDefault(DEFAULT_PASSWORD);

    public static final ConfigEntry<Integer> PARTITION_COLUMN_LENGTH_CONFIG =
        new ConfigBuilder(PARTITION_COLUMN_LENGTH)
            .doc("Length of table_metrics.table_partition column.")
            .version(ConfigConstants.VERSION_1_2_0)
            .intConf()
            .checkValue(
                value -> value != null && value > 0,
                "partitionColumnLength must be a positive integer")
            .createWithDefault(DEFAULT_PARTITION_COLUMN_LENGTH);

    /**
     * Creates H2 metrics storage config from raw properties.
     *
     * @param properties h2Metrics scoped properties
     */
    public H2MetricsRepositoryConfig(Map<String, String> properties) {
      super(false);
      loadFromMap(properties, k -> true);
    }
  }
}
