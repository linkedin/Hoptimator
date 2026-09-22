package com.linkedin.hoptimator.pinot;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validated;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import com.linkedin.hoptimator.util.DeploymentService;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deployer for Pinot tables. Provisions a table's schema and (offline by default) table config via
 * the Pinot Controller REST API, mirroring the Venice deployer's lifecycle.
 *
 * <p>Column roles and date-time format/granularity are driven by connector hints — see
 * {@link PinotSchemas} for the full hint vocabulary ({@code metrics}, {@code timeColumns},
 * {@code primaryTimeColumn}, {@code format.<col>}, {@code granularity.<col>}, ...). Table-level
 * hints handled here: {@code tableType} ({@code OFFLINE} default / {@code REALTIME}) and
 * {@code numReplicas} (default 1).
 *
 * <p>A {@code REALTIME} table additionally synthesizes a Kafka stream ingestion config from hints
 * (see {@link PinotStreamConfigs}) and requires a primary time column (Pinot needs a time column to
 * name/flush consuming segments).
 *
 * <p>TODO(known gap): OFFLINE {@code segmentPushType} (APPEND/REFRESH) and retention
 * ({@code segmentsConfig}) are not modeled yet on this generic controller path — only
 * {@code tableType}, {@code numReplicas} and the time column are set.
 */
public class PinotDeployer implements Deployer, Validated {

  private static final Logger log = LoggerFactory.getLogger(PinotDeployer.class);

  private static final int DEFAULT_NUM_REPLICAS = 1;

  protected final Source source;
  protected final Properties properties;
  protected final DeploymentContext context;

  public PinotDeployer(Source source, Properties properties, DeploymentContext context) {
    this.source = source;
    this.properties = properties;
    this.context = context;
  }

  protected PinotControllerClient createControllerClient() {
    return new PinotControllerClient(properties.getProperty(PinotDriver.CONTROLLER_URL));
  }

  @Override
  public void validate(Validator.Issues issues, DeploymentContext context) {
    if (properties.getProperty(PinotDriver.CONTROLLER_URL) == null) {
      issues.error("Missing required property '" + PinotDriver.CONTROLLER_URL + "' for Pinot table "
          + source.table());
      return;
    }
    RelDataType rowType;
    try {
      rowType = HoptimatorDriver.rowType(source, context);
    } catch (SQLException e) {
      issues.error("Failed to derive schema for Pinot table " + source.table() + ": " + e.getMessage());
      return;
    }
    validateRowType(issues, rowType);
  }

  /**
   * Pre-flights the same checks Pinot's controller enforces (via {@link PinotSchemas#validationErrors}),
   * so a bad spec cannot pass through and create something. For a REALTIME table, additionally
   * requires a primary time column and a valid Kafka stream config (via {@link PinotStreamConfigs}).
   */
  void validateRowType(Validator.Issues issues, RelDataType rowType) {
    Map<String, String> hints = hints();
    PinotSchemas.validationErrors(source.table(), rowType, hints).forEach(issues::error);
    if (isRealtime(hints)) {
      if (PinotSchemas.primaryTimeColumn(hints, PinotSchemas.timeColumns(hints)) == null) {
        issues.error("REALTIME Pinot table " + source.table() + " requires a primary time column: declare"
            + " it via 'timeColumn'/'timeColumns' (and 'primaryTimeColumn' when more than one).");
      }
      PinotStreamConfigs.validationErrors(source.table(), hints).forEach(issues::error);
    }
  }

  @Override
  public boolean exists() throws SQLException {
    try (PinotControllerClient client = createControllerClient()) {
      return client.tableExists(source.table());
    } catch (IOException e) {
      throw new SQLException("Failed to check whether Pinot table exists: " + source.table(), e);
    }
  }

  @Override
  public void create() throws SQLException {
    try (PinotControllerClient client = createControllerClient()) {
      if (client.tableExists(source.table())) {
        log.info("Pinot table {} already exists, skipping creation", source.table());
        return;
      }
      client.addSchema(buildSchema(), false);
      client.addTable(buildTableConfig());
      log.info("Successfully created Pinot table {}", source.table());
    } catch (IOException e) {
      throw new SQLException("Failed to create Pinot table: " + source.table(), e);
    }
  }

  @Override
  public void update() throws SQLException {
    try (PinotControllerClient client = createControllerClient()) {
      if (!client.tableExists(source.table())) {
        client.addSchema(buildSchema(), false);
        client.addTable(buildTableConfig());
        log.info("Successfully created Pinot table {}", source.table());
      } else {
        // Pinot allows backward-compatible schema evolution in place; also reconcile the table config
        // (e.g. numReplicas or REALTIME stream config changes) via the update endpoint.
        client.updateSchema(buildSchema());
        client.updateTable(buildTableConfig());
        log.info("Successfully updated Pinot table {}", source.table());
      }
    } catch (IOException e) {
      throw new SQLException("Failed to update Pinot table: " + source.table(), e);
    }
  }

  @Override
  public void delete() throws SQLException {
    try (PinotControllerClient client = createControllerClient()) {
      if (!client.tableExists(source.table())) {
        log.info("Pinot table {} not found, skipping deletion", source.table());
        return;
      }
      client.dropTable(source.table());
      // Drop the schema too, but tolerate failure: a schema can be shared (e.g. a hybrid
      // OFFLINE+REALTIME table, or another table type still referencing it), in which case the
      // controller rejects the schema delete. That must not fail the table delete.
      try {
        client.dropSchema(source.table());
      } catch (IOException e) {
        log.warn("Dropped Pinot table {} but could not drop its schema (it may still be referenced): {}",
            source.table(), e.getMessage());
      }
      log.info("Successfully deleted Pinot table {}", source.table());
    } catch (IOException e) {
      throw new SQLException("Failed to delete Pinot table: " + source.table(), e);
    }
  }

  @Override
  public List<String> specify() throws SQLException {
    return Collections.emptyList();
  }

  @Override
  public void restore() {
    log.warn("Restoring Pinot table is currently not supported");
  }

  protected Schema buildSchema() throws SQLException {
    return buildSchema(HoptimatorDriver.rowType(source, context));
  }

  Schema buildSchema(RelDataType rowType) {
    return PinotSchemas.build(source.table(), rowType, hints());
  }

  protected TableConfig buildTableConfig() {
    Map<String, String> hints = hints();
    boolean realtime = isRealtime(hints);
    TableType tableType = realtime ? TableType.REALTIME : TableType.OFFLINE;
    int numReplicas = hints.containsKey("numReplicas")
        ? Integer.parseInt(hints.get("numReplicas")) : DEFAULT_NUM_REPLICAS;
    TableConfigBuilder builder = new TableConfigBuilder(tableType)
        .setTableName(source.table())
        .setNumReplicas(numReplicas);
    String primaryTimeColumn = PinotSchemas.primaryTimeColumn(hints, PinotSchemas.timeColumns(hints));
    if (primaryTimeColumn != null) {
      builder.setTimeColumnName(primaryTimeColumn);
    }
    if (realtime) {
      // Synthesize the Kafka stream ingestion config; the Pinot server consumes the topic directly.
      IngestionConfig ingestionConfig = new IngestionConfig();
      ingestionConfig.setStreamIngestionConfig(
          new StreamIngestionConfig(Collections.singletonList(PinotStreamConfigs.build(hints))));
      builder.setIngestionConfig(ingestionConfig);
    }
    return builder.build();
  }

  private static boolean isRealtime(Map<String, String> hints) {
    return "REALTIME".equalsIgnoreCase(hints.get("tableType"));
  }

  /** Connector hints for the table: {@link Source} options take precedence over connection hints. */
  private Map<String, String> hints() {
    Map<String, String> merged = new HashMap<>(DeploymentService.parseHints(properties));
    if (source.options() != null) {
      merged.putAll(source.options());
    }
    return merged;
  }
}

