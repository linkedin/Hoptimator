package com.linkedin.hoptimator.venice;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.avro.AvroConverter;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import com.linkedin.hoptimator.util.ConfigService;
import com.linkedin.hoptimator.util.ConnectionService;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerClientFactory;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Deployer for Venice stores.
 * Handles creation, update, and deletion of Venice stores based on Hoptimator schema.
 */
public class VeniceDeployer implements Deployer {

  private static final Logger log = LoggerFactory.getLogger(VeniceDeployer.class);
  private static final String VENICE_CONFIG = "venice.config";

  protected final Source source;
  protected final HoptimatorConnection connection;

  public VeniceDeployer(Source source, HoptimatorConnection connection) {
    this.source = source;
    this.connection = connection;
  }

  @Override
  public void create() throws SQLException {
    try (ControllerClient controllerClient = createControllerClient()) {
      if (checkStoreExists(controllerClient)) {
        log.info("Venice store {} already exists, skipping creation", source.table());
        return;
      }
      Pair<Schema, Schema> keyPayloadSchema = getKeyPayloadSchema();
      createVeniceStore(controllerClient, keyPayloadSchema.left, keyPayloadSchema.right);
    } catch (RuntimeException e) {
      throw new SQLException("Failed to create Venice store: " + source.table(), e);
    }
  }

  @Override
  public void delete() throws SQLException {
    try (ControllerClient controllerClient = createControllerClient()) {
      if (!checkStoreExists(controllerClient)) {
        log.info("Venice store {} not found, skipping deletion", source.table());
        return;
      }
      ControllerResponse resp = controllerClient.disableAndDeleteStore(source.table());
      if (resp.isError()) {
        throw new SQLNonTransientException(String.format("Failed to delete Venice store=%s, errorType=%s, error=%s",
            source.table(), resp.getErrorType(), resp.getError()));
      }
      log.info("Successfully deleted Venice store {}", source.table());
    } catch (RuntimeException e) {
      throw new SQLException("Failed to delete Venice store: " + source.table(), e);
    }
  }

  @Override
  public void update() throws SQLException {
    try (ControllerClient controllerClient = createControllerClient()) {
      Pair<Schema, Schema> keyPayloadSchema = getKeyPayloadSchema();
      if (!checkStoreExists(controllerClient)) {
        createVeniceStore(controllerClient, keyPayloadSchema.left, keyPayloadSchema.right);
      } else {
        // Verify key schema has not changed - Venice does not support key schema evolution
        validateKeySchemaUnchanged(controllerClient, keyPayloadSchema.left);

        SchemaResponse resp = controllerClient.addValueSchema(source.table(), keyPayloadSchema.right.toString());
        if (resp.isError()) {
          throw new SQLNonTransientException(String.format("Failed to update Venice store=%s, errorType=%s, error=%s",
              source.table(), resp.getErrorType(), resp.getError()));
        }
        log.info("Successfully updated Venice store {} with new value schema ID: {}", source.table(), resp.getId());
      }
    } catch (RuntimeException e) {
      throw new SQLException("Failed to update Venice store: " + source.table(), e);
    }
  }

  @Override
  public List<String> specify() throws SQLException {
    return Collections.emptyList();
  }

  @Override
  public void restore() {
    // TODO: Restoration can be complicated
  }

  protected Pair<Schema, Schema> getKeyPayloadSchema() throws SQLException {
    return AvroConverter.avroKeyPayloadSchema("com.linkedin.hoptimator",
        source.table() + "_Key",
        source.table() + "_Value",
        HoptimatorDriver.rowType(source, connection),
        ConnectionService.configure(source, connection));
  }

  private boolean checkStoreExists(ControllerClient controllerClient) {
    StoreResponse response = controllerClient.getStore(source.table());
    return response.getStore() != null;
  }

  /**
   * Validates that the key schema has not changed from what's currently in Venice.
   * Venice does not support key schema evolution after store creation.
   *
   * @param controllerClient the Venice controller client
   * @param newKeySchema the new key schema to validate
   * @throws SQLException if the key schema has changed
   */
  private void validateKeySchemaUnchanged(ControllerClient controllerClient, Schema newKeySchema) throws SQLException {
    SchemaResponse keySchemaResponse = controllerClient.getKeySchema(source.table());
    if (keySchemaResponse.isError()) {
      throw new SQLNonTransientException(String.format(
          "Failed to retrieve key schema for Venice store=%s, errorType=%s, error=%s",
          source.table(), keySchemaResponse.getErrorType(), keySchemaResponse.getError()));
    }

    String existingKeySchemaStr = keySchemaResponse.getSchemaStr();
    if (existingKeySchemaStr == null) {
      throw new SQLNonTransientException(String.format(
          "No key schema found for Venice store=%s", source.table()));
    }

    Schema existingKeySchema = new Schema.Parser().parse(existingKeySchemaStr);

    // Compare schemas - Venice doesn't allow key schema changes
    if (!existingKeySchema.equals(newKeySchema)) {
      throw new SQLNonTransientException(String.format(
          "Key schema evolution is not supported in Venice. Store=%s has existing key schema but attempted to change it. "
          + "Existing: %s, New: %s",
          source.table(), existingKeySchema.toString(true), newKeySchema.toString(true)));
    }
  }

  private void createVeniceStore(ControllerClient controllerClient, Schema keySchema, Schema valueSchema) throws SQLException {
    ControllerResponse resp = controllerClient.createNewStore(source.table(), "", keySchema.toString(), valueSchema.toString());
    if (resp.isError()) {
      throw new SQLNonTransientException(String.format("Failed to create Venice store=%s, errorType=%s, error=%s",
          source.table(), resp.getErrorType(), resp.getError()));
    }
    log.info("Successfully created Venice store {}", source.table());
  }

  private ControllerClient createControllerClient() throws SQLException {
    Properties veniceProperties = ConfigService.config(connection, false, VENICE_CONFIG);
    veniceProperties.putAll(connection.connectionProperties());
    String routerUrl = veniceProperties.getProperty("router.url");

    String clusterStr = veniceProperties.getProperty("clusters");
    if (clusterStr == null || clusterStr.isEmpty()) {
      throw new SQLNonTransientException("Missing clusters property in Venice configuration");
    }
    // TODO: make more robust, should have the ability to dynamically specify the cluster to deploy to.
    // Use the first cluster for deployment operations
    String cluster = clusterStr.split(",")[0];

    if (routerUrl == null) {
      throw new SQLNonTransientException("Missing router.url in Venice configuration");
    }

    // Initialize SSL factory if configured
    String sslConfigPath = veniceProperties.getProperty("ssl-config-path");
    Optional<SSLFactory> sslFactory;
    if (sslConfigPath != null) {
      try {
        log.info("Using SSL configs at {}", sslConfigPath);
        Properties sslProperties = SslUtils.loadSSLConfig(sslConfigPath);
        String sslFactoryClassName = sslProperties.getProperty(ClusterSchema.SSL_FACTORY_CLASS_NAME,
            ClusterSchema.DEFAULT_SSL_FACTORY_CLASS_NAME);
        sslFactory = Optional.of(SslUtils.getSSLFactory(sslProperties, sslFactoryClassName));
      } catch (Exception e) {
        throw new SQLNonTransientException("Problem loading SSL configs at " + sslConfigPath, e);
      }
    } else {
      sslFactory = Optional.empty();
    }

    if (routerUrl.contains("localhost")) {
      return new LocalControllerClient(cluster, routerUrl, sslFactory);
    } else {
      return ControllerClientFactory.getControllerClient(cluster, routerUrl, sslFactory);
    }
  }
}
