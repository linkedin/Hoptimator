package com.linkedin.hoptimator.venice;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validated;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.avro.AvroConverter;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import com.linkedin.hoptimator.util.ConnectionService;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerClientFactory;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;


/**
 * Deployer for Venice stores.
 * Handles creation, update, and deletion of Venice stores based on Hoptimator schema.
 *
 * <p>Implements {@link Validated} to pre-check Venice configuration and schema constraints
 * before any deployment side effects.
 */
public class VeniceDeployer implements Deployer, Validated {

  private static final Logger log = LoggerFactory.getLogger(VeniceDeployer.class);

  protected final Source source;
  protected final Properties properties;
  protected final HoptimatorConnection connection;

  public VeniceDeployer(Source source, Properties properties, HoptimatorConnection connection) {
    this.source = source;
    this.properties = properties;
    this.connection = connection;
  }

  @Override
  public void validate(Validator.Issues issues) {
    String storeName = source.table();

    // Validate Venice configuration
    String routerUrl = properties.getProperty("router.url");
    if (routerUrl == null || routerUrl.isEmpty()) {
      issues.error("Missing required property 'router.url' in Venice configuration for store " + storeName);
    }

    String clusterStr = properties.getProperty("clusters");
    if (clusterStr == null || clusterStr.isEmpty()) {
      issues.error("Missing required property 'clusters' in Venice configuration for store " + storeName);
    }

    // Validate schema can be generated
    try {
      Pair<Schema, Schema> keyPayloadSchema = getKeyPayloadSchema();
      Schema keySchema = keyPayloadSchema.left;
      Schema valueSchema = keyPayloadSchema.right;

      if (keySchema == null) {
        issues.error("Failed to generate key schema for Venice store " + storeName);
      }
      if (valueSchema == null) {
        issues.error("Failed to generate value schema for Venice store " + storeName);
      }

      // If store exists, validate key schema hasn't changed and value schema is backwards compatible
      try (ControllerClient controllerClient = createControllerClient()) {
        if (checkStoreExists(controllerClient)) {
          // Validate key schema hasn't changed
          SchemaResponse keySchemaResponse = controllerClient.getKeySchema(storeName);
          if (!keySchemaResponse.isError() && keySchemaResponse.getSchemaStr() != null) {
            Schema existingKeySchema = new Schema.Parser().parse(keySchemaResponse.getSchemaStr());
            if (!existingKeySchema.equals(keySchema)) {
              issues.error("Key schema evolution is not supported in Venice. Store " + storeName
                  + " has existing key schema but attempted to change it.");
            }
          }

          // Validate value schema is backward compatible
          SchemaResponse valueSchemaResponse = controllerClient.getValueSchema(storeName, -1); // -1 gets latest
          if (!valueSchemaResponse.isError() && valueSchemaResponse.getSchemaStr() != null) {
            Schema existingValueSchema = new Schema.Parser().parse(valueSchemaResponse.getSchemaStr());

            // Check backward compatibility (new schema as reader, old schema as writer)
            SchemaCompatibility.SchemaPairCompatibility compatibility =
                SchemaCompatibility.checkReaderWriterCompatibility(valueSchema, existingValueSchema);

            if (compatibility.getType() != SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE) {
              issues.error("Value schema is not backward compatible with existing schema for store " + storeName
                  + ": " + compatibility.getDescription());
            }
          }
        }
      } catch (Exception e) {
        // Log but don't fail validation if we can't check existing store
        log.debug("Could not check existing store {} during validation: {}", storeName, e.getMessage());
      }
    } catch (Exception e) {
      issues.error("Failed to generate schemas for Venice store " + storeName + ": " + e.getMessage());
    }
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
    log.warn("Restoring Venice store is currently not supported");
  }

  protected Pair<Schema, Schema> getKeyPayloadSchema() throws SQLException {
    return AvroConverter.avroKeyPayloadSchema("com.linkedin.hoptimator",
        source.table() + "_Key",
        source.table() + "_Value",
        HoptimatorDriver.rowType(source, connection),
        ConnectionService.configure(source, connection));
  }

  private boolean checkStoreExists(ControllerClient controllerClient) {
    try {
      StoreResponse response = controllerClient.getStore(source.table());
      return response.getStore() != null;
    } catch (VeniceHttpException e) {
      if (e.getHttpStatusCode() == 404) {
        return false;
      }
      throw e;
    }
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

  protected ControllerClient createControllerClient() throws SQLException {
    String routerUrl = properties.getProperty("router.url");
    String clusterStr = properties.getProperty("clusters");

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
    String sslConfigPath = properties.getProperty("ssl-config-path");
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
