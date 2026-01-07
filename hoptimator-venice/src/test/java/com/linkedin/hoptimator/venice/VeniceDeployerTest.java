package com.linkedin.hoptimator.venice;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import java.sql.DriverManager;
import java.sql.SQLNonTransientException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Tests for VeniceDeployer.
 *
 * These tests require a running Venice instance configured via `make deploy-venice`
 */
@Tag("integration")
public class VeniceDeployerTest {

  private static final String JDBC_URL = "jdbc:hoptimator://fun=mysql";
  private static final String TEST_STORE = "deployer_test_store";
  private static final String VENICE_ROUTER_URL = "http://localhost:7777";
  private static final String VENICE_CLUSTER = "venice-cluster0";

  private HoptimatorConnection connection;

  private VeniceDeployer deployer;

  @BeforeEach
  public void setUp() throws Exception {
    // Create connection to Hoptimator
    connection = (HoptimatorConnection) DriverManager.getConnection(JDBC_URL);

    // Add test table to VENICE schema
    SchemaPlus rootSchema = connection.calciteConnection().getRootSchema();
    SchemaPlus veniceSchema = rootSchema.subSchemas().get("VENICE");
    if (veniceSchema != null) {
      veniceSchema.add(TEST_STORE, createTestTable());
    }

    Map<String, String> options = new HashMap<>();
    Source source = new Source("venice", List.of("VENICE", TEST_STORE), options);
    deployer = new VeniceDeployer(source, connection);

    // Clean up any existing test store
    deployer.delete();
  }

  @AfterEach
  public void tearDown() throws Exception {
    deployer.delete();
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  public void testDeleteStore() throws Exception {
    // Delete the store
    deployer.delete();

    // Verify store no longer exists
    assertFalse(storeExists(TEST_STORE));
  }

  @Test
  public void testValidStoreUpdate() throws Exception {
    // Update should create the store if it doesn't exist
    deployer.update();

    // Verify store exists
    assertTrue(storeExists(TEST_STORE));

    // Define a new table schema that will replace the old
    AbstractTable newTable = new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
        builder.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        builder.add("value", typeFactory.createSqlType(SqlTypeName.DOUBLE));
        builder.add("newField", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true));
        return builder.build();
      }
    };

    // Add the new schema into the VENICE schema
    SchemaPlus rootSchema = connection.calciteConnection().getRootSchema();
    SchemaPlus veniceSchema = rootSchema.subSchemas().get("VENICE");
    assertNotNull(veniceSchema);
    veniceSchema.add(TEST_STORE, newTable);

    // Update the value schema of the store
    deployer.update();

    // Fetch schemas from Venice
    Pair<Schema, Schema> veniceSchemas = getStoreKeyPayloadSchema(TEST_STORE);
    Schema veniceKeySchema = veniceSchemas.left;
    Schema veniceValueSchema = veniceSchemas.right;

    // Fetch the expected schemas from deployer (based on connection's table)
    Pair<Schema, Schema> expectedSchemas = deployer.getKeyPayloadSchema();

    // Verify schemas in Venice match what was generated from the connection's table
    assertEquals(expectedSchemas.left.toString(), veniceKeySchema.toString(),
        "Key schema in Venice should match schema generated from connection table");
    assertEquals(expectedSchemas.right.toString(), veniceValueSchema.toString(),
        "Value schema in Venice should match schema generated from connection table");

    // Verify the key field is present in key schema
    assertNotNull(veniceKeySchema.getField("id"), "Key schema should have 'id' field");

    // Verify the value fields are present in value schema
    assertNotNull(veniceValueSchema.getField("name"), "Value schema should have 'name' field");
    assertNotNull(veniceValueSchema.getField("value"), "Value schema should have 'value' field");
    assertNotNull(veniceValueSchema.getField("newField"), "Value schema should have 'newField' field");
  }

  @Test
  public void testInvalidValueSchemaStoreUpdate() throws Exception {
    // Update should create the store if it doesn't exist
    deployer.update();

    // Verify store exists
    assertTrue(storeExists(TEST_STORE));

    // Invalid value schema, new field does not have a default
    AbstractTable newTable = new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
        builder.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        builder.add("value", typeFactory.createSqlType(SqlTypeName.DOUBLE));
        builder.add("newField", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        return builder.build();
      }
    };

    // Add the new schema into the VENICE schema
    SchemaPlus rootSchema = connection.calciteConnection().getRootSchema();
    SchemaPlus veniceSchema = rootSchema.subSchemas().get("VENICE");
    assertNotNull(veniceSchema);
    veniceSchema.add(TEST_STORE, newTable);

    // Update the value schema of the store
    assertThrows(SQLNonTransientException.class, () -> deployer.update());
  }

  @Test
  public void testInvalidKeySchemaStoreUpdate() throws Exception {
    // Update should create the store if it doesn't exist
    deployer.update();

    // Verify store exists
    assertTrue(storeExists(TEST_STORE));

    // Invalid value schema, new field does not have a default
    AbstractTable newTable = new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
        builder.add("KEY_newField", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true));
        builder.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        builder.add("value", typeFactory.createSqlType(SqlTypeName.DOUBLE));
        return builder.build();
      }
    };

    // Add the new schema into the VENICE schema
    SchemaPlus rootSchema = connection.calciteConnection().getRootSchema();
    SchemaPlus veniceSchema = rootSchema.subSchemas().get("VENICE");
    assertNotNull(veniceSchema);
    veniceSchema.add(TEST_STORE, newTable);

    // Update the value schema of the store
    assertThrows(SQLNonTransientException.class, () -> deployer.update());
  }

  @Test
  public void testCreateStore() throws Exception {
    // Create the store in Venice
    deployer.create();

    // Verify store was created
    assertTrue(storeExists(TEST_STORE));

    // Fetch schemas from Venice
    Pair<Schema, Schema> veniceSchemas = getStoreKeyPayloadSchema(TEST_STORE);
    Schema veniceKeySchema = veniceSchemas.left;
    Schema veniceValueSchema = veniceSchemas.right;

    // Verify key schema metadata
    assertNotNull(veniceKeySchema);
    assertEquals(TEST_STORE + "_Key", veniceKeySchema.getName());

    // Verify value schema metadata
    assertNotNull(veniceValueSchema);
    assertEquals(TEST_STORE + "_Value", veniceValueSchema.getName());

    // Fetch the expected schemas from deployer (based on connection's table)
    Pair<Schema, Schema> expectedSchemas = deployer.getKeyPayloadSchema();

    // Verify schemas in Venice match what was generated from the connection's table
    assertEquals(expectedSchemas.left.toString(), veniceKeySchema.toString(),
        "Key schema in Venice should match schema generated from connection table");
    assertEquals(expectedSchemas.right.toString(), veniceValueSchema.toString(),
        "Value schema in Venice should match schema generated from connection table");

    // Verify the key field is present in key schema
    assertNotNull(veniceKeySchema.getField("id"), "Key schema should have 'id' field");

    // Verify the value fields are present in value schema
    assertNotNull(veniceValueSchema.getField("name"), "Value schema should have 'name' field");
    assertNotNull(veniceValueSchema.getField("value"), "Value schema should have 'value' field");
  }

  /**
   * Check if a store exists in Venice.
   */
  private boolean storeExists(String storeName) {
    try (ControllerClient controllerClient = new LocalControllerClient(VENICE_CLUSTER, VENICE_ROUTER_URL, Optional.empty())) {
      StoreResponse response = controllerClient.getStore(storeName);
      return response.getStore() != null;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Fetches the key and value schemas from Venice for a given store.
   * Uses StoreSchemaFetcher to retrieve the actual schemas stored in Venice.
   */
  private Pair<Schema, Schema> getStoreKeyPayloadSchema(String storeName) {
    StoreSchemaFetcher storeSchemaFetcher = ClientFactory.createStoreSchemaFetcher(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(VENICE_ROUTER_URL));

    Schema keySchema = storeSchemaFetcher.getKeySchema();
    Schema valueSchema = storeSchemaFetcher.getLatestValueSchema();

    return new Pair<>(keySchema, valueSchema);
  }

  /**
   * Creates a test table with a simple schema for testing.
   * Schema: KEY_id (INTEGER), name (VARCHAR), value (DOUBLE)
   */
  private Table createTestTable() {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
        builder.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        builder.add("value", typeFactory.createSqlType(SqlTypeName.DOUBLE));
        return builder.build();
      }
    };
  }
}
