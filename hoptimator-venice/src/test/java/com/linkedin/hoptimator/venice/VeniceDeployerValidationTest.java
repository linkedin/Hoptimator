package com.linkedin.hoptimator.venice;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.StoreInfo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.avro.Schema;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
class VeniceDeployerValidationTest {

  @Mock
  private HoptimatorConnection mockConnection;

  @Mock
  private ControllerClient mockControllerClient;

  // --- validate() tests ---

  @Test
  void testValidateFailsMissingRouterUrl() {
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() throws SQLException {
        throw new SQLException("not available");
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Missing required property 'router.url'"));
  }

  @Test
  void testValidateFailsMissingClusters() {
    Properties props = new Properties();
    props.setProperty("router.url", "http://test-url");
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() throws SQLException {
        throw new SQLException("not available");
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Missing required property 'clusters'"));
  }

  @Test
  void testValidateReportsNullKeySchema() {
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://test-url");
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(null, Schema.create(Schema.Type.STRING));
      }

      @Override
      protected ControllerClient createControllerClient() throws SQLException {
        throw new SQLException("test - no client");
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Failed to generate key schema"));
  }

  @Test
  void testValidateReportsNullValueSchema() {
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://test-url");
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(Schema.create(Schema.Type.STRING), null);
      }

      @Override
      protected ControllerClient createControllerClient() throws SQLException {
        throw new SQLException("test - no client");
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Failed to generate value schema"));
  }

  @Test
  void testValidateReportsSchemaGenerationError() {
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://test-url");
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() throws SQLException {
        throw new SQLException("Schema generation failed");
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Failed to generate schemas"));
  }

  @Test
  void testValidateReportsValueSchemaIncompatibility() {
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://test-url");
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());

    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema newValueSchema = Schema.create(Schema.Type.INT);
    Schema existingValueSchema = Schema.create(Schema.Type.STRING);

    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    when(mockControllerClient.getStore("myStore")).thenReturn(storeResponse);

    SchemaResponse keySchemaResponse = mock(SchemaResponse.class);
    when(keySchemaResponse.isError()).thenReturn(false);
    when(keySchemaResponse.getSchemaStr()).thenReturn(keySchema.toString());
    when(mockControllerClient.getKeySchema("myStore")).thenReturn(keySchemaResponse);

    SchemaResponse valueSchemaResponse = mock(SchemaResponse.class);
    when(valueSchemaResponse.isError()).thenReturn(false);
    when(valueSchemaResponse.getSchemaStr()).thenReturn(existingValueSchema.toString());
    when(mockControllerClient.getValueSchema("myStore", -1)).thenReturn(valueSchemaResponse);

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(keySchema, newValueSchema);
      }

      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("not backward compatible"));
  }

  // --- delete() error path ---

  @Test
  void testDeleteStoreFailsOnError() {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://test-url");

    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    when(mockControllerClient.getStore("myStore")).thenReturn(storeResponse);

    ControllerResponse deleteResponse = mock(ControllerResponse.class);
    when(deleteResponse.isError()).thenReturn(true);
    when(deleteResponse.getError()).thenReturn("delete failed");
    when(mockControllerClient.disableAndDeleteStore("myStore")).thenReturn(deleteResponse);

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    assertThrows(SQLNonTransientException.class, deployer::delete);
  }

  // --- createControllerClient() error paths ---

  @Test
  void testCreateControllerClientMissingClusters() {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();
    props.setProperty("router.url", "http://test-url");

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection);

    assertThrows(SQLNonTransientException.class, deployer::createControllerClient);
  }

  @Test
  void testCreateControllerClientEmptyClusters() {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();
    props.setProperty("router.url", "http://test-url");
    props.setProperty("clusters", "");

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection);

    assertThrows(SQLNonTransientException.class, deployer::createControllerClient);
  }

  @Test
  void testCreateControllerClientMissingRouterUrl() {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection);

    assertThrows(SQLNonTransientException.class, deployer::createControllerClient);
  }

  @Test
  void testCreateControllerClientWithInvalidSslConfigThrows() {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();
    props.setProperty("router.url", "http://test-url");
    props.setProperty("clusters", "test-cluster");
    props.setProperty("ssl-config-path", "/nonexistent/ssl/config");

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection);

    assertThrows(SQLNonTransientException.class, deployer::createControllerClient);
  }

  // --- create() error paths ---

  @Test
  void testCreateStoreFailsOnError() {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://test-url");

    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    StoreResponse storeResponse = mock(StoreResponse.class);
    when(storeResponse.getStore()).thenReturn(null);
    when(mockControllerClient.getStore("myStore")).thenReturn(storeResponse);

    NewStoreResponse createResponse = mock(NewStoreResponse.class);
    when(createResponse.isError()).thenReturn(true);
    when(createResponse.getError()).thenReturn("creation failed");
    when(mockControllerClient.createNewStore(eq("myStore"), anyString(), anyString(), anyString()))
        .thenReturn(createResponse);

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(keySchema, valueSchema);
      }

      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    assertThrows(SQLNonTransientException.class, deployer::create);
  }

  @Test
  void testCreateWrapsRuntimeException() {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://test-url");

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.STRING));
      }

      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    when(mockControllerClient.getStore("myStore")).thenThrow(new RuntimeException("boom"));

    SQLException ex = assertThrows(SQLException.class, deployer::create);
    assertTrue(ex.getMessage().contains("Failed to create Venice store"));
  }

  // --- delete() RuntimeException wrapping ---

  @Test
  void testDeleteWrapsRuntimeException() {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://test-url");

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    when(mockControllerClient.getStore("myStore")).thenThrow(new RuntimeException("boom"));

    SQLException ex = assertThrows(SQLException.class, deployer::delete);
    assertTrue(ex.getMessage().contains("Failed to delete Venice store"));
  }

  // --- update() error paths ---

  @Test
  void testUpdateWrapsRuntimeException() {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://test-url");

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.STRING));
      }

      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    when(mockControllerClient.getStore("myStore")).thenThrow(new RuntimeException("boom"));

    SQLException ex = assertThrows(SQLException.class, deployer::update);
    assertTrue(ex.getMessage().contains("Failed to update Venice store"));
  }

  @Test
  void testUpdateFailsWhenKeySchemaRetrievalErrors() {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://test-url");

    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    when(mockControllerClient.getStore("myStore")).thenReturn(storeResponse);

    SchemaResponse keySchemaResponse = mock(SchemaResponse.class);
    when(keySchemaResponse.isError()).thenReturn(true);
    when(keySchemaResponse.getError()).thenReturn("schema error");
    when(mockControllerClient.getKeySchema("myStore")).thenReturn(keySchemaResponse);

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(keySchema, valueSchema);
      }

      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    assertThrows(SQLNonTransientException.class, deployer::update);
  }

  @Test
  void testUpdateFailsWhenKeySchemaStringIsNull() {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://test-url");

    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    when(mockControllerClient.getStore("myStore")).thenReturn(storeResponse);

    SchemaResponse keySchemaResponse = mock(SchemaResponse.class);
    when(keySchemaResponse.isError()).thenReturn(false);
    when(keySchemaResponse.getSchemaStr()).thenReturn(null);
    when(mockControllerClient.getKeySchema("myStore")).thenReturn(keySchemaResponse);

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(keySchema, valueSchema);
      }

      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    assertThrows(SQLNonTransientException.class, deployer::update);
  }

  @Test
  void testUpdateFailsWhenKeySchemaChanged() {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://test-url");

    Schema newKeySchema = Schema.create(Schema.Type.INT);
    Schema valueSchema = Schema.create(Schema.Type.STRING);
    Schema existingKeySchema = Schema.create(Schema.Type.STRING);

    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    when(mockControllerClient.getStore("myStore")).thenReturn(storeResponse);

    SchemaResponse keySchemaResponse = mock(SchemaResponse.class);
    when(keySchemaResponse.isError()).thenReturn(false);
    when(keySchemaResponse.getSchemaStr()).thenReturn(existingKeySchema.toString());
    when(mockControllerClient.getKeySchema("myStore")).thenReturn(keySchemaResponse);

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(newKeySchema, valueSchema);
      }

      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    SQLNonTransientException ex = assertThrows(SQLNonTransientException.class, deployer::update);
    assertTrue(ex.getMessage().contains("Key schema evolution is not supported"));
  }

  // --- checkStoreExists() with VeniceHttpException ---

  @Test
  void testCheckStoreExistsReturnsfalseFor404() throws Exception {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://test-url");

    when(mockControllerClient.getStore("myStore")).thenThrow(new VeniceHttpException(404, "Not found"));

    // delete() calls checkStoreExists - if 404, it should skip deletion gracefully
    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    deployer.delete(); // should not throw
  }

  @Test
  void testCheckStoreExistsRethrowsNon404() {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://test-url");

    when(mockControllerClient.getStore("myStore")).thenThrow(new VeniceHttpException(500, "Server error"));

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    // RuntimeException from VeniceHttpException gets wrapped in SQLException
    assertThrows(SQLException.class, deployer::delete);
  }

  // --- validate() controller client exception during store check ---

  @Test
  void testValidateHandlesControllerClientException() {
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://test-url");
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());

    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(keySchema, valueSchema);
      }

      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    when(mockControllerClient.getStore("myStore")).thenThrow(new RuntimeException("connection failed"));

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    // Should not fail validation, just log debug
    assertTrue(issues.valid());
  }

  // --- restore() ---

  @Test
  void testRestoreDoesNotThrow() {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection);
    deployer.restore();
  }

  // --- validate() empty (not null) router.url still triggers error ---

  @Test
  void testValidateFailsWithEmptyRouterUrl() {
    Properties props = new Properties();
    props.setProperty("router.url", ""); // empty, not null
    props.setProperty("clusters", "test-cluster");
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() throws SQLException {
        throw new SQLException("not available");
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Missing required property 'router.url'"));
  }

  // --- validate() empty (not null) clusters still triggers error ---

  @Test
  void testValidateFailsWithEmptyClusters() {
    Properties props = new Properties();
    props.setProperty("router.url", "http://test-url");
    props.setProperty("clusters", ""); // empty, not null
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() throws SQLException {
        throw new SQLException("not available");
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Missing required property 'clusters'"));
  }

  // --- validate() valid properties produce no config errors ---

  @Test
  void testValidatePassesWithBothRequiredProperties() {
    Properties props = new Properties();
    props.setProperty("router.url", "http://test-url");
    props.setProperty("clusters", "test-cluster");
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());

    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    StoreResponse storeResponse = mock(StoreResponse.class);
    when(storeResponse.getStore()).thenReturn(null); // store doesn't exist → skip schema comparison
    when(mockControllerClient.getStore("myStore")).thenReturn(storeResponse);

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(keySchema, valueSchema);
      }

      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertTrue(issues.valid());
  }

  // --- validate() keySchemaResponse.isError()==true → skips key-schema-change check (no error) ---

  @Test
  void testValidateSkipsKeySchemaCheckWhenResponseHasError() {
    Properties props = new Properties();
    props.setProperty("router.url", "http://test-url");
    props.setProperty("clusters", "test-cluster");
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());

    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeResponse.getStore()).thenReturn(storeInfo); // store exists
    when(mockControllerClient.getStore("myStore")).thenReturn(storeResponse);

    SchemaResponse keySchemaResponse = mock(SchemaResponse.class);
    when(keySchemaResponse.isError()).thenReturn(true); // error → skip key schema comparison
    when(mockControllerClient.getKeySchema("myStore")).thenReturn(keySchemaResponse);

    SchemaResponse valueSchemaResponse = mock(SchemaResponse.class);
    when(valueSchemaResponse.isError()).thenReturn(true); // error → skip value schema comparison
    when(mockControllerClient.getValueSchema("myStore", -1)).thenReturn(valueSchemaResponse);

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(keySchema, valueSchema);
      }

      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    // No error should be recorded — both schema responses had errors so comparisons were skipped
    assertTrue(issues.valid());
  }

  // --- validate() keySchemaResponse.getSchemaStr()==null → skips key-schema-change check ---

  @Test
  void testValidateSkipsKeySchemaCheckWhenSchemaStrIsNull() {
    Properties props = new Properties();
    props.setProperty("router.url", "http://test-url");
    props.setProperty("clusters", "test-cluster");
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());

    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeResponse.getStore()).thenReturn(storeInfo); // store exists
    when(mockControllerClient.getStore("myStore")).thenReturn(storeResponse);

    SchemaResponse keySchemaResponse = mock(SchemaResponse.class);
    when(keySchemaResponse.isError()).thenReturn(false);
    when(keySchemaResponse.getSchemaStr()).thenReturn(null); // null schema str → skip
    when(mockControllerClient.getKeySchema("myStore")).thenReturn(keySchemaResponse);

    SchemaResponse valueSchemaResponse = mock(SchemaResponse.class);
    when(valueSchemaResponse.isError()).thenReturn(false);
    when(valueSchemaResponse.getSchemaStr()).thenReturn(null); // null schema str → skip
    when(mockControllerClient.getValueSchema("myStore", -1)).thenReturn(valueSchemaResponse);

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(keySchema, valueSchema);
      }

      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    // No error: both schema strs were null so comparisons were skipped
    assertTrue(issues.valid());
  }

  // --- valueSchemaResponse.isError()==false && schemaStr!=null → performs compatibility check ---

  @Test
  void testValidatePassesWhenValueSchemaIsCompatible() {
    Properties props = new Properties();
    props.setProperty("router.url", "http://test-url");
    props.setProperty("clusters", "test-cluster");
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());

    // Both key and value schema are STRING — compatible
    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    when(mockControllerClient.getStore("myStore")).thenReturn(storeResponse);

    SchemaResponse keySchemaResponse = mock(SchemaResponse.class);
    when(keySchemaResponse.isError()).thenReturn(false);
    when(keySchemaResponse.getSchemaStr()).thenReturn(keySchema.toString());
    when(mockControllerClient.getKeySchema("myStore")).thenReturn(keySchemaResponse);

    SchemaResponse valueSchemaResponse = mock(SchemaResponse.class);
    when(valueSchemaResponse.isError()).thenReturn(false);
    when(valueSchemaResponse.getSchemaStr()).thenReturn(valueSchema.toString()); // same schema → compatible
    when(mockControllerClient.getValueSchema("myStore", -1)).thenReturn(valueSchemaResponse);

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(keySchema, valueSchema);
      }

      @Override
      protected ControllerClient createControllerClient() {
        return mockControllerClient;
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertTrue(issues.valid());
  }

  // --- createControllerClient() without ssl-config-path → uses plain client via localhost ---

  @Test
  void testCreateControllerClientWithoutSslUsesLocalhostClient() throws Exception {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();
    props.setProperty("clusters", "test-cluster");
    props.setProperty("router.url", "http://localhost:1234"); // no ssl-config-path

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection);
    ControllerClient client = deployer.createControllerClient();
    assertTrue(client instanceof LocalControllerClient);
    client.close();
  }
}
