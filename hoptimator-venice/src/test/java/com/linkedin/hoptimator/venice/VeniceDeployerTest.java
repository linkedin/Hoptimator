package com.linkedin.hoptimator.venice;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.StoreInfo;
import org.apache.avro.Schema;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.BeforeEach;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Tests for VeniceDeployer using mocks.
 */
@ExtendWith(MockitoExtension.class)
public class VeniceDeployerTest {

  private static final String TEST_STORE = "test_store";
  private static final String VENICE_CLUSTER = "test-cluster";
  private static final String VENICE_ROUTER_URL = "test-url";

  @Mock
  private HoptimatorConnection mockConnection;

  @Mock
  private ControllerClient mockControllerClient;

  private Properties properties;

  @BeforeEach
  public void setUp() {
    properties = new Properties();
    properties.setProperty("clusters", VENICE_CLUSTER);
    properties.setProperty("router.url", VENICE_ROUTER_URL);
  }

  private VeniceDeployer createDeployer(Source source) {
    return new TestableVeniceDeployer(source, properties, mockConnection, mockControllerClient);
  }

  /**
   * Testable subclass that allows injecting a mock ControllerClient.
   */
  private static class TestableVeniceDeployer extends VeniceDeployer {
    private final ControllerClient mockClient;

    TestableVeniceDeployer(Source source, Properties properties,
        HoptimatorConnection connection, ControllerClient mockClient) {
      super(source, properties, connection);
      this.mockClient = mockClient;
    }

    @Override
    protected ControllerClient createControllerClient() {
      return mockClient;
    }
  }

  @Test
  public void testDeleteStore() throws Exception {
    Source source = new Source("venice", List.of("VENICE", TEST_STORE), Collections.emptyMap());

    // Mock store exists
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    when(mockControllerClient.getStore(TEST_STORE)).thenReturn(storeResponse);

    // Mock successful deletion
    ControllerResponse deleteResponse = mock(ControllerResponse.class);
    when(deleteResponse.isError()).thenReturn(false);
    when(mockControllerClient.disableAndDeleteStore(TEST_STORE)).thenReturn(deleteResponse);

    VeniceDeployer deployer = createDeployer(source);
    deployer.delete();

    verify(mockControllerClient).disableAndDeleteStore(TEST_STORE);
  }

  @Test
  public void testDeleteNonExistentStore() throws Exception {
    Source source = new Source("venice", List.of("VENICE", TEST_STORE), Collections.emptyMap());

    // Mock store doesn't exist
    StoreResponse storeResponse = mock(StoreResponse.class);
    when(storeResponse.getStore()).thenReturn(null);
    when(mockControllerClient.getStore(TEST_STORE)).thenReturn(storeResponse);

    VeniceDeployer deployer = createDeployer(source);
    deployer.delete();

    // Should not attempt to delete
    verify(mockControllerClient, never()).disableAndDeleteStore(anyString());
  }

  @Test
  public void testUpdateCreatesStoreIfNotExists() throws Exception {
    Source source = new Source("venice", List.of("VENICE", TEST_STORE), Collections.emptyMap());

    // Mock store doesn't exist
    StoreResponse storeResponse = mock(StoreResponse.class);
    when(storeResponse.getStore()).thenReturn(null);
    when(mockControllerClient.getStore(TEST_STORE)).thenReturn(storeResponse);

    // Mock successful store creation
    NewStoreResponse createResponse = mock(NewStoreResponse.class);
    when(createResponse.isError()).thenReturn(false);
    when(mockControllerClient.createNewStore(eq(TEST_STORE), anyString(), anyString(), anyString()))
        .thenReturn(createResponse);

    // Mock schema generation
    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = Schema.create(Schema.Type.STRING);
    VeniceDeployer deployer = new TestableVeniceDeployer(source, properties, mockConnection, mockControllerClient) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() throws SQLException {
        return new Pair<>(keySchema, valueSchema);
      }
    };

    deployer.update();

    verify(mockControllerClient).createNewStore(eq(TEST_STORE), anyString(), anyString(), anyString());
  }

  @Test
  public void testUpdateAddsValueSchemaIfStoreExists() throws Exception {
    Source source = new Source("venice", List.of("VENICE", TEST_STORE), Collections.emptyMap());

    // Mock store exists
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    when(mockControllerClient.getStore(TEST_STORE)).thenReturn(storeResponse);

    // Mock key schema response (same schema)
    Schema keySchema = Schema.create(Schema.Type.STRING);
    SchemaResponse keySchemaResponse = mock(SchemaResponse.class);
    when(keySchemaResponse.getSchemaStr()).thenReturn(keySchema.toString());
    when(mockControllerClient.getKeySchema(TEST_STORE)).thenReturn(keySchemaResponse);

    // Mock successful value schema addition
    Schema valueSchema = Schema.create(Schema.Type.STRING);
    SchemaResponse addValueResponse = mock(SchemaResponse.class);
    when(addValueResponse.isError()).thenReturn(false);
    when(addValueResponse.getId()).thenReturn(2);
    when(mockControllerClient.addValueSchema(eq(TEST_STORE), anyString())).thenReturn(addValueResponse);

    VeniceDeployer deployer = new TestableVeniceDeployer(source, properties, mockConnection, mockControllerClient) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(keySchema, valueSchema);
      }
    };

    deployer.update();

    verify(mockControllerClient).addValueSchema(eq(TEST_STORE), anyString());
    verify(mockControllerClient, never()).createNewStore(anyString(), anyString(), anyString(), anyString(), anyString());
  }

  @Test
  public void testUpdateFailsWhenValueSchemaAdditionFails() throws Exception {
    Source source = new Source("venice", List.of("VENICE", TEST_STORE), Collections.emptyMap());

    // Mock store exists
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    when(mockControllerClient.getStore(TEST_STORE)).thenReturn(storeResponse);

    // Mock key schema response
    Schema keySchema = Schema.create(Schema.Type.STRING);
    SchemaResponse keySchemaResponse = mock(SchemaResponse.class);
    when(keySchemaResponse.getSchemaStr()).thenReturn(keySchema.toString());
    when(mockControllerClient.getKeySchema(TEST_STORE)).thenReturn(keySchemaResponse);

    // Mock failed value schema addition
    Schema valueSchema = Schema.create(Schema.Type.STRING);
    SchemaResponse addValueResponse = mock(SchemaResponse.class);
    when(addValueResponse.isError()).thenReturn(true);
    when(addValueResponse.getError()).thenReturn("Schema incompatible");
    when(mockControllerClient.addValueSchema(eq(TEST_STORE), anyString())).thenReturn(addValueResponse);

    VeniceDeployer deployer = new TestableVeniceDeployer(source, properties, mockConnection, mockControllerClient) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(keySchema, valueSchema);
      }
    };

    assertThrows(SQLNonTransientException.class, deployer::update);
  }

  @Test
  public void testCreateNewStore() throws Exception {
    Source source = new Source("venice", List.of("VENICE", TEST_STORE), Collections.emptyMap());

    // Mock store doesn't exist
    StoreResponse storeResponse = mock(StoreResponse.class);
    when(storeResponse.getStore()).thenReturn(null);
    when(mockControllerClient.getStore(TEST_STORE)).thenReturn(storeResponse);

    // Mock successful store creation
    NewStoreResponse createResponse = mock(NewStoreResponse.class);
    when(createResponse.isError()).thenReturn(false);
    when(mockControllerClient.createNewStore(eq(TEST_STORE), anyString(), anyString(), anyString()))
        .thenReturn(createResponse);

    // Mock schema generation
    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = Schema.create(Schema.Type.STRING);
    VeniceDeployer deployer = new TestableVeniceDeployer(source, properties, mockConnection, mockControllerClient) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() throws SQLException {
        return new Pair<>(keySchema, valueSchema);
      }
    };

    deployer.create();

    verify(mockControllerClient).createNewStore(eq(TEST_STORE), anyString(), anyString(), anyString());
  }

  @Test
  public void testCreateExistingStoreSkipsCreation() throws Exception {
    Source source = new Source("venice", List.of("VENICE", TEST_STORE), Collections.emptyMap());

    // Mock store exists
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    when(mockControllerClient.getStore(TEST_STORE)).thenReturn(storeResponse);

    // Mock schema generation
    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = Schema.create(Schema.Type.STRING);
    VeniceDeployer deployer = new TestableVeniceDeployer(source, properties, mockConnection, mockControllerClient) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() {
        return new Pair<>(keySchema, valueSchema);
      }
    };

    deployer.create();

    // Should not attempt to create
    verify(mockControllerClient, never()).createNewStore(anyString(), anyString(), anyString(), anyString(), anyString());
  }

  @Test
  public void testSpecifyReturnsEmptyList() throws Exception {
    Source source = new Source("venice", List.of("VENICE", TEST_STORE), Collections.emptyMap());
    VeniceDeployer deployer = createDeployer(source);

    assertTrue(deployer.specify().isEmpty());
  }

  // --- validate() tests ---

  @Test
  public void testValidatePassesForNewStore() throws Exception {
    Source source = new Source("venice", List.of("VENICE", TEST_STORE), Collections.emptyMap());

    // Mock store doesn't exist
    StoreResponse storeResponse = mock(StoreResponse.class);
    when(storeResponse.getStore()).thenReturn(null);
    when(mockControllerClient.getStore(TEST_STORE)).thenReturn(storeResponse);

    // Mock schema generation
    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = Schema.create(Schema.Type.STRING);
    VeniceDeployer deployer = new TestableVeniceDeployer(source, properties, mockConnection, mockControllerClient) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() throws SQLException {
        return new Pair<>(keySchema, valueSchema);
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertTrue(issues.valid(), "Expected no validation errors for new store. Issues: " + issues.toString());
  }

  @Test
  public void testValidatePassesForExistingStoreWithSameKeySchema() throws Exception {
    Source source = new Source("venice", List.of("VENICE", TEST_STORE), Collections.emptyMap());

    // Mock store exists
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    when(mockControllerClient.getStore(TEST_STORE)).thenReturn(storeResponse);

    // Mock key schema response (same schema)
    Schema keySchema = Schema.create(Schema.Type.STRING);
    SchemaResponse keySchemaResponse = mock(SchemaResponse.class);
    when(keySchemaResponse.isError()).thenReturn(false);
    when(keySchemaResponse.getSchemaStr()).thenReturn(keySchema.toString());
    when(mockControllerClient.getKeySchema(TEST_STORE)).thenReturn(keySchemaResponse);

    // Mock value schema response
    Schema valueSchema = Schema.create(Schema.Type.STRING);
    SchemaResponse valueSchemaResponse = mock(SchemaResponse.class);
    when(valueSchemaResponse.isError()).thenReturn(false);
    when(valueSchemaResponse.getSchemaStr()).thenReturn(valueSchema.toString());
    when(mockControllerClient.getValueSchema(eq(TEST_STORE), eq(-1))).thenReturn(valueSchemaResponse);

    VeniceDeployer deployer = new TestableVeniceDeployer(source, properties, mockConnection, mockControllerClient) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() throws SQLException {
        return new Pair<>(keySchema, valueSchema);
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertTrue(issues.valid(), "Expected no validation errors when key schema unchanged. Issues: " + issues.toString());
  }

  @Test
  public void testValidateRejectsKeySchemaChange() throws Exception {
    Source source = new Source("venice", List.of("VENICE", TEST_STORE), Collections.emptyMap());

    // Mock store exists
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    when(mockControllerClient.getStore(TEST_STORE)).thenReturn(storeResponse);

    // Mock key schema response (different schema)
    Schema oldKeySchema = Schema.create(Schema.Type.STRING);
    Schema newKeySchema = Schema.create(Schema.Type.INT);
    SchemaResponse keySchemaResponse = mock(SchemaResponse.class);
    when(keySchemaResponse.isError()).thenReturn(false);
    when(keySchemaResponse.getSchemaStr()).thenReturn(oldKeySchema.toString());
    when(mockControllerClient.getKeySchema(TEST_STORE)).thenReturn(keySchemaResponse);

    VeniceDeployer deployer = new TestableVeniceDeployer(source, properties, mockConnection, mockControllerClient) {
      @Override
      protected Pair<Schema, Schema> getKeyPayloadSchema() throws SQLException {
        return new Pair<>(newKeySchema, Schema.create(Schema.Type.STRING));
      }
    };

    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid(), "Expected validation error for key schema change");
    assertTrue(issues.toString().contains("Key schema evolution is not supported"),
        "Error message should mention key schema evolution");
  }
}
