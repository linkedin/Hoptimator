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
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
class VeniceDeployerTest {

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

    assertTrue(issues.valid(), "Expected no validation errors for new store. Issues: " + issues);
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

    assertTrue(issues.valid(), "Expected no validation errors when key schema unchanged. Issues: " + issues);
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

  // --- validate() missing/empty required properties ---

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

  // --- validate() schema generation errors ---

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

  // --- validate() schema compatibility checks ---

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

  // --- delete() error paths ---

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
  void testCheckStoreExistsReturnsFalseFor404() throws Exception {
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

  // --- restore() ---

  @Test
  void testRestoreDoesNotThrow() {
    Source source = new Source("venice", List.of("VENICE", "myStore"), Collections.emptyMap());
    Properties props = new Properties();

    VeniceDeployer deployer = new VeniceDeployer(source, props, mockConnection);
    deployer.restore();
  }
}
