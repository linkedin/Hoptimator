package com.linkedin.hoptimator.venice;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.security.SSLFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
class ClusterSchemaTest {

  @Mock
  private ControllerClient mockControllerClient;

  @Mock
  private StoreSchemaFetcher mockSchemaFetcher;

  @Mock
  private VeniceStore mockVeniceStore;

  private Properties properties;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.setProperty("router.url", "http://localhost:1234");
    properties.setProperty("clusters", "test-cluster");
  }

  private ClusterSchema createTestableSchema() {
    return new ClusterSchema(properties) {
      @Override
      protected ControllerClient createControllerClient(String cluster, Optional<SSLFactory> sslFactory) {
        return mockControllerClient;
      }

      @Override
      protected StoreSchemaFetcher createStoreSchemaFetcher(String storeName) {
        return mockSchemaFetcher;
      }

      @Override
      protected VeniceStore createVeniceStore(String store, StoreSchemaFetcher storeSchemaFetcher) {
        return mockVeniceStore;
      }
    };
  }

  @Test
  void testFilterStoreHintsReturnsMatchingHints() {
    ClusterSchema schema = new ClusterSchema(properties);

    Map<String, String> allHints = Map.of(
        "venice.myStore.valueSchemaId", "5",
        "venice.myStore.partitions", "10",
        "venice.otherStore.valueSchemaId", "3"
    );

    Map<String, String> result = schema.filterStoreHints("myStore", allHints);

    assertEquals(2, result.size());
    assertEquals("5", result.get("valueSchemaId"));
    assertEquals("10", result.get("partitions"));
  }

  @Test
  void testFilterStoreHintsReturnsEmptyWhenNoMatch() {
    ClusterSchema schema = new ClusterSchema(properties);

    Map<String, String> allHints = Map.of(
        "venice.otherStore.key", "value"
    );

    Map<String, String> result = schema.filterStoreHints("myStore", allHints);

    assertTrue(result.isEmpty());
  }

  // --- tables().get() / loadTable() tests ---

  @Test
  void testLoadTableReturnsStoreWhenFound() {
    D2ServiceDiscoveryResponse discoverResponse = mock(D2ServiceDiscoveryResponse.class);
    when(discoverResponse.isError()).thenReturn(false);
    when(discoverResponse.getCluster()).thenReturn("test-cluster");
    when(mockControllerClient.discoverCluster("myStore")).thenReturn(discoverResponse);

    ClusterSchema schema = createTestableSchema();
    Lookup<Table> tables = schema.tables();
    Table result = tables.get("myStore");

    assertNotNull(result);
    assertEquals(mockVeniceStore, result);
  }

  @Test
  void testLoadTableReturnsNullWhenStoreNotFound() {
    D2ServiceDiscoveryResponse discoverResponse = mock(D2ServiceDiscoveryResponse.class);
    when(discoverResponse.isError()).thenReturn(true);
    when(discoverResponse.getErrorType()).thenReturn(ErrorType.STORE_NOT_FOUND);
    when(mockControllerClient.discoverCluster("unknownStore")).thenReturn(discoverResponse);

    ClusterSchema schema = createTestableSchema();
    Lookup<Table> tables = schema.tables();
    Table result = tables.get("unknownStore");

    assertNull(result);
  }

  @Test
  void testLoadTableThrowsOnOtherError() {
    D2ServiceDiscoveryResponse discoverResponse = mock(D2ServiceDiscoveryResponse.class);
    when(discoverResponse.isError()).thenReturn(true);
    when(discoverResponse.getErrorType()).thenReturn(ErrorType.GENERAL_ERROR);
    when(discoverResponse.getError()).thenReturn("something went wrong");
    when(mockControllerClient.discoverCluster("badStore")).thenReturn(discoverResponse);

    ClusterSchema schema = createTestableSchema();
    Lookup<Table> tables = schema.tables();

    RuntimeException ex = assertThrows(RuntimeException.class, () -> tables.get("badStore"));
    assertTrue(ex.getMessage().contains("badStore"));
  }

  @Test
  void testLoadTableReturnsNullWhenStoreInDifferentCluster() {
    D2ServiceDiscoveryResponse discoverResponse = mock(D2ServiceDiscoveryResponse.class);
    when(discoverResponse.isError()).thenReturn(false);
    when(discoverResponse.getCluster()).thenReturn("other-cluster");
    when(mockControllerClient.discoverCluster("remoteStore")).thenReturn(discoverResponse);

    ClusterSchema schema = createTestableSchema();
    Lookup<Table> tables = schema.tables();
    Table result = tables.get("remoteStore");

    assertNull(result);
  }

  // --- loadAllTables tests ---

  @Test
  void testLoadAllTablesPopulatesFromStoreList() {
    MultiStoreResponse storeListResponse = mock(MultiStoreResponse.class);
    when(storeListResponse.getStores()).thenReturn(new String[]{"store1", "store2"});
    when(mockControllerClient.queryStoreList(false)).thenReturn(storeListResponse);

    ClusterSchema schema = createTestableSchema();
    Lookup<Table> tables = schema.tables();

    // getNames triggers loadAllTables
    assertNotNull(tables.getNames(LikePattern.any()));
  }

  // --- createControllerClient tests ---

  @Test
  void testCreateControllerClientWithLocalhostUrl() {
    properties.setProperty("router.url", "http://localhost:5555");
    ClusterSchema schema = new ClusterSchema(properties);
    ControllerClient client = schema.createControllerClient("test-cluster", Optional.empty());
    assertNotNull(client);
    client.close();
  }

  // --- createVeniceStore with real hints ---

  @Test
  void testCreateVeniceStoreWithFilteredHints() {
    properties.setProperty("hints", "venice.myStore.valueSchemaId=5");

    ClusterSchema schema = new ClusterSchema(properties) {
      @Override
      protected ControllerClient createControllerClient(String cluster, Optional<SSLFactory> sslFactory) {
        return mockControllerClient;
      }

      @Override
      protected StoreSchemaFetcher createStoreSchemaFetcher(String storeName) {
        return mockSchemaFetcher;
      }
    };

    VeniceStore store = schema.createVeniceStore("myStore", mockSchemaFetcher);
    assertNotNull(store);
  }
}
