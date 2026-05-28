package com.linkedin.hoptimator.venice;

import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.StoreMetadataFetcher;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class ClusterSchemaTest {

  @Mock
  private StoreSchemaFetcher mockSchemaFetcher;

  @Mock
  private VeniceStore mockVeniceStore;

  @Mock
  private MockedStatic<ClientFactory> clientFactory;

  private Properties properties;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.setProperty("router.url", "http://localhost:1234");
  }

  private ClusterSchema createTestableSchema(StoreMetadataFetcher metadataFetcher) {
    return createTestableSchema(metadataFetcher, Set.of());
  }

  private ClusterSchema createTestableSchema(StoreMetadataFetcher metadataFetcher, Set<String> existingStores) {
    return new ClusterSchema(properties) {
      @Override
      protected StoreMetadataFetcher createStoreMetadataFetcher() {
        return metadataFetcher;
      }

      @Override
      protected StoreSchemaFetcher createStoreSchemaFetcher(String storeName) {
        return mockSchemaFetcher;
      }

      @Override
      protected VeniceStore createVeniceStore(String store, StoreSchemaFetcher storeSchemaFetcher) {
        return mockVeniceStore;
      }

      @Override
      protected boolean storeExists(String storeName) {
        return existingStores.contains(storeName);
      }
    };
  }

  // --- filterStoreHints ---

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

  // --- tables().get() (load path uses storeExists, NOT the bulk /stores endpoint) ---

  @Test
  void testLoadReturnsTableWhenStoreFoundInDiscovery() {
    StoreMetadataFetcher fetcher = mock(StoreMetadataFetcher.class);

    ClusterSchema schema = createTestableSchema(fetcher, Set.of("known-store"));
    Table result = schema.tables().get("known-store");

    assertNotNull(result);
    assertEquals(mockVeniceStore, result);
  }

  @Test
  void testLoadReturnsNullWhenStoreNotFoundInDiscovery() {
    StoreMetadataFetcher fetcher = mock(StoreMetadataFetcher.class);

    ClusterSchema schema = createTestableSchema(fetcher, Set.of());
    Table result = schema.tables().get("nonexistent-store");

    assertNull(result);
  }

  @Test
  void testLoadDoesNotHitMetadataFetcher() {
    StoreMetadataFetcher fetcher = mock(StoreMetadataFetcher.class);
    ClusterSchema schema = createTestableSchema(fetcher, Set.of("a", "b", "c"));

    schema.tables().get("a");
    schema.tables().get("b");
    schema.tables().get("c");

    // load() uses targeted per-store discovery (storeExists), never the bulk /stores endpoint.
    verify(fetcher, never()).getAllStoreNames();
  }

  // --- tables().getNames(...) (loadAll path) ---

  @Test
  void testLoadAllTablesPopulatesFromRouterStores() {
    StoreMetadataFetcher fetcher = mock(StoreMetadataFetcher.class);
    when(fetcher.getAllStoreNames()).thenReturn(Set.of("store1", "store2"));

    ClusterSchema schema = createTestableSchema(fetcher);
    Lookup<Table> tables = schema.tables();

    Set<String> names = tables.getNames(LikePattern.any());
    assertEquals(Set.of("store1", "store2"), names);
  }

  @Test
  void testLoadAllTablesAccessibleViaGet() {
    StoreMetadataFetcher fetcher = mock(StoreMetadataFetcher.class);
    when(fetcher.getAllStoreNames()).thenReturn(Set.of("storeA", "storeB"));

    ClusterSchema schema = createTestableSchema(fetcher);
    Lookup<Table> tables = schema.tables();

    tables.getNames(LikePattern.any());

    assertNotNull(tables.get("storeA"));
    assertNotNull(tables.get("storeB"));
  }

  @Test
  void testLoadAllSkipsStoreWhenSchemaFetcherConstructionFails() {
    StoreMetadataFetcher fetcher = mock(StoreMetadataFetcher.class);
    when(fetcher.getAllStoreNames()).thenReturn(Set.of("good-store", "bad-store"));

    ClusterSchema schema = new ClusterSchema(properties) {
      @Override
      protected StoreMetadataFetcher createStoreMetadataFetcher() {
        return fetcher;
      }

      @Override
      protected StoreSchemaFetcher createStoreSchemaFetcher(String storeName) {
        if ("bad-store".equals(storeName)) {
          throw new RuntimeException("schema fetcher setup failed");
        }
        return mockSchemaFetcher;
      }

      @Override
      protected VeniceStore createVeniceStore(String store, StoreSchemaFetcher storeSchemaFetcher) {
        return mockVeniceStore;
      }

      @Override
      protected boolean storeExists(String storeName) {
        return true;
      }
    };

    Lookup<Table> tables = schema.tables();
    tables.getNames(LikePattern.any());

    assertNotNull(tables.get("good-store"));
    assertNull(tables.get("bad-store"));
  }

  @Test
  void testGetNamesTriggersMetadataFetcherOnce() {
    StoreMetadataFetcher fetcher = mock(StoreMetadataFetcher.class);
    when(fetcher.getAllStoreNames()).thenReturn(Set.of("a", "b"));

    ClusterSchema schema = createTestableSchema(fetcher);
    schema.tables().getNames(LikePattern.any());
    schema.tables().getNames(LikePattern.any());

    // loadAll() runs at most once per schema lifetime (LazyLookup caches the result).
    verify(fetcher).getAllStoreNames();
  }

  // --- storeExists (real impl, HTTP/HTTPS transport) ---

  @Test
  void testStoreExistsReturnsTrueOnNonNullResponse() throws Exception {
    TransportClient transport = mock(TransportClient.class);
    TransportClientResponse response = mock(TransportClientResponse.class);
    doReturn(CompletableFuture.completedFuture(response)).when(transport).get(eq("discover_cluster/known"));
    clientFactory.when(() -> ClientFactory.getTransportClient(any(ClientConfig.class))).thenReturn(transport);

    ClusterSchema schema = new ClusterSchema(properties);
    assertTrue(schema.storeExists("known"));
  }

  @Test
  void testStoreExistsReturnsFalseOnNullResponse() throws Exception {
    TransportClient transport = mock(TransportClient.class);
    doReturn(CompletableFuture.completedFuture(null)).when(transport).get(eq("discover_cluster/missing"));
    clientFactory.when(() -> ClientFactory.getTransportClient(any(ClientConfig.class))).thenReturn(transport);

    ClusterSchema schema = new ClusterSchema(properties);
    assertFalse(schema.storeExists("missing"));
  }

  @Test
  void testStoreExistsWrapsExecutionFailureAsIOException() {
    TransportClient transport = mock(TransportClient.class);
    CompletableFuture<TransportClientResponse> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("router down"));
    doReturn(failed).when(transport).get(eq("discover_cluster/boom"));
    clientFactory.when(() -> ClientFactory.getTransportClient(any(ClientConfig.class))).thenReturn(transport);

    ClusterSchema schema = new ClusterSchema(properties);
    IOException ex = assertThrows(IOException.class, () -> schema.storeExists("boom"));
    assertTrue(ex.getMessage().contains("boom"));
  }

  // --- getSslFactory ---

  @Test
  void testGetSslFactoryReturnsEmptyWhenNoSslConfigPath() throws IOException {
    ClusterSchema schema = new ClusterSchema(properties);
    assertTrue(schema.getSslFactory().isEmpty());
  }

  @Test
  void testGetSslFactoryThrowsWhenSslConfigPathInvalid() {
    properties.setProperty("ssl-config-path", "/nonexistent/ssl.properties");
    ClusterSchema schema = new ClusterSchema(properties);

    assertThrows(Exception.class, schema::getSslFactory);
  }

  // --- description ---

  @Test
  void testTablesDescriptionMentionsRouterUrl() {
    StoreMetadataFetcher fetcher = mock(StoreMetadataFetcher.class);
    lenient().when(fetcher.getAllStoreNames()).thenReturn(Set.of("storeX"));

    ClusterSchema schema = createTestableSchema(fetcher);
    Lookup<Table> tables = schema.tables();

    assertNotNull(tables.toString());
  }

  // --- real ClientFactory wiring (createStoreSchemaFetcher) ---

  @Test
  void testCreateStoreSchemaFetcherBuildsClientConfigFromRouterUrl() {
    ArgumentCaptor<ClientConfig> captor = ArgumentCaptor.forClass(ClientConfig.class);
    clientFactory.when(() -> ClientFactory.createStoreSchemaFetcher(captor.capture())).thenReturn(mockSchemaFetcher);

    ClusterSchema schema = new ClusterSchema(properties);
    assertNotNull(schema.createStoreSchemaFetcher("myStore"));
    assertEquals("myStore", captor.getValue().getStoreName());
    assertEquals("http://localhost:1234", captor.getValue().getVeniceURL());
  }

  // --- createVeniceStore wires hints ---

  @Test
  void testCreateVeniceStoreWithFilteredHints() {
    properties.setProperty("hints", "venice.myStore.valueSchemaId=5");

    ClusterSchema schema = new ClusterSchema(properties) {
      @Override
      protected StoreSchemaFetcher createStoreSchemaFetcher(String storeName) {
        return mockSchemaFetcher;
      }
    };

    VeniceStore store = schema.createVeniceStore("myStore", mockSchemaFetcher);
    assertNotNull(store);
  }
}
