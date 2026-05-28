package com.linkedin.hoptimator.venice;

import com.linkedin.hoptimator.jdbc.schema.LazyLookup;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.RouterBasedStoreMetadataFetcher;
import com.linkedin.venice.client.store.StoreMetadataFetcher;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.LazyReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.linkedin.hoptimator.util.DeploymentService.parseHints;

public class ClusterSchema extends AbstractSchema {
  private static final Logger log = LoggerFactory.getLogger(ClusterSchema.class);

  protected static final String SSL_FACTORY_CLASS_NAME = "ssl.factory.class.name";
  protected static final String DEFAULT_SSL_FACTORY_CLASS_NAME = "com.linkedin.venice.security.DefaultSSLFactory";
  protected static final String ROUTER_URL = "router.url";
  protected static final String SSL_CONFIG_PATH = "ssl-config-path";
  private static final String STORE_HINT_KEY_PREFIX = "venice.%s.";
  private static final String DISCOVER_CLUSTER_PATH = "discover_cluster";
  private static final long DISCOVERY_TIMEOUT_SECONDS = 5;

  protected final Properties properties;
  private final LazyReference<Lookup<Table>> tables = new LazyReference<>();

  public ClusterSchema(Properties properties) {
    this.properties = properties;
  }

  protected StoreMetadataFetcher createStoreMetadataFetcher() {
    return new RouterBasedStoreMetadataFetcher(
        new ClientConfig<>()
            .setVeniceURL(properties.getProperty(ROUTER_URL)));
  }

  protected StoreSchemaFetcher createStoreSchemaFetcher(String storeName) {
    return ClientFactory.createStoreSchemaFetcher(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(properties.getProperty(ROUTER_URL)));
  }

  protected VeniceStore createVeniceStore(String store, StoreSchemaFetcher storeSchemaFetcher) {
    Map<String, String> filteredHints = filterStoreHints(store, parseHints(properties));
    return new VeniceStore(storeSchemaFetcher, new VeniceStoreConfig(filteredHints));
  }

  protected Map<String, String> filterStoreHints(String store, Map<String, String> allHints) {
    String prefix = String.format(STORE_HINT_KEY_PREFIX, store);
    return allHints.entrySet().stream()
        .filter(e -> e.getKey().startsWith(prefix))
        .collect(Collectors.toMap(
            e -> e.getKey().substring(prefix.length()),
            Map.Entry::getValue
        ));
  }

  @Override
  public Lookup<Table> tables() {
    return tables.getOrCompute(() -> new LazyLookup<>() {

      @Override
      protected Map<String, Table> loadAll() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        try (StoreMetadataFetcher fetcher = createStoreMetadataFetcher()) {
          Set<String> storeNames = fetcher.getAllStoreNames();
          log.info("Discovered {} Venice stores via router /stores endpoint", storeNames.size());
          for (String storeName : storeNames) {
            try {
              tableMap.put(storeName, createVeniceStore(storeName, createStoreSchemaFetcher(storeName)));
            } catch (Exception e) {
              log.warn("Skipping Venice store {} due to setup failure", storeName, e);
            }
          }
        }
        return tableMap;
      }

      @Override
      protected @Nullable Table load(String name) throws Exception {
        if (!storeExists(name)) {
          return null;
        }
        return createVeniceStore(name, createStoreSchemaFetcher(name));
      }

      @Override
      protected String getDescription() {
        return "Venice router " + properties.getProperty(ROUTER_URL);
      }
    });
  }

  /**
   * Targeted per-store existence check against the router's {@code discover_cluster} endpoint
   * over HTTP/HTTPS. A {@code null} response indicates HTTP 404 per the transport callback
   * contract, which means the store does not exist.
   */
  protected boolean storeExists(String storeName) throws IOException {
    try (TransportClient transport = ClientFactory.getTransportClient(
        new ClientConfig<>()
            .setVeniceURL(properties.getProperty(ROUTER_URL))
            .setSslFactory(getSslFactory().orElse(null)))) {
      TransportClientResponse response = transport.get(DISCOVER_CLUSTER_PATH + "/" + storeName)
          .get(DISCOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      return response != null;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while discovering Venice store " + storeName, e);
    } catch (ExecutionException | TimeoutException e) {
      throw new IOException("Failed to discover Venice store " + storeName, e);
    }
  }

  protected Optional<SSLFactory> getSslFactory() throws IOException {
    String sslConfigPath = properties.getProperty(SSL_CONFIG_PATH);
    if (sslConfigPath != null) {
      log.debug("Using ssl configs at {}", sslConfigPath);
      Properties sslProperties = SslUtils.loadSSLConfig(sslConfigPath);
      String sslFactoryClassName =
          sslProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      return Optional.of(SslUtils.getSSLFactory(sslProperties, sslFactoryClassName));
    }
    return Optional.empty();
  }
}
