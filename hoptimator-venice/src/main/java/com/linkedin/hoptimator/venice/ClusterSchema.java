package com.linkedin.hoptimator.venice;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerClientFactory;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;

import static com.linkedin.hoptimator.util.DeploymentService.parseHints;

public class ClusterSchema extends AbstractSchema {
  private static final Logger log = LoggerFactory.getLogger(ClusterSchema.class);

  protected static final String SSL_FACTORY_CLASS_NAME = "ssl.factory.class.name";
  protected static final String DEFAULT_SSL_FACTORY_CLASS_NAME = "com.linkedin.venice.security.DefaultSSLFactory";
  protected final Properties properties;
  protected final Map<String, Table> tableMap = new HashMap<>();
  private static final String STORE_HINT_KEY_PREFIX = "venice.%s.";

  public ClusterSchema(Properties properties) {
    this.properties = properties;
  }

  public void populate() throws InterruptedException, ExecutionException, IOException {
    tableMap.clear();
    String clusterStr = properties.getProperty("clusters");
    List<String> clusters = Arrays.asList(clusterStr.split(","));

    log.info("Loading Venice stores for cluster {}", clusters);

    String sslConfigPath = properties.getProperty("ssl-config-path");
    Optional<SSLFactory> sslFactory = Optional.empty();
    if (sslConfigPath != null) {
      log.info("Using ssl configs at {}", sslConfigPath);
      Properties sslProperties = SslUtils.loadSSLConfig(sslConfigPath);
      String sslFactoryClassName = sslProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      sslFactory = Optional.of(SslUtils.getSSLFactory(sslProperties, sslFactoryClassName));
    }

    for (String cluster : clusters) {
      try (ControllerClient controllerClient = createControllerClient(cluster, sslFactory)) {
        String[] stores = controllerClient.queryStoreList(false).getStores();
        log.info("Loaded {} Venice stores.", stores.length);
        for (String store : stores) {
          StoreSchemaFetcher storeSchemaFetcher = createStoreSchemaFetcher(store);
          tableMap.put(store, createVeniceStore(store, storeSchemaFetcher));
        }
      }
    }
  }

  protected ControllerClient createControllerClient(String cluster, Optional<SSLFactory> sslFactory) {
    String routerUrl = properties.getProperty("router.url");
    if (routerUrl.contains("localhost")) {
      return new LocalControllerClient(cluster, routerUrl, sslFactory);
    } else {
      return ControllerClientFactory.getControllerClient(cluster, routerUrl, sslFactory);
    }
  }

  protected StoreSchemaFetcher createStoreSchemaFetcher(String storeName) {
    return ClientFactory.createStoreSchemaFetcher(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(properties.getProperty("router.url")));
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
  public Map<String, Table> getTableMap() {
    return tableMap;
  }
}
