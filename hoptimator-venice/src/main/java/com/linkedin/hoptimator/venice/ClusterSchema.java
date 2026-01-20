package com.linkedin.hoptimator.venice;

import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.exceptions.ErrorType;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.LazyReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.jdbc.schema.LazyTableLookup;
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
  private static final String STORE_HINT_KEY_PREFIX = "venice.%s.";
  private final LazyReference<Lookup<Table>> tables = new LazyReference<>();

  public ClusterSchema(Properties properties) {
    this.properties = properties;
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
  public Lookup<Table> tables() {
    return tables.getOrCompute(() -> new LazyTableLookup<>() {

      @Override
      protected Map<String, Table> loadAllTables() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        String clusterStr = properties.getProperty("clusters");
        List<String> clusters = Arrays.asList(clusterStr.split(","));

        for (String cluster : clusters) {
          try (ControllerClient controllerClient = createControllerClient(cluster, getSslFactory())) {
            String[] stores = controllerClient.queryStoreList(false).getStores();
            for (String store : stores) {
              StoreSchemaFetcher storeSchemaFetcher = createStoreSchemaFetcher(store);
              tableMap.put(store, createVeniceStore(store, storeSchemaFetcher));
            }
          }
        }
        return tableMap;
      }

      @Override
      protected @Nullable Table loadTable(String name) throws Exception {
        String clusterStr = properties.getProperty("clusters");
        List<String> clusters = Arrays.asList(clusterStr.split(","));

        try (ControllerClient controllerClient = createControllerClient(clusters.get(0), getSslFactory())) {
          ControllerResponse controllerResponse = controllerClient.discoverCluster(name);
          if (controllerResponse.isError() && controllerResponse.getErrorType().equals(ErrorType.STORE_NOT_FOUND)) {
            return null;
          } else if (controllerResponse.isError()) {
            throw new RuntimeException(String.format("Error fetching store errorType=%s, error=%s",
                controllerResponse.getErrorType(), controllerResponse.getError()));
          }

          // Venice does not currently have the ability to list all clusters so the "clusters" property
          // is required as part of the JDBC driver. To keep loadTable and loadAllTables consistent, we
          // check that the fetched store actually belongs to one of these clusters, otherwise we could end up
          // with different results.
          if (!clusters.contains(controllerResponse.getCluster())) {
            return null;
          }
        }

        StoreSchemaFetcher storeSchemaFetcher = createStoreSchemaFetcher(name);
        return createVeniceStore(name, storeSchemaFetcher);
      }

      @Override
      protected String getSchemaDescription() {
        return "Venice clusters " + properties.getProperty("clusters");
      }

      private Optional<SSLFactory> getSslFactory() throws IOException {
        String sslConfigPath = properties.getProperty("ssl-config-path");
        if (sslConfigPath != null) {
          log.debug("Using ssl configs at {}", sslConfigPath);
          Properties sslProperties = SslUtils.loadSSLConfig(sslConfigPath);
          String sslFactoryClassName =
              sslProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
          return Optional.of(SslUtils.getSSLFactory(sslProperties, sslFactoryClassName));
        }
        return Optional.empty();
      }
    });
  }
}
