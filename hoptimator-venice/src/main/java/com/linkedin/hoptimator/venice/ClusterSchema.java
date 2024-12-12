package com.linkedin.hoptimator.venice;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerClientFactory;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;


public class ClusterSchema extends AbstractSchema {

  private static final Logger log = LoggerFactory.getLogger(ClusterSchema.class);
  private static final String SSL_FACTORY_CLASS_NAME = "ssl.factory.class.name";
  public static final String DEFAULT_SSL_FACTORY_CLASS_NAME = "com.linkedin.venice.security.DefaultSSLFactory";

  private final Properties properties;
  private final Map<String, Table> tableMap = new HashMap<>();

  public ClusterSchema(Properties properties) {
    this.properties = properties;
  }

  public void populate() throws InterruptedException, ExecutionException, IOException {
    tableMap.clear();
    String controllerUrl = properties.getProperty("controller.url");
    String cluster = properties.getProperty("cluster");
    boolean isLocalCluster = false;
    if (controllerUrl.contains("localhost")) {
      isLocalCluster = true;
    }
    log.info("Loading Venice stores on {} for cluster {}", controllerUrl, cluster);

    String sslConfigPath = properties.getProperty("ssl-config-path");
    Optional<SSLFactory> sslFactory = Optional.empty();
    if (sslConfigPath != null) {
      log.info("Using ssl configs at {}", sslConfigPath);
      Properties sslProperties = SslUtils.loadSSLConfig(sslConfigPath);
      String sslFactoryClassName = sslProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      sslFactory = Optional.of(SslUtils.getSSLFactory(sslProperties, sslFactoryClassName));
    }
    ControllerClient controllerClient;
    if (isLocalCluster) {
      controllerClient = new LocalControllerClient(cluster, controllerUrl, sslFactory);
    } else {
      controllerClient = ControllerClientFactory.getControllerClient(cluster, controllerUrl, sslFactory);
    }
    String[] stores = controllerClient.queryStoreList(false).getStores();
    log.info("Loaded {} Venice stores.",  stores.length);
    for (String store : stores) {
      tableMap.put(store, new VeniceStore(store, properties));
    }
    if (isLocalCluster) {
      controllerClient.close();
    }
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }
}
