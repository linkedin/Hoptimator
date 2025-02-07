package com.linkedin.hoptimator.k8s;

import java.util.Properties;
import java.sql.SQLException;
import java.sql.Wrapper;

import org.apache.calcite.schema.SchemaPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.Catalog;


/** The k8s catalog. */
class K8sCatalog implements Catalog {

  private static final Logger log = LoggerFactory.getLogger(K8sCatalog.class);

  @Override
  public String name() {
    return "k8s";
  }

  @Override
  public String description() {
    return "K8s catalog";
  }

  @Override
  public void register(Wrapper parentSchema, Properties connectionProperties) throws SQLException {
    SchemaPlus schemaPlus = parentSchema.unwrap(SchemaPlus.class);
    K8sContext context = new K8sContext(connectionProperties);
    log.info("Using K8s context " + context.toString());
    K8sMetadata metadata = new K8sMetadata(context);
    schemaPlus.add("k8s", metadata);
    metadata.databaseTable().addDatabases(schemaPlus, connectionProperties);
    metadata.viewTable().addViews(schemaPlus);
  }
}
