package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.sql.Wrapper;

import org.apache.calcite.schema.SchemaPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.Catalog;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;


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
  public void register(Wrapper wrapper) throws SQLException {
    SchemaPlus schemaPlus = wrapper.unwrap(SchemaPlus.class);
    HoptimatorConnection conn = wrapper.unwrap(HoptimatorConnection.class);
    K8sContext context = K8sContext.create(conn);
    log.info("Using K8s context " + context);
    K8sMetadata metadata = new K8sMetadata(conn, context);
    schemaPlus.add("k8s", metadata);
    metadata.databaseTable().addDatabases(schemaPlus, conn);
    metadata.viewTable().addViews(schemaPlus);
    metadata.viewTable().registerMaterializations(conn);
  }
}
