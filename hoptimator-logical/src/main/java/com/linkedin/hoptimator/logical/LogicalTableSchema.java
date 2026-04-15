package com.linkedin.hoptimator.logical;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableList;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.LazyReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.jdbc.schema.LazyTableLookup;
import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.K8sUtils;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;


/**
 * A Calcite schema that lists {@link LogicalTable} instances from K8s CRDs.
 *
 * <p>CRDs are filtered by the {@link LogicalTableDriver#DATABASE_LABEL} label, which the deployer
 * sets to the database/schema name at creation time. Row type resolution is performed
 * lazily inside each {@link LogicalTable} on first access — not eagerly here.
 */
public class LogicalTableSchema extends AbstractSchema implements Database {

  private static final Logger log = LoggerFactory.getLogger(LogicalTableSchema.class);

  private final K8sContext context;
  private final String databaseName;
  private final String resolvedTier;
  private final LazyReference<Lookup<Table>> tableLookup = new LazyReference<>();
  private final K8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> logicalTableApi;

  public LogicalTableSchema(Properties properties, K8sContext context, String databaseName) {
    this(properties, context, databaseName, new K8sApi<>(context, K8sApiEndpoints.LOGICAL_TABLES));
  }

  /** Package-private constructor for testing — accepts an injectable API instance. */
  LogicalTableSchema(Properties properties, K8sContext context, String databaseName,
      K8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> logicalTableApi) {
    this.context = context;
    this.databaseName = databaseName;
    this.resolvedTier = properties != null
        ? properties.getProperty(LogicalTableDriver.TIER_PROPERTY) : null;
    this.logicalTableApi = logicalTableApi;
  }

  @Override
  public String databaseName() {
    return databaseName;
  }

  @Override
  public Lookup<Table> tables() {
    return tableLookup.getOrCompute(() -> new LazyTableLookup<>() {
      @Override
      protected Map<String, Table> loadAllTables() throws Exception {
        return loadTableMap();
      }

      @Override
      @Nullable
      protected Table loadTable(String name) throws Exception {
        // Direct CRD lookup by name avoids a full list scan for single-table access.
        // getIfExists() returns null on 404 (table not found) and throws SQLException
        // for any other K8s error (auth, network, server error), which propagates here.
        String expectedCrdName = K8sUtils.canonicalizeName(databaseName, name);
        V1alpha1LogicalTable crd = logicalTableApi.getIfExists(context.namespace(), expectedCrdName);
        return crd != null ? tableFromCrd(crd) : null;
      }

      @Override
      protected String getSchemaDescription() {
        return "LogicalTableSchema(" + databaseName + ")";
      }
    });
  }

  @Override
  protected Map<String, Table> getTableMap() {
    try {
      return loadTableMap();
    } catch (Exception e) {
      log.warn("Failed to load logical tables for schema {}", databaseName, e);
      return Collections.emptyMap();
    }
  }

  private Map<String, Table> loadTableMap() throws SQLException {
    Collection<V1alpha1LogicalTable> crds = logicalTableApi.list();

    Map<String, Table> result = new HashMap<>();
    for (V1alpha1LogicalTable crd : crds) {
      Table table = tableFromCrd(crd);
      if (table == null) {
        continue;
      }
      // Use spec.tableName (the original declared name) not the K8s CRD metadata.name
      // (which is a compound "database-table" key for K8s uniqueness).
      result.put(((LogicalTable) table).name(), table);
    }
    log.debug("Loaded {} logical tables for schema {}", result.size(), databaseName);
    return Collections.unmodifiableMap(result);
  }

  @Nullable
  Table tableFromCrd(V1alpha1LogicalTable crd) {
    if (crd.getMetadata() == null || crd.getSpec() == null) {
      return null;
    }
    Map<String, String> labels = crd.getMetadata().getLabels();
    String label = labels != null ? labels.get(LogicalTableDriver.DATABASE_LABEL) : null;
    if (!databaseName.equalsIgnoreCase(label)) {
      return null;
    }
    if (crd.getSpec().getTiers() == null || crd.getSpec().getTiers().isEmpty()) {
      log.warn("LogicalTable CRD {} has no tier bindings; skipping", crd.getMetadata().getName());
      return null;
    }
    // spec.tableName is the original declared name; metadata.name is the compound K8s resource name.
    String tableName = crd.getSpec().getTableName() != null
        ? crd.getSpec().getTableName() : crd.getMetadata().getName();
    return new LogicalTable(tableName, crd.getSpec().getTiers(), resolvedTier, context);
  }
}
