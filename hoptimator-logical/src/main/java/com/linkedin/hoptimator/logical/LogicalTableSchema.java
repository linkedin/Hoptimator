package com.linkedin.hoptimator.logical;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

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
 * <p>CRDs are filtered by the {@link LogicalTableSchema#LogicalTableDriver.DATABASE_LABEL} label, which the deployer
 * sets to the database/schema name at creation time. Row type resolution is performed
 * lazily inside each {@link LogicalTable} on first access — not eagerly here.
 */
public class LogicalTableSchema extends AbstractSchema implements Database {

  /** @see LogicalTableDriver#LogicalTableDriver.DATABASE_LABEL */

  private static final Logger log = LoggerFactory.getLogger(LogicalTableSchema.class);

  private final K8sContext context;
  private final String databaseName;
  private final String resolvedTier;
  private final LazyReference<Lookup<Table>> tableLookup = new LazyReference<>();

  public LogicalTableSchema(Properties properties, K8sContext context, String databaseName) {
    this.context = context;
    this.databaseName = databaseName;
    this.resolvedTier = properties != null
        ? properties.getProperty(LogicalTableDriver.TIER_PROPERTY) : null;
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
        // Construct the expected CRD name from schema + table name for a direct K8s lookup,
        // avoiding a full list scan for single-table access.
        String expectedCrdName = K8sUtils.canonicalizeName(databaseName, name);
        try {
          V1alpha1LogicalTable crd = new K8sApi<>(context, K8sApiEndpoints.LOGICAL_TABLES).get(expectedCrdName);
          return tableFromCrd(crd);
        } catch (Exception e) {
          log.debug("LogicalTable CRD {} not found for table {}: {}", expectedCrdName, name, e.getMessage());
          return null;
        }
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
    Collection<V1alpha1LogicalTable> crds = new K8sApi<>(context, K8sApiEndpoints.LOGICAL_TABLES).list();

    Map<String, Table> result = new HashMap<>();
    for (V1alpha1LogicalTable crd : crds) {
      Table table = tableFromCrd(crd);
      if (table == null) {
        continue;
      }
      result.put(crd.getMetadata().getName(), table);
    }
    log.debug("Loaded {} logical tables for schema {}", result.size(), databaseName);
    return Collections.unmodifiableMap(result);
  }

  @Nullable
  private Table tableFromCrd(V1alpha1LogicalTable crd) {
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
    String tableName = crd.getMetadata().getName();
    return new LogicalTable(tableName, crd.getSpec().getTiers(), resolvedTier, context);
  }
}
