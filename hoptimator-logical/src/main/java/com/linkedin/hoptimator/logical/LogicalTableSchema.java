package com.linkedin.hoptimator.logical;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableList;


/**
 * A Calcite schema that lists {@link LogicalTable} instances from K8s CRDs,
 * filtered to those whose tier key set matches the configured tier map.
 *
 * <p>Implements {@link Database} so the operator can discover the schema name.
 */
public class LogicalTableSchema extends AbstractSchema implements Database {

  private static final Logger log = LoggerFactory.getLogger(LogicalTableSchema.class);

  /** Cache TTL in milliseconds (30 seconds). */
  static final long CACHE_TTL_MS = 30_000L;

  private final Map<String, String> tierFilter;
  private final K8sContext context;
  private final String schemaName;

  // Simple TTL cache
  private volatile Map<String, Table> cachedTableMap = null;
  private volatile long cacheTimestamp = 0L;
  private final LazyReference<Lookup<Table>> tableLookup = new LazyReference<>();

  /**
   * @param tierFilter  tier name to Database CRD name map parsed from the JDBC URL;
   *                    only CRDs whose tier key set equals this key set are included
   * @param context     K8s context used to list LogicalTable CRDs
   * @param schemaName  name this schema will be registered under (e.g. "LOGICAL")
   */
  public LogicalTableSchema(Map<String, String> tierFilter, K8sContext context, String schemaName) {
    this.tierFilter = Collections.unmodifiableMap(new HashMap<>(tierFilter));
    this.context = context;
    this.schemaName = schemaName;
  }

  @Override
  public String databaseName() {
    return schemaName;
  }

  @Override
  public Lookup<Table> tables() {
    // Invalidate lazy reference if cache is stale so that we re-query K8s.
    // LazyReference doesn't support invalidation, so we use our own TTL cache
    // and wrap it in a fresh LazyTableLookup each time tables() is called.
    return new LazyTableLookup<Table>() {
      @Override
      protected Map<String, Table> loadAllTables() throws Exception {
        return getTableMapCached();
      }

      @Override
      @Nullable
      protected Table loadTable(String name) throws Exception {
        return getTableMapCached().get(name);
      }

      @Override
      protected String getSchemaDescription() {
        return "LogicalTableSchema(" + schemaName + ")";
      }
    };
  }

  // Keep getTableMap() for compatibility with tests / callers that call it directly.
  @Override
  protected Map<String, Table> getTableMap() {
    try {
      return getTableMapCached();
    } catch (Exception e) {
      log.warn("Failed to load logical tables for schema {}", schemaName, e);
      return Collections.emptyMap();
    }
  }

  private Map<String, Table> getTableMapCached() throws SQLException {
    long now = System.currentTimeMillis();
    if (cachedTableMap != null && (now - cacheTimestamp) < CACHE_TTL_MS) {
      return cachedTableMap;
    }
    Map<String, Table> fresh = loadTableMap();
    cachedTableMap = fresh;
    cacheTimestamp = now;
    return fresh;
  }

  private Map<String, Table> loadTableMap() throws SQLException {
    Set<String> expectedTierKeys = tierFilter.keySet();
    Collection<V1alpha1LogicalTable> crds =
        new K8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList>(context, K8sApiEndpoints.LOGICAL_TABLES)
            .list();

    Map<String, Table> result = new HashMap<>();
    for (V1alpha1LogicalTable crd : crds) {
      if (crd.getSpec() == null) {
        continue;
      }
      Map<?, ?> crdTiers = crd.getSpec().getTiers();
      Set<?> crdTierKeys = (crdTiers == null) ? Collections.emptySet() : crdTiers.keySet();
      if (!crdTierKeys.equals(expectedTierKeys)) {
        continue; // tier set mismatch — belongs to a different flavor
      }
      String tableName = crd.getMetadata() != null ? crd.getMetadata().getName() : null;
      if (tableName == null) {
        continue;
      }
      result.put(tableName, new LogicalTable(tableName, crd.getSpec()));
    }
    log.debug("Loaded {} logical tables for schema {}", result.size(), schemaName);
    return Collections.unmodifiableMap(result);
  }
}
