package com.linkedin.hoptimator.logical;

import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.LazyReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.jdbc.schema.LazyTableLookup;
import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;


/**
 * A Calcite schema that lists {@link LogicalTable} instances from K8s CRDs.
 *
 * <p>CRDs are filtered by the {@link #SCHEMA_LABEL} label, which the deployer sets to the
 * database/schema name at creation time. For each matching CRD, the row type is resolved
 * dynamically from the nearline physical tier's JDBC schema.
 */
public class LogicalTableSchema extends AbstractSchema implements Database {

  /** K8s label key used to identify which logical schema a CRD belongs to. */
  public static final String SCHEMA_LABEL = "logical-database";

  private static final Logger log = LoggerFactory.getLogger(LogicalTableSchema.class);

  private final K8sContext context;
  private final String databaseName;
  private final LazyReference<Lookup<Table>> tableLookup = new LazyReference<>();

  public LogicalTableSchema(Properties properties, K8sContext context, String databaseName) {
    this.context = context;
    this.databaseName = databaseName;
  }

  @Override
  public String databaseName() {
    return databaseName;
  }

  @Override
  public Lookup<Table> tables() {
    return tableLookup.getOrCompute(() -> new LazyTableLookup<Table>() {
      @Override
      protected Map<String, Table> loadAllTables() throws Exception {
        return loadTableMap();
      }

      @Override
      @Nullable
      protected Table loadTable(String name) throws Exception {
        return loadTableMap().get(name);
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
    Collection<V1alpha1LogicalTable> crds =
        new K8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList>(context, K8sApiEndpoints.LOGICAL_TABLES).list();
    K8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new K8sApi<V1alpha1Database, V1alpha1DatabaseList>(context, K8sApiEndpoints.DATABASES);

    Map<String, Table> result = new HashMap<>();
    for (V1alpha1LogicalTable crd : crds) {
      if (crd.getMetadata() == null || crd.getSpec() == null) {
        continue;
      }
      // Filter by schema label.
      Map<String, String> labels = crd.getMetadata().getLabels();
      String label = labels != null ? labels.get(SCHEMA_LABEL) : null;
      if (!databaseName.equalsIgnoreCase(label)) {
        continue;
      }

      String tableName = crd.getMetadata().getName();
      Map<String, V1alpha1LogicalTableSpecTiers> tiers = crd.getSpec().getTiers();

      // Resolve row type from the physical tier (prefer nearline; fall back to first available).
      RelDataType rowType = resolveRowType(tableName, tiers, dbApi);
      result.put(tableName, new LogicalTable(tableName, rowType, tiers));
    }
    log.debug("Loaded {} logical tables for schema {}", result.size(), databaseName);
    return Collections.unmodifiableMap(result);
  }

  /**
   * Resolves the row type for a logical table by opening a JDBC connection to the
   * preferred physical tier (nearline first, then any other tier) and navigating
   * to the table in that schema.
   *
   * @return resolved row type, or {@code null} if resolution fails (table will show
   *         with an empty schema rather than being hidden entirely)
   */
  @Nullable
  private RelDataType resolveRowType(String tableName,
      @Nullable Map<String, V1alpha1LogicalTableSpecTiers> tiers,
      K8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi) {
    if (tiers == null || tiers.isEmpty()) {
      return null;
    }

    // Prefer nearline as the schema source of truth; fall back to the first tier.
    Map.Entry<String, V1alpha1LogicalTableSpecTiers> preferredEntry =
        tiers.containsKey("nearline")
            ? Map.entry("nearline", tiers.get("nearline"))
            : tiers.entrySet().iterator().next();
    V1alpha1LogicalTableSpecTiers tierBinding = preferredEntry.getValue();
    if (tierBinding == null || tierBinding.getDatabaseCrdName() == null) {
      return null;
    }

    String databaseCrdName = tierBinding.getDatabaseCrdName();
    try {
      V1alpha1Database dbCrd = dbApi.get(databaseCrdName);
      if (dbCrd.getSpec() == null) {
        return null;
      }
      String tierUrl = dbCrd.getSpec().getUrl();
      String tierSchema = dbCrd.getSpec().getSchema();

      try (Connection tierConn = DriverManager.getConnection(tierUrl)) {
        CalciteConnection calciteConn = tierConn.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConn.getRootSchema();
        SchemaPlus schema = rootSchema.subSchemas().get(tierSchema);
        if (schema == null) {
          log.warn("Schema {} not found in tier {} for table {}", tierSchema, databaseCrdName, tableName);
          return null;
        }
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        Table tierTable = schema.tables().get(tableName);
        if (tierTable == null) {
          log.debug("Table {} not yet present in tier {} schema {}", tableName, databaseCrdName, tierSchema);
          return null;
        }
        return tierTable.getRowType(typeFactory);
      }
    } catch (Exception e) {
      log.warn("Could not resolve row type for table {} from tier {}: {}",
          tableName, databaseCrdName, e.getMessage());
      return null;
    }
  }
}
