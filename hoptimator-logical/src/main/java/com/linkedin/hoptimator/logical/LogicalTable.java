package com.linkedin.hoptimator.logical;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;


/**
 * A Calcite table backed by a {@code LogicalTable} CRD.
 *
 * <p>The row type is resolved lazily on first access by opening a JDBC connection to
 * the preferred physical tier (nearline first) and reading the schema from there.
 * This avoids eagerly opening connections for every table during schema load.
 */
public final class LogicalTable extends AbstractTable {

  private static final Logger log = LoggerFactory.getLogger(LogicalTable.class);

  private final String name;
  private final Map<String, V1alpha1LogicalTableSpecTiers> tiers;
  /** Tier name to use for connector config resolution (from the JDBC URL {@code tier=} property). */
  @Nullable
  private final String resolvedTier;
  /** K8s context used to look up Database CRDs for row type resolution. */
  @Nullable
  private final K8sContext context;

  /** Lazily resolved row type; null until first call to getRowType(). */
  @Nullable
  private volatile RelDataType rowType;

  public LogicalTable(String name, Map<String, V1alpha1LogicalTableSpecTiers> tiers,
      @Nullable String resolvedTier, @Nullable K8sContext context) {
    if (tiers == null || tiers.isEmpty()) {
      throw new IllegalArgumentException(
          "LogicalTable '" + name + "' must have at least one tier binding; got: " + tiers);
    }
    this.name = name;
    this.tiers = tiers;
    this.resolvedTier = resolvedTier;
    this.context = context;
  }

  /** Constructor without a tier hint or K8s context — for testing only. */
  LogicalTable(String name, V1alpha1LogicalTableSpec spec) {
    this(name, spec.getTiers(), null, null);
  }

  public String name() {
    return name;
  }

  /**
   * Returns the tier map for this logical table.
   * Keys are tier names (e.g. "nearline", "offline", "online");
   * values carry the physical database CRD binding.
   */
  public Map<String, V1alpha1LogicalTableSpecTiers> tiers() {
    return tiers;
  }

  /**
   * Returns the Database CRD name of the tier this table resolves to for pipeline/connector
   * config generation. Returns null if no tier hint was specified.
   *
   * <p>For example, a Flink streaming job should resolve to "nearline" (Kafka), while a batch
   * query should resolve to "offline" (OpenHouse). The hint is set via the JDBC URL
   * {@code tier=nearline} property on the logical Database CRD.
   */
  @Nullable
  public String resolvedDatabaseCrdName() {
    if (resolvedTier == null || !tiers.containsKey(resolvedTier)) {
      return null;
    }
    V1alpha1LogicalTableSpecTiers binding = tiers.get(resolvedTier);
    return binding != null ? binding.getDatabaseCrdName() : null;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType != null) {
      return rowType;
    }
    rowType = resolveRowType();
    if (rowType != null) {
      return rowType;
    }
    return typeFactory.builder().build();
  }

  /**
   * Lazily resolves the row type by opening a JDBC connection to the preferred physical tier.
   * Prefers "nearline" as the schema source of truth; falls back to the first available tier.
   */
  @Nullable
  private RelDataType resolveRowType() {
    if (context == null) {
      return null;
    }

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
      V1alpha1Database dbCrd = new K8sApi<V1alpha1Database, V1alpha1DatabaseList>(
          context, K8sApiEndpoints.DATABASES).get(databaseCrdName);
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
          log.warn("Schema {} not found in tier {} for table {}", tierSchema, databaseCrdName, name);
          return null;
        }
        RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        Table tierTable = schema.tables().get(name);
        if (tierTable == null) {
          log.debug("Table {} not yet present in tier {} schema {}", name, databaseCrdName, tierSchema);
          return null;
        }
        return tierTable.getRowType(factory);
      }
    } catch (Exception e) {
      log.warn("Could not resolve row type for table {} from tier {}: {}",
          name, databaseCrdName, e.getMessage());
      return null;
    }
  }
}
