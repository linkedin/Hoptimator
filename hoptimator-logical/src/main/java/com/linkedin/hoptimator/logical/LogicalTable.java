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
  private final K8sContext context;

  public LogicalTable(String name, Map<String, V1alpha1LogicalTableSpecTiers> tiers,
      @Nullable String resolvedTier, K8sContext context) {
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



  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataType rowType = resolveRowType();
    if (rowType != null) {
      return rowType;
    }
    return typeFactory.createUnknownType();
  }

  /**
   * Lazily resolves the row type by opening a JDBC connection to the preferred physical tier.
   * Prefers "nearline" as the schema source of truth; falls back to the first available tier.
   */
  @Nullable
  private RelDataType resolveRowType() {
    // Validate resolvedTier first (always fail-fast on bad config, regardless of context).
    // Priority: explicit resolvedTier hint > "nearline" default > first available tier.
    Map.Entry<String, V1alpha1LogicalTableSpecTiers> preferredEntry;
    if (resolvedTier != null) {
      if (!tiers.containsKey(resolvedTier)) {
        throw new IllegalStateException("Resolved tier '" + resolvedTier
            + "' does not exist in logical table '" + name + "'. Available tiers: " + tiers.keySet());
      }
      preferredEntry = Map.entry(resolvedTier, tiers.get(resolvedTier));
    } else if (tiers.containsKey("nearline")) {
      preferredEntry = Map.entry("nearline", tiers.get("nearline"));
    } else {
      preferredEntry = tiers.entrySet().iterator().next();
    }

    if (context == null) {
      return null;  // No K8s context (test-only constructor) — caller handles null as unknown type
    }

    V1alpha1LogicalTableSpecTiers tierBinding = preferredEntry.getValue();
    if (tierBinding == null || tierBinding.getDatabaseCrdName() == null) {
      return null;
    }

    String databaseCrdName = tierBinding.getDatabaseCrdName();
    try {
      V1alpha1Database dbCrd = new K8sApi<>(context, K8sApiEndpoints.DATABASES).get(databaseCrdName);
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
