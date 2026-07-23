package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.avro.AvroConverter;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Properties;


/**
 * A {@link DeploymentContext} for the direct (non-SQL) API path. Unlike
 * {@link CalciteDeploymentContext}, it holds no Calcite {@link HoptimatorConnection}:
 *
 * <ul>
 *   <li>the caller's original Avro schema is <em>carried</em> (rather than a table being looked up
 *       from a Calcite {@code Table}); deployers that speak Avro (schema registry, Venice, ...) use
 *       it verbatim via {@link #avroSchema()}, and {@link #rowType()} is derived from it on demand —
 *       the schema is the single source of truth, so the two can't drift;
 *   <li>connection-level {@link #properties()} are a plain bag;
 *   <li>per-{@code Database} config is resolved through an injected {@link DatabaseConfigResolver},
 *       so this context does not depend on how the {@code Database} registry is stored.
 * </ul>
 *
 * <p>Because it is not a {@link CalciteDeploymentContext}, deployers cannot reach a
 * {@code java.sql.Connection} through it -- the direct path stays decoupled from Calcite.
 */
public final class DirectDeploymentContext implements DeploymentContext {

  private final Properties properties;
  private final DatabaseConfigResolver databaseConfigResolver;
  private final @Nullable Schema avroSchema;
  private @Nullable RelDataType rowType;  // lazily derived from avroSchema

  /** For schema-free operations such as delete, where no row type or Avro schema is needed. */
  public DirectDeploymentContext(Properties properties, DatabaseConfigResolver databaseConfigResolver) {
    this(properties, databaseConfigResolver, null);
  }

  public DirectDeploymentContext(Properties properties, DatabaseConfigResolver databaseConfigResolver,
      @Nullable Schema avroSchema) {
    this.properties = properties;
    this.databaseConfigResolver = databaseConfigResolver;
    this.avroSchema = avroSchema;
  }

  /**
   * The row type for the table being deployed, derived on first use from the carried
   * {@link #avroSchema()}. Throws for schema-free operations (e.g. a delete) that carry no schema.
   */
  public RelDataType rowType() {
    if (avroSchema == null) {
      throw new IllegalStateException("No Avro schema is carried by this context (e.g. a delete). "
          + "A deployer requested a row type on a schema-free operation.");
    }
    if (rowType == null) {
      rowType = AvroConverter.rel(avroSchema, new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
    }
    return rowType;
  }

  /**
   * The caller's original (merged key+value) Avro schema for the table being deployed, or
   * {@code null} when the caller supplied none (e.g. a delete). Deployers should prefer this over
   * re-synthesizing Avro from {@link #rowType()}, since the row type cannot represent Avro
   * namespaces, nested record identities, unions, or defaults.
   */
  public @Nullable Schema avroSchema() {
    return avroSchema;
  }

  @Override
  public Properties properties() {
    return properties;
  }

  @Override
  public @Nullable Properties databaseProperties(@Nullable String catalog, @Nullable String schema,
      String connectionPrefix) throws SQLException {
    return databaseConfigResolver.databaseProperties(catalog, schema, connectionPrefix);
  }
}
