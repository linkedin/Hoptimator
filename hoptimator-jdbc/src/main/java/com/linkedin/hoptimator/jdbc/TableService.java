package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.PendingDelete;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.avro.AvroConverter;
import com.linkedin.hoptimator.util.DeploymentService;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;


/**
 * SQL-free entry point for creating and deleting a table (a {@link com.linkedin.hoptimator.Source})
 * from a table path plus an Avro schema, without going through Calcite SQL parsing or DDL.
 *
 * <p>This is the programmatic counterpart to {@code CREATE TABLE} / {@code DROP TABLE}: the row
 * type is derived from the supplied Avro schema (rather than SQL column declarations), but the
 * table is validated and deployed through exactly the same {@code Validator} / {@code Deployer}
 * SPI as the DDL path.
 *
 * <p>These entry points are <em>connection-free</em>: they take connection-level
 * {@link Properties} (and, for create, log hooks) and resolve the {@code Database} registry and
 * per-database config registry-natively (see {@link DatabaseConfigResolvers#forProperties}) — the
 * direct API opens no JDBC {@link java.sql.Connection}.
 */
public final class TableService {

  private TableService() {
  }

  /**
   * Creates (or dry-run specifies) a table from an Avro schema, without a JDBC connection.
   *
   * @param connectionProperties connection-level properties (namespace, {@code k8s.*}, hints, mode)
   * @param logHooks             sinks for human-readable deploy log lines; may be empty
   * @param path                 the fully-qualified table path (e.g. {@code [DATABASE, TABLE]} or
   *                             {@code [CATALOG, DATABASE, TABLE]})
   * @param avroSchema           the Avro schema describing the table's row type
   * @param options              table options (equivalent to DDL {@code WITH (...)}); may be empty
   * @param updateIfExists       when {@code false}, creating a table that already
   *                             exists fails. When {@code true}, an existing table is updated in
   *                             place (schema evolution / config change), and a table that does not
   *                             yet exist is still created. This flag is authoritative on the direct
   *                             path and is not overridden by the connection's {@code mode}.
   * @param dryRun               when {@code true}, validate and render specs without mutating
   *                             anything (like {@code !specify} / the {@code Plan} RPC)
   * @return the specs (populated only for dry-run), the resolved row type, and the table path
   * @throws SQLException on validation or deployment errors
   */
  public static HoptimatorDdlUtils.SpecifyResult create(Properties connectionProperties,
      List<Consumer<String>> logHooks, List<String> path, Schema avroSchema, Map<String, String> options,
      boolean updateIfExists, boolean dryRun) throws SQLException {
    if (path == null || path.size() < 2) {
      throw new SQLException("A table path must include at least a database and a table name.");
    }
    if (avroSchema == null) {
      throw new SQLException("An Avro schema is required to create a table.");
    }
    DatabaseConfigResolver resolver = DatabaseConfigResolvers.forProperties(connectionProperties);

    // updateIfExists is authoritative for the direct path: it maps straight to CREATE (fail if the
    // table already exists, enforced store-natively by the deployers) or UPDATE (create-or-update),
    // independent of the connection's mode. Dry-run always resolves to SPECIFY.
    HoptimatorDdlUtils.DdlMode mode = dryRun
        ? HoptimatorDdlUtils.DdlMode.SPECIFY
        : (updateIfExists ? HoptimatorDdlUtils.DdlMode.UPDATE : HoptimatorDdlUtils.DdlMode.CREATE);

    RelDataType rowType = AvroConverter.rel(avroSchema, new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
    if (!rowType.isStruct()) {
      throw new SQLException("The Avro schema must be a record; got " + avroSchema.getType() + ".");
    }

    // No SQL column strategies/defaults on the Avro path.
    InitializerExpressionFactory ief = new NullInitializerExpressionFactory();

    // Resolve the target database identifier registry-natively and carry the row type on the
    // context: the direct path touches no Calcite catalog. The table path is the caller's path.
    String database = resolver.databaseName(path);
    String tableName = path.get(path.size() - 1);
    DirectDeploymentContext context = new DirectDeploymentContext(connectionProperties, resolver, rowType);

    return HoptimatorDdlUtils.deployTableInternal(logHooks, context, null, path, false,
        database, tableName, rowType, ief, options, false, updateIfExists, mode);
  }

  /**
   * Deletes a table, mirroring {@code DROP TABLE}, without a JDBC connection. Unlike {@link #create},
   * this needs no schema — it resolves the {@link Source} from the path and runs the same pre-delete
   * dependency guard and deployer teardown as the DDL path.
   *
   * @param connectionProperties connection-level properties (namespace, {@code k8s.*}, hints)
   * @param path                 the fully-qualified table path (e.g. {@code [DATABASE, TABLE]})
   * @throws SQLException on validation or teardown errors
   */
  public static void delete(Properties connectionProperties, List<String> path) throws SQLException {
    if (path == null || path.size() < 2) {
      throw new SQLException("A table path must include at least a database and a table name.");
    }

    DatabaseConfigResolver resolver = DatabaseConfigResolvers.forProperties(connectionProperties);
    String database = resolver.databaseName(path);
    Source source = new Source(database, path, Map.of());
    DeploymentContext context = new DirectDeploymentContext(connectionProperties, resolver, null);

    Collection<Deployer> deployers = null;
    try {
      // Pre-delete dependency guard (mirrors the DDL DROP path).
      ValidationService.validateOrThrow(new PendingDelete<>(source), context);
      deployers = DeploymentService.deployers(source, context);
      DeploymentService.delete(deployers);
    } catch (SQLException | RuntimeException e) {
      if (deployers != null) {
        DeploymentService.restore(deployers);
      }
      throw e;
    }
  }
}
