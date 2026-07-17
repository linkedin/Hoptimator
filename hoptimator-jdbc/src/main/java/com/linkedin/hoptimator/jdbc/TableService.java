package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.PendingDelete;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.avro.AvroConverter;
import com.linkedin.hoptimator.util.DeploymentService;
import org.apache.avro.Schema;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;


/**
 * SQL-free entry point for creating a table (a {@link com.linkedin.hoptimator.Source}) from a
 * table path plus an Avro schema, without going through Calcite SQL parsing or DDL.
 *
 * <p>This is the programmatic counterpart to {@code CREATE TABLE}: the row type is derived from
 * the supplied Avro schema (rather than from SQL column declarations), but the resulting table is
 * validated and deployed through exactly the same {@code Validator} / {@code Deployer} SPI as the
 * DDL path via {@link HoptimatorDdlUtils#deployTableInternal}. Callers still supply a
 * {@link HoptimatorConnection} — it hosts the {@code Database} registry that deployers use to
 * resolve per-database connection config — but no SQL is parsed or planned.
 */
public final class TableService {

  private TableService() {
  }

  /**
   * Creates (or dry-run specifies) a table from an Avro schema.
   *
   * @param conn       the Hoptimator connection (provides the Database registry / config)
   * @param path       the fully-qualified table path (e.g. {@code [DATABASE, TABLE]} or
   *                   {@code [CATALOG, DATABASE, TABLE]})
   * @param avroSchema the Avro schema describing the table's row type
   * @param options    table options (equivalent to DDL {@code WITH (...)}); may be empty
   * @param orReplace  whether an existing table may be replaced/updated (like {@code CREATE OR
   *                   REPLACE}); ignored in {@code apply} mode, where creation is always idempotent
   * @param dryRun     when {@code true}, validate and render specs without mutating anything
   *                   (like {@code !specify} / the {@code Plan} RPC)
   * @return the specs (populated only for dry-run), the resolved row type, and the table path
   * @throws SQLException on validation or deployment errors
   */
  public static HoptimatorDdlUtils.SpecifyResult create(HoptimatorConnection conn, List<String> path,
      Schema avroSchema, Map<String, String> options, boolean orReplace, boolean dryRun)
      throws SQLException {
    if (path == null || path.size() < 2) {
      throw new SQLException("A table path must include at least a database and a table name.");
    }
    if (avroSchema == null) {
      throw new SQLException("An Avro schema is required to create a table.");
    }

    HoptimatorDdlUtils.DdlMode mode =
        dryRun ? HoptimatorDdlUtils.DdlMode.SPECIFY : HoptimatorDdlUtils.effectiveMode(orReplace, conn);

    CalcitePrepare.Context ctx = conn.createPrepareContext();
    RelDataType rowType = AvroConverter.rel(avroSchema, ctx.getTypeFactory());
    if (!rowType.isStruct()) {
      throw new SQLException("The Avro schema must be a record; got " + avroSchema.getType() + ".");
    }

    SqlIdentifier name = new SqlIdentifier(path, SqlParserPos.ZERO);
    HoptimatorDdlUtils.CreateTarget target =
        HoptimatorDdlUtils.resolveCreateTarget(ctx, conn, mode.mutable(), name);

    // No SQL column strategies/defaults on the Avro path.
    InitializerExpressionFactory ief = new NullInitializerExpressionFactory();

    // The direct path carries the resolved row type and resolves Database config through a
    // resolver, so it needs no Calcite connection in the deploy SPI.
    DirectDeploymentContext context = new DirectDeploymentContext(
        conn.connectionProperties(), new CalciteDatabaseConfigResolver(conn), rowType);

    return HoptimatorDdlUtils.deployTableInternal(conn, context, ctx, target.pair, target.isNewSchema,
        target.database, target.tableName, rowType, ief, options, false, orReplace, mode);
  }

  /**
   * Deletes a table, mirroring {@code DROP TABLE}. Unlike {@link #create}, this needs no schema —
   * it resolves the {@link Source} from the path and runs the same pre-delete dependency guard
   * and deployer teardown as the DDL path.
   *
   * @param conn the Hoptimator connection (provides the Database registry / config)
   * @param path the fully-qualified table path (e.g. {@code [DATABASE, TABLE]})
   * @throws SQLException on validation or teardown errors
   */
  public static void delete(HoptimatorConnection conn, List<String> path) throws SQLException {
    if (path == null || path.size() < 2) {
      throw new SQLException("A table path must include at least a database and a table name.");
    }

    CalcitePrepare.Context ctx = conn.createPrepareContext();
    SqlIdentifier name = new SqlIdentifier(path, SqlParserPos.ZERO);
    HoptimatorDdlUtils.CreateTarget target =
        HoptimatorDdlUtils.resolveCreateTarget(ctx, conn, false, name);

    List<String> tablePath = new ArrayList<>(target.pair.left.path(null));
    if (target.isNewSchema) {
      tablePath.add(target.database);
    }
    tablePath.add(target.tableName);

    Source source = new Source(target.database, tablePath, Map.of());
    DeploymentContext context = conn.deploymentContext();

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
