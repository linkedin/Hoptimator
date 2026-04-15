/*
 * N.B. much of this code is copy-pasted from the base class in
 * upstream Apache Calcite.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.hoptimator.jdbc;

import com.google.common.collect.ImmutableList;
import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.MaterializedView;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateMaterializedView;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateTable;
import com.linkedin.hoptimator.util.DeploymentService;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcCatalogSchema;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcTable;
import com.linkedin.hoptimator.util.planner.PipelineRel;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.ContextSqlValidator;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class HoptimatorDdlUtils {
  private HoptimatorDdlUtils() {
  }

  /**
   * The result of a {@link #specifyFromSql} call: the YAML artifact specs, the sink row type,
   * and the fully-qualified path of the sink (viewPath).
   *
   * <ul>
   *   <li>{@code specs} — YAML artifacts produced by each deployer (empty for dry-run no-ops).
   *   <li>{@code sinkRowType} — row type of the sink; same as the (renamed) query output.
   *   <li>{@code viewPath} — fully-qualified path including catalog, schema, and view name.
   * </ul>
   */
  public static final class SpecifyResult {
    public final List<String> specs;
    public final RelDataType sinkRowType;
    /** Fully-qualified path of the sink (catalog + schema + table). */
    public final List<String> viewPath;

    SpecifyResult(List<String> specs, RelDataType sinkRowType, List<String> viewPath) {
      this.specs = Collections.unmodifiableList(specs);
      this.sinkRowType = sinkRowType;
      this.viewPath = Collections.unmodifiableList(viewPath);
    }
  }

  /**
   * Controls whether a DDL operation performs a real deployment (CREATE or UPDATE)
   * or a dry-run (SPECIFY).
   */
  enum DdlMode {
    CREATE {
      @Override
      List<String> executeDeployers(Collection<Deployer> deployers, Connection conn) throws SQLException {
        DeploymentService.create(deployers);
        return Collections.emptyList();
      }

      @Override
      boolean mutable() {
        return true;
      }
    },
    UPDATE {
      @Override
      List<String> executeDeployers(Collection<Deployer> deployers, Connection conn) throws SQLException {
        DeploymentService.update(deployers);
        return Collections.emptyList();
      }

      @Override
      boolean mutable() {
        return true;
      }
    },
    SPECIFY {
      @Override
      List<String> executeDeployers(Collection<Deployer> deployers, Connection conn) throws SQLException {
        List<String> specs = new ArrayList<>();
        for (Deployer deployer : deployers) {
          specs.addAll(deployer.specify());
        }
        return specs;
      }

      @Override
      boolean mutable() {
        return false;
      }
    };

    abstract List<String> executeDeployers(Collection<Deployer> deployers, Connection conn) throws SQLException;

    abstract boolean mutable();
  }

  // N.B. copy-pasted from Apache Calcite
  /** Returns the schema in which to create an object;
   * the left part is null if the schema does not exist. */
  public static Pair<CalciteSchema, String> schema(CalcitePrepare.Context context, boolean mutable, SqlIdentifier id) {
    final String name;
    final List<String> path;
    if (id.isSimple()) {
      path = context.getDefaultSchemaPath();
      name = id.getSimple();
    } else {
      path = Util.skipLast(id.names);
      name = Util.last(id.names);
    }
    CalciteSchema schema = mutable ? context.getMutableRootSchema() : context.getRootSchema();
    for (String p : path) {
      schema = Objects.requireNonNull(schema).subSchemas().get(p);
    }
    return Pair.of(schema, name);
  }

  /** Returns the catalog in which to create an object;
   * the left part is null if the catalog does not exist. */
  public static Pair<CalciteSchema, String> catalog(CalcitePrepare.Context context, boolean mutable, SqlIdentifier id) {
    if (id.names.size() < 3) {
      throw new IllegalArgumentException("CATALOG.SCHEMA.TABLE identified expected but found: " + id);
    }
    final List<String> schemaTablePath = Util.last(id.names, 2);
    final List<String> catalogPath = Util.skipLast(id.names, 2);
    CalciteSchema schema = mutable ? context.getMutableRootSchema() : context.getRootSchema();
    for (String p : catalogPath) {
      schema = Objects.requireNonNull(schema).subSchemas().get(p);
    }
    return Pair.of(schema, String.join(".", schemaTablePath));
  }

  // N.B. copy-pasted from Apache Calcite
  /** Wraps a query to rename its columns. Used by CREATE VIEW and CREATE
   * MATERIALIZED VIEW. */
  public static SqlNode renameColumns(SqlNodeList columnList, SqlNode query) {
    if (columnList == null) {
      return query;
    }
    final SqlParserPos p = query.getParserPosition();
    final SqlNodeList selectList = SqlNodeList.SINGLETON_STAR;
    final SqlCall from = SqlStdOperatorTable.AS.createCall(p,
        Arrays.asList(query, new SqlIdentifier("_", p), columnList));
    return new SqlSelect(p, null, selectList, from, null, null, null, null, null, null, null, null, null);
  }

  // N.B. copy-pasted from Apache Calcite
  public static ViewTable viewTable(CalcitePrepare.Context context, String sql, CalcitePrepareImpl impl,
      List<String> schemaPath, List<String> viewPath) {
    CalcitePrepare.AnalyzeViewResult analyzed = impl.analyzeView(context, sql, false);
    RelProtoDataType protoType = RelDataTypeImpl.proto(analyzed.rowType);
    return new ViewTable(Object.class, protoType, sql, schemaPath, viewPath);
  }

  public static String viewName(SqlIdentifier id) {
    final String name;
    if (id.isSimple()) {
      name = id.getSimple();
    } else {
      name = Util.last(id.names);
    }
    return name;
  }

  // Returns the pair of the schema to the current state of the table prior to this change.
  public static Pair<SchemaPlus, Table> snapshotAndSetSinkSchema(CalcitePrepare.Context context, CalcitePrepareImpl impl,
      PipelineRel.Implementor plan, SqlCreateMaterializedView create, String querySql) {
    final Pair<CalciteSchema, String> pair = schema(context, false, create.name);
    if (!(pair.left.schema instanceof Database)) {
      throw new HoptimatorDdlExecutor.DdlException(create, pair.left.plus().getName() + " is not a physical database.");
    }
    return snapshotAndSetSinkSchema(context, impl, plan, querySql, pair);
  }

  public static Pair<SchemaPlus, Table> snapshotAndSetSinkSchema(CalcitePrepare.Context context, CalcitePrepareImpl impl,
      PipelineRel.Implementor plan, String querySql, Pair<CalciteSchema, String> schemaPair) {
    String database = ((Database) schemaPair.left.schema).databaseName();
    final List<String> schemaPath = schemaPair.left.path(null);
    final List<String> viewPath = new ArrayList<>(schemaPath);
    final List<String> sinkPath = new ArrayList<>(schemaPath);
    String viewName = schemaPair.right;
    viewPath.add(viewName);
    String[] viewParts = viewName.split("\\$", 2);
    String sinkName = viewParts[0];
    sinkPath.add(sinkName);

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    ViewTable viewTable = viewTable(context, querySql, impl, schemaPath, viewPath);
    MaterializedViewTable materializedViewTable = new MaterializedViewTable(viewTable);
    RelDataType viewRowType = materializedViewTable.getRowType(typeFactory);

    final SchemaPlus schemaPlus = schemaPair.left.plus();
    Table sink = schemaPlus.tables().get(sinkName);
    final RelDataType rowType;
    if (sink != null) {
      // For "partial views", the sink may already exist. Use the existing row type.
      rowType = sink.getRowType(typeFactory);
    } else {
      // For normal views, we create the sink based on the view row type.
      rowType = viewRowType;
    }

    Table currentViewTable = schemaPlus.tables().get(viewName);
    // Need to add the view table to the connection so that the ConnectorService can find it when resolving options.
    schemaPlus.add(viewName, materializedViewTable);
    plan.setSink(database, sinkPath, rowType, Collections.emptyMap());
    return Pair.of(schemaPlus, currentViewTable);
  }

  /**
   * Shared implementation of the {@code CREATE MATERIALIZED VIEW} pipeline for both real
   * deployment and dry-run (SPECIFY) modes.
   *
   * <p>Deployer execution is delegated to {@link DdlMode#executeDeployers}, which either calls
   * {@link DeploymentService#create}/{@link DeploymentService#update} or collects YAML
   * specs via {@link Deployer#specify()}.
   *
   * @param ctx     the Calcite prepare context
   * @param prepare the HoptimatorDriver prepare helper
   * @param conn    the JDBC connection
   * @param create  the parsed DDL node
   * @param mode    whether to CREATE, UPDATE, or SPECIFY
   * @return an empty list for CREATE/UPDATE, or the YAML spec strings for SPECIFY
   * @throws SQLException on validation or deployment errors
   */
  static SpecifyResult processCreateMaterializedView(CalcitePrepare.Context ctx,
      HoptimatorDriver.Prepare prepare, HoptimatorConnection conn,
      SqlCreateMaterializedView create, DdlMode mode) throws SQLException {
    HoptimatorConnection.HoptimatorConnectionDualLogger logger = conn.getLogger(HoptimatorDdlUtils.class);
    // Validate the DDL statement.
    logger.info("Validating statement: {}", create);
    ValidationService.validateOrThrow(create);

    // Extract query SQL (rename columns if a column list was provided) and plan the query.
    // This is done first — before schema/conflict checks — so that:
    //   1. sinkRowType is always available, even for IF NOT EXISTS early returns.
    //   2. root is computed once and reused for pipeline planning below.
    final SqlNode q = renameColumns(create.columnList, create.query);
    final String sql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    RelRoot root = prepare.convert(ctx, sql).root;
    RelDataType sinkRowType = root.rel.getRowType();

    // Navigate to the schema (mutable only when actually deploying).
    final Pair<CalciteSchema, String> pair = schema(ctx, mode.mutable(), create.name);
    if (pair.left == null) {
      throw new SQLException("Schema for " + create.name + " not found.");
    }
    final SchemaPlus schemaPlus = pair.left.plus();

    // Check for conflicting tables
    if (schemaPlus.tables().get(pair.right) != null) {
      if (schemaPlus.tables().get(pair.right) instanceof HoptimatorJdbcTable) {
        throw new SQLException(
            "Cannot overwrite physical table " + pair.right + " with a view.");
      }
      // Materialized view exists.
      if (!create.ifNotExists && !create.getReplace()) {
        throw new SQLException(
            "View " + pair.right + " already exists. Use CREATE OR REPLACE to update.");
      }
      if (create.getReplace()) {
        schemaPlus.removeTable(pair.right);
      } else {
        // IF NOT EXISTS — nothing to do.
        List<String> viewPath = new ArrayList<>(pair.left.path(null));
        viewPath.add(pair.right);
        return new SpecifyResult(Collections.emptyList(), sinkRowType, viewPath);
      }
    }

    if (!(pair.left.schema instanceof Database)) {
      throw new SQLException(schemaPlus.getName() + " is not a physical database.");
    }
    String database = ((Database) pair.left.schema).databaseName();

    final List<String> schemaPath = pair.left.path(null);
    String viewName = pair.right;
    List<String> viewPath = new ArrayList<>(schemaPath);
    viewPath.add(viewName);
    logger.info("Validated sql statement. The view is named {} and has path {}", viewName, viewPath);

    // Build pipeline name and set it on the connection properties.
    // Support "partial views", i.e. CREATE VIEW FOO$BAR, where the view name is "foo-bar" and the sink is just FOO.
    String[] viewParts = viewName.split("\\$", 2);
    String sinkName = viewParts[0];
    String pipelineName = database + "-" + sinkName;
    if (viewParts.length > 1) {
      pipelineName = pipelineName + "-" + viewParts[1];
    }
    logger.info("Pipeline name for view {} is {}", viewName, pipelineName);
    Properties connectionProperties = conn.connectionProperties();
    connectionProperties.setProperty(DeploymentService.PIPELINE_OPTION, pipelineName);

    // Plan the pipeline
    PipelineRel.Implementor plan = DeploymentService.plan(root, conn.materializations(), connectionProperties);

    // Snapshot the current schema state and set the sink schema for planning.
    Pair<SchemaPlus, Table> schemaSnapshot = snapshotAndSetSinkSchema(ctx, prepare, plan, sql, pair);
    logger.info("Added materialized view {} to schema {}", viewName, schemaPlus.getName());

    Collection<Deployer> deployers = null;
    boolean success = false;
    try {
      // Build the pipeline and create the MaterializedView hook.
      Pipeline pipeline = plan.pipeline(viewName, conn);
      MaterializedView hook = new MaterializedView(database, viewPath, sql, pipeline.job().sql(), pipeline);

      // Validate the hook and its deployers.
      logger.info("Validating materialized view {}", viewName);
      ValidationService.validateOrThrow(hook);
      deployers = DeploymentService.deployers(hook, conn);
      logger.info("Validating deployable resources for materialized view {}", viewName);
      ValidationService.validateOrThrow(deployers);
      logger.info("Validated materialized view {}", viewName);

      // Execute (create/update) or collect specs (specify).
      if (mode == DdlMode.UPDATE) {
        logger.info("Deploying update materialized view {}", viewName);
      } else if (mode == DdlMode.CREATE) {
        logger.info("Deploying create materialized view {}", viewName);
      } else {
        logger.info("Specifying materialized view {}", viewName);
      }
      List<String> specs = mode.executeDeployers(deployers, conn);
      if (mode.mutable()) {
        logger.info("Deployed materialized view {}", viewName);
      } else {
        // SPECIFY (dry-run): roll back any side effects made by deployers during specify().
        DeploymentService.restore(deployers);
      }
      success = true;
      return new SpecifyResult(specs, sinkRowType, viewPath);
    } catch (SQLException | RuntimeException e) {
      logger.info("Failed to deploy materialized view {}", viewName);
      if (deployers != null) {
        DeploymentService.restore(deployers);
        logger.info("Restored deployable resources for materialized view {}", viewName);
      }
      throw e;
    } finally {
      // Restore the schema snapshot.
      // For SPECIFY (dry-run): always restore — the view was temporarily added to the schema
      // for planning purposes and must be removed afterward.
      // For CREATE/UPDATE on failure: restore to undo the partial schema mutation.
      // For CREATE/UPDATE on success: do NOT restore — the view should remain in the schema.
      if (!success || !mode.mutable()) {
        if (schemaSnapshot != null) {
          if (schemaSnapshot.right != null) {
            schemaSnapshot.left.add(viewName, schemaSnapshot.right);
            logger.info("Restored schema for materialized view {}", viewName);
          } else {
            schemaSnapshot.left.removeTable(viewName);
            logger.info("Removed schema for materialized view {}", viewName);
          }
        }
      }
    }
  }

  /**
   * Shared implementation of the {@code CREATE TABLE} pipeline for both real deployment
   * and dry-run (SPECIFY) modes.
   *
   * <p>Handles both 2-level (SCHEMA.TABLE) and 3-level (CATALOG.SCHEMA.TABLE) paths.
   *
   * @param ctx    the Calcite prepare context
   * @param conn   the JDBC connection
   * @param create the parsed DDL node
   * @param mode   whether to CREATE, UPDATE, or SPECIFY
   * @return an empty list for CREATE/UPDATE, or the YAML spec strings for SPECIFY
   * @throws SQLException on validation or deployment errors
   */
  static SpecifyResult processCreateTable(CalcitePrepare.Context ctx, HoptimatorConnection conn,
      SqlCreateTable create, DdlMode mode) throws SQLException {
    HoptimatorConnection.HoptimatorConnectionDualLogger logger = conn.getLogger(HoptimatorDdlUtils.class);

    logger.info("Validating statement: {}", create);
    ValidationService.validateOrThrow(create);

    // TODO: Add support for populating new tables from a query as a one-time operation.
    if (create.query != null) {
      throw new SQLException("Populating new tables is not currently supported.");
    }
    if (create.columnList == null) {
      throw new SQLException("No columns provided.");
    }

    boolean isNewSchema = false;
    Pair<CalciteSchema, String> pair = schema(ctx, mode.mutable(), create.name);
    if (pair.left == null) {
      // If the schema is not found, it might be because it's a 3-level path (CATALOG.SCHEMA.TABLE)
      if (create.name.names.size() > 2) {
        pair = catalog(ctx, mode.mutable(), create.name);
        isNewSchema = true;
        if (pair.left == null) {
          throw new SQLException("Catalog for " + create.name + " not found.");
        }
      } else {
        throw new SQLException("Schema for " + create.name + " not found.");
      }
    }

    final SchemaPlus schemaPlus = pair.left.plus();
    String database = null;
    String tableName;
    if (isNewSchema) {
      int idx = pair.right.indexOf(".");
      database = pair.right.substring(0, idx);
      tableName = pair.right.substring(idx + 1);
    } else {
      tableName = pair.right;
    }

    if (!isNewSchema && schemaPlus.tables().get(tableName) != null) {
      if (!create.ifNotExists && !create.getReplace()) {
        // They did not specify IF NOT EXISTS, so give error.
        throw new SQLException(
            "Table " + tableName + " already exists. Use CREATE OR REPLACE to update.");
      }
    }

    // Build row type and column definitions.
    final JavaTypeFactory typeFactory = ctx.getTypeFactory();
    final ImmutableList.Builder<ColumnDef> columnDefBuilder = ImmutableList.builder();
    final RelDataTypeFactory.Builder relBuilder = typeFactory.builder();
    final ContextSqlValidator validator = new ContextSqlValidator(ctx, true);
    for (SqlNode columnNode : create.columnList) {
      if (columnNode instanceof SqlColumnDeclaration) {
        final SqlColumnDeclaration columnDeclaration = (SqlColumnDeclaration) columnNode;
        final RelDataType type = columnDeclaration.dataType.deriveType(validator, true);
        relBuilder.add(columnDeclaration.name.getSimple(), type);
        columnDefBuilder.add(ColumnDef.of(columnDeclaration.expression, type, columnDeclaration.strategy));
      } else if (columnNode instanceof SqlKeyConstraint) {
        // TODO: Support UNIQUE & PRIMARY KEY
        logger.info("Unsupported column declaration: " + columnNode.getClass());
      } else {
        throw new SQLException("Unsupported column declaration: " + columnNode.getClass());
      }
    }
    final RelDataType rowType = relBuilder.build();
    final List<ColumnDef> columns = columnDefBuilder.build();
    final InitializerExpressionFactory ief =
        new NullInitializerExpressionFactory() {
          @Override
          public ColumnStrategy generationStrategy(RelOptTable table, int iColumn) {
            return columns.get(iColumn).strategy;
          }

          @Override
          public RexNode newColumnDefaultValue(RelOptTable table, int iColumn,
              InitializerContext context) {
            final ColumnDef columnDef = columns.get(iColumn);
            if (columnDef.expr != null) {
              final SqlNode validated = context.validateExpression(rowType, columnDef.expr);
              // The explicit specified type should have the same nullability
              // with the column expression inferred type
              return context.convertExpression(validated);
            }
            return super.newColumnDefaultValue(table, iColumn, context);
          }
        };

    if (database == null) {
      if (pair.left.schema instanceof Database) {
        database = ((Database) pair.left.schema).databaseName();
      } else {
        database = conn.getSchema();
      }
    }

    // Snapshot current state for rollback (only meaningful when the schema already exists).
    Pair<SchemaPlus, Table> schemaSnapshot = null;
    if (!isNewSchema) {
      Table currentTable = schemaPlus.tables().get(tableName);
      schemaSnapshot = Pair.of(schemaPlus, currentTable);
    }

    // Table does not exist. Create it.
    // Add a temporary table with the correct row type so deployers can resolve the schema
    // TODO: This may cause problems if we reuse connections, only the next connection will load this as a HoptimatorJdbcTable.
    if (isNewSchema) {
      HoptimatorJdbcCatalogSchema catalogSchema = schemaPlus.unwrap(HoptimatorJdbcCatalogSchema.class);
      if (catalogSchema == null) {
        throw new SQLException("Catalog for " + schemaPlus.getName() + " not found.");
      }
      SchemaPlus databaseSchema = schemaPlus.add(database, catalogSchema.createSchema(database));
      logger.info("Added schema {} to catalog {}", database, schemaPlus.getName());
      databaseSchema.add(tableName, new TemporaryTable(rowType, database, ief));
      logger.info("Added table {} to schema {}", tableName, databaseSchema.getName());
    } else {
      schemaPlus.add(tableName, new TemporaryTable(rowType, database, ief));
      logger.info("Added table {} to schema {}", tableName, schemaPlus.getName());
    }

    final List<String> schemaPath = pair.left.path(null);
    List<String> tablePath = new ArrayList<>(schemaPath);
    if (isNewSchema) {
      tablePath.add(database);
    }
    tablePath.add(tableName);

    Map<String, String> tableOptions = options(create.options);
    Source source = new Source(database, tablePath, tableOptions);

    Collection<Deployer> deployers = null;
    boolean success = false;
    try {
      logger.info("Validating new table {}", source);
      ValidationService.validateOrThrow(source);
      deployers = DeploymentService.deployers(source, conn);
      logger.info("Validating deployable resources for table {}", tableName);
      ValidationService.validateOrThrow(deployers);

      if (mode == DdlMode.UPDATE) {
        logger.info("Deploying update table {}", source);
      } else if (mode == DdlMode.CREATE) {
        logger.info("Deploying create table {}", source);
      } else {
        logger.info("Specifying table {}", source);
      }
      List<String> specs = mode.executeDeployers(deployers, conn);
      if (mode.mutable()) {
        logger.info("Deployed table {}", source);
      } else {
        // SPECIFY (dry-run): roll back any side effects made by deployers during specify()
        DeploymentService.restore(deployers);
      }
      success = true;
      return new SpecifyResult(specs, rowType, tablePath);
    } catch (SQLException | RuntimeException e) {
      logger.info("Failed to deploy table {}", tableName);
      if (deployers != null) {
        DeploymentService.restore(deployers);
        logger.info("Restored deployable resources for table {}", tableName);
      }
      throw e;
    } finally {
      // For SPECIFY (dry-run): always restore schema.
      // For CREATE/UPDATE on success: do NOT restore.
      // For CREATE/UPDATE on failure: restore.
      if (!success || !mode.mutable()) {
        if (schemaSnapshot != null) {
          if (schemaSnapshot.right == null) {
            schemaSnapshot.left.removeTable(tableName);
            logger.info("Removed schema for table {}", tableName);
          } else {
            schemaPlus.add(tableName, schemaSnapshot.right);
            logger.info("Restored schema for table {}", tableName);
          }
        } else {
          // isNewSchema case on failure: remove the newly created sub-schema.
          pair.left.removeSubSchema(database);
          logger.info("Removed schema {} from catalog", database);
        }
      }
    }
  }

  /**
   * Returns the YAML specs that would be created for any supported SQL statement —
   * {@code CREATE TABLE}, {@code CREATE MATERIALIZED VIEW}, or {@code INSERT INTO}.
   *
   * <p>This is the shared implementation behind {@code !specify} in Quidem tests, the
   * interactive CLI, and more. It is a strict dry-run: no schema
   * mutations, no deployer create/update calls.
   * Returns both the YAML artifact specs and the sink row type for the given SQL statement in
   * a single pass.
   *
   * <p>Supported statement types:
   * <ul>
   *   <li><b>CREATE MATERIALIZED VIEW</b> — column renames applied; query planned once.
   *   <li><b>CREATE TABLE</b> — row type from column declarations; no query planning.
   *   <li><b>SELECT / INSERT INTO</b> — query output row type.
   * </ul>
   *
   * @throws SQLException for unsupported DDL (e.g. DROP, CREATE VIEW)
   */
  public static SpecifyResult specifyFromSql(String sql, HoptimatorConnection conn) throws SQLException {
    SqlNode sqlNode = HoptimatorDriver.parseQuery(conn, sql);

    if (sqlNode instanceof SqlCreateTable) {
      return processCreateTable(conn.createPrepareContext(), conn, (SqlCreateTable) sqlNode, DdlMode.SPECIFY);
    }

    if (sqlNode instanceof SqlCreateMaterializedView) {
      return processCreateMaterializedView(conn.createPrepareContext(), new HoptimatorDriver.Prepare(conn),
          conn, (SqlCreateMaterializedView) sqlNode, DdlMode.SPECIFY);
    }

    if (sqlNode.getKind().belongsTo(SqlKind.DDL)) {
      throw new SQLException("Unsupported DDL statement: " + sql);
    }

    // Plain SELECT / INSERT INTO path.
    String viewName = "SINK";
    RelRoot root = HoptimatorDriver.convert(conn, sql).root;
    RelDataType sinkRowType = root.rel.getRowType();
    Properties connectionProperties = conn.connectionProperties();
    RelOptTable table = root.rel.getTable();
    List<String> viewPath;
    if (table != null) {
      List<String> qualifiedName = table.getQualifiedName();
      connectionProperties.setProperty(DeploymentService.PIPELINE_OPTION, String.join(".", qualifiedName));
      viewName = qualifiedName.get(qualifiedName.size() - 1);
      viewPath = new ArrayList<>(qualifiedName);
    } else {
      // No INSERT INTO target — name the virtual sink "SINK" and record it as the pipeline.
      connectionProperties.setProperty(DeploymentService.PIPELINE_OPTION, viewName);
      viewPath = new ArrayList<>(List.of("DEFAULT", viewName));
    }

    PipelineRel.Implementor plan = DeploymentService.plan(root, conn.materializations(), connectionProperties);

    // For plain SELECT queries (no INSERT INTO target), the planner has no sink.
    // Set up a virtual sink so that pipeline.job().sink() is non-null
    CalcitePrepare.Context ctx = conn.createPrepareContext();
    Pair<SchemaPlus, Table> sinkSnapshot = null;
    if (table == null) {
      ViewTable sinkViewTable = viewTable(ctx, sql, new HoptimatorDriver.Prepare(conn),
          List.of("DEFAULT"), viewPath);
      sinkRowType = sinkViewTable.getRowType(ctx.getTypeFactory());
      plan.setSink("DEFAULT", viewPath, sinkRowType, Collections.emptyMap());
      // Register the virtual sink in the schema so deployers can resolve it; snapshot for rollback.
      Pair<CalciteSchema, String> sinkSchemaPair = schema(ctx, false,
          new SqlIdentifier(viewName, SqlParserPos.ZERO));
      if (sinkSchemaPair.left != null) {
        SchemaPlus sinkSchemaPlus = sinkSchemaPair.left.plus();
        Table existing = sinkSchemaPlus.tables().get(sinkSchemaPair.right);
        sinkSnapshot = Pair.of(sinkSchemaPlus, existing);
        sinkSchemaPlus.add(sinkSchemaPair.right, new MaterializedViewTable(sinkViewTable));
      }
    }

    try {
      Pipeline pipeline = plan.pipeline(viewName, conn);
      List<String> specs = new ArrayList<>();
      for (Source source : pipeline.sources()) {
        specs.addAll(DeploymentService.specify(source, conn));
      }
      specs.addAll(DeploymentService.specify(pipeline.sink(), conn));
      specs.addAll(DeploymentService.specify(pipeline.job(), conn));
      return new SpecifyResult(specs, sinkRowType, viewPath);
    } finally {
      // Restore the schema — the virtual sink must not persist after this call.
      if (sinkSnapshot != null) {
        if (sinkSnapshot.right != null) {
          sinkSnapshot.left.add(viewName, sinkSnapshot.right);
        } else {
          sinkSnapshot.left.removeTable(viewName);
        }
      }
    }
  }

  public static Map<String, String> options(SqlNodeList optionList) {
    Map<String, String> options = new HashMap<>();
    if (optionList != null) {
      for (int i = 0; i < optionList.size() - 1; i += 2) {
        SqlNode k = optionList.get(i);
        SqlLiteral v = (SqlLiteral) optionList.get(i + 1);
        String keyStr;
        if (k instanceof SqlIdentifier) {
          keyStr = String.join(".", ((SqlIdentifier) k).names);
        } else {
          keyStr = ((SqlLiteral) k).getValueAs(String.class);
        }
        options.put(keyStr, v.getValueAs(String.class));
      }
    }
    return options;
  }

  /** Captures a column's default expression and generation strategy for use when constructing
   * an {@link InitializerExpressionFactory} on the temporary table created during CREATE TABLE. */
  static final class ColumnDef {
    final @Nullable SqlNode expr;
    final RelDataType type;
    final ColumnStrategy strategy;

    private ColumnDef(@Nullable SqlNode expr, RelDataType type, ColumnStrategy strategy) {
      this.expr = expr;
      this.type = type;
      this.strategy = requireNonNull(strategy, "strategy");
      checkArgument(
          strategy == ColumnStrategy.NULLABLE
              || strategy == ColumnStrategy.NOT_NULLABLE
              || expr != null);
    }

    static ColumnDef of(@Nullable SqlNode expr, RelDataType type, ColumnStrategy strategy) {
      return new ColumnDef(expr, type, strategy);
    }
  }

  /**
   * Registers a temporary table in a schema with the given row type, and returns a
   * {@link Runnable} that restores the schema to its prior state (for rollback).
   * The {@code databaseName} is stored on the {@link TemporaryTable} so that DROP TABLE
   * can construct the correct {@link com.linkedin.hoptimator.Source} from it.
   *
   * @param schema        the schema to register the temporary table in
   * @param tableName     the table name to register under
   * @param rowType       the row type the temporary table should expose
   * @param databaseName  the database name the temporary table belongs to
   * @return a rollback {@link Runnable} that restores the schema to its prior state
   */
  static Runnable registerTemporaryTable(SchemaPlus schema, String tableName,
      RelDataType rowType, String databaseName) {
    Table existing = schema.tables().get(tableName);
    schema.add(tableName, new TemporaryTable(rowType, databaseName));
    if (existing == null) {
      return () -> schema.removeTable(tableName);
    } else {
      return () -> schema.add(tableName, existing);
    }
  }

  /**
   * Registers a {@link TemporaryTable} in the correct tier schema within {@code conn},
   * handling both the simple (2-level: SCHEMA.TABLE) and catalog (3-level: CATALOG.SCHEMA.TABLE)
   * cases, mirroring the {@code isNewSchema} logic in {@link HoptimatorDdlExecutor}.
   *
   * @param conn          the main connection whose root schema to navigate
   * @param catalog       optional catalog name; null for simple schemas
   * @param schema        the database/schema name to register in
   * @param tableName     the table name to register
   * @param rowType       the row type the temporary table should expose
   * @param databaseName  the database name the temporary table belongs to
   * @return a rollback {@link Runnable} that restores the schema to its prior state
   */
  public static Runnable registerTemporaryTableInSchema(HoptimatorConnection conn,
      @Nullable String catalog, @Nullable String schema, String tableName, RelDataType rowType,
      String databaseName) throws SQLException {
    SchemaPlus rootSchema = conn.calciteConnection().getRootSchema();

    if (catalog != null) {
      if (schema == null) {
        throw new SQLException("Catalog '" + catalog + "' is present but no schema name was"
            + " provided for table " + tableName);
      }
      SchemaPlus catalogSchemaPlus = rootSchema.subSchemas().get(catalog);
      if (catalogSchemaPlus == null) {
        throw new SQLException("Catalog '" + catalog + "' not found in main connection"
            + " while pre-registering row type for table " + tableName);
      }
      SchemaPlus databaseSchema = catalogSchemaPlus.subSchemas().get(schema);
      if (databaseSchema == null) {
        HoptimatorJdbcCatalogSchema catalogSchema;
        try {
          catalogSchema = catalogSchemaPlus.unwrap(HoptimatorJdbcCatalogSchema.class);
        } catch (ClassCastException e) {
          catalogSchema = null;
        }
        if (catalogSchema == null) {
          throw new SQLException("Catalog '" + catalog
              + "' is not a HoptimatorJdbcCatalogSchema; cannot create database schema '"
              + schema + "' for table " + tableName);
        }
        SchemaPlus newSchema = catalogSchemaPlus.add(schema, catalogSchema.createSchema(schema));
        return registerTemporaryTable(newSchema, tableName, rowType, databaseName);
      } else {
        return registerTemporaryTable(databaseSchema, tableName, rowType, databaseName);
      }
    } else if (schema != null) {
      SchemaPlus tierSchema = rootSchema.subSchemas().get(schema);
      if (tierSchema == null) {
        throw new SQLException("Schema '" + schema + "' not found in main connection"
            + " while pre-registering row type for table " + tableName);
      }
      return registerTemporaryTable(tierSchema, tableName, rowType, databaseName);
    } else {
      return registerTemporaryTable(rootSchema, tableName, rowType, databaseName);
    }
  }
}
