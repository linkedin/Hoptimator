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
import com.linkedin.hoptimator.DatabaseDeployable;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.MaterializedView;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateDatabase;
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
   * Connection property that controls how plain {@code CREATE} statements behave.
   *
   * <ul>
   *   <li>{@code create} (default) — strict create. {@code CREATE} fails if the resource
   *       already exists; {@code CREATE OR REPLACE} updates.</li>
   *   <li>{@code apply} — declarative, K8s-style reconciliation. Both {@code CREATE} and
   *       {@code CREATE OR REPLACE} converge the resource to the declared definition, so
   *       running the same script twice is a no-op. Idempotent by design.</li>
   *   <li>{@code validate} — dry-run. Resolves to {@link DdlMode#VALIDATE}: every statement is
   *       fully validated (deployers resolved and validated) but no real object is created,
   *       updated, or deleted. Unlike {@code !specify}, in-memory schema changes are kept, so a
   *       series of statements validates against each other. With respect to {@code OR REPLACE}
   *       it behaves like {@code apply}: an already-existing resource converges, it does not
   *       error.</li>
   * </ul>
   *
   * <p>Set per connection on the JDBC URL, e.g. {@code jdbc:hoptimator://...;mode=apply}
   * — see {@code docs/user-guide/ddl-reference.md}.
   */
  public static final String MODE_PROPERTY = "mode";

  /** Default value of {@link #MODE_PROPERTY}. */
  public static final String MODE_CREATE = "create";

  /** Apply-mode value of {@link #MODE_PROPERTY}. */
  public static final String MODE_APPLY = "apply";

  /** Validate-only (dry-run) value of {@link #MODE_PROPERTY}. Resolves to {@link DdlMode#VALIDATE}. */
  public static final String MODE_VALIDATE = "validate";

  /**
   * Resolves the effective {@link DdlMode} for a {@code CREATE} statement, combining the
   * statement's {@code OR REPLACE} flag with the connection's {@link #MODE_PROPERTY}.
   *
   * <p>In {@code create} mode (the default): {@code CREATE} → {@link DdlMode#CREATE} and
   * {@code CREATE OR REPLACE} → {@link DdlMode#UPDATE}. In {@code apply} mode: both forms
   * resolve to {@link DdlMode#UPDATE}, making CREATE idempotent. In {@code validate} mode:
   * both forms resolve to {@link DdlMode#VALIDATE}, the non-deploying dry-run (which, like
   * apply, never errors on an already-existing resource).
   */
  static DdlMode effectiveMode(boolean orReplace, HoptimatorConnection conn) {
    if (isValidateMode(conn)) {
      return DdlMode.VALIDATE;
    }
    if (isApplyMode(conn)) {
      return DdlMode.UPDATE;
    }
    return orReplace ? DdlMode.UPDATE : DdlMode.CREATE;
  }

  /** Whether the connection is configured for apply-mode DDL. */
  static boolean isApplyMode(HoptimatorConnection conn) {
    return modeIs(conn, MODE_APPLY);
  }

  /** Whether the connection is configured for validate-only (dry-run) DDL. */
  static boolean isValidateMode(HoptimatorConnection conn) {
    return modeIs(conn, MODE_VALIDATE);
  }

  private static boolean modeIs(HoptimatorConnection conn, String expected) {
    Properties props = conn.connectionProperties();
    if (props == null) {
      return false;
    }
    String mode = props.getProperty(MODE_PROPERTY, MODE_CREATE);
    return expected.equalsIgnoreCase(mode);
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
   * The four ways a {@code CREATE}/{@code DROP} statement can be carried out. Rather than have
   * callers test {@code mode == X} or reason about "mutable" schemas, each mode answers a few
   * intent questions that read directly at the call site:
   *
   * <ul>
   *   <li>{@link #executeDeployers} — the deployer action: deploy ({@code CREATE}/{@code UPDATE}),
   *       render YAML specs ({@code SPECIFY}), or do nothing ({@code VALIDATE}).</li>
   *   <li>{@link #shouldDeploy()} — does this mode make real changes to deployed resources
   *       (create/update/delete)? {@code true} for {@code CREATE}/{@code UPDATE}; {@code false}
   *       for the two dry-run modes.</li>
   *   <li>{@link #shouldRestoreSchema()} — should in-memory schema changes be discarded afterwards?
   *       {@code true} only for {@code SPECIFY} (the one-shot {@code !specify} render). The other
   *       modes keep their changes so a series of statements validates against each other.</li>
   *   <li>{@link #failsIfResourceExists()} — strict create: error if the target already exists?
   *       {@code true} only for {@code CREATE}; the rest converge on the existing resource.</li>
   * </ul>
   *
   * <p>{@code VALIDATE} (dry-run) is {@code shouldDeploy()==false} yet {@code shouldRestoreSchema()
   * ==false}: it deploys nothing but still evolves the in-memory schema, so e.g. a dry-run
   * {@code DROP VIEW} followed by a query against that view fails validation, even though no real
   * {@code View} object was deleted.
   *
   * <p><b>Why two non-deploying modes (VALIDATE keeps schema changes, SPECIFY restores) instead of
   * one?</b> It is tempting to unify them and have the {@code !specify} caller own the restore —
   * e.g. by snapshotting the schema and discarding changes, or by running against a throwaway
   * mutable root. Calcite does not make that clean:
   * <ul>
   *   <li>{@code Schema.snapshot()} is a read-consistency <i>view</i>: it shares the underlying
   *       {@code tableMap}, so mutating it writes through to the live schema — not a rollback.</li>
   *   <li>A throwaway mutable root cannot be empty (DDL navigates into existing schemas and
   *       resolves source tables), and Calcite has no copy-on-write schema. A read-through
   *       overlay wrapping the backing {@code Schema} loses session-local additions (an earlier
   *       {@code CREATE VIEW} lives in {@code CalciteSchema.tableMap}, not the backing schema),
   *       and copying full schema state forces lazy/remote table enumeration.</li>
   * </ul>
   * The robust mechanism is the <i>targeted</i> per-statement snapshot the {@code processCreate*}
   * methods already compute (they know the exact table/view/sink added). So we keep the snapshot
   * there and let {@link #shouldRestoreSchema()} decide whether to discard it.
   */
  enum DdlMode {
    CREATE {
      @Override
      List<String> executeDeployers(Collection<Deployer> deployers, Connection conn) throws SQLException {
        DeploymentService.create(deployers);
        return Collections.emptyList();
      }

      @Override
      boolean shouldDeploy() {
        return true;
      }

      @Override
      boolean shouldRestoreSchema() {
        return false;
      }

      @Override
      boolean failsIfResourceExists() {
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
      boolean shouldDeploy() {
        return true;
      }

      @Override
      boolean shouldRestoreSchema() {
        return false;
      }

      @Override
      boolean failsIfResourceExists() {
        return false;
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
      boolean shouldDeploy() {
        return false;
      }

      @Override
      boolean shouldRestoreSchema() {
        return true;
      }

      @Override
      boolean failsIfResourceExists() {
        return false;
      }
    },
    VALIDATE {
      @Override
      List<String> executeDeployers(Collection<Deployer> deployers, Connection conn) {
        // Dry-run: validation has already happened by the time we get here. Deploy nothing and
        // render nothing. The in-memory schema changes are kept (shouldRestoreSchema()==false)
        // so a series of statements validates against each other.
        return Collections.emptyList();
      }

      @Override
      boolean shouldDeploy() {
        return false;
      }

      @Override
      boolean shouldRestoreSchema() {
        return false;
      }

      @Override
      boolean failsIfResourceExists() {
        return false;
      }
    };

    /** Runs the deployers per this mode: create, update, render specs, or nothing. */
    abstract List<String> executeDeployers(Collection<Deployer> deployers, Connection conn) throws SQLException;

    /** Whether this mode makes real changes to deployed resources (create/update/delete). */
    abstract boolean shouldDeploy();

    /** Whether in-memory schema changes should be discarded after the statement (the {@code !specify} render). */
    abstract boolean shouldRestoreSchema();

    /** Whether a plain {@code CREATE} over an already-existing resource is an error (strict create). */
    abstract boolean failsIfResourceExists();
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
    ValidationService.validateOrThrow(create, conn);

    // Extract query SQL (rename columns if a column list was provided) and plan the query.
    // This is done first — before schema/conflict checks — so that:
    //   1. sinkRowType is always available, even for IF NOT EXISTS early returns.
    //   2. root is computed once and reused for pipeline planning below.
    final SqlNode q = renameColumns(create.columnList, create.query);
    final String sql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    RelRoot root = prepare.convert(ctx, sql).root;
    RelDataType sinkRowType = root.rel.getRowType();

    // Navigate to the schema. !specify (SPECIFY) works against the read-only root and restores;
    // every other mode keeps its changes, so it uses the mutable root.
    final Pair<CalciteSchema, String> pair = schema(ctx, !mode.shouldRestoreSchema(), create.name);
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
      // Strict CREATE is the only mode that errors on an existing resource; UPDATE (apply mode /
      // OR REPLACE), VALIDATE (dry-run), and SPECIFY (render) all converge on it.
      boolean replaceExisting;
      if (mode.failsIfResourceExists()) {
        if (!create.ifNotExists) {
          throw new SQLException(
              "View " + pair.right + " already exists. Use CREATE OR REPLACE to update.");
        }
        replaceExisting = false;
      } else {
        replaceExisting = true;
      }
      if (replaceExisting) {
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
      ValidationService.validateOrThrow(hook, conn);
      deployers = DeploymentService.deployers(hook, conn);
      logger.info("Validating deployable resources for materialized view {}", viewName);
      ValidationService.validateOrThrow(deployers, conn);
      logger.info("Validated materialized view {}", viewName);

      // Execute (create/update), render specs (specify), or do nothing (validate dry-run).
      if (mode == DdlMode.UPDATE) {
        logger.info("Deploying update materialized view {}", viewName);
      } else if (mode == DdlMode.CREATE) {
        logger.info("Deploying create materialized view {}", viewName);
      } else if (mode == DdlMode.VALIDATE) {
        logger.info("Validating (dry-run) materialized view {}", viewName);
      } else {
        logger.info("Specifying materialized view {}", viewName);
      }
      List<String> specs = mode.executeDeployers(deployers, conn);
      if (mode.shouldRestoreSchema()) {
        // Render-only (!specify): roll back any side effects deployers made while producing specs.
        DeploymentService.restore(deployers);
      } else if (mode.shouldDeploy()) {
        logger.info("Deployed materialized view {}", viewName);
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
      // Restore the in-memory schema only when the mode wants it discarded (SPECIFY render) or the
      // statement failed (undo the partial mutation). On success, CREATE/UPDATE/VALIDATE all keep
      // the view in-memory so subsequent statements see it — VALIDATE does so without deploying.
      if (!success || mode.shouldRestoreSchema()) {
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
    ValidationService.validateOrThrow(create, conn);

    // TODO: Add support for populating new tables from a query as a one-time operation.
    if (create.query != null) {
      throw new SQLException("Populating new tables is not currently supported.");
    }
    if (create.columnList == null) {
      throw new SQLException("No columns provided.");
    }

    boolean isNewSchema = false;
    // !specify (SPECIFY) navigates the read-only root and restores; other modes keep changes.
    Pair<CalciteSchema, String> pair = schema(ctx, !mode.shouldRestoreSchema(), create.name);
    if (pair.left == null) {
      // If the schema is not found, it might be because it's a 3-level path (CATALOG.SCHEMA.TABLE)
      if (create.name.names.size() > 2) {
        pair = catalog(ctx, !mode.shouldRestoreSchema(), create.name);
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
      // Strict CREATE is the only mode that errors on an existing table; UPDATE (apply mode /
      // OR REPLACE), VALIDATE (dry-run) and SPECIFY (render) all converge on it.
      boolean wouldFail = mode.failsIfResourceExists() && !create.ifNotExists;
      if (wouldFail) {
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
      ValidationService.validateOrThrow(source, conn);
      deployers = DeploymentService.deployers(source, conn);
      logger.info("Validating deployable resources for table {}", tableName);
      ValidationService.validateOrThrow(deployers, conn);

      if (mode == DdlMode.UPDATE) {
        logger.info("Deploying update table {}", source);
      } else if (mode == DdlMode.CREATE) {
        logger.info("Deploying create table {}", source);
      } else if (mode == DdlMode.VALIDATE) {
        logger.info("Validating (dry-run) table {}", source);
      } else {
        logger.info("Specifying table {}", source);
      }
      List<String> specs = mode.executeDeployers(deployers, conn);
      if (mode.shouldRestoreSchema()) {
        // Render-only (!specify): roll back any side effects deployers made while producing specs.
        DeploymentService.restore(deployers);
      } else if (mode.shouldDeploy()) {
        logger.info("Deployed table {}", source);
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
      // Restore the in-memory schema only when the mode wants it discarded (SPECIFY render) or the
      // statement failed. On success, CREATE/UPDATE/VALIDATE keep the table in-memory so subsequent
      // statements validate against it — VALIDATE does so without deploying.
      if (!success || mode.shouldRestoreSchema()) {
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
   * Shared implementation of the {@code CREATE DATABASE} pipeline for both real deployment
   * and dry-run (SPECIFY) modes.
   *
   * @param conn   the JDBC connection
   * @param create the parsed DDL node
   * @param mode   whether to CREATE, UPDATE, or SPECIFY
   * @return a SpecifyResult (specs are empty for CREATE/UPDATE, YAML for SPECIFY)
   * @throws SQLException on validation or deployment errors
   */
  static SpecifyResult processCreateDatabase(HoptimatorConnection conn,
      SqlCreateDatabase create, DdlMode mode) throws SQLException {
    HoptimatorConnection.HoptimatorConnectionDualLogger logger = conn.getLogger(HoptimatorDdlUtils.class);

    logger.info("Validating statement: {}", create);
    ValidationService.validateOrThrow(create, conn);

    if (create.name.names.size() > 1) {
      throw new SQLException("Database names cannot be compound identifiers.");
    }
    String name = create.name.names.get(0);

    Map<String, String> dbOptions = options(create.options);
    DatabaseDeployable database = new DatabaseDeployable(name, dbOptions);

    Collection<Deployer> deployers = null;
    try {
      logger.info("Validating database {}", name);
      ValidationService.validateOrThrow(database, conn);
      deployers = DeploymentService.deployers(database, conn);
      ValidationService.validateOrThrow(deployers, conn);

      List<String> specs = mode.executeDeployers(deployers, conn);
      if (mode.shouldRestoreSchema()) {
        // Render-only (!specify): roll back any side effects deployers made while producing specs.
        DeploymentService.restore(deployers);
      } else if (mode.shouldDeploy()) {
        logger.info("Deployed database {}", name);
      }
      return new SpecifyResult(specs, null, Collections.singletonList(name));
    } catch (SQLException | RuntimeException e) {
      logger.info("Failed to deploy database {}", name);
      if (deployers != null) {
        DeploymentService.restore(deployers);
        logger.info("Restored deployable resources for database {}", name);
      }
      throw e;
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

    if (sqlNode instanceof SqlCreateDatabase) {
      return processCreateDatabase(conn, (SqlCreateDatabase) sqlNode, DdlMode.SPECIFY);
    }

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

  /**
   * Removes {@code tableName} from the tier schema identified by {@code (catalog, schema)}, the
   * inverse of {@link #registerTemporaryTableInSchema}. Silent no-op if the catalog, schema or
   * table entry does not exist (e.g. the connection was opened after the table was already gone)
   * — remove must never fail a user-visible DROP that has already succeeded at the backend.
   */
  public static void removeTableFromSchema(HoptimatorConnection conn,
      @Nullable String catalog, @Nullable String schema, String tableName) {
    if (conn == null) {
      return;
    }
    SchemaPlus rootSchema = conn.calciteConnection().getRootSchema();
    SchemaPlus target;
    if (catalog != null) {
      SchemaPlus catalogSchemaPlus = rootSchema.subSchemas().get(catalog);
      if (catalogSchemaPlus == null) {
        return;
      }
      if (schema == null) {
        target = catalogSchemaPlus;
      } else {
        target = catalogSchemaPlus.subSchemas().get(schema);
      }
    } else if (schema != null) {
      target = rootSchema.subSchemas().get(schema);
    } else {
      target = rootSchema;
    }
    if (target != null && target.tables().get(tableName) != null) {
      target.removeTable(tableName);
    }
  }
}
