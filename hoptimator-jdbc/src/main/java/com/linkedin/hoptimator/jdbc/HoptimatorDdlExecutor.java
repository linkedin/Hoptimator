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
import com.linkedin.hoptimator.Trigger;
import com.linkedin.hoptimator.UserJob;
import com.linkedin.hoptimator.View;
import com.linkedin.hoptimator.jdbc.ddl.HoptimatorDdlParserImpl;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateMaterializedView;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateTrigger;
import com.linkedin.hoptimator.util.ArrayTable;
import com.linkedin.hoptimator.util.DeploymentService;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcSchema;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcTable;
import com.linkedin.hoptimator.util.planner.PipelineRel;
import java.io.Reader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.ContextSqlValidator;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.server.ServerDdlExecutor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlDropMaterializedView;
import org.apache.calcite.sql.ddl.SqlDropObject;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlDropView;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.Pair;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;


public final class HoptimatorDdlExecutor extends ServerDdlExecutor {

  private final HoptimatorConnection connection;
  private final HoptimatorConnection.HoptimatorConnectionDualLogger logger;

  public HoptimatorDdlExecutor(HoptimatorConnection connection) {
    try {
      if (connection.isReadOnly()) {
        throw new DdlException("Cannot execute DDL in read-only mode");
      }
    } catch (SQLException e) {
      throw new DdlException("Failed to check read-only mode", e);
    }
    this.connection = connection;
    this.logger = connection.getLogger(HoptimatorDdlExecutor.class);
  }

  public static final SqlParserImplFactory PARSER_FACTORY = new SqlParserImplFactory() {
    @Override
    public SqlAbstractParserImpl getParser(Reader stream) {
      SqlAbstractParserImpl parser = HoptimatorDdlParserImpl.FACTORY.getParser(stream);
      parser.setConformance(SqlConformanceEnum.BABEL);
      return parser;
    }

    @Override
    public DdlExecutor getDdlExecutor() {
      return HoptimatorDdlExecutor.INSTANCE;
    }
  };

  // N.B. originally copy-pasted from Apache Calcite

  /** Executes a {@code CREATE VIEW} command. */
  @Override
  public void execute(SqlCreateView create, CalcitePrepare.Context context) {
    logger.info("Validating statement: {}", create);
    try {
      ValidationService.validateOrThrow(create);
    } catch (SQLException e) {
      throw new DdlException(create, e.getMessage(), e);
    }

    final Pair<CalciteSchema, String> pair = HoptimatorDdlUtils.schema(context, true, create.name);
    if (pair.left == null) {
      throw new DdlException(create, "Schema for " + create.name + " not found.");
    }
    final SchemaPlus schemaPlus = pair.left.plus();
    if (schemaPlus.tables().get(pair.right) instanceof HoptimatorJdbcTable) {
      throw new DdlException(create,
          "Cannot overwrite physical table " + pair.right + " with a view.");
    }
    for (Function function : schemaPlus.getFunctions(pair.right)) {
      if (function.getParameters().isEmpty()) {
        if (!create.getReplace()) {
          throw new DdlException(create,
              "View " + pair.right + " already exists. Use CREATE OR REPLACE to update.");
        }
        pair.left.removeFunction(pair.right);
      }
    }

    final SqlNode q = HoptimatorDdlUtils.renameColumns(create.columnList, create.query);
    final String sql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    final List<String> schemaPath = pair.left.path(null);
    String viewName = pair.right;
    List<String> viewPath = new ArrayList<>(schemaPath);
    viewPath.add(viewName);
    ViewTable viewTable = HoptimatorDdlUtils.viewTable(context, sql, new HoptimatorDriver.Prepare(connection), schemaPath, viewPath);
    View view = new View(viewPath, sql);
    logger.info("Validated sql statement. The view is named {} and has path {}",
        viewName, viewPath);

    Collection<Deployer> deployers = null;
    try {
      logger.info("Validating deployable resources for view {}", viewName);
      ValidationService.validateOrThrow(viewTable);
      deployers = DeploymentService.deployers(view, connection);
      ValidationService.validateOrThrow(deployers);
      logger.info("Validated view {}", viewName);
      if (create.getReplace()) {
        logger.info("Deploying update view {}", viewName);
        DeploymentService.update(deployers);
      } else {
        logger.info("Deploying create view {}", viewName);
        DeploymentService.create(deployers);
      }
      logger.info("Deployed view {}", viewName);
      schemaPlus.add(viewName, viewTable);
      logger.info("Added view {} to schema {}", viewName, schemaPlus.getName());
    } catch (SQLException | RuntimeException e) {
      logger.info("Failed to deploy view {}", viewName);
      if (deployers != null) {
        DeploymentService.restore(deployers);
        logger.info("Restored deployable resources for view {}", viewName);
      }
      throw new DdlException(create, e.getMessage(), e);
    }
    logger.info("CREATE VIEW {} completed", viewName);
  }

  // N.B. originally copy-pasted from Apache Calcite

  /** Executes a {@code CREATE MATERIALIZED VIEW} command. */
  public void execute(SqlCreateMaterializedView create, CalcitePrepare.Context context) {
    logger.info("Validating statement: {}", create);
    try {
      ValidationService.validateOrThrow(create);
    } catch (SQLException e) {
      throw new DdlException(create, e.getMessage(), e);
    }

    final Pair<CalciteSchema, String> pair = HoptimatorDdlUtils.schema(context, true, create.name);
    if (pair.left == null) {
      throw new DdlException(create, "Schema for " + create.name + " not found.");
    }
    final SchemaPlus schemaPlus = pair.left.plus();
    if (schemaPlus.tables().get(pair.right) != null) {
      if (schemaPlus.tables().get(pair.right) instanceof HoptimatorJdbcTable) {
        throw new DdlException(create,
            "Cannot overwrite physical table " + pair.right + " with a view.");
      }
      // Materialized view exists.
      if (!create.ifNotExists && !create.getReplace()) {
        // They did not specify IF NOT EXISTS, so give error.
        throw new DdlException(create,
            "View " + pair.right + " already exists. Use CREATE OR REPLACE to update.");
      }
      if (create.getReplace()) {
        schemaPlus.removeTable(pair.right);
      } else {
        // nothing to do
        return;
      }
    }

    final SqlNode q = HoptimatorDdlUtils.renameColumns(create.columnList, create.query);
    final String sql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    final List<String> schemaPath = pair.left.path(null);

    Collection<Deployer> deployers = null;

    String schemaName = schemaPlus.getName();
    String viewName = pair.right;
    List<String> viewPath = new ArrayList<>(schemaPath);
    viewPath.add(viewName);

    Pair<SchemaPlus, Table> schemaSnapshot = null;
    if (!(pair.left.schema instanceof Database)) {
      throw new DdlException(create, schemaName + " is not a physical database.");
    }
    String database = ((Database) pair.left.schema).databaseName();
    try {
      logger.info("Validated sql statement. The view is named {} and has path {}",
          viewName, viewPath);

      // Support "partial views", i.e. CREATE VIEW FOO$BAR, where the view name
      // is "foo-bar" and the sink is just FOO.
      String[] viewParts = viewName.split("\\$", 2);
      String sinkName = viewParts[0];
      String pipelineName = database + "-" + sinkName;
      if (viewParts.length > 1) {
        pipelineName = pipelineName + "-" + viewParts[1];
      }
      logger.info("Pipeline name for view {} is {}", viewName, pipelineName);
      Properties connectionProperties = connection.connectionProperties();
      connectionProperties.setProperty(DeploymentService.PIPELINE_OPTION, pipelineName);

      // Plan a pipeline to materialize the view.
      RelRoot root = new HoptimatorDriver.Prepare(connection).convert(context, sql).root;
      PipelineRel.Implementor plan = DeploymentService.plan(root, connection.materializations(), connectionProperties);
      schemaSnapshot = HoptimatorDdlUtils.snapshotAndSetSinkSchema(context, new HoptimatorDriver.Prepare(connection), plan, sql, pair);
      logger.info("Added materialized view {} to schema {}", viewName, schemaPlus.getName());
      Pipeline pipeline = plan.pipeline(viewName, connection);
      MaterializedView hook = new MaterializedView(database, viewPath, sql, pipeline.job().sql(), pipeline);
      // TODO support CREATE ... WITH (options...)
      logger.info("Validating materialized view {}", viewName);
      ValidationService.validateOrThrow(hook);
      deployers = DeploymentService.deployers(hook, connection);
      logger.info("Validating deployable resources for materialized view {}", viewName);
      ValidationService.validateOrThrow(deployers);
      logger.info("Validated materialized view {}", viewName);
      if (create.getReplace()) {
        logger.info("Deploying update materialized view {}", viewName);
        DeploymentService.update(deployers);
      } else {
        logger.info("Deploying create materialized view {}", viewName);
        DeploymentService.create(deployers);
      }
      logger.info("Deployed materialized view {}", viewName);
    } catch (SQLException | RuntimeException e) {
      logger.info("Failed to deploy materialized view {}", viewName);
      if (deployers != null) {
        DeploymentService.restore(deployers);
        logger.info("Restored deployable resources for materialized view {}", viewName);
      }
      if (schemaSnapshot != null) {
        if (schemaSnapshot.right == null) {
          schemaSnapshot.left.removeTable(viewName);
          logger.info("Removed schema for materialized view {}", viewName);
        } else {
          schemaPlus.add(viewName, schemaSnapshot.right);
          logger.info("Restored schema for materialized view {}", viewName);
        }
      }
      throw new DdlException(create, e.getMessage(), e);
    }
    logger.info("CREATE MATERIALIZED VIEW {} completed", viewName);
  }

  /** Executes a {@code CREATE TRIGGER} command. */
  public void execute(SqlCreateTrigger create, CalcitePrepare.Context context) {
    logger.info("Validating statement: {}", create);
    try {
      ValidationService.validateOrThrow(create);
    } catch (SQLException e) {
      throw new DdlException(create, e.getMessage(), e);
    }

    if (create.name.names.size() > 1) {
      throw new DdlException(create, "Triggers cannot belong to a schema or database.");
    }
    String name = create.name.names.get(0);

    Pair<CalciteSchema, String> pair = HoptimatorDdlUtils.schema(context, true, create.target);
    if (pair.left == null) {
      throw new DdlException(create, "Schema for " + create.target + " not found.");
    }
    SchemaPlus schemaPlus = pair.left.plus();
    Table target = schemaPlus.tables().get(pair.right);
    if (target == null) {
      throw new DdlException(create, "View/table " + create.target + " not found.");
    }

    List<String> targetSchemaPath = pair.left.path(null);
    String targetName = pair.right;
    List<String> targetPath = new ArrayList<>(targetSchemaPath);
    targetPath.add(targetName);

    Map<String, String> options = HoptimatorDdlUtils.options(create.options);

    String jobName = ((SqlLiteral) create.job).getValueAs(String.class);
    String jobNamespace = create.namespace != null
        ? ((SqlLiteral) create.namespace).getValueAs(String.class) : null;
    String cronSchedule = create.cron != null
        ? ((SqlLiteral) create.cron).getValueAs(String.class) : null;
    UserJob job = new UserJob(jobNamespace, jobName);
    Trigger trigger = new Trigger(name, job, targetPath, cronSchedule, options);

    Collection<Deployer> deployers = null;
    try {
      logger.info("Validating trigger {} with deployers", name);
      ValidationService.validateOrThrow(trigger);
      deployers = DeploymentService.deployers(trigger, connection);
      ValidationService.validateOrThrow(deployers);
      logger.info("Validated trigger {}", name);
      if (create.getReplace()) {
        logger.info("Updating trigger {}", name);
        DeploymentService.update(deployers);
      } else {
        logger.info("Creating trigger {}", name);
        DeploymentService.create(deployers);
      }
      logger.info("Deployed trigger {}", name);
      logger.info("CREATE TRIGGER {} completed", name);
    } catch (Exception e) {
      if (deployers != null) {
        DeploymentService.restore(deployers);
      }
      throw new DdlException(create, e.getMessage(), e);
    }
  }

  // N.B. originally copy-pasted from Apache Calcite

  /** Executes a {@code CREATE TABLE} command. */
  public void execute(SqlCreateTable create, CalcitePrepare.Context context) {
    logger.info("Validating statement: {}", create);
    try {
      ValidationService.validateOrThrow(create);
    } catch (SQLException e) {
      throw new DdlException(create, e.getMessage(), e);
    }

    final Pair<CalciteSchema, String> pair = HoptimatorDdlUtils.schema(context, true, create.name);
    if (pair.left == null) {
      throw new DdlException(create, "Schema for " + create.name + " not found.");
    }

    // TODO: Add support for populating new tables from a query as a one-time operation.
    if (create.query != null) {
      throw new DdlException(create, "Populating new tables is not currently supported.");
    }
    if (create.columnList == null) {
      throw new DdlException(create, "No columns provided.");
    }

    final SchemaPlus schemaPlus = pair.left.plus();
    final String tableName = pair.right;
    if (schemaPlus.tables().get(tableName) != null) {
      if (!create.ifNotExists && !create.getReplace()) {
        // They did not specify IF NOT EXISTS, so give error.
        throw new DdlException(create,
            "Table " + tableName + " already exists. Use CREATE OR REPLACE to update.");
      }
    }

    Collection<Deployer> deployers = null;
    Pair<SchemaPlus, Table> schemaSnapshot = null;
    try {
      String database;
      if (pair.left.schema instanceof Database) {
        database = ((Database) pair.left.schema).databaseName();
      } else {
        database = connection.getSchema();
      }

      final JavaTypeFactory typeFactory = context.getTypeFactory();
      final ImmutableList.Builder<ColumnDef> columnDefBuilder = ImmutableList.builder();
      final RelDataTypeFactory.Builder relBuilder = typeFactory.builder();
      final SqlValidator validator = new ContextSqlValidator(context, true);
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
            @Override public ColumnStrategy generationStrategy(RelOptTable table,
                int iColumn) {
              return columns.get(iColumn).strategy;
            }

            @Override public RexNode newColumnDefaultValue(RelOptTable table,
                int iColumn, InitializerContext context) {
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
      // Table does not exist. Create it.
      Table currentViewTable = schemaPlus.tables().get(tableName);
      schemaSnapshot = Pair.of(schemaPlus, currentViewTable);

      // Add a temporary table with the correct row type so deployers can resolve the schema
      // TODO: This may cause problems if we reuse connections, only the next connection will load this as a HoptimatorJdbcTable.
      Table tempTable = new TemporaryTable(rowType, ief, database);
      schemaPlus.add(tableName, tempTable);
      logger.info("Added table {} to schema {}", tableName, schemaPlus.getName());

      final List<String> schemaPath = pair.left.path(null);
      List<String> tablePath = new ArrayList<>(schemaPath);
      tablePath.add(tableName);

      Source source = new Source(database, tablePath, Collections.emptyMap());
      logger.info("Validating new table {}", source);
      ValidationService.validateOrThrow(source);
      deployers = DeploymentService.deployers(source, connection);
      logger.info("Validating deployable resources for table {}", tableName);
      ValidationService.validateOrThrow(deployers);
      if (create.getReplace()) {
        logger.info("Deploying update table {}", source);
        DeploymentService.update(deployers);
      } else {
        logger.info("Deploying create table {}", source);
        DeploymentService.create(deployers);
      }
      logger.info("Deployed table {}", source);
    } catch (SQLException | RuntimeException e) {
      logger.info("Failed to deploy table {}", tableName);
      if (deployers != null) {
        DeploymentService.restore(deployers);
        logger.info("Restored deployable resources for table {}", tableName);
      }
      if (schemaSnapshot != null) {
        if (schemaSnapshot.right == null) {
          schemaSnapshot.left.removeTable(tableName);
          logger.info("Removed schema for table {}", tableName);
        } else {
          schemaPlus.add(tableName, schemaSnapshot.right);
          logger.info("Restored schema for table {}", tableName);
        }
      }
      throw new DdlException(create, e.getMessage(), e);
    }
    logger.info("CREATE TABLE {} completed", tableName);
  }

  // N.B. originally copy-pasted from Apache Calcite

  /** Executes {@code DROP FUNCTION}, {@code DROP TABLE}, {@code DROP MATERIALIZED VIEW}, {@code DROP TYPE},
   * {@code DROP VIEW} commands. */
  @Override
  public void execute(SqlDropObject drop, CalcitePrepare.Context context) {
    logger.info("Validating statement: {}", drop);
    try {
      ValidationService.validateOrThrow(drop);
    } catch (SQLException e) {
      throw new DdlException(drop, e.getMessage(), e);
    }

    // The logic below is only applicable for DROP VIEW and DROP MATERIALIZED VIEW.
    if (!drop.getKind().equals(SqlKind.DROP_MATERIALIZED_VIEW)
        && !drop.getKind().equals(SqlKind.DROP_VIEW)
        && !drop.getKind().equals(SqlKind.DROP_TABLE)) {
      super.execute(drop, context);
      return;
    }

    final Pair<CalciteSchema, String> pair = HoptimatorDdlUtils.schema(context, false, drop.name);
    String tableName = pair.right;

    SchemaPlus schemaPlus = pair.left != null ? pair.left.plus() : context.getRootSchema().plus();
    Table table = schemaPlus.tables().get(tableName);
    if (table == null) {
      if (drop.ifExists) {
        return;
      }
      throw new DdlException(drop, "Element " + tableName + " not found.");
    }

    final List<String> schemaPath = pair.left.path(null);
    List<String> tablePath = new ArrayList<>(schemaPath);
    tablePath.add(tableName);

    Collection<Deployer> deployers = null;
    try {
      if (table instanceof MaterializedViewTable) {
        if (!(drop instanceof SqlDropMaterializedView)) {
          throw new DdlException(drop,
              "Element " + tableName + " is a materialized view and does not correspond to " + drop.getOperator());
        }
        MaterializedViewTable materializedViewTable = (MaterializedViewTable) table;
        View view = new View(tablePath, materializedViewTable.viewSql());
        deployers = DeploymentService.deployers(view, connection);
        logger.info("Deleting materialized view {}", tableName);
        DeploymentService.delete(deployers);
        schemaPlus.removeTable(tableName);
        logger.info("Removed materialized table {} from schema {}", tableName, schemaPlus.getName());
      } else if (table instanceof ViewTable) {
        if (!(drop instanceof SqlDropView)) {
          throw new DdlException(drop,
              "Element " + tableName + " is a view and does not correspond to " + drop.getOperator());
        }
        ViewTable viewTable = (ViewTable) table;
        View view = new View(tablePath, viewTable.getViewSql());
        deployers = DeploymentService.deployers(view, connection);
        logger.info("Deleting view {}", tableName);
        DeploymentService.delete(deployers);
        schemaPlus.removeTable(tableName);
        logger.info("Removed view {} from schema {}", tableName, schemaPlus.getName());
      } else if (table instanceof HoptimatorJdbcTable || table instanceof TemporaryTable) {
        if (!(drop instanceof SqlDropTable)) {
          throw new DdlException(drop,
              "Element " + tableName + " is a table and does not correspond to " + drop.getOperator());
        }
        Source source;
        if (table instanceof HoptimatorJdbcTable) {
          HoptimatorJdbcTable jdbcTable = (HoptimatorJdbcTable) table;
          HoptimatorJdbcSchema jdbcSchema = (HoptimatorJdbcSchema) jdbcTable.jdbcTable().jdbcSchema;
          source = new Source(jdbcSchema.databaseName(), tablePath, Collections.emptyMap());
        } else {
          TemporaryTable temporaryTable = (TemporaryTable) table;
          source = new Source(temporaryTable.databaseName(), tablePath, Collections.emptyMap());
        }
        deployers = DeploymentService.deployers(source, connection);
        logger.info("Deleting table {}", tableName);
        DeploymentService.delete(deployers);
        schemaPlus.removeTable(tableName);
        logger.info("Removed table {} from schema {}", tableName, schemaPlus.getName());
      } else {
        throw new SQLException(String.format("Unsupported drop type %s.", table.getClass().getSimpleName()));
      }
    } catch (SQLException | RuntimeException e) {
      logger.info("Failed to delete element {}", tableName);
      if (deployers != null) {
        DeploymentService.restore(deployers);
        logger.info("Restored deployable resources for element {}", tableName);
      }
      throw new DdlException(drop, e.getMessage(), e);
    }
    logger.info("{} completed", drop, tableName);
  }

  /** Unchecked exception related to a DDL statement. */
  static public class DdlException extends RuntimeException {

    private DdlException(SqlNode node, SqlParserPos pos, String msg, Throwable cause) {
      super("Cannot " + node.toString() + " at line " + pos.getLineNum() + " col " + pos.getColumnNum()
          + ": " + msg, cause);
    }

    DdlException(String msg, Throwable cause) {
      super(msg, cause);
    }

    DdlException(SqlNode node, String msg, Throwable cause) {
      this(node, node.getParserPosition(), msg, cause);
    }

    DdlException(SqlNode node, String msg) {
      this(node, msg, null);
    }

    DdlException(String msg) {
      this(msg, null);
    }
  }

  private final static class ColumnDef {
    final @Nullable SqlNode expr;
    final RelDataType type;
    final ColumnStrategy strategy;

    private ColumnDef(@Nullable SqlNode expr, RelDataType type,
        ColumnStrategy strategy) {
      this.expr = expr;
      this.type = type;
      this.strategy = requireNonNull(strategy, "strategy");
      checkArgument(
          strategy == ColumnStrategy.NULLABLE
              || strategy == ColumnStrategy.NOT_NULLABLE
              || expr != null);
    }

    static ColumnDef of(@Nullable SqlNode expr, RelDataType type,
        ColumnStrategy strategy) {
      return new ColumnDef(expr, type, strategy);
    }
  }

  /**
   * Temporary table implementation used during CREATE TABLE to provide row type information
   * to deployers before the actual table exists in the underlying database.
   */
  private static class TemporaryTable extends ArrayTable<Object[]> {
    private final InitializerExpressionFactory initializerExpressionFactory;
    private final String databaseName;

    TemporaryTable(RelDataType rowType, InitializerExpressionFactory initializerExpressionFactory, String databaseName) {
      super(Object[].class, rowType);
      this.initializerExpressionFactory = initializerExpressionFactory;
      this.databaseName = databaseName;
    }

    @Override public <C extends Object> @Nullable C unwrap(Class<C> aClass) {
      if (aClass.isInstance(initializerExpressionFactory)) {
        return aClass.cast(initializerExpressionFactory);
      }
      return super.unwrap(aClass);
    }

    String databaseName() {
      return databaseName;
    }
  }
}
