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

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.MaterializedView;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.View;
import com.linkedin.hoptimator.util.DeploymentService;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcTable;
import com.linkedin.hoptimator.util.planner.PipelineRel;
import java.io.Reader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.server.ServerDdlExecutor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlDropObject;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.Pair;


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
      SqlAbstractParserImpl parser = SqlDdlParserImpl.FACTORY.getParser(stream);
      parser.setConformance(SqlConformanceEnum.BABEL);
      return parser;
    }

    @Override
    public DdlExecutor getDdlExecutor() {
      return HoptimatorDdlExecutor.INSTANCE;
    }
  };

  // N.B. copy-pasted from Apache Calcite

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
    List<String> schemaPath = pair.left.path(null);
    String viewName = pair.right;
    List<String> viewPath = new ArrayList<>(schemaPath);
    viewPath.add(viewName);
    ViewTable viewTable = HoptimatorDdlUtils.viewTable(context, sql, new HoptimatorDriver.Prepare(connection), schemaPath, viewPath);
    View view = new View(viewPath, sql);
    logger.info("Validated sql statement. The view is named {} and has path {}",
        viewName, viewPath);

    Collection<Deployer> deployers = null;
    try {
      logger.info("Validating view {} with deployers", viewName);
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
      logger.info("CREATE VIEW {} completed", viewName);
    } catch (Exception e) {
      if (deployers != null) {
        DeploymentService.restore(deployers);
      }
      throw new DdlException(create, e.getMessage(), e);
    }
  }

  // N.B. copy-pasted from Apache Calcite

  /** Executes a {@code CREATE MATERIALIZED VIEW} command. */
  @Override
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
    try {
      if (!(pair.left.schema instanceof Database)) {
        throw new DdlException(create, schemaName + " is not a physical database.");
      }
      String database = ((Database) pair.left.schema).databaseName();

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
      logger.info("Added view {} to schema {}", viewName, schemaPlus.getName());
      Pipeline pipeline = plan.pipeline(viewName, connection);
      MaterializedView hook = new MaterializedView(database, viewPath, sql, pipeline.job().sql(), pipeline);
      // TODO support CREATE ... WITH (options...)
      ValidationService.validateOrThrow(hook);
      deployers = DeploymentService.deployers(hook, connection);
      logger.info("Validating view {} with deployers", viewName);
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
    } catch (SQLException | RuntimeException e) {
      if (deployers != null) {
        DeploymentService.restore(deployers);
      }
      if (schemaSnapshot != null) {
        if (schemaSnapshot.right == null) {
          schemaSnapshot.left.removeTable(viewName);
        } else {
          schemaPlus.add(viewName, schemaSnapshot.right);
        }
      }
      throw new DdlException(create, e.getMessage(), e);
    }
    logger.info("CREATE MATERIALIZED VIEW {} completed", viewName);
  }

  // N.B. largely copy-pasted from Apache Calcite

  /** Executes {@code DROP FUNCTION}, {@code DROP TABLE}, {@code DROP MATERIALIZED VIEW}, {@code DROP TYPE},
   * {@code DROP VIEW} commands. */
  @Override
  public void execute(SqlDropObject drop, CalcitePrepare.Context context) {
    // The logic below is only applicable for DROP VIEW and DROP MATERIALIZED VIEW.
    if (!drop.getKind().equals(SqlKind.DROP_MATERIALIZED_VIEW) && !drop.getKind().equals(SqlKind.DROP_VIEW)) {
      super.execute(drop, context);
      return;
    }

    final Pair<CalciteSchema, String> pair = HoptimatorDdlUtils.schema(context, false, drop.name);
    String viewName = pair.right;

    SchemaPlus schemaPlus = pair.left != null ? pair.left.plus() : context.getRootSchema().plus();
    Table table = schemaPlus.tables().get(viewName);
    if (table == null) {
      if (drop.ifExists) {
        return;
      }
      throw new DdlException(drop, "View " + viewName + " not found.");
    }

    final List<String> schemaPath = pair.left.path(null);
    List<String> viewPath = new ArrayList<>(schemaPath);
    viewPath.add(viewName);

    Collection<Deployer> deployers = null;
    try {
      if (table instanceof MaterializedViewTable) {
        MaterializedViewTable materializedViewTable = (MaterializedViewTable) table;
        View view = new View(viewPath, materializedViewTable.viewSql());
        deployers = DeploymentService.deployers(view, connection);
        DeploymentService.delete(deployers);
      } else if (table instanceof ViewTable) {
        ViewTable viewTable = (ViewTable) table;
        View view = new View(viewPath, viewTable.getViewSql());
        deployers = DeploymentService.deployers(view, connection);
        DeploymentService.delete(deployers);
      } else {
        throw new DdlException(drop, viewName + " is not a view.");
      }
    } catch (Exception e) {
      if (deployers != null) {
        DeploymentService.restore(deployers);
      }
      throw new DdlException(drop, e.getMessage(), e);
    }
    schemaPlus.removeTable(viewName);
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
}
