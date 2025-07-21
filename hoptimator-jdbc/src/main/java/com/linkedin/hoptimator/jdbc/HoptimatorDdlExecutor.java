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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.server.ServerDdlExecutor;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlDropObject;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;


public final class HoptimatorDdlExecutor extends ServerDdlExecutor {

  private final HoptimatorConnection connection;
  private final Properties connectionProperties;

  public HoptimatorDdlExecutor(HoptimatorConnection connection) {
    this.connection = connection;
    this.connectionProperties = connection.connectionProperties();
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
    try {
      ValidationService.validateOrThrow(create);
    } catch (SQLException e) {
      throw new DdlException(create, e.getMessage(), e);
    }

    final Pair<CalciteSchema, String> pair = schema(context, true, create.name);
    if (pair.left == null) {
      throw new DdlException(create, "Schema for " + create.name + " not found.");
    }
    final SchemaPlus schemaPlus = pair.left.plus();
    if (schemaPlus.getTable(pair.right) instanceof HoptimatorJdbcTable) {
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

    final SqlNode q = renameColumns(create.columnList, create.query);
    final String sql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    List<String> schemaPath = pair.left.path(null);
    String viewName = pair.right;
    List<String> viewPath = new ArrayList<>(schemaPath);
    viewPath.add(viewName);
    CalcitePrepare.AnalyzeViewResult analyzed = HoptimatorDriver.analyzeView(connection, sql);
    RelProtoDataType protoType = RelDataTypeImpl.proto(analyzed.rowType);
    ViewTable viewTable = new ViewTable(Object.class, protoType, sql, schemaPath, viewPath);
    View view = new View(viewPath, sql);

    Collection<Deployer> deployers = null;
    try {
      ValidationService.validateOrThrow(viewTable);
      deployers = DeploymentService.deployers(view, connectionProperties);
      ValidationService.validateOrThrow(deployers);
      if (create.getReplace()) {
        DeploymentService.update(deployers);
      } else {
        DeploymentService.create(deployers);
      }
      schemaPlus.add(viewName, viewTable);
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
    try {
      ValidationService.validateOrThrow(create);
    } catch (SQLException e) {
      throw new DdlException(create, e.getMessage(), e);
    }

    final Pair<CalciteSchema, String> pair = schema(context, true, create.name);
    if (pair.left == null) {
      throw new DdlException(create, "Schema for " + create.name + " not found.");
    }
    final SchemaPlus schemaPlus = pair.left.plus();
    if (schemaPlus.getTable(pair.right) != null) {
      if (schemaPlus.getTable(pair.right) instanceof HoptimatorJdbcTable) {
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

    final SqlNode q = renameColumns(create.columnList, create.query);
    final String sql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    final List<String> schemaPath = pair.left.path(null);

    Collection<Deployer> deployers = null;

    String schemaName = schemaPlus.getName();
    String viewName = pair.right;
    List<String> viewPath = new ArrayList<>(schemaPath);
    viewPath.add(viewName);

    Table currentViewTable = schemaPlus.getTable(viewName);
    try {
      if (!(pair.left.schema instanceof Database)) {
        throw new DdlException(create, schemaName + " is not a physical database.");
      }
      String database = ((Database) pair.left.schema).databaseName();

      // Table does not exist. Create it.
      RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      CalcitePrepare.AnalyzeViewResult analyzed = HoptimatorDriver.analyzeView(connection, sql);
      RelProtoDataType protoType = RelDataTypeImpl.proto(analyzed.rowType);
      ViewTable viewTable = new ViewTable(Object.class, protoType, sql, schemaPath, viewPath);
      MaterializedViewTable materializedViewTable = new MaterializedViewTable(viewTable);
      RelDataType viewRowType = materializedViewTable.getRowType(typeFactory);

      // Support "partial views", i.e. CREATE VIEW FOO$BAR, where the view name
      // is "foo-bar" and the sink is just FOO.
      String[] viewParts = viewName.split("\\$", 2);
      String sinkName = viewParts[0];
      String pipelineName = database + "-" + sinkName;
      if (viewParts.length > 1) {
        pipelineName = pipelineName + "-" + viewParts[1];
      }
      connectionProperties.setProperty(DeploymentService.PIPELINE_OPTION, pipelineName);
      List<String> sinkPath = new ArrayList<>(schemaPath);
      sinkPath.add(sinkName);
      Table sink = schemaPlus.getTable(sinkName);

      final RelDataType rowType;
      if (sink != null) {
        // For "partial views", the sink may already exist. Use the existing row type.
        rowType = sink.getRowType(typeFactory);
      } else {
        // For normal views, we create the sink based on the view row type.
        rowType = viewRowType;
      }

      // Plan a pipeline to materialize the view.
      RelRoot root = new HoptimatorDriver.Prepare(connection).convert(context, sql).root;
      PipelineRel.Implementor plan = DeploymentService.plan(root, connection.materializations(), connectionProperties);
      plan.setSink(database, sinkPath, rowType, Collections.emptyMap());

      // Need to add the view table to the connection so that the ConnectorService can find it when resolving options.
      schemaPlus.add(viewName, materializedViewTable);
      Pipeline pipeline = plan.pipeline(viewName, connectionProperties);
      MaterializedView hook = new MaterializedView(database, viewPath, sql, pipeline.job().sql(), pipeline);
      // TODO support CREATE ... WITH (options...)
      ValidationService.validateOrThrow(hook);
      deployers = DeploymentService.deployers(hook, connectionProperties);
      ValidationService.validateOrThrow(deployers);
      if (create.getReplace()) {
        DeploymentService.update(deployers);
      } else {
        DeploymentService.create(deployers);
      }
    } catch (SQLException | RuntimeException e) {
      if (deployers != null) {
        DeploymentService.restore(deployers);
      }
      if (currentViewTable == null) {
        schemaPlus.removeTable(viewName);
      } else {
        schemaPlus.add(viewName, currentViewTable);
      }
      throw new DdlException(create, e.getMessage(), e);
    }
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

    final Pair<CalciteSchema, String> pair = schema(context, false, drop.name);
    String viewName = pair.right;

    SchemaPlus schemaPlus = pair.left.plus();
    Table table = schemaPlus.getTable(viewName);
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
        deployers = DeploymentService.deployers(view, connectionProperties);
        DeploymentService.delete(deployers);
      } else if (table instanceof ViewTable) {
        ViewTable viewTable = (ViewTable) table;
        View view = new View(viewPath, viewTable.getViewSql());
        deployers = DeploymentService.deployers(view, connectionProperties);
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

  // N.B. copy-pasted from Apache Calcite

  /** Returns the schema in which to create an object. */
  static Pair<CalciteSchema, String> schema(CalcitePrepare.Context context, boolean mutable, SqlIdentifier id) {
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
      schema = Objects.requireNonNull(schema).getSubSchema(p, true);
    }
    return Pair.of(schema, name);
  }

  // N.B. copy-pasted from Apache Calcite

  /** Wraps a query to rename its columns. Used by CREATE VIEW and CREATE
   * MATERIALIZED VIEW. */
  static SqlNode renameColumns(SqlNodeList columnList, SqlNode query) {
    if (columnList == null) {
      return query;
    }
    final SqlParserPos p = query.getParserPosition();
    final SqlNodeList selectList = SqlNodeList.SINGLETON_STAR;
    final SqlCall from = SqlStdOperatorTable.AS.createCall(p,
        Arrays.asList(query, new SqlIdentifier("_", p), columnList));
    return new SqlSelect(p, null, selectList, from, null, null, null, null, null, null, null, null, null);
  }

  /** Unchecked exception related to a DDL statement. */
  static public class DdlException extends RuntimeException {

    private DdlException(SqlNode node, SqlParserPos pos, String msg, Throwable cause) {
      super("Cannot " + node.toString() + " at line " + pos.getLineNum() + " col " + pos.getColumnNum()
          + ": " + msg, cause);
    }

    DdlException(SqlNode node, String msg, Throwable cause) {
      this(node, node.getParserPosition(), msg, cause);
    }

    DdlException(SqlNode node, String msg) {
      this(node, msg, null);
    }
  }
}
