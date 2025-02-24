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

import java.io.Reader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.server.ServerDdlExecutor;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
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

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.MaterializedView;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.jdbc.schema.HoptimatorViewTableMacro;
import com.linkedin.hoptimator.util.DeploymentService;
import com.linkedin.hoptimator.util.planner.PipelineRel;

import static org.apache.calcite.util.Static.RESOURCE;


public final class HoptimatorDdlExecutor extends ServerDdlExecutor {

  private final Properties connectionProperties;

  public HoptimatorDdlExecutor(Properties connectionProperties) {
    this.connectionProperties = connectionProperties;
  }

  @SuppressWarnings("unused") // used via reflection
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
    final Pair<CalciteSchema, String> pair = schema(context, true, create.name);
    final SchemaPlus schemaPlus = pair.left.plus();
    for (Function function : schemaPlus.getFunctions(pair.right)) {
      if (function.getParameters().isEmpty()) {
        if (!create.getReplace()) {
          throw SqlUtil.newContextException(create.name.getParserPosition(), RESOURCE.viewExists(pair.right));
        }
        pair.left.removeFunction(pair.right);
      }
    }
    final SqlNode q = renameColumns(create.columnList, create.query);
    final String sql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    List<String> schemaPath = pair.left.path(null);
    String schemaName = schemaPlus.getName();
    String viewName = pair.right;
    List<String> viewPath = new ArrayList<>();
    viewPath.addAll(schemaPath);
    viewPath.add(viewName);
    HoptimatorViewTableMacro viewTableMacro = new HoptimatorViewTableMacro(CalciteSchema.from(schemaPlus),
        sql, schemaPath, viewPath, false);
    ViewTable viewTable = (ViewTable) viewTableMacro.apply(connectionProperties);
    try {
      ValidationService.validateOrThrow(viewTable, TranslatableTable.class);
      if (create.getReplace()) {
        DeploymentService.update(viewTable, ViewTable.class, connectionProperties);
      } else {
        DeploymentService.create(viewTable, ViewTable.class, connectionProperties);
      }
      schemaPlus.add(viewName, viewTable);
    } catch (Exception e) {
      throw new RuntimeException("Cannot CREATE VIEW in " + schemaName + ": " + e.getMessage(), e);
    }
  }

  // N.B. copy-pasted from Apache Calcite

  /** Executes a {@code CREATE MATERIALIZED VIEW} command. */
  @Override
  public void execute(SqlCreateMaterializedView create, CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair = schema(context, true, create.name);
    if (pair.left == null) {
      throw SqlUtil.newContextException(create.name.getParserPosition(),
          RESOURCE.schemaNotFound(create.name.getSimple()));
    }
    if (pair.left.plus().getTable(pair.right) != null) {
      // Materialized view exists.
      if (!create.ifNotExists && !create.getReplace()) {
        // They did not specify IF NOT EXISTS, so give error.
        throw SqlUtil.newContextException(create.name.getParserPosition(), RESOURCE.tableExists(pair.right));
      }
      if (create.getReplace()) {
        pair.left.plus().removeTable(pair.right);
      } else {
        // nothing to do
        return;
      }
    }

    final SqlNode q = renameColumns(create.columnList, create.query);
    final String sql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    final List<String> schemaPath = pair.left.path(null);

    SchemaPlus schemaPlus = pair.left.plus();
    String schemaName = schemaPlus.getName();
    String viewName = pair.right;
    List<String> viewPath = new ArrayList<>();
    viewPath.addAll(schemaPath);
    viewPath.add(viewName);
    try {
      if (!(pair.left.schema instanceof Database)) {
        throw new RuntimeException(schemaName + " is not a physical database.");
      }
      String database = ((Database) pair.left.schema).databaseName();

      // Table does not exist. Create it.
      RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      HoptimatorViewTableMacro viewTableMacro = new HoptimatorViewTableMacro(CalciteSchema.from(schemaPlus),
          sql, schemaPath, viewPath, false);
      MaterializedViewTable materializedViewTable = new MaterializedViewTable(viewTableMacro, connectionProperties);
      RelDataType viewRowType = materializedViewTable.getRowType(typeFactory);

      // Support "partial views", i.e. CREATE VIEW FOO$BAR, where the view name
      // is "foo-bar" and the sink is just FOO.
      String sinkName = viewName.split("\\$", 2)[0];
      List<String> sinkPath = new ArrayList<>();
      sinkPath.addAll(schemaPath);
      sinkPath.add(sinkName);
      Table sink = pair.left.plus().getTable(sinkName);

      final RelDataType rowType;
      if (sink != null) {
        // For "partial views", the sink may already exist. Use the existing row type.
        rowType = sink.getRowType(typeFactory);
      } else {
        // For normal views, we create the sink based on the view row type.
        rowType = viewRowType;
      }

      // Plan a pipeline to materialize the view.
      RelRoot root = new HoptimatorDriver.Prepare(connectionProperties)
          .convert(context, sql).root;
      PipelineRel.Implementor plan = DeploymentService.plan(root, connectionProperties);
      plan.setSink(database, sinkPath, rowType, Collections.emptyMap());
      Pipeline pipeline = plan.pipeline(viewName);

      MaterializedView hook = new MaterializedView(database, viewPath, sql, plan.sql(), plan.pipeline(viewName));
      // TODO support CREATE ... WITH (options...)
      ValidationService.validateOrThrow(hook, MaterializedView.class);
      pipeline.update();
      if (create.getReplace()) {
        DeploymentService.update(hook, MaterializedView.class, connectionProperties);
      } else {
        DeploymentService.create(hook, MaterializedView.class, connectionProperties);
      }
      schemaPlus.add(viewName, materializedViewTable);
    } catch (Exception e) {
      throw new RuntimeException("Cannot CREATE MATERIALIZED VIEW in " + schemaName + ": " + e.getMessage(), e);
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
    String schemaName = schemaPlus.getName();
    Table table = schemaPlus.getTable(viewName);
    if (table == null) {
      if (drop.ifExists) {
        return;
      }
      throw SqlUtil.newContextException(drop.name.getParserPosition(), RESOURCE.tableNotFound(viewName));
    }

    final List<String> schemaPath = pair.left.path(null);
    List<String> viewPath = new ArrayList<>();
    viewPath.addAll(schemaPath);
    viewPath.add(viewName);

    if (table instanceof MaterializedViewTable) {
      MaterializedViewTable materializedViewTable = (MaterializedViewTable) table;
      try {
        DeploymentService.delete(materializedViewTable.viewTable(), ViewTable.class, connectionProperties);
      } catch (SQLException e) {
        throw new RuntimeException("Cannot DROP MATERIALIZED VIEW in " + schemaName + ": " + e.getMessage(), e);
      }
    } else if (table instanceof ViewTable) {
      ViewTable viewTable = (ViewTable) table;
      try {
        DeploymentService.delete(viewTable, ViewTable.class, connectionProperties);
      } catch (SQLException e) {
        throw new RuntimeException("Cannot DROP VIEW in " + schemaName + ": " + e.getMessage(), e);
      }
    } else {
      throw new RuntimeException("Cannot DROP in " + schemaName + ": " + viewName + " is not a view.");
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
      schema = schema.getSubSchema(p, true);
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
        Arrays.asList(new SqlNode[]{query, new SqlIdentifier("_", p), columnList}));
    return new SqlSelect(p, null, selectList, from, null, null, null, null, null, null, null, null, null);
  }
}
