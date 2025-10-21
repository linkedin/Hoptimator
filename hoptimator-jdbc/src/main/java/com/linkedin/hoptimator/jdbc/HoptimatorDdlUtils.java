package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.util.planner.PipelineRel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;


public final class HoptimatorDdlUtils {
  private HoptimatorDdlUtils() {
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
      schema = Objects.requireNonNull(schema).getSubSchema(p, true);
    }
    return Pair.of(schema, name);
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
}
