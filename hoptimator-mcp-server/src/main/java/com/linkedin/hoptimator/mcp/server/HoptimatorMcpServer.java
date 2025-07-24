package com.linkedin.hoptimator.mcp.server;

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import com.linkedin.hoptimator.util.DeploymentService;
import com.linkedin.hoptimator.util.planner.PipelineRel;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import com.google.gson.Gson;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import java.util.Objects;
import java.util.Properties;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import static io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import static io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import static io.modelcontextprotocol.spec.McpSchema.ServerCapabilities;
import static io.modelcontextprotocol.spec.McpSchema.Tool;

public final class HoptimatorMcpServer {

  public static final String PIPELINE_STATUS_QUERY = "select * from \"k8s\".pipelines where name = ?";
  public static final String PIPELINE_ELEMENT_STATUS_QUERY =
      "select name, ready, failed, message\n" + "from \"k8s\".pipeline_elements t1 inner join \n"
          + "(select * from \"k8s\".pipeline_element_map where pipeline_name=?) t2 on t1.name = t2.element_name";

  public static final String PIPELINE_DESCRIBE_QUERY = "select p.name, \"SQL\" from \"k8s\".pipelines as p natural join "
      + "\"k8s\".views where p.name = ?";

  private HoptimatorMcpServer() {
  }

  public static void main(String[] args) throws Exception {
    HoptimatorConnection conn = (HoptimatorConnection) DriverManager.getConnection("jdbc:hoptimator://fun=mysql");
    Gson gson = new Gson();

    StdioServerTransportProvider transportProvider = new StdioServerTransportProvider();

    String fetchSchemaSchema = "{\"type\" : \"object\", \"properties\" : {}}";
    SyncToolSpecification fetchSchemas = new SyncToolSpecification(
        new Tool("fetch_schemas", "Fetches all catalogs and schemas", fetchSchemaSchema), (x, args2) -> {
          try {
            DatabaseMetaData metaData = conn.getMetaData();
            List<String> result = new ArrayList<>();
            ResultSet rs = metaData.getSchemas();
            while (rs.next()) {
              result.add(rs.getString(1)); //"TABLE_CAT"));
            }
            return new CallToolResult(gson.toJson(result), false);
          } catch (Exception e) {
            return new CallToolResult("ERROR: " + e, true);
          }
        });

    String fetchTableSchema = "{\"type\" : \"object\", \"properties\" : {\"schema\" : {\"type\" : \"string\"}}}";
    SyncToolSpecification fetchTables = new SyncToolSpecification(
        new Tool("fetch_tables", "Fetches all Tables with an optional schema argument to only "
            + "fetch tables of that schema type", fetchTableSchema), (x, args2) -> {
          String schema = (String) args2.get("schema");
          if (schema == null) {
            schema = "%";
          }
          try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(null, schema, "%", null);
            List<Map<String, String>> tables = new ArrayList<>();
            while (rs.next()) {
              Map<String, String> table = new HashMap<>();
              table.put("TABLE_CAT", rs.getString(1));
              table.put("TABLE_SCHEM", rs.getString(2));
              table.put("TABLE_NAME", rs.getString(3));
              table.put("TABLE_TYPE", rs.getString(4));
              tables.add(table);
            }
            return new CallToolResult(gson.toJson(tables), false);
          } catch (Exception e) {
            return new CallToolResult("ERROR: " + e, true);
          }
        });

    String fetchPipelineSchema = "{\"type\" : \"object\", \"properties\" : {}}";
    SyncToolSpecification fetchPipelines = new SyncToolSpecification(
        new Tool("fetch_pipelines", "Fetches all currently deployed pipelines", fetchPipelineSchema), (x, args2) -> {
          try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from \"k8s\".pipelines");
            return new CallToolResult(gson.toJson(collect(rs)), false);
          } catch (Exception e) {
            return new CallToolResult("ERROR: " + e, true);
          }
        });

    String fetchPipelineStatusSchema = "{\"type\" : \"object\", \"properties\" : {\"pipeline\" : {\"type\" : \"string\"}},"
        + "\"required\" : [\"pipeline\"]}";
    SyncToolSpecification fetchPipelineStatus = new SyncToolSpecification(
        new Tool("fetch_pipeline_status", "Fetches the deployment and running status of a "
            + " specified pipeline", fetchPipelineStatusSchema), (x, args2) -> {
          String name = (String) args2.get("pipeline");
          List<Map<String, String>> status;
          try (PreparedStatement stmt = conn.prepareStatement(PIPELINE_STATUS_QUERY)) {
            stmt.setString(1, name);
            ResultSet rs = stmt.executeQuery();
            status = collect(rs);
          } catch (Exception e) {
            return new CallToolResult("ERROR: " + e, true);
          }

          if (!status.isEmpty()) {
            try (PreparedStatement stmt = conn.prepareStatement(PIPELINE_ELEMENT_STATUS_QUERY)) {
              stmt.setString(1, name);
              ResultSet rs = stmt.executeQuery();
              List<Map<String, String>> elementStatus = collect(rs);
              status.get(0).put("elementStatuses", gson.toJson(elementStatus));
            } catch (Exception e) {
              return new CallToolResult("ERROR: " + e, true);
            }
          }
          return new CallToolResult(gson.toJson(status), false);
        });

    String describeTableSchema = "{\"type\" : \"object\", \"properties\" : {\"schema\" : {\"type\" : \"string\"},"
        + "\"table\" : {\"type\" : \"string\"}}, \"required\" : [\"table\"]}";
    SyncToolSpecification describeTable = new SyncToolSpecification(
        new Tool("describe_table", "Describes the columns of a specified table", describeTableSchema),
        (x, args2) -> {
          String table = (String) args2.get("table");
          String schema = (String) args2.get("schema");
          if (schema == null) {
            schema = "%";
          }
          Map<String, Object> tableDefinition = new HashMap<>();

          try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(null, schema, table, null);

            if (rs.next()) {
              tableDefinition = getTableInfo(conn, rs.getString(1),
                  rs.getString(2), rs.getString(3),
                  rs.getString(4));
            }
            return new CallToolResult(gson.toJson(tableDefinition), false);
          } catch (Exception e) {
            return new CallToolResult("ERROR: " + e, true);
          }
        });

    String describePipelineSchema = "{\"type\" : \"object\", \"properties\" : {\"pipeline\" : {\"type\" : \"string\"}},"
        + "\"required\" : [\"pipeline\"]}";
    SyncToolSpecification describePipeline = new SyncToolSpecification(
        new Tool("describe_pipeline", "Describes the SQL that makes up a specified deployed pipeline",
            describePipelineSchema), (x, args2) -> {
          String pipeline = (String) args2.get("pipeline");
          try (PreparedStatement stmt = conn.prepareStatement(PIPELINE_DESCRIBE_QUERY)) {
            stmt.setString(1, pipeline);
            ResultSet rs = stmt.executeQuery();
            return new CallToolResult(gson.toJson(collect(rs)), false);
          } catch (Exception e) {
            return new CallToolResult("ERROR: " + e, true);
          }
        });

    String planSchema = "{\"type\" : \"object\", \"properties\" : {\"sql\" : {\"type\" : \"string\"}},"
        + "\"required\" : [\"sql\"]}";
    SyncToolSpecification plan = new SyncToolSpecification(
        new Tool("plan", "Describes the plan for pipeline creation for the provided create, "
            + "or update SQL. This call should always be used prior to modifying a pipeline", planSchema),
        (x, args2) -> {
          String sql = (String) args2.get("sql");
          // Validate the SQL is a query statement
          if (!isModifyStatement(sql)) {
            return new CallToolResult("ERROR: The provided SQL is not a valid modify statement.", true);
          }

          try {
            String querySql;
            SqlCreateMaterializedView create;
            SqlNode sqlNode = HoptimatorDriver.parseQuery(conn, sql);
            if (sqlNode instanceof SqlCreateMaterializedView) {
              create = (SqlCreateMaterializedView) sqlNode;
              final SqlNode q = renameColumns(create.columnList, create.query);
              querySql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
            } else {
              return new CallToolResult("Unsupported DDL statement: " + sql, true);
            }

            RelRoot root = HoptimatorDriver.convert(conn, querySql).root;
            Properties connectionProperties = conn.connectionProperties();
            RelOptTable table = root.rel.getTable();
            if (table != null) {
              connectionProperties.setProperty(DeploymentService.PIPELINE_OPTION, String.join(".", table.getQualifiedName()));
            }
            PipelineRel.Implementor sqlPlan = DeploymentService.plan(root, conn.materializations(), connectionProperties);
            setSink(conn, sqlPlan, create, querySql);
            Pipeline pipeline = sqlPlan.pipeline(viewName(create.name), conn);
            List<String> specs = new ArrayList<>();
            for (Source source : pipeline.sources()) {
              specs.addAll(DeploymentService.specify(source, conn));
            }
            specs.addAll(DeploymentService.specify(pipeline.sink(), conn));
            specs.addAll(DeploymentService.specify(pipeline.job(), conn));
            return new CallToolResult(gson.toJson(specs), false);
          } catch (SQLException e) {
            return new CallToolResult("ERROR: " + e, true);
          }
        });

    String querySchema = "{\"type\" : \"object\", \"properties\" : {\"sql\" : {\"type\" : \"string\"}},"
        + "\"required\" : [\"sql\"]}";
    SyncToolSpecification query = new SyncToolSpecification(
        new Tool("query", "Executes select queries", querySchema), (x, args2) -> {
          String sql = (String) args2.get("sql");
          // Validate the SQL is a query statement
          if (!isQueryStatement(sql)) {
            return new CallToolResult("ERROR: The provided SQL is not a valid query statement.", true);
          }

          try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            return new CallToolResult(gson.toJson(collect(rs)), false);
          } catch (Exception e) {
            return new CallToolResult("ERROR: " + e, true);
          }
        });

    String modifySchema = "{\"type\" : \"object\", \"properties\" : {\"sql\" : {\"type\" : \"string\"}},"
        + "\"required\" : [\"sql\"]}";
    SyncToolSpecification modify = new SyncToolSpecification(
        new Tool("modify", "Supports pipeline modification via the execution of create, update, "
            + " and drop statements. This call should be used cautiously and should always be proceeded by a"
            + " 'plan' call to validate the statement for created or updated pipelines.",
            modifySchema), (x, args2) -> {
          String sql = (String) args2.get("sql");
          // Validate the SQL is a data modification statement
          if (!isModifyStatement(sql)) {
            return new CallToolResult("ERROR: The provided SQL is not a valid data modification statement.", true);
          }

          try (Statement stmt = conn.createStatement()) {
            int rowCount = stmt.executeUpdate(sql);
            return new CallToolResult(rowCount + " rows modified", false);
          } catch (Exception e) {
            return new CallToolResult("ERROR: " + e, true);
          }
        });

    McpSyncServer server = McpServer.sync(transportProvider)
        .serverInfo("hoptimator", "0.0.0")
        .capabilities(ServerCapabilities.builder()
            .tools(true)         // Enable tool support
            .build())
        .build();

    server.addTool(fetchSchemas);
    server.addTool(fetchTables);
    server.addTool(fetchPipelines);
    server.addTool(fetchPipelineStatus);
    server.addTool(describeTable);
    server.addTool(describePipeline);
    server.addTool(plan);
    server.addTool(query);
    server.addTool(modify);
  }

  private static Map<String, Object> getTableInfo(Connection conn, String cat, String sch, String table, String type) throws SQLException {
    List<Map<String, Object>> columns = getColumns(conn, cat, sch, table);
    Map<String, Object> pkConstraint = getPkConstraint(conn, cat, sch, table);

    @SuppressWarnings("unchecked")
    List<String> primaryKeys = pkConstraint != null
        ? (List<String>) pkConstraint.get("constrained_columns")
        : new ArrayList<>();
    List<Map<String, Object>> foreignKeys = getForeignKeys(conn, cat, sch, table);

    Map<String, Object> tableInfo = new HashMap<>();
    tableInfo.put("TABLE_CAT", cat);
    tableInfo.put("TABLE_SCHEM", sch);
    tableInfo.put("TABLE_NAME", table);
    tableInfo.put("TABLE_TYPE", type);
    tableInfo.put("columns", columns);
    tableInfo.put("primary_keys", primaryKeys);
    tableInfo.put("foreign_keys", foreignKeys);

    // Mark columns that are primary keys
    for (Map<String, Object> column : columns) {
      column.put("primary_key", primaryKeys.contains(column.get("COLUMN_NAME")));
    }

    return tableInfo;
  }

  private static List<Map<String, Object>> getColumns(Connection conn, String cat, String sch, String table) throws SQLException {
    List<Map<String, Object>> columns = new ArrayList<>();
    DatabaseMetaData metaData = conn.getMetaData();
    ResultSet rs = metaData.getColumns(cat, sch, table, null);

    while (rs.next()) {
      Map<String, Object> column = new HashMap<>();
      column.put("COLUMN_NAME", rs.getString(4));
      column.put("TYPE_NAME", rs.getString(6));
      column.put("COLUMN_SIZE", rs.getInt(7));
      column.put("NUM_PREC_RADIX", rs.getInt(10));
      column.put("COLUMN_DEF", rs.getString(13));
      columns.add(column);
    }

    return columns;
  }

  private static Map<String, Object> getPkConstraint(Connection conn, String cat, String sch, String table) throws SQLException {
    DatabaseMetaData metaData = conn.getMetaData();
    ResultSet rs = metaData.getPrimaryKeys(cat, sch, table);

    List<String> columns = new ArrayList<>();
    String name = null;

    while (rs.next()) {
      columns.add(rs.getString(4)); //"COLUMN_NAME"));
      if (name == null) {
        name = rs.getString(6); //"PK_NAME");
      }
    }

    if (!columns.isEmpty()) {
      Map<String, Object> constraint = new HashMap<>();
      constraint.put("constrained_columns", columns);
      constraint.put("name", name);
      return constraint;
    }

    return null;
  }

  private static List<Map<String, Object>> getForeignKeys(Connection conn, String cat, String sch, String table) throws SQLException {
    DatabaseMetaData metaData = conn.getMetaData();
    ResultSet rs = metaData.getImportedKeys(cat, sch, table);

    Map<String, Map<String, Object>> fkeysMap = new HashMap<>();

    while (rs.next()) {
      String fkName = rs.getString(12); //"FK_NAME");

      Map<String, Object> fkey = fkeysMap.get(fkName);
      if (fkey == null) {
        fkey = new HashMap<>();
        fkey.put("name", fkName);
        fkey.put("constrained_columns", new ArrayList<String>());
        fkey.put("referred_cat", rs.getString(1));
        fkey.put("referred_schem", rs.getString(2));
        fkey.put("referred_table", rs.getString(3));
        fkey.put("referred_columns", new ArrayList<String>());
        fkey.put("options", new HashMap<>());
        fkeysMap.put(fkName, fkey);
      }

      @SuppressWarnings("unchecked")
      List<String> constrainedColumns = (List<String>) fkey.get("constrained_columns");
      constrainedColumns.add(rs.getString(8)); //"FKCOLUMN_NAME"));

      @SuppressWarnings("unchecked")
      List<String> referredColumns = (List<String>) fkey.get("referred_columns");
      referredColumns.add(rs.getString(4)); //"PKCOLUMN_NAME"));
    }

    return new ArrayList<>(fkeysMap.values());
  }

  private static List<Map<String, String>> collect(ResultSet rs) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    int columnCount = meta.getColumnCount();
    List<Map<String, String>> data = new ArrayList<>();
    while (rs.next()) {
      Map<String, String> row = new HashMap<>();

      for (int i = 1; i <= columnCount; i++) {
        String columnName = meta.getColumnName(i);
        Object value = rs.getObject(i);

        if (value != null) {
          String stringValue = value.toString();
          row.put(columnName, stringValue);
        } else {
          row.put(columnName, null);
        }
      }
      data.add(row);
    }
    return data;
  }

  private static boolean isQueryStatement(String sql) {
    String upperSql = sql.trim().toUpperCase();
    return upperSql.startsWith("SELECT");
  }

  private static boolean isModifyStatement(String sql) {
    String upperSql = sql.trim().toUpperCase();
    return (upperSql.startsWith("CREATE") && upperSql.contains("VIEW")) || upperSql.startsWith("DROP");
  }

  private static void setSink(HoptimatorConnection conn, PipelineRel.Implementor plan,
      SqlCreateMaterializedView create, String querySql) throws SQLException {
    if (create == null) {
      return;
    }
    final Pair<CalciteSchema, String> pair = schema(conn.createPrepareContext(), create.name);
    String database = ((Database) pair.left.schema).databaseName();
    final List<String> schemaPath = pair.left.path(null);
    List<String> sinkPath = new ArrayList<>(schemaPath);
    String sinkName = pair.right.split("\\$", 2)[0];
    sinkPath.add(sinkName);

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    CalcitePrepare.AnalyzeViewResult analyzed = HoptimatorDriver.analyzeView(conn, querySql);
    RelProtoDataType protoType = RelDataTypeImpl.proto(analyzed.rowType);
    RelDataType viewRowType = protoType.apply(typeFactory);

    final SchemaPlus schemaPlus = pair.left.plus();
    Table sink = schemaPlus.getTable(sinkName);
    final RelDataType rowType;
    if (sink != null) {
      // For "partial views", the sink may already exist. Use the existing row type.
      rowType = sink.getRowType(typeFactory);
    } else {
      // For normal views, we create the sink based on the view row type.
      rowType = viewRowType;
    }

    plan.setSink(database, sinkPath, rowType, Collections.emptyMap());
  }

  private static SqlNode renameColumns(SqlNodeList columnList, SqlNode query) {
    if (columnList == null) {
      return query;
    }
    final SqlParserPos p = query.getParserPosition();
    final SqlNodeList selectList = SqlNodeList.SINGLETON_STAR;
    final SqlCall from = SqlStdOperatorTable.AS.createCall(p,
        Arrays.asList(query, new SqlIdentifier("_", p), columnList));
    return new SqlSelect(p, null, selectList, from, null, null, null, null, null, null, null, null, null);
  }

  private static Pair<CalciteSchema, String> schema(CalcitePrepare.Context context, SqlIdentifier id) {
    final String name;
    final List<String> path;
    if (id.isSimple()) {
      path = context.getDefaultSchemaPath();
      name = id.getSimple();
    } else {
      path = Util.skipLast(id.names);
      name = Util.last(id.names);
    }
    CalciteSchema schema = context.getRootSchema();
    for (String p : path) {
      schema = Objects.requireNonNull(schema).getSubSchema(p, true);
    }
    return Pair.of(schema, name);
  }

  private static String viewName(SqlIdentifier id) {
    final String name;
    if (id.isSimple()) {
      name = id.getSimple();
    } else {
      name = Util.last(id.names);
    }
    return name;
  }
}
