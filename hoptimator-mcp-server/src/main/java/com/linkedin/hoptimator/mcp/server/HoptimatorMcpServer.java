package com.linkedin.hoptimator.mcp.server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import com.google.gson.Gson;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;

import static io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import static io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import static io.modelcontextprotocol.spec.McpSchema.ServerCapabilities;
import static io.modelcontextprotocol.spec.McpSchema.Tool;


public final class HoptimatorMcpServer {

  private HoptimatorMcpServer() {
  }

  public static void main(String[] args) throws Exception {
    Connection conn = DriverManager.getConnection("jdbc:hoptimator://fun=mysql");
    Gson gson = new Gson();

    String sqlSchema = "{\"type\" : \"object\", \"id\" : \"urn:jsonschema:Sql\","
        + "\"properties\" : {\"sql\" : {\"type\" : \"string\"}}}";
    StdioServerTransportProvider transportProvider = new StdioServerTransportProvider();
    SyncToolSpecification query = new SyncToolSpecification(
      new Tool("query", "SQL Query", sqlSchema), (x, args2) -> {
        try (Statement stmt = conn.createStatement()) {
          String sql = rewriteCommands((String) args2.get("sql"));
          ResultSet rs = stmt.executeQuery(sql);
          return new CallToolResult(gson.toJson(collect(rs)), false);
        } catch (Exception e) {
          return new CallToolResult("ERROR: " + e.toString(), true);
        }
      });

    McpSyncServer server = McpServer.sync(transportProvider)
        .serverInfo("hoptimator", "0.0.0")
        .capabilities(ServerCapabilities.builder()
        .tools(true)         // Enable tool support
        .build())
        .build();

    server.addTool(query);
    while (true) {
      Thread.sleep(1000L);
    }
  }

  private static String rewriteCommands(String sql) {
    if ("show tables".equalsIgnoreCase(sql)) {
      return "select * from \"metadata\".tables";
    }
    if ("show databases".equalsIgnoreCase(sql)) {
      return "select * from \"k8s\".databases";
    }
    if ("show pipelines".equalsIgnoreCase(sql)) {
      return "select * from \"k8s\".pipelines";
    }
    return sql;
  }

  private static List<Map<String, String>> collect(ResultSet rs) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    int n = meta.getColumnCount();
    List<Map<String, String>> results = new ArrayList<>();
    while (rs.next()) {
      Map<String, String> row = new HashMap<>();
      for (int j = 1; j <= n; j++) {
        row.put(meta.getColumnName(j), rs.getString(j)); 
      }
      results.add(row);
    }
    return results;
  }
}
