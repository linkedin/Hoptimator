package com.linkedin.hoptimator.mysql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This discovers tables for a specific database.
 */
public class TableSchema extends AbstractSchema {

  private static final Logger log = LoggerFactory.getLogger(TableSchema.class);

  private final Properties properties;
  private final String database;
  private final Map<String, Table> tableMap = new HashMap<>();

  public TableSchema(Properties properties, String database) {
    this.properties = properties;
    this.database = database;
  }

  public void populate() throws SQLException {
    tableMap.clear();

    String url = properties.getProperty("url");
    String user = properties.getProperty("user", "");
    String password = properties.getProperty("password", "");

    log.debug("Loading MySQL tables from database={}, url={}", database, url);

    try (Connection conn = DriverManager.getConnection(url, user, password)) {
      DatabaseMetaData metaData = conn.getMetaData();

      // Get all tables in this database
      try (ResultSet rs = metaData.getTables(database, null, "%", new String[]{"TABLE"})) {
        int count = 0;
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");

          MySqlTable table = new MySqlTable(database, tableName, properties);
          tableMap.put(tableName, table);
          count++;
        }
        log.debug("Loaded {} tables from {}", count, database);
      }
    }
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }
}
