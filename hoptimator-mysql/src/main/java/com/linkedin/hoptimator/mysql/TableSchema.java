package com.linkedin.hoptimator.mysql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.LazyReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.jdbc.schema.LazyTableLookup;


/**
 * This discovers tables for a specific database.
 */
public class TableSchema extends AbstractSchema {

  private static final Logger log = LoggerFactory.getLogger(TableSchema.class);

  private final Properties properties;
  private final String database;
  private final LazyReference<Lookup<Table>> tables = new LazyReference<>();

  public TableSchema(Properties properties, String database) {
    this.properties = properties;
    this.database = database;
  }

  @Override
  public Lookup<Table> tables() {
    return tables.getOrCompute(() -> new LazyTableLookup<>() {

      @Override
      protected Map<String, Table> loadAllTables() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        String url = properties.getProperty("url");
        String user = properties.getProperty("user", "");
        String password = properties.getProperty("password", "");

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
          DatabaseMetaData metaData = conn.getMetaData();

          // Get all tables in this database
          try (ResultSet rs = metaData.getTables(database, null, "%", new String[]{"TABLE"})) {
            while (rs.next()) {
              String tableName = rs.getString("TABLE_NAME");
              MySqlTable table = new MySqlTable(database, tableName, properties);
              tableMap.put(tableName, table);
            }
          }
        }
        return tableMap;
      }

      @Override
      protected @Nullable Table loadTable(String name) throws Exception {
        String url = properties.getProperty("url");
        String user = properties.getProperty("user", "");
        String password = properties.getProperty("password", "");

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
          DatabaseMetaData metaData = conn.getMetaData();
          try (ResultSet rs = metaData.getTables(database, null, name, new String[]{"TABLE"})) {
            if (rs.next()) {
              return new MySqlTable(database, name, properties);
            }
          }
        }
        return null;
      }

      @Override
      protected String getSchemaDescription() {
        return "MySQL database '" + database + "' at " + properties.getProperty("url");
      }
    });
  }
}
