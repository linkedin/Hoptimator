package com.linkedin.hoptimator.mysql;

import com.linkedin.hoptimator.jdbc.schema.LazyLookup;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.LazyReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * A catalog schema for MySQL that lazily discovers databases as sub-schemas.
 *
 * <p>Databases are loaded on-demand when first accessed rather than all at once
 * during driver connection, following the same pattern as {@link TableSchema} for tables.
 */
public class MySqlCatalogSchema extends AbstractSchema {

  private static final Logger log = LoggerFactory.getLogger(MySqlCatalogSchema.class);

  private final Properties properties;
  private final LazyReference<Lookup<Schema>> subSchemasRef = new LazyReference<>();

  public MySqlCatalogSchema(Properties properties) {
    this.properties = properties;
  }

  @Override
  public Lookup<Schema> subSchemas() {
    return subSchemasRef.getOrCompute(() -> new LazyLookup<Schema>() {

      @Override
      protected Map<String, Schema> loadAll() throws Exception {
        Map<String, Schema> schemas = new HashMap<>();
        String url = properties.getProperty("url");
        String user = properties.getProperty("user", "");
        String password = properties.getProperty("password", "");
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
          DatabaseMetaData metaData = conn.getMetaData();
          try (ResultSet rs = metaData.getCatalogs()) {
            while (rs.next()) {
              String database = rs.getString("TABLE_CAT");
              schemas.put(database, createTableSchema(database));
              log.debug("Discovered MySQL database: {}", database);
            }
          }
        }
        return schemas;
      }

      @Override
      protected @Nullable Schema load(String name) throws Exception {
        String url = properties.getProperty("url");
        String user = properties.getProperty("user", "");
        String password = properties.getProperty("password", "");
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
          DatabaseMetaData metaData = conn.getMetaData();
          try (ResultSet rs = metaData.getCatalogs()) {
            while (rs.next()) {
              String database = rs.getString("TABLE_CAT");
              if (database.equalsIgnoreCase(name)) {
                return createTableSchema(database);
              }
            }
          }
        }
        return null;
      }

      @Override
      protected String getDescription() {
        return "MySQL at " + properties.getProperty("url");
      }
    });
  }

  protected TableSchema createTableSchema(String database) {
    return new TableSchema(properties, database);
  }
}
