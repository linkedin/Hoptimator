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
 * Root schema for MySQL that lazily discovers databases as sub-schemas.
 *
 * <p>When used as the backing {@link Schema} for a Calcite root schema, MySQL databases
 * appear directly at the top level (e.g., {@code testdb.mytable}) without opening a MySQL
 * connection at connect time.
 *
 * <p>Databases are loaded on-demand following the same {@link LazyLookup} pattern used by
 * {@link TableSchema} for tables: individual databases are loaded by {@code load(name)} and
 * all databases by {@code loadAll()} (e.g., for wildcard patterns or SHOW DATABASES).
 */
class MySqlRootSchema extends AbstractSchema {

  private static final Logger log = LoggerFactory.getLogger(MySqlRootSchema.class);

  private final Properties properties;
  private final LazyReference<Lookup<Schema>> subSchemasRef = new LazyReference<>();

  MySqlRootSchema(Properties properties) {
    this.properties = properties;
  }

  @Override
  public Lookup<Schema> subSchemas() {
    return subSchemasRef.getOrCompute(() -> new LazyLookup<>() {

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
        // The MySQL JDBC driver does not allow you to fetch a single database by name.
        // So we have to fetch all databases and then find the one we want, if it exists.
        // Leverage LazyLookup ability to force the loading and caching of all schemas
        // as an optimization for future calls.
        return cacheAll().get(name);
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
