package com.linkedin.hoptimator.catalog.mysql;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import com.linkedin.hoptimator.catalog.ConfigProvider;
import com.linkedin.hoptimator.catalog.DataType;
import com.linkedin.hoptimator.catalog.Database;
import com.linkedin.hoptimator.catalog.DatabaseSchema;
import com.linkedin.hoptimator.catalog.ResourceProvider;
import com.linkedin.hoptimator.catalog.TableLister;
import com.linkedin.hoptimator.catalog.TableResolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import javax.sql.DataSource;

/**
 * Wrapper around flink-cdc-connector's mysql-cdc Flink connector.
 **/
public class MySqlCdcSchemaFactory implements SchemaFactory {
  private final Logger log = LoggerFactory.getLogger(MySqlCdcSchemaFactory.class);

  @Override
  @SuppressWarnings("unchecked")
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    String hostname = (String) operand.get("hostname");
    Integer port = (Integer) operand.get("port");
    String username = (String) operand.get("username");
    String password = (String) operand.get("password");
    String databaseName = (String) operand.getOrDefault("database", name);
    String driver = (String) operand.get("driver");
    String catalog = (String) operand.get("catalog");
    String urlSuffix = (String) operand.get("urlSuffix");
    String jdbcUrl = "jdbc:mysql://" + hostname + ":" + Integer.toString(port) + "/" + databaseName;
    log.info("Connecting to mysql db at {}.", jdbcUrl);
    DataSource dataSource = JdbcSchema.dataSource(jdbcUrl, driver, username, password);
    JdbcSchema jdbcSchema = JdbcSchema.create(parentSchema, name, dataSource, catalog, null);
    Map<String, Object> connectorConfig = (Map<String, Object>) operand.get("connectorConfig");
    ConfigProvider connectorConfigProvider = ConfigProvider.from(connectorConfig)
      .with("connector", "mysql-cdc")
      .with("hostname", hostname)
      .with("port", port)
      .with("username", username)
      .with("password", password)
      .with("database-name", databaseName)
      .with("table-name", x -> x);
    TableLister lister = () -> jdbcSchema.getTableNames();
    TableResolver resolver = x ->  jdbcSchema.getTable(x).getRowType(DataType.DEFAULT_TYPE_FACTORY);
    Database database = new Database(name, lister, resolver, connectorConfigProvider);
    return new DatabaseSchema(database);
  }
}
