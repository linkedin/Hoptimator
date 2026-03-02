package com.linkedin.hoptimator.mysql;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validated;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Deployer for MySQL tables. Creates tables in the synchronous DDL hot path.
 *
 * <p>Implements {@link Validated} to pre-check table constraints
 * before any deployment side effects.
 */
public class MySqlDeployer implements Deployer, Validated {

  private static final Logger log = LoggerFactory.getLogger(MySqlDeployer.class);

  private static final String KEY_PREFIX = "KEY_";

  private final Source source;
  private final Properties properties;
  private final HoptimatorConnection hoptimatorConnection;
  private boolean created = false;

  public MySqlDeployer(Source source, Properties properties, HoptimatorConnection connection) {
    this.source = source;
    this.properties = properties;
    this.hoptimatorConnection = connection;
  }

  /**
   * Parses KEY_ prefixed fields from the table schema.
   * Returns a list of column names (without KEY_ prefix) that should be primary keys.
   */
  private List<String> parseKeyFields(RelDataType rowType) {
    List<String> keyFields = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      if (field.getName().startsWith(KEY_PREFIX)) {
        String keyName = field.getName().substring(KEY_PREFIX.length());
        keyFields.add(keyName);
      }
    }
    return keyFields;
  }

  /**
   * Converts Calcite SQL type to MySQL type string.
   */
  private String toMySqlType(RelDataTypeField field) {
    String typeName = field.getType().getSqlTypeName().getName();
    switch (typeName) {
      case "INTEGER":
        return "INT";
      case "BIGINT":
        return "BIGINT";
      case "VARCHAR":
        int precision = field.getType().getPrecision();
        return precision > 0 ? "VARCHAR(" + precision + ")" : "TEXT";
      case "CHAR":
        return "CHAR(" + field.getType().getPrecision() + ")";
      case "BOOLEAN":
        return "BOOLEAN";
      case "DOUBLE":
        return "DOUBLE";
      case "FLOAT":
        return "FLOAT";
      case "DECIMAL":
        return "DECIMAL(" + field.getType().getPrecision() + "," + field.getType().getScale() + ")";
      case "DATE":
        return "DATE";
      case "TIME":
        return "TIME";
      case "TIMESTAMP":
        return "TIMESTAMP";
      default:
        return "TEXT";
    }
  }

  @Override
  public void validate(Validator.Issues issues) {
    String tableName = source.table();
    String database = source.schema();

    if (database == null || tableName == null) {
      issues.error("Database & table names are required for MySQL creation " + tableName);
      return;
    }

    // Validate identifiers for SQL injection
    if (!isValidIdentifier(database)) {
      issues.error("Invalid database name: " + database);
      return;
    }
    if (!isValidIdentifier(tableName)) {
      issues.error("Invalid table name: " + tableName);
      return;
    }

    // Get desired schema and validate KEY fields
    RelDataType newRowType;
    List<String> newKeyFields;
    try {
      newRowType = HoptimatorDriver.rowType(source, hoptimatorConnection);
      newKeyFields = parseKeyFields(newRowType);

      if (newKeyFields.isEmpty()) {
        issues.error("No KEY_ fields found in table " + tableName + ". At least one KEY_ field is required.");
      }

      // Validate all column names
      for (RelDataTypeField field : newRowType.getFieldList()) {
        String fieldName = field.getName();
        String columnName = fieldName.startsWith(KEY_PREFIX)
            ? fieldName.substring(KEY_PREFIX.length())
            : fieldName;
        if (!isValidIdentifier(columnName)) {
          issues.error("Invalid column name: " + columnName);
        }
      }
    } catch (SQLException e) {
      issues.error("Failed to get schema for table " + tableName + ": " + e.getMessage());
      return;
    }

    // Check if table exists and validate KEY field changes
    try (Connection conn = getConnection()) {
      if (tableExists(conn, database, tableName)) {
        log.debug("Table {} already exists in database {}", tableName, database);

        // Validate KEY field names haven't changed
        List<String> existingKeyFields = getExistingPrimaryKeyColumns(conn, database, tableName);
        if (!existingKeyFields.equals(newKeyFields)) {
          issues.error("Cannot modify KEY fields for table " + tableName
              + ". Existing keys: " + existingKeyFields + ", New keys: " + newKeyFields
              + ". KEY field changes are not supported.");
        }

        // Validate KEY field types haven't changed
        Map<String, ColumnInfo> existingColumns = getExistingColumns(conn, database, tableName);
        for (String keyField : newKeyFields) {
          ColumnInfo existingCol = existingColumns.get(keyField);
          if (existingCol != null && existingCol.isKey) {
            // Find the new type for this key field
            for (RelDataTypeField field : newRowType.getFieldList()) {
              String fieldName = field.getName();
              if (fieldName.startsWith(KEY_PREFIX)) {
                String columnName = fieldName.substring(KEY_PREFIX.length());
                if (columnName.equals(keyField)) {
                  String newType = toMySqlType(field);
                  String existingType = existingCol.type;
                  
                  // Normalize types for comparison (remove size for basic comparison)
                  String normalizedNew = newType.replaceAll("\\(.*?\\)", "");
                  String normalizedExisting = existingType.replaceAll("\\(.*?\\)", "");
                  
                  if (!normalizedNew.equalsIgnoreCase(normalizedExisting)) {
                    issues.error("Cannot modify KEY field type for table " + tableName
                        + ". KEY field '" + keyField + "' has existing type " + existingType
                        + " but attempted to change to " + newType
                        + ". KEY field type changes are not supported.");
                  }
                  break;
                }
              }
            }
          }
        }
      }
    } catch (SQLException e) {
      issues.error("Failed to validate table " + tableName + " in database " + database + ": " + e.getMessage());
    }
  }

  @Override
  public void create() throws SQLException {
    String tableName = source.table();
    String database = source.schema();

    if (database == null) {
      throw new SQLException("Database name is required for MySQL table " + tableName);
    }

    try (Connection conn = getConnection()) {
      // Ensure database exists first
      ensureDatabaseExists(conn, database);

      if (tableExists(conn, database, tableName)) {
        log.info("MySQL table {} already exists, skipping creation", tableName);
        return;
      }

      createTable(conn, database, tableName);
    } catch (SQLException e) {
      throw new SQLException("Failed to create table " + tableName + " in database " + database, e);
    }
  }

  @Override
  public void delete() throws SQLException {
    String tableName = source.table();
    String database = source.schema();

    try (Connection conn = getConnection()) {
      // Check if table exists first - make delete idempotent
      if (!tableExists(conn, database, tableName)) {
        log.info("Table {} does not exist in database {}, skipping deletion", tableName, database);
        return;
      }

      String dropTableSql = "DROP TABLE `" + escapeIdentifier(database) + "`.`" + escapeIdentifier(tableName) + "`";
      log.info("Deleting MySQL table {} from database {} with SQL: {}", tableName, database, dropTableSql);

      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(dropTableSql);
        log.info("Successfully deleted table {}", tableName);
      }

      // Check if database is now empty and drop it if so
      if (isDatabaseEmpty(conn, database)) {
        String dropDbSql = "DROP DATABASE `" + escapeIdentifier(database) + "`";
        log.info("Database {} is now empty, dropping it with SQL: {}", database, dropDbSql);
        try (Statement stmt = conn.createStatement()) {
          stmt.executeUpdate(dropDbSql);
          log.info("Successfully dropped empty database {}", database);
        }
      }

    } catch (SQLException e) {
      throw new SQLException("Failed to delete table " + tableName + " from database " + database, e);
    }
  }

  @Override
  public void update() throws SQLException {
    String tableName = source.table();
    String database = source.schema();

    if (database == null) {
      throw new SQLException("Database name is required for MySQL table " + tableName);
    }

    try (Connection conn = getConnection()) {
      // Ensure database exists first
      ensureDatabaseExists(conn, database);

      if (!tableExists(conn, database, tableName)) {
        // Table doesn't exist, create it
        log.info("Table {} does not exist. Creating in database {}", tableName, database);
        createTable(conn, database, tableName);
      } else {
        // Table exists, apply schema changes
        log.info("Table {} already exists in database {}. Checking for schema changes.", tableName, database);
        alterTable(conn, database, tableName);
      }
    } catch (SQLException e) {
      throw new SQLException("Failed to update table " + tableName + " in database " + database, e);
    }
  }

  @Override
  public List<String> specify() throws SQLException {
    return Collections.emptyList();
  }

  @Override
  public void restore() {
    if (!created) {
      return;
    }
    String tableName = source.table();
    log.warn("Rollback requested for table {}. The table may need to be rolled back manually.", tableName);
  }

  private Connection getConnection() throws SQLException {
    String url = properties.getProperty("url");
    String user = properties.getProperty("user", "");
    String password = properties.getProperty("password", "");

    if (url == null) {
      throw new SQLException("Missing required property 'url' for MySQL connection");
    }

    return DriverManager.getConnection(url, user, password);
  }

  /**
   * Represents a column in the database schema.
   */
  private static class ColumnInfo {
    final String name;
    final String type;
    final boolean nullable;
    final boolean isKey;

    ColumnInfo(String name, String type, boolean nullable, boolean isKey) {
      this.name = name;
      this.type = type;
      this.nullable = nullable;
      this.isKey = isKey;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ColumnInfo)) {
        return false;
      }
      ColumnInfo that = (ColumnInfo) o;
      return nullable == that.nullable && isKey == that.isKey
          && name.equals(that.name) && type.equalsIgnoreCase(that.type);
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }
  }

  /**
   * Validates that an identifier (database, table, or column name) is safe to use.
   * MySQL identifiers can contain alphanumeric characters, underscores, and dollar signs.
   */
  private boolean isValidIdentifier(String identifier) {
    if (identifier == null || identifier.isEmpty()) {
      return false;
    }
    // MySQL identifiers: alphanumeric, underscore, dollar sign, max 64 chars
    return identifier.matches("^[a-zA-Z0-9_$]{1,64}$");
  }

  /**
   * Escapes an identifier by wrapping in backticks and escaping any backticks within.
   */
  private String escapeIdentifier(String identifier) {
    // Escape backticks by doubling them
    return identifier.replace("`", "``");
  }

  /**
   * Ensures the database exists, creating it if necessary.
   */
  private void ensureDatabaseExists(Connection conn, String database) throws SQLException {
    if (!isValidIdentifier(database)) {
      throw new SQLException("Invalid database name: " + database);
    }
    String createDbSql = "CREATE DATABASE IF NOT EXISTS `" + escapeIdentifier(database) + "`";
    log.debug("Ensuring database exists: {}", createDbSql);

    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(createDbSql);
      log.info("Database {} is ready", database);
    }
  }

  /**
   * Alters an existing table to match the desired schema.
   */
  private void alterTable(Connection conn, String database, String tableName) throws SQLException {
    RelDataType newRowType = HoptimatorDriver.rowType(source, hoptimatorConnection);

    // Get existing schema
    Map<String, ColumnInfo> existingColumns = getExistingColumns(conn, database, tableName);

    // Build desired schema
    Map<String, ColumnInfo> desiredColumns = buildDesiredColumns(newRowType);

    // Detect changes
    List<String> alterStatements = new ArrayList<>();

    // Find columns to add
    for (Map.Entry<String, ColumnInfo> entry : desiredColumns.entrySet()) {
      String colName = entry.getKey();
      ColumnInfo desired = entry.getValue();

      if (!existingColumns.containsKey(colName)) {
        // New column - add it
        String addColSql = "ALTER TABLE `" + escapeIdentifier(database) + "`.`"
            + escapeIdentifier(tableName) + "` ADD COLUMN `" + escapeIdentifier(colName)
            + "` " + desired.type + (desired.nullable ? "" : " NOT NULL");
        alterStatements.add(addColSql);
        log.info("Will add column {} to table {}", colName, tableName);
      } else {
        // Existing column - check if it needs modification
        ColumnInfo existing = existingColumns.get(colName);
        if (!existing.equals(desired)) {
          // Column definition changed
          String modifyColSql = "ALTER TABLE `" + escapeIdentifier(database) + "`.`"
              + escapeIdentifier(tableName) + "` MODIFY COLUMN `" + escapeIdentifier(colName)
              + "` " + desired.type + (desired.nullable ? "" : " NOT NULL");
          alterStatements.add(modifyColSql);
          log.info("Will modify column {} in table {}", colName, tableName);
        }
      }
    }

    // Find columns to drop
    for (String existingCol : existingColumns.keySet()) {
      if (!desiredColumns.containsKey(existingCol)) {
        // Column removed
        String dropColSql = "ALTER TABLE `" + escapeIdentifier(database) + "`.`"
            + escapeIdentifier(tableName) + "` DROP COLUMN `" + escapeIdentifier(existingCol) + "`";
        alterStatements.add(dropColSql);
        log.info("Will drop column {} from table {}", existingCol, tableName);
      }
    }

    // Execute ALTER statements
    if (alterStatements.isEmpty()) {
      log.info("No schema changes needed for table {}", tableName);
    } else {
      try (Statement stmt = conn.createStatement()) {
        for (String alterSql : alterStatements) {
          log.info("Executing: {}", alterSql);
          stmt.executeUpdate(alterSql);
        }
        log.info("Successfully applied {} schema change(s) to table {}", alterStatements.size(), tableName);
      }
    }
  }

  /**
   * Gets existing columns from the database.
   */
  private Map<String, ColumnInfo> getExistingColumns(Connection conn, String database, String tableName)
      throws SQLException {
    Map<String, ColumnInfo> columns = new LinkedHashMap<>();
    DatabaseMetaData metaData = conn.getMetaData();

    // Get primary key columns
    Set<String> pkColumns = new HashSet<>();
    try (ResultSet rs = metaData.getPrimaryKeys(database, null, tableName)) {
      while (rs.next()) {
        pkColumns.add(rs.getString("COLUMN_NAME"));
      }
    }

    // Get all columns
    try (ResultSet rs = metaData.getColumns(database, null, tableName, null)) {
      while (rs.next()) {
        String colName = rs.getString("COLUMN_NAME");
        String colType = rs.getString("TYPE_NAME");
        int colSize = rs.getInt("COLUMN_SIZE");
        boolean nullable = rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
        boolean isKey = pkColumns.contains(colName);

        // Reconstruct type with size if applicable
        String fullType = colType;
        if ("VARCHAR".equalsIgnoreCase(colType) || "CHAR".equalsIgnoreCase(colType)) {
          fullType = colType + "(" + colSize + ")";
        }

        columns.put(colName, new ColumnInfo(colName, fullType, nullable, isKey));
      }
    }

    return columns;
  }

  /**
   * Gets existing primary key columns.
   */
  private List<String> getExistingPrimaryKeyColumns(Connection conn, String database, String tableName)
      throws SQLException {
    List<String> pkColumns = new ArrayList<>();
    DatabaseMetaData metaData = conn.getMetaData();

    try (ResultSet rs = metaData.getPrimaryKeys(database, null, tableName)) {
      while (rs.next()) {
        pkColumns.add(rs.getString("COLUMN_NAME"));
      }
    }

    return pkColumns;
  }

  /**
   * Builds desired column map from RelDataType.
   */
  private Map<String, ColumnInfo> buildDesiredColumns(RelDataType rowType) throws SQLException {
    Map<String, ColumnInfo> columns = new LinkedHashMap<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      String fieldName = field.getName();
      String columnName;
      boolean isKey;

      if (fieldName.startsWith(KEY_PREFIX)) {
        columnName = fieldName.substring(KEY_PREFIX.length());
        isKey = true;
      } else {
        columnName = fieldName;
        isKey = false;
      }

      if (!isValidIdentifier(columnName)) {
        throw new SQLException("Invalid column name: " + columnName);
      }

      String mysqlType = toMySqlType(field);
      boolean nullable = field.getType().isNullable();

      columns.put(columnName, new ColumnInfo(columnName, mysqlType, nullable, isKey));
    }

    return columns;
  }

  /**
   * Builds CREATE TABLE SQL from the source schema, parsing KEY_ fields as primary keys.
   */
  private String buildCreateTableSql(String database, String tableName) throws SQLException {
    if (!isValidIdentifier(database)) {
      throw new SQLException("Invalid database name: " + database);
    }
    if (!isValidIdentifier(tableName)) {
      throw new SQLException("Invalid table name: " + tableName);
    }

    RelDataType rowType = HoptimatorDriver.rowType(source, hoptimatorConnection);
    List<String> keyFields = parseKeyFields(rowType);

    if (keyFields.isEmpty()) {
      throw new SQLException("No KEY_ fields found in table " + tableName + ". At least one KEY_ field is required.");
    }

    StringBuilder sql = new StringBuilder();
    sql.append("CREATE TABLE `").append(escapeIdentifier(database)).append("`.`")
       .append(escapeIdentifier(tableName)).append("` (");

    List<String> columnDefs = new ArrayList<>();

    // Process all fields
    for (RelDataTypeField field : rowType.getFieldList()) {
      String fieldName = field.getName();
      String columnName;

      if (fieldName.startsWith(KEY_PREFIX)) {
        // This is a key field - strip prefix and add to columns
        columnName = fieldName.substring(KEY_PREFIX.length());
      } else {
        // Regular column
        columnName = fieldName;
      }

      if (!isValidIdentifier(columnName)) {
        throw new SQLException("Invalid column name: " + columnName);
      }

      String mysqlType = toMySqlType(field);
      String nullable = field.getType().isNullable() ? "" : " NOT NULL";
      columnDefs.add("`" + escapeIdentifier(columnName) + "` " + mysqlType + nullable);
    }

    sql.append(String.join(", ", columnDefs));

    // Add PRIMARY KEY constraint
    if (!keyFields.isEmpty()) {
      String keyList = keyFields.stream()
          .map(k -> "`" + escapeIdentifier(k) + "`")
          .collect(Collectors.joining(", "));
      sql.append(", PRIMARY KEY (").append(keyList).append(")");
    }

    sql.append(")");

    return sql.toString();
  }

  /**
   * Checks if a table exists in the database.
   */
  private boolean tableExists(Connection conn, String database, String tableName) throws SQLException {
    DatabaseMetaData metaData = conn.getMetaData();
    try (ResultSet rs = metaData.getTables(database, null, tableName, new String[]{"TABLE"})) {
      return rs.next();
    }
  }

  /**
   * Checks if a database has no tables.
   */
  private boolean isDatabaseEmpty(Connection conn, String database) throws SQLException {
    DatabaseMetaData metaData = conn.getMetaData();
    try (ResultSet rs = metaData.getTables(database, null, null, new String[]{"TABLE"})) {
      return !rs.next(); // Empty if no tables found
    }
  }

  /**
   * Creates a new MySQL table.
   */
  private void createTable(Connection conn, String database, String tableName) throws SQLException {
    String createTableSql = buildCreateTableSql(database, tableName);
    log.info("Creating MySQL table {} in database {} with SQL: {}", tableName, database, createTableSql);

    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(createTableSql);
      created = true;
      log.info("Successfully created table {}", tableName);
    }
  }
}
