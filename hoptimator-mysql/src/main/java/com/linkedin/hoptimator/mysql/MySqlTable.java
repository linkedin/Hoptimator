package com.linkedin.hoptimator.mysql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A table from a MySQL database. */
public class MySqlTable extends AbstractTable {

  private static final Logger log = LoggerFactory.getLogger(MySqlTable.class);

  private final String database;
  private final String table;
  private final Properties properties;

  public MySqlTable(String database, String table, Properties properties) {
    this.database = database;
    this.table = table;
    this.properties = properties;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

    // Infer schema from MySQL database metadata
    try (Connection conn = DriverManager.getConnection(
        properties.getProperty("url"),
        properties.getProperty("user", ""),
        properties.getProperty("password", ""))) {

      DatabaseMetaData metaData = conn.getMetaData();
      try (ResultSet rs = metaData.getColumns(database, null, table, null)) {
        while (rs.next()) {
          String columnName = rs.getString("COLUMN_NAME");
          int sqlType = rs.getInt("DATA_TYPE");
          int nullable = rs.getInt("NULLABLE");

          SqlTypeName typeName = jdbcTypeToSqlType(sqlType);
          builder.add(columnName, typeName);
          if (nullable == DatabaseMetaData.columnNullable) {
            builder.nullable(true);
          }
        }
      }
    } catch (SQLException e) {
      log.error("Failed to infer schema for table {}.{}", database, table, e);
      // Return a minimal schema with a single VARCHAR column as fallback
      return builder.add("ERROR", SqlTypeName.VARCHAR).build();
    }

    // MySQL schemas are already flat
    return builder.build();
  }

  // TODO: Make this comprehensive by following https://dev.mysql.com/doc/connector-j/en/connector-j-reference-type-conversions.html
  private SqlTypeName jdbcTypeToSqlType(int jdbcType) {
    // Map JDBC types to Calcite SQL types
    switch (jdbcType) {
      case java.sql.Types.CHAR:
      case java.sql.Types.VARCHAR:
      case java.sql.Types.LONGVARCHAR:
        return SqlTypeName.VARCHAR;
      case java.sql.Types.NUMERIC:
      case java.sql.Types.DECIMAL:
        return SqlTypeName.DECIMAL;
      case java.sql.Types.BIT:
      case java.sql.Types.BOOLEAN:
        return SqlTypeName.BOOLEAN;
      case java.sql.Types.TINYINT:
        return SqlTypeName.TINYINT;
      case java.sql.Types.SMALLINT:
        return SqlTypeName.SMALLINT;
      case java.sql.Types.INTEGER:
        return SqlTypeName.INTEGER;
      case java.sql.Types.BIGINT:
        return SqlTypeName.BIGINT;
      case java.sql.Types.REAL:
        return SqlTypeName.REAL;
      case java.sql.Types.FLOAT:
      case java.sql.Types.DOUBLE:
        return SqlTypeName.DOUBLE;
      case java.sql.Types.BINARY:
      case java.sql.Types.VARBINARY:
      case java.sql.Types.LONGVARBINARY:
        return SqlTypeName.VARBINARY;
      case java.sql.Types.DATE:
        return SqlTypeName.DATE;
      case java.sql.Types.TIME:
        return SqlTypeName.TIME;
      case java.sql.Types.TIMESTAMP:
        return SqlTypeName.TIMESTAMP;
      default:
        log.warn("Unknown JDBC type {} for table {}.{}, defaulting to VARCHAR",
            jdbcType, database, table);
        return SqlTypeName.VARCHAR;
    }
  }
}
