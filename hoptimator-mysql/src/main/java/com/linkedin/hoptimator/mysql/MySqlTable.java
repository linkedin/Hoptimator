package com.linkedin.hoptimator.mysql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;


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
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
        return SqlTypeName.VARCHAR;
      case Types.NUMERIC:
      case Types.DECIMAL:
        return SqlTypeName.DECIMAL;
      case Types.BIT:
      case Types.BOOLEAN:
        return SqlTypeName.BOOLEAN;
      case Types.TINYINT:
        return SqlTypeName.TINYINT;
      case Types.SMALLINT:
        return SqlTypeName.SMALLINT;
      case Types.INTEGER:
        return SqlTypeName.INTEGER;
      case Types.BIGINT:
        return SqlTypeName.BIGINT;
      case Types.REAL:
        return SqlTypeName.REAL;
      case Types.FLOAT:
      case Types.DOUBLE:
        return SqlTypeName.DOUBLE;
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return SqlTypeName.VARBINARY;
      case Types.DATE:
        return SqlTypeName.DATE;
      case Types.TIME:
        return SqlTypeName.TIME;
      case Types.TIMESTAMP:
        return SqlTypeName.TIMESTAMP;
      default:
        log.warn("Unknown JDBC type {} for table {}.{}, defaulting to VARCHAR",
            jdbcType, database, table);
        return SqlTypeName.VARCHAR;
    }
  }
}
