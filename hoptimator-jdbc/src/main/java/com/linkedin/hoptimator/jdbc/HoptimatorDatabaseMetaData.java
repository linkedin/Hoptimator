package com.linkedin.hoptimator.jdbc;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.jdbc.CalciteMetaImpl;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.util.Pair;
import org.apache.commons.dbcp2.DelegatingDatabaseMetaData;


public class HoptimatorDatabaseMetaData extends DelegatingDatabaseMetaData {

  private final HoptimatorConnection connection;

  private static final String CATALOG_QUERY = "SELECT CATALOG AS TABLE_CAT FROM \"k8s\".databases WHERE CATALOG IS NOT NULL";
  private static final String SCHEMA_QUERY = "SELECT CATALOG AS TABLE_CATALOG, SCHEMA AS TABLE_SCHEM FROM \"k8s\".databases";

  /** The columns returned by {@link DatabaseMetaData#getCatalogs}. */
  private static final List<Pair<String, Integer>> CATALOG_COLUMNS =
      ImmutableList.of(
          new Pair<>("TABLE_CAT", Types.VARCHAR));

  /** The columns returned by {@link DatabaseMetaData#getSchemas}. */
  private static final List<Pair<String, Integer>> SCHEMA_COLUMNS =
      ImmutableList.of(
          new Pair<>("TABLE_SCHEM", Types.VARCHAR),
          new Pair<>("TABLE_CATALOG", Types.VARCHAR));

  /** The columns returned by {@link DatabaseMetaData#getTables}. */
  private static final List<Pair<String, Integer>> TABLE_COLUMNS =
      ImmutableList.of(
          new Pair<>("TABLE_CAT", Types.VARCHAR),
          new Pair<>("TABLE_SCHEM", Types.VARCHAR),
          new Pair<>("TABLE_NAME", Types.VARCHAR),
          new Pair<>("TABLE_TYPE", Types.VARCHAR),
          new Pair<>("REMARKS", Types.VARCHAR),
          new Pair<>("TYPE_CAT", Types.VARCHAR),
          new Pair<>("TYPE_SCHEM", Types.VARCHAR),
          new Pair<>("TYPE_NAME", Types.VARCHAR),
          new Pair<>("SELF_REFERENCING_COL_NAME", Types.VARCHAR),
          new Pair<>("REF_GENERATION", Types.VARCHAR));

  /** The columns returned by {@link DatabaseMetaData#getColumns}. */
  private static final List<Pair<String, Integer>> COLUMN_COLUMNS =
      ImmutableList.of(
          new Pair<>("TABLE_CAT", Types.VARCHAR),
          new Pair<>("TABLE_SCHEM", Types.VARCHAR),
          new Pair<>("TABLE_NAME", Types.VARCHAR),
          new Pair<>("COLUMN_NAME", Types.VARCHAR),
          new Pair<>("DATA_TYPE", Types.INTEGER),
          new Pair<>("TYPE_NAME", Types.VARCHAR),
          new Pair<>("COLUMN_SIZE", Types.INTEGER),
          new Pair<>("BUFFER_LENGTH", Types.INTEGER),
          new Pair<>("DECIMAL_DIGITS", Types.INTEGER),
          new Pair<>("NUM_PREC_RADIX", Types.INTEGER),
          new Pair<>("NULLABLE", Types.INTEGER),
          new Pair<>("REMARKS", Types.VARCHAR),
          new Pair<>("COLUMN_DEF", Types.VARCHAR),
          new Pair<>("SQL_DATA_TYPE", Types.INTEGER),
          new Pair<>("SQL_DATETIME_SUB", Types.INTEGER),
          new Pair<>("CHAR_OCTET_LENGTH", Types.INTEGER),
          new Pair<>("ORDINAL_POSITION", Types.INTEGER),
          new Pair<>("IS_NULLABLE", Types.VARCHAR),
          new Pair<>("SCOPE_CATALOG", Types.VARCHAR),
          new Pair<>("SCOPE_SCHEMA", Types.VARCHAR),
          new Pair<>("SCOPE_TABLE", Types.VARCHAR),
          new Pair<>("SOURCE_DATA_TYPE", Types.SMALLINT),
          new Pair<>("IS_AUTOINCREMENT", Types.VARCHAR),
          new Pair<>("IS_GENERATEDCOLUMN", Types.VARCHAR));

  /**
   * Constructs a new instance for the given delegating connection and database metadata.
   *
   * @param connection       the delegating connection
   * @param databaseMetaData the database metadata
   */
  public HoptimatorDatabaseMetaData(HoptimatorConnection connection, DatabaseMetaData databaseMetaData) {
    super(new org.apache.commons.dbcp2.DelegatingConnection<>(connection), databaseMetaData);
    this.connection = connection;
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    List<List<Object>> results = new ArrayList<>();
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(CATALOG_QUERY)) {
      while (rs.next()) {
        results.add(Collections.singletonList(rs.getString("TABLE_CAT")));
      }
    }
    return new HoptimatorResultSet(results, CATALOG_COLUMNS);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(SCHEMA_QUERY)) {
      return getSchemas(rs, null);
    }
  }

  @Override
  public ResultSet getSchemas(@Nullable String catalog, @Nullable final String schemaPattern) throws SQLException {
    StringBuilder query = new StringBuilder(SCHEMA_QUERY);
    if (catalog != null && catalog.isEmpty()) {
      query.append(" WHERE CATALOG IS NOT NULL");
    } else if (catalog != null) {
      query.append(" WHERE CATALOG = ?");
    }

    try (PreparedStatement pStmt = connection.prepareStatement(query.toString())) {
      if (catalog != null) {
        pStmt.setString(1, catalog);
      }

      try (ResultSet rs = pStmt.executeQuery()) {
        return getSchemas(rs, schemaPattern);
      }
    }
  }

  private HoptimatorResultSet getSchemas(ResultSet rs, @Nullable String schemaPattern) throws SQLException {
    List<List<Object>> results = new ArrayList<>();
    while (rs.next()) {
      String catalog = rs.getString("TABLE_CATALOG");
      boolean catalogNull = rs.wasNull();
      String schema = rs.getString("TABLE_SCHEM");
      boolean schemaNull = rs.wasNull();

      LikePattern likePattern;
      Pattern regexPattern;
      if (schemaPattern == null) {
        likePattern = LikePattern.any(); // Matches everything, same as null schema
        regexPattern = null;
      } else {
        likePattern = new LikePattern(schemaPattern);
        regexPattern = LikePattern.likeToRegex(schemaPattern);
      }
      if (!catalogNull && schemaNull) {
        Schema catalogSchema = connection.calciteConnection().getRootSchema().subSchemas().get(catalog);
        if (catalogSchema != null) {
          for (String schemaName : catalogSchema.subSchemas().getNames(likePattern)) {
            List<Object> result = Arrays.asList(
                schemaName, // TABLE_SCHEM
                catalog     // TABLE_CATALOG
            );
            results.add(result);
          }
        }
      } else {
        if (regexPattern != null && !regexPattern.matcher(schema).matches()) {
          continue;
        }
        List<Object> result = Arrays.asList(
            schema, // TABLE_SCHEM
            catalog // TABLE_CATALOG
        );
        results.add(result);
      }
    }
    return new HoptimatorResultSet(results, SCHEMA_COLUMNS);
  }

  @Override
  public ResultSet getTables(@Nullable String catalog, @Nullable String schemaPattern, @Nullable String tableNamePattern,
      @Nullable String[] types) throws SQLException {
    List<List<Object>> results = new ArrayList<>();
    try (HoptimatorResultSet rs = (HoptimatorResultSet) getSchemas(catalog, schemaPattern)) {
      while (rs.next()) {
        String catalogRs = rs.getString("TABLE_CATALOG");
        boolean catalogNull = rs.wasNull();
        String schemaRs = rs.getString("TABLE_SCHEM");
        boolean schemaNull = rs.wasNull();

        Schema schema = connection.calciteConnection().getRootSchema();
        if (!catalogNull) {
          schema = Objects.requireNonNull(schema.subSchemas().get(catalogRs));
        }
        if (!schemaNull) {
          schema = Objects.requireNonNull(schema.subSchemas().get(schemaRs));
        }

        LikePattern likePattern = tableNamePattern == null ? LikePattern.any() : new LikePattern(tableNamePattern);
        for (String tableName : schema.tables().getNames(likePattern)) {
          Table table = Objects.requireNonNull(schema.tables().get(tableName));
          if (types != null && types.length > 0) {
            boolean found = false;
            for (String type : types) {
              if (type.equals(table.getJdbcTableType().jdbcName)) {
                found = true;
                break;
              }
            }
            if (!found) {
              continue;
            }
          }
          List<Object> result = Arrays.asList(
              catalogRs,                          // TABLE_CAT
              schemaRs,                           // TABLE_SCHEM
              tableName,                          // TABLE_NAME
              table.getJdbcTableType().jdbcName,  // TABLE_TYPE
              null,                               // REMARKS
              null,                               // TYPE_CAT
              null,                               // TYPE_SCHEM
              null,                               // TYPE_NAME
              null,                               // SELF_REFERENCING_COL_NAME
              null                                // REF_GENERATION
          );
          results.add(result);
        }
      }
    }

    return new HoptimatorResultSet(results, TABLE_COLUMNS);
  }

  @Override
  public ResultSet getColumns(@Nullable String catalog, @Nullable String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException {
    List<List<Object>> results = new ArrayList<>();
    try (HoptimatorResultSet rs = (HoptimatorResultSet) getTables(catalog, schemaPattern, tableNamePattern, null)) {
      while (rs.next()) {
        String catalogRs = rs.getString("TABLE_CAT");
        boolean catalogNull = rs.wasNull();
        String schemaRs = rs.getString("TABLE_SCHEM");
        boolean schemaNull = rs.wasNull();
        String tableName = rs.getString("TABLE_NAME");

        Schema schema = connection.calciteConnection().getRootSchema();
        if (!catalogNull) {
          schema = Objects.requireNonNull(schema.subSchemas().get(catalogRs));
        }
        if (!schemaNull) {
          schema = Objects.requireNonNull(schema.subSchemas().get(schemaRs));
        }
        Table table = Objects.requireNonNull(schema.tables().get(tableName));
        CalciteMetaImpl.CalciteMetaTable metaTable = new CalciteMetaImpl.CalciteMetaTable(table, catalogRs, schemaRs, tableName);
        CalciteMetaImpl metaImpl = CalciteMetaImpl.create(connection.calciteConnection());

        List<MetaImpl.MetaColumn> metaColumns = metaImpl.columns(metaTable)
            .where(v -> LikePattern.matcher(columnNamePattern).apply(v.getName()))
            .toList();
        for (MetaImpl.MetaColumn metaColumn : metaColumns) {
          List<Object> result = Arrays.asList(
              metaColumn.tableCat,          // TABLE_CAT
              metaColumn.tableSchem,        // TABLE_SCHEM
              metaColumn.tableName,         // TABLE_NAME
              metaColumn.columnName,        // COLUMN_NAME
              metaColumn.dataType,          // DATA_TYPE
              metaColumn.typeName,          // TYPE_NAME
              metaColumn.columnSize,        // COLUMN_SIZE
              metaColumn.bufferLength,      // BUFFER_LENGTH
              metaColumn.decimalDigits,     // DECIMAL_DIGITS
              metaColumn.numPrecRadix,      // NUM_PREC_RADIX
              metaColumn.nullable,          // NULLABLE
              metaColumn.remarks,           // REMARKS
              metaColumn.columnDef,         // COLUMN_DEF
              metaColumn.sqlDataType,       // SQL_DATA_TYPE
              metaColumn.sqlDatetimeSub,    // SQL_DATETIME_SUB
              metaColumn.charOctetLength,   // CHAR_OCTET_LENGTH
              metaColumn.ordinalPosition,   // ORDINAL_POSITION
              metaColumn.isNullable,        // IS_NULLABLE
              metaColumn.scopeCatalog,      // SCOPE_CATALOG
              metaColumn.scopeSchema,       // SCOPE_SCHEMA
              metaColumn.scopeTable,        // SCOPE_TABLE
              metaColumn.sourceDataType,    // SOURCE_DATA_TYPE
              metaColumn.isAutoincrement,   // IS_AUTOINCREMENT
              metaColumn.isGeneratedcolumn  // IS_GENERATEDCOLUMN
          );
          results.add(result);
        }
      }
    }
    return new HoptimatorResultSet(results, COLUMN_COLUMNS);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }
}
