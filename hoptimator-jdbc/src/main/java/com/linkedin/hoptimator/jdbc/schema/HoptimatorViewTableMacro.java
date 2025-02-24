package com.linkedin.hoptimator.jdbc.schema;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This file is copy-pasted from {@link ViewTableMacro} with the only modification being
 * how the connection is instantiated.
 */
public class HoptimatorViewTableMacro extends ViewTableMacro {

  private final Boolean modifiable;
  public HoptimatorViewTableMacro(CalciteSchema schema, String viewSql,
      @Nullable List<String> schemaPath, @Nullable List<String> viewPath,
      @Nullable Boolean modifiable) {
    super(schema, viewSql, schemaPath, viewPath, modifiable);
    this.modifiable = modifiable;
  }

  public TranslatableTable apply(Properties properties) {
    CalciteConnection connection;
    try {
      connection = DriverManager.getConnection("jdbc:calcite:", properties)
          .unwrap(CalciteConnection.class);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    CalcitePrepare.AnalyzeViewResult parsed =
        Schemas.analyzeView(connection, schema, schemaPath, viewSql, viewPath,
            modifiable != null && modifiable);
    final List<String> schemaPath1 =
        schemaPath != null ? schemaPath : schema.path(null);
    if ((modifiable == null || modifiable)
        && parsed.modifiable
        && parsed.table != null) {
      return modifiableViewTable(parsed, viewSql, schemaPath1, viewPath, schema);
    } else {
      return viewTable(parsed, viewSql, schemaPath1, viewPath);
    }
  }
}
