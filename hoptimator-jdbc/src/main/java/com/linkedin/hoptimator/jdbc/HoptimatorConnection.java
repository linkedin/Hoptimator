package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.util.ConnectionService;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;

import com.linkedin.hoptimator.avro.AvroConverter;
import com.linkedin.hoptimator.util.DelegatingConnection;
import org.apache.calcite.util.Util;


public class HoptimatorConnection extends DelegatingConnection {

  private final CalciteConnection connection;
  private final Properties connectionProperties;
  private final List<RelOptMaterialization> materializations = new ArrayList<>();

  public HoptimatorConnection(CalciteConnection connection, Properties connectionProperties) {
    super(connection);
    this.connection = connection;
    this.connectionProperties = connectionProperties;
  }

  public ResolvedTable resolve(List<String> tablePath, Map<String, String> hints) throws SQLException {
    try {
      String tableSql = "SELECT * FROM " + tablePath.stream()
          .map(x -> "\"" + x + "\"")
          .collect(Collectors.joining("."));
      RelNode tableRel = HoptimatorDriver.convert(this, tableSql).root.rel;
      String namespace = "com.linkedin."
            + tablePath.stream()
            .map(x -> x.toLowerCase(Locale.ROOT))
            .limit(tablePath.size() - 1)
            .collect(Collectors.joining("."));
      String schemaName = tablePath.get(tablePath.size() - 1).toLowerCase(Locale.ROOT);
      Schema avroSchema = AvroConverter.avro(namespace, schemaName, tableRel.getRowType());
      String database = databaseName(this.createPrepareContext(), tablePath);
      Source source = new Source(database, tablePath, hints);
      Sink sink = new Sink(database, tablePath, hints);
      return new ResolvedTable(tablePath, avroSchema, ConnectionService.configure(source, this),
          ConnectionService.configure(sink, this));
    } catch (Exception e) {
      throw new SQLException("Failed to resolve " + String.join(".", tablePath) + ": " + e.getMessage(), e);
    }
  }

  @Override
  public Statement createStatement() throws SQLException {
    return connection.createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return connection.prepareStatement(sql);
  }

  public Properties connectionProperties() {
    return connectionProperties;
  }

  public CalcitePrepare.Context createPrepareContext() {
    return connection.createPrepareContext();
  }

  public CalciteConnection calciteConnection() {
    return connection;
  }

  public HoptimatorConnection withProperties(Properties properties) {
    return new HoptimatorConnection(connection, properties);
  }

  public void registerMaterialization(List<String> viewPath, String querySql) {
    String tableSql = "SELECT * FROM " + viewPath.stream().map(x -> "\"" + x + "\"").collect(Collectors.joining("."));
    RelNode tableRel = HoptimatorDriver.convert(this, tableSql).root.rel;
    RelNode queryRel = HoptimatorDriver.convert(this, querySql).root.rel;
    registerMaterialization(viewPath, tableRel, queryRel);
  }

  private void registerMaterialization(List<String> viewPath, RelNode tableRel, RelNode queryRel) {
    materializations.add(new RelOptMaterialization(tableRel, queryRel, null, viewPath));
  }

  public List<RelOptMaterialization> materializations() {
    return materializations;
  }

  private static String databaseName(CalcitePrepare.Context context, List<String> tablePath) throws SQLException {
    final List<String> path = Util.skipLast(tablePath);
    CalciteSchema schema = context.getRootSchema();
    for (String p : path) {
      schema = Objects.requireNonNull(schema).getSubSchema(p, true);
    }
    if (schema == null || !(schema.schema instanceof Database)) {
      throw new SQLException(tablePath + " is not a physical database.");
    }
    return ((Database) schema.schema).databaseName();
  }
}
