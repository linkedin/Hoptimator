package com.linkedin.hoptimator.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;

import com.linkedin.hoptimator.util.DelegatingConnection;


public class HoptimatorConnection extends DelegatingConnection {

  private final CalciteConnection connection;
  private final Properties connectionProperties;
  private final List<RelOptMaterialization> materializations = new ArrayList<>();

  public HoptimatorConnection(CalciteConnection connection, Properties connectionProperties) {
    super(connection);
    this.connection = connection;
    this.connectionProperties = connectionProperties;
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
}
