package com.linkedin.hoptimator.kafka;

import com.linkedin.hoptimator.jdbc.CalciteDriver;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientException;
import java.util.Properties;


/** JDBC driver for Kafka topics. */
public class KafkaDriver extends CalciteDriver {

  public static final String CONNECTION_PREFIX = "jdbc:kafka://";

  static {
    new KafkaDriver().register();
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECTION_PREFIX;
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(this.getClass(), "kafka.properties", "kafka", "0", "kafka", "0");
  }

  @Override
  protected void configureConnection(CalciteConnection connection, String url, Properties properties) throws SQLException {
    connection.setAutoCommit(true); // to prevent rollback()
    SchemaPlus rootSchema = connection.getRootSchema();
    ClusterSchema schema = createClusterSchema(properties);
    rootSchema.add("KAFKA", schema);
  }

  @Override
  protected SQLException connectionException(String url, Exception failure) {
    if (failure instanceof IOException) {
      return new SQLTransientException("Problem loading " + url, failure);
    }
    return new SQLNonTransientException("Problem loading " + url, failure);
  }

  protected ClusterSchema createClusterSchema(Properties properties) {
    return new ClusterSchema(properties);
  }
}
