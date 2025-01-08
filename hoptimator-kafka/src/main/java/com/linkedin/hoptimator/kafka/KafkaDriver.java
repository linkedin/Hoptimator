package com.linkedin.hoptimator.kafka;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.schema.SchemaPlus;

import com.linkedin.hoptimator.util.ConfigService;


/** JDBC driver for Kafka topics. */
public class KafkaDriver extends Driver {

  static {
    new KafkaDriver().register();
  }

  @Override
  protected String getConnectStringPrefix() {
    return "jdbc:kafka://";
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(this.getClass(), "kafka.properties", "kafka", "0", "kafka", "0");
  }

  @Override
  public Connection connect(String url, Properties props) throws SQLException {
    if (!url.startsWith(getConnectStringPrefix())) {
      return null;
    }
    // Connection string properties are given precedence over config properties
    Properties properties = ConfigService.config();
    properties.putAll(ConnectStringParser.parse(url.substring(getConnectStringPrefix().length())));
    try {
      Connection connection = super.connect(url, props);
      if (connection == null) {
        throw new IOException("Could not connect to " + url);
      }
      connection.setAutoCommit(true); // to prevent rollback()
      CalciteConnection calciteConnection = (CalciteConnection) connection;
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      ClusterSchema schema = new ClusterSchema(properties);
      schema.populate();
      rootSchema.add("KAFKA", schema);
      return connection;
    } catch (Exception e) {
      throw new SQLException("Problem loading " + url, e);
    }
  }
}
