package com.linkedin.hoptimator.kafka;

import com.linkedin.hoptimator.jdbc.CalciteDriver;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientException;
import java.util.Properties;

import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;


/** JDBC driver for Kafka topics. */
public class KafkaDriver extends CalciteDriver {

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
    Properties properties = new Properties();
    properties.putAll(props); // in case the driver is loaded via getConnection()
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
      rootSchema.add("KAFKA", schema);
      return connection;
    } catch (IOException e) {
      throw new SQLTransientException("Problem loading " + url, e);
    } catch (Exception e) {
      throw new SQLNonTransientException("Problem loading " + url, e);
    }
  }
}
