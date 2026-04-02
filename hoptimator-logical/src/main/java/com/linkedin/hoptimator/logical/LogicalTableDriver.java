package com.linkedin.hoptimator.logical;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientConnectionException;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import com.linkedin.hoptimator.jdbc.CalciteDriver;
import com.linkedin.hoptimator.k8s.K8sContext;


/**
 * JDBC driver for the Logical Table abstraction.
 *
 * <p>URL format:
 * <pre>jdbc:logical://nearline=xinfra-tracking;offline=openhouse;online=venice</pre>
 *
 * <p>The schema name under which the {@link LogicalTableSchema} is registered must be
 * provided via the {@code schema} connection property (set by the operator when it
 * reads the Database CRD). If not provided, it defaults to {@code "LOGICAL"}.
 */
public class LogicalTableDriver extends CalciteDriver {

  public static final String CONNECT_STRING_PREFIX = "jdbc:logical://";
  static final String SCHEMA_PROPERTY = "schema";
  static final String DEFAULT_SCHEMA_NAME = "LOGICAL";

  static {
    new LogicalTableDriver().register();
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(this.getClass(), "logical.properties", "logical", "0", "logical", "0");
  }

  @Override
  public Connection connect(String url, Properties props) throws SQLException {
    if (!url.startsWith(getConnectStringPrefix())) {
      return null;
    }

    // Merge URL params into props; URL params take precedence.
    Properties properties = new Properties();
    properties.putAll(props);
    properties.putAll(ConnectStringParser.parse(url.substring(getConnectStringPrefix().length())));

    // Parse the tier map from the URL.
    Map<String, String> tierMap = LogicalTableUrlParser.tierMap(url);

    // Determine the schema name from connection properties (set by the operator).
    String schemaName = properties.getProperty(SCHEMA_PROPERTY, DEFAULT_SCHEMA_NAME);

    try {
      Connection connection = super.connect(url, props);
      if (connection == null) {
        throw new IOException("Could not connect to " + url);
      }
      connection.setAutoCommit(true); // prevent rollback()

      CalciteConnection calciteConnection = (CalciteConnection) connection;
      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      K8sContext context = K8sContext.create(connection);
      LogicalTableSchema logicalSchema = new LogicalTableSchema(tierMap, context, schemaName);
      rootSchema.add(schemaName, logicalSchema);

      return connection;
    } catch (IOException e) {
      throw new SQLTransientConnectionException("Problem loading " + url, e);
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLNonTransientException("Problem loading " + url, e);
    }
  }
}
