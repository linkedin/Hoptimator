package com.linkedin.hoptimator.logical;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientConnectionException;
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
  /** Connection property that hints which tier to resolve for pipeline planning (e.g. "nearline", "online"). */
  public static final String TIER_PROPERTY = "tier";
  /** K8s label key identifying which logical database a LogicalTable CRD belongs to. */
  public static final String DATABASE_LABEL = "logical-database";

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

    // Validate tier count: a logical table must span at least 2 tiers.
    long tierCount = properties.stringPropertyNames().stream()
        .filter(LogicalTier::isTier).count();
    if (tierCount < 2) {
      throw new SQLNonTransientException("Logical database URL must declare at least 2 tiers "
          + "(e.g. jdbc:logical://nearline=kafka-database;online=venice) but only "
          + tierCount + " tier(s) found in: " + url);
    }

    // The database name (Database CRD metadata.name) is injected by K8sDatabaseTable.
    // It is used as the label filter in LogicalTableSchema and matches source.database()
    // in deployer/provider contexts. The Calcite schema registration name is handled by
    // the outer catalog (K8sDatabaseTable.addDatabases) — not by this driver.
    String databaseName = properties.getProperty("database");
    if (databaseName == null || databaseName.isEmpty()) {
      throw new SQLNonTransientException(
          "Missing 'database' property in logical database URL. "
          + "Ensure the Database CRD has metadata.name set (injected by K8sDatabaseTable).");
    }

    try {
      Connection connection = super.connect(url, props);
      if (connection == null) {
        throw new IOException("Could not connect to " + url);
      }
      connection.setAutoCommit(true); // prevent rollback()

      CalciteConnection calciteConnection = (CalciteConnection) connection;
      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      K8sContext context = K8sContext.create(connection);
      LogicalTableSchema logicalSchema = new LogicalTableSchema(properties, context, databaseName);
      rootSchema.add(databaseName, logicalSchema);

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
