package com.linkedin.hoptimator.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.util.Properties;
import java.util.logging.LogManager;

import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.Catalog;


/** Driver for :jdbc:hoptimator:// connections. */
public class HoptimatorDriver implements java.sql.Driver {
  private static final Logger logger = LoggerFactory.getLogger(HoptimatorDriver.class);
  private static final HoptimatorDriver INSTANCE = new HoptimatorDriver();

  public static final String CONNECTION_PREFIX = "jdbc:hoptimator://";

  static {
    {
      try {
        DriverManager.registerDriver(INSTANCE);
      } catch (SQLException e) {
        throw new RuntimeException("Failed to register Hoptimator driver.", e);
      }
    }
  }

  public static CalcitePrepare.ConvertResult convert(HoptimatorConnection conn, String sql) {
    CalcitePrepare.Context context = conn.createPrepareContext();
    return new Prepare(conn).convert(context, sql);
  }

  public static CalcitePrepare.AnalyzeViewResult analyzeView(HoptimatorConnection conn, String sql)  {
    CalcitePrepare.Context context = conn.createPrepareContext();
    return new Prepare(conn).analyzeView(context, sql, false);
  }

  @Override
  public boolean acceptsURL(String url) {
    return url.startsWith(CONNECTION_PREFIX);
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 1;
  }

  @Override
  public java.util.logging.Logger getParentLogger() {
    return LogManager.getLogManager().getLogger(logger.getName());
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return new DriverPropertyInfo[0];
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Connection connect(String url, Properties props) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    try {
      // Load properties from the URL and from getConnection()'s properties.
      // URL properties take precedence.
      Properties properties = new Properties();
      properties.putAll(props); // via getConnection()
      properties.putAll(ConnectStringParser.parse(url.substring(CONNECTION_PREFIX.length())));

      // For [Calcite]Driver.connect() to work, we need [Calcite]Driver.createPrepare()
      // to return our Prepare. But our Prepare requires a HoptimatorConnection, which
      // we cannot construct yet.
      ConnectionHolder holder = new ConnectionHolder();
      Connection connection = new Driver().withPrepareFactory(() -> new Prepare(holder))
          .connect("jdbc:calcite:", properties);
      if (connection == null) {
        throw new IOException("Could not connect to " + url + ": Could not create Calcite connection.");
      }
      CalciteConnection calciteConnection = (CalciteConnection) connection;
      calciteConnection.setAutoCommit(true); // to prevent rollback()
      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // built-in schemas
      rootSchema.add("DEFAULT", new AbstractSchema());

      calciteConnection.setSchema("DEFAULT");

      HoptimatorConnection hoptimatorConnection = new HoptimatorConnection(calciteConnection, properties);
      holder.connection = hoptimatorConnection;

      Wrapped wrapped = new Wrapped(hoptimatorConnection, rootSchema);
      String[] catalogs = properties.getProperty("catalogs", "").split(",");

      if (catalogs.length == 0 || catalogs[0].isEmpty()) {
        // load all catalogs (typical usage)
        for (Catalog catalog : CatalogService.catalogs()) {
          catalog.register(wrapped);
        }
      } else {
        // load specific catalogs when loaded as `jdbc:hoptimator://catalogs=foo,bar`
        for (String catalog : catalogs) {
          CatalogService.catalog(catalog).register(wrapped);
        }
      }
      return hoptimatorConnection;
    } catch (IOException | SQLTransientException e) {
      throw new SQLTransientConnectionException("Problem loading " + url, e);
    } catch (Exception e) {
      throw new SQLNonTransientException("Problem loading " + url, e);
    }
  }

  private static final class ConnectionHolder {
    HoptimatorConnection connection;
  }

  public static class Prepare extends CalcitePrepareImpl {

    private final ConnectionHolder holder;

    Prepare(ConnectionHolder holder) {
      this.holder = holder;
    }

    Prepare(HoptimatorConnection connection) {
      this.holder = new ConnectionHolder();
      this.holder.connection = connection;
    }

    @Override
    protected SqlParser.Config parserConfig() {
      return SqlParser.config().withParserFactory(HoptimatorDdlExecutor.PARSER_FACTORY);
    }

    @Override
    public void executeDdl(Context context, SqlNode node) {
      new HoptimatorDdlExecutor(holder.connection).executeDdl(context, node);
    }
  }
}
