package com.linkedin.hoptimator.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.Catalog;
import com.linkedin.hoptimator.util.WrappedSchemaPlus;


/** Driver for :jdbc:hoptimator:// connections. */
public class HoptimatorDriver extends Driver {
  private final static Logger LOG = LoggerFactory.getLogger(HoptimatorDriver.class);

  public HoptimatorDriver() {
    super();
  }

  private HoptimatorDriver(Supplier<CalcitePrepare> prepareFactory) {
    super(prepareFactory);
  }

  static {
    new HoptimatorDriver().register();
  }

  public static CalcitePrepare.ConvertResult convert(HoptimatorConnection conn, String sql) {
    CalcitePrepare.Context context = conn.createPrepareContext();
    return new Prepare(conn.connectionProperties()).convert(context, sql);
  }

  @Override
  protected String getConnectStringPrefix() {
    return "jdbc:hoptimator://";
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(this.getClass(), "hoptimator.properties", "hoptimator", "0", "hoptimator", "0");
  }

  @Override
  public Connection connect(String url, Properties props) throws SQLException {
    if (!url.startsWith(getConnectStringPrefix())) {
      return null;
    }
    try {
      if (prepareFactory == null) {
        // funky way of extending Driver with a custom Prepare:
        return withPrepareFactory(() -> new Prepare(props))
          .connect(url, props);
      }
      Connection connection = super.connect(url, props);
      if (connection == null) {
        throw new IOException("Could not connect to " + url);
      }
      connection.setAutoCommit(true); // to prevent rollback()
      CalciteConnection calciteConnection = (CalciteConnection) connection;
      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      // built-in schemas
      rootSchema.add("DEFAULT", new AbstractSchema());

      calciteConnection.setSchema("DEFAULT");

      WrappedSchemaPlus wrappedRootSchema = new WrappedSchemaPlus(rootSchema);

      // Load properties from the URL and from getConnection()'s properties.
      // URL properties take precedence.
      Properties properties = new Properties();
      properties.putAll(props); // via getConnection()
      properties.putAll(ConnectStringParser.parse(url.substring(getConnectStringPrefix().length())));
      String[] catalogs = properties.getProperty("catalogs", "").split(",");

      if (catalogs.length == 0 || catalogs[0].length() == 0) {
        // load all catalogs (typical usage)
        for (Catalog catalog : CatalogService.catalogs()) {
          catalog.register(wrappedRootSchema, properties);
        }
      } else {
        // load specific catalogs when loaded as `jdbc:hoptimator://catalogs=foo,bar`
        for (String catalog : catalogs) {
          CatalogService.catalog(catalog).register(wrappedRootSchema, props);
        }
      }

      return new HoptimatorConnection(calciteConnection, properties);
    } catch (Exception e) {
      throw new SQLException("Problem loading " + url, e);
    }
  }

  @Override
  public Driver withPrepareFactory(Supplier<CalcitePrepare> prepareFactory) {
      return new HoptimatorDriver(prepareFactory);
  }

  public static HintStrategyTable hintStrategyTable() {
    return HintStrategyTable.builder()
        .hintStrategy("OPTIONS", HintPredicates.TABLE_SCAN)
        .build();
  }

  public static Map<String, Map<String, String>> assembleTableHints(SqlNode sqlNode) {
    Map<String, Map<String, String>> tableHints = new LinkedHashMap<>();
    if (sqlNode instanceof SqlSelect) {
      if (!((SqlSelect) sqlNode).hasHints()) {
        return tableHints;
      }
      List<RelHint> hints = SqlUtil.getRelHint(hintStrategyTable(), ((SqlSelect) sqlNode).getHints());
      for (RelHint hint : hints) {
        for (Map.Entry<String, String> entry : hint.kvOptions.entrySet()) {
          String[] keyPath = entry.getKey().split("\\$", 2);
          if (keyPath.length == 2) {
            String tableName = keyPath[0];
            String hintName = keyPath[1];
            Map<String, String> tableHint = tableHints.computeIfAbsent(tableName, k -> new LinkedHashMap<>());
            tableHint.put(hintName, entry.getValue());
          } else {
            LOG.warn("Hint key '{}' does not contain table name, format as 'tableName$hintName'", entry.getKey());
          }
        }
      }
    }
    return tableHints;
  }

  public static class Prepare extends CalcitePrepareImpl {

    private final Properties connectionProperties;

    Prepare(Properties connectionProperties) {
      this.connectionProperties = connectionProperties;
    }

    @Override
    protected SqlParser.Config parserConfig() {
      return SqlParser.config().withParserFactory(HoptimatorDdlExecutor.PARSER_FACTORY);
    }

    @Override
    public void executeDdl(Context context, SqlNode node) {
      new HoptimatorDdlExecutor(connectionProperties).executeDdl(context, node);
    }
  }
}
