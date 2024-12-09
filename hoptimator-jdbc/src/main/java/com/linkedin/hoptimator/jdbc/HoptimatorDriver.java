package com.linkedin.hoptimator.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;

import com.linkedin.hoptimator.Catalog;
import com.linkedin.hoptimator.util.WrappedSchemaPlus;


/** Driver for :jdbc:hoptimator:// connections. */
public class HoptimatorDriver extends Driver {

  public HoptimatorDriver() {
    super(() -> new Prepare());
  }

  static {
    new HoptimatorDriver().register();
  }

  public static CalcitePrepare.ConvertResult convert(CalcitePrepare.Context context, String sql) {
    return new Prepare().convert(context, sql);
  }

  public static CalcitePrepare.ConvertResult convert(CalciteConnection connection, String sql) {
    return convert(connection.createPrepareContext(), sql);
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
      Connection connection = super.connect(url, props);
      if (connection == null) {
        throw new IOException("Could not connect to " + url);
      }
      connection.setAutoCommit(true); // to prevent rollback()
      CalciteConnection calciteConnection = (CalciteConnection) connection;
      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      String remaining = url.substring(getConnectStringPrefix().length()).trim();
      String[] catalogs = remaining.split(",");

      // built-in schemas
      rootSchema.add("DEFAULT", new AbstractSchema());

      calciteConnection.setSchema("DEFAULT");

      WrappedSchemaPlus wrappedRootSchema = new WrappedSchemaPlus(rootSchema);
      if (catalogs.length == 0 || catalogs[0].length() == 0) {
        // load all catalogs (typical usage)
        for (Catalog catalog : CatalogService.catalogs()) {
          catalog.register(wrappedRootSchema);
        }
      } else {
        // load specific catalogs when loaded as `jdbc:hoptimator://foo,bar`
        for (String catalog : catalogs) {
          CatalogService.catalog(catalog).register(wrappedRootSchema);
        }
      }

      return connection;
    } catch (Exception e) {
      throw new SQLException("Problem loading " + url, e);
    }
  }

  public static class Prepare extends CalcitePrepareImpl {

    @Override
    protected SqlParser.Config parserConfig() {
      return SqlParser.config().withParserFactory(HoptimatorDdlExecutor.PARSER_FACTORY);
    }

    @Override
    public void executeDdl(Context context, SqlNode node) {
      new HoptimatorDdlExecutor().executeDdl(context, node);
    }
  }
}
