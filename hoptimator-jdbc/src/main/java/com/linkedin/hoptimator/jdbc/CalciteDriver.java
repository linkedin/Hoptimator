package com.linkedin.hoptimator.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.jdbc.CalciteFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.Driver;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;


// CalciteDriver is an extension of Driver that extends the connect() method to allow for disabling caching.
// Caching enables snapshots via CachingCalciteSchema which has the side effect of preloading all tables for a driver
public class CalciteDriver extends Driver {

  public CalciteDriver() {
    this(null);
  }

  protected CalciteDriver(@Nullable Supplier<CalcitePrepare> prepareFactory) {
    super(prepareFactory);
  }

  @Override
  public CalciteDriver withPrepareFactory(Supplier<CalcitePrepare> prepareFactory) {
    requireNonNull(prepareFactory, "prepareFactory");
    if (this.prepareFactory == prepareFactory) {
      return this;
    }
    return new CalciteDriver(prepareFactory);
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return connect(url, info, false);
  }

  public Connection connect(String url, Properties info, boolean cache) throws SQLException {
    if (!this.acceptsURL(url)) {
      return null;
    } else {
      String prefix = this.getConnectStringPrefix();

      assert url.startsWith(prefix);

      String urlSuffix = url.substring(prefix.length());
      Properties info2 = ConnectStringParser.parse(urlSuffix, info);
      CalciteSchema rootSchema = CalciteSchema.createRootSchema(true, cache);
      AvaticaConnection connection = ((CalciteFactory) this.factory)
          .newConnection(this, this.factory, url, info2, rootSchema, null);
      this.handler.onConnectionInit(connection);
      return connection;
    }
  }
}
