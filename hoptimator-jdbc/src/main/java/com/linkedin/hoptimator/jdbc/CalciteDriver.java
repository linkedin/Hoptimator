package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.jdbc.CalciteFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.Driver;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Supplier;

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
      CalciteSchema rootSchema = createRootSchema(cache, info2);
      AvaticaConnection connection = ((CalciteFactory) this.factory)
          .newConnection(this, this.factory, url, info2, rootSchema, null);
      onConnectionInit(connection);
      return connection;
    }
  }

  /**
   * Called after a new connection is created. The default implementation invokes
   * {@code handler.onConnectionInit()}, which triggers Calcite's lattice scan and
   * schema model processing. Subclasses that don't use lattices or model files may
   * override this to skip the scan and avoid eager schema loading.
   */
  protected void onConnectionInit(AvaticaConnection connection) throws SQLException {
    this.handler.onConnectionInit(connection);
  }

  /**
   * Creates the root {@link CalciteSchema} for a new connection.
   *
   * <p>Subclasses may override this to supply a custom backing schema — for example, one
   * that lazily discovers sub-schemas from an external data source without opening a
   * connection at driver connect time.
   *
   * @param cache      whether the schema should use Calcite's caching layer
   * @param properties the merged connection properties (URL params + driver props)
   */
  protected CalciteSchema createRootSchema(boolean cache, Properties properties) {
    return CalciteSchema.createRootSchema(true, cache);
  }
}
