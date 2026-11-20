package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.Handler;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.Driver;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;


// CalciteDriver is an extension of Driver that extends the connect() method to allow for disabling caching.
// Caching enables snapshots via CachingCalciteSchema which has the side effect of preloading all tables for a driver
public class CalciteDriver extends Driver {
  private static final Logger LOG = LoggerFactory.getLogger(CalciteDriver.class);

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
  public final Connection connect(String url, Properties info) throws SQLException {
    return connect(url, info, false);
  }

  public final Connection connect(String url, Properties info, boolean cache) throws SQLException {
    if (!this.acceptsURL(url)) {
      return null;
    }
    Properties properties = connectionProperties(url, info);
    AvaticaConnection connection = null;
    try {
      Properties info2 = (Properties) properties.clone();
      // Report temporal fractional-seconds precision in DECIMAL_DIGITS, where Calcite's JDBC
      // federation reader (JdbcSchema#getRelDataType) looks — otherwise TIMESTAMP(3) columns
      // surface as TIMESTAMP(0) across the federation. See HoptimatorMetaColumnFactory.
      info2.putIfAbsent(CalciteConnectionProperty.META_COLUMN_FACTORY.camelName(),
          HoptimatorMetaColumnFactory.class.getName());
      CalciteSchema rootSchema = createRootSchema(info2);
      connection = ((CalciteFactory) this.factory)
          .newConnection(this, this.factory, url, info2, rootSchema, null);
      onConnectionInit(connection);
      configureConnection((CalciteConnection) connection, url, properties);
      return connection;
    } catch (Exception failure) {
      closeOnFailure(connection, failure);
      throw connectionException(url, failure);
    } catch (Error failure) {
      closeOnFailure(connection, failure);
      throw failure;
    }
  }

  /** Merges URL properties over caller properties. Override to validate before acquiring resources. */
  protected Properties connectionProperties(String url, Properties info) throws SQLException {
    return ConnectStringParser.parse(url.substring(getConnectStringPrefix().length()), info);
  }

  /** Configures an acquired connection after Calcite initialization; failures trigger physical cleanup. */
  protected void configureConnection(CalciteConnection connection, String url, Properties properties) throws Exception {
  }

  /** Maps initialization failures after cleanup, preserving checked SQL and unchecked failures by default. */
  protected SQLException connectionException(String url, Exception failure) {
    if (failure instanceof SQLException) {
      return (SQLException) failure;
    }
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    return new SQLException("Problem loading " + url, failure);
  }

  /** Transfers an owned resource to the physical connection, not a pooled connection lease. */
  public static void addConnectionResource(CalciteConnection connection, AutoCloseable resource) throws SQLException {
    CalciteSchema root = CalciteSchema.from(connection.getRootSchema());
    if (!(root instanceof HoptimatorCalciteSchema)) {
      SQLException failure = new SQLException("Connection does not support owned resources");
      try {
        resource.close();
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        failure.addSuppressed(e);
      }
      throw failure;
    }
    ((HoptimatorCalciteSchema) root).addConnectionResource(resource);
  }

  protected static void closeOnFailure(@Nullable Connection connection, Throwable failure) {
    if (connection != null) {
      try {
        connection.close();
      } catch (Exception | Error e) {
        if (e != failure) {
          failure.addSuppressed(e);
        }
      }
    }
  }

  @Override
  protected Handler createHandler() {
    Handler delegate = super.createHandler();
    return new Handler() {
      @Override public void onConnectionInit(AvaticaConnection connection) throws SQLException {
        delegate.onConnectionInit(connection);
      }

      @Override public void onConnectionClose(AvaticaConnection connection) {
        RuntimeException failure = null;
        try {
          delegate.onConnectionClose(connection);
        } catch (RuntimeException e) {
          failure = e;
        }
        CalciteSchema root = CalciteSchema.from(((CalciteConnection) connection).getRootSchema());
        if (root instanceof HoptimatorCalciteSchema) {
          try {
            ((HoptimatorCalciteSchema) root).closeConnectionResources();
          } catch (SQLException e) {
            if (failure == null) {
              failure = new RuntimeException("Failed to close connection resources", e);
            } else {
              failure.addSuppressed(e);
            }
          }
        }
        if (failure != null) {
          LOG.warn("Failed to close physical connection resources", failure);
        }
      }

      @Override public void onStatementExecute(AvaticaStatement statement, ResultSink resultSink) {
        delegate.onStatementExecute(statement, resultSink);
      }

      @Override public void onStatementClose(AvaticaStatement statement) {
        delegate.onStatementClose(statement);
      }
    };
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
   * @param properties the merged connection properties (URL params + driver props)
   */
  protected CalciteSchema createRootSchema(Properties properties) {
    return HoptimatorCalciteSchema.createRootSchema(null, includeMetadataSchema());
  }

  /**
   * Whether to include the Calcite built-in {@code metadata} schema
   * ({@code metadata.COLUMNS}, {@code metadata.TABLES}, etc.) in the root schema.
   *
   * <p>Returns {@code false} by default. Override in drivers that need
   * {@code SELECT * FROM metadata.COLUMNS WHERE ...} to work.
   */
  protected boolean includeMetadataSchema() {
    return true;
  }
}
