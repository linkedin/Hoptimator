package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.util.DeploymentService;
import java.sql.Connection;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;


// A connection should be cached and kept open for the duration of a single request.
// This allows sink schemas that may not exist yet to be added to the root schema and later queried.
public final class ConnectionCache {

  private static final ConcurrentHashMap<CacheKey, Connection> CACHED_CONNECTIONS = new ConcurrentHashMap<>();

  /* Prevent the ConnectionCache class from being instantiated. */
  private ConnectionCache() { }

  public static Connection getConnection(String url, Properties info) {
    CacheKey cacheKey = new CacheKey(url, info);
    Connection connection = CACHED_CONNECTIONS.get(cacheKey);
    try {
      if (connection != null && !connection.isClosed()) {
        return connection;
      }
    } catch (Exception e) {
      return null;
    }
    return null;
  }

  public static void setConnection(String url, Properties info, Connection connection) {
    CacheKey cacheKey = new CacheKey(url, info);
    CACHED_CONNECTIONS.put(cacheKey, connection);
  }

  private static final class CacheKey {
    private final String url;
    private final Properties props;

    CacheKey(String url, Properties info) {
      this.url = url;
      this.props = new Properties();
      this.props.putAll(info);

      // TODO: Need to find a better way to passthrough the pipeline option
      this.props.remove(DeploymentService.PIPELINE_OPTION);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return Objects.equals(this.url, cacheKey.url)
          && Objects.equals(this.props, cacheKey.props);
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.url, this.props);
    }
  }
}
