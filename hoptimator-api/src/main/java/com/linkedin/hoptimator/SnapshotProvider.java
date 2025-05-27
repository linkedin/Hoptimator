package com.linkedin.hoptimator;

import java.sql.SQLException;
import java.util.Properties;


public interface SnapshotProvider {

  void snapshot(Properties connectionProperties);

  <T> void store(T obj) throws SQLException;

  void restore();
}
