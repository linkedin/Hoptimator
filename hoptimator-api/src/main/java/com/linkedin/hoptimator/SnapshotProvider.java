package com.linkedin.hoptimator;

import java.sql.SQLException;
import java.util.Properties;


public interface SnapshotProvider {

  <T> void store(T obj, Properties connectionProperties) throws SQLException;

  void restore();
}
