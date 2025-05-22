package com.linkedin.hoptimator;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;


public interface SnapshotProvider {

  void snapshot(List<String> specs, Properties connectionProperties) throws SQLException;

  void restore() throws SQLException;
}
