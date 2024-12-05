package com.linkedin.hoptimator;

import java.util.Map;
import java.sql.SQLException;

public interface Connector<T> {
  
  Map<String, String> configure(T t) throws SQLException;
}
