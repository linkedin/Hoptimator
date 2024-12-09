package com.linkedin.hoptimator;

import java.sql.SQLException;
import java.util.Map;


public interface Connector<T> {

  Map<String, String> configure(T t) throws SQLException;
}
