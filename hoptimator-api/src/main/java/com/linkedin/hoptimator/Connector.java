package com.linkedin.hoptimator;

import java.sql.SQLException;
import java.util.Map;


public interface Connector {

  Map<String, String> configure() throws SQLException;
}
