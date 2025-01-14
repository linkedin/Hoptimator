package com.linkedin.hoptimator;

import javax.sql.DataSource;

/** An execution engine. */
public interface Engine {

  String engineName();

  DataSource dataSource();

  SqlDialect dialect();

  String url();
}
