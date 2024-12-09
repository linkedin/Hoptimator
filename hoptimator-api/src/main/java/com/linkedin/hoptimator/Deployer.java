package com.linkedin.hoptimator;

import java.sql.SQLException;
import java.util.List;


public interface Deployer<T> {

  void create(T t) throws SQLException;

  void update(T t) throws SQLException;

  void delete(T t) throws SQLException;

  List<String> specify(T t) throws SQLException;
}
