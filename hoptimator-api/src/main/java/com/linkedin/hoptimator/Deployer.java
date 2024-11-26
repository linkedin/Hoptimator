package com.linkedin.hoptimator;

import java.util.List;
import java.sql.SQLException;

public interface Deployer<T> {
  
  void create(T t) throws SQLException;
  void update(T t) throws SQLException;
  void delete(T t) throws SQLException;
  List<String> specify(T t) throws SQLException;
}
