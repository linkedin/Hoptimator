package com.linkedin.hoptimator.catalog;

import java.util.concurrent.ExecutionException;

/** Provides a database, i.e. a set of related Tables with unique names. */
public interface Adapter extends TableLister {

  /** Database this Adapter provides */
  String database();

  /** A specific Table in this database. */
  AdapterTable table(String name) throws InterruptedException, ExecutionException;
}
