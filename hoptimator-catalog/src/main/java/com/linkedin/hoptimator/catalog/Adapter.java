package com.linkedin.hoptimator.catalog;

import java.util.concurrent.ExecutionException;

/** An Adapter provides a database, i.e. a set of related Tables with unique names.
 */
public interface Adapter extends TableLister {
  String database();
  AdapterTable table(String name) throws InterruptedException, ExecutionException;
}
