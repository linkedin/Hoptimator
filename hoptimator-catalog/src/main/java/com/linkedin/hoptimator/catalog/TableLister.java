package com.linkedin.hoptimator.catalog;

import java.util.Collection;
import java.util.concurrent.ExecutionException;


/** Lists tables within a database */
public interface TableLister {
  Collection<String> list() throws InterruptedException, ExecutionException;
}
