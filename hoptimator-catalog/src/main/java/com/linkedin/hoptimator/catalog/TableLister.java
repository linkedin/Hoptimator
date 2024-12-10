package com.linkedin.hoptimator.catalog;

import java.util.Collection;


/** Lists tables within a database */
public interface TableLister {
  Collection<String> list();
}
