package com.linkedin.hoptimator.util;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.sql.SQLException;

/** A list of rows corresponding to remote objects behind some API. */
public class RemoteRowList<T, U> extends AbstractCollection<U> {
  private final Api<T> api;
  private final RowMapper<T, U> mapper;

  public RemoteRowList(Api<T> api, RowMapper<T, U> mapper) {
    this.api = api;
    this.mapper = mapper;
  }

  @Override
  public boolean add(U u) {
    try {
      api.create(mapper.fromRow(u));
    } catch (SQLException e) {
      throw new RuntimeException("Could not add row.", e);
    }
    return true;
  }

  @Override
  public Iterator<U> iterator() {
    final Iterator<T> iter;
    try {
      iter = api.list().iterator();
    } catch (SQLException e) {
      throw new RuntimeException("Could not list rows.", e);
    }
    return new Iterator<U>() {
      private T pos;

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public U next() {
        pos = iter.next();
        return mapper.toRow(pos);
      }

      @Override
      public void remove() {
        try {
          api.delete(pos);
        } catch (SQLException e) {
          throw new RuntimeException("Could not remove row.", e);
        }
      }
    };
  }

  @Override
  public int size() {
    try {
      return api.list().size();
    } catch (SQLException e) {
      throw new RuntimeException("Could not list rows.", e);
    }
  }
}
