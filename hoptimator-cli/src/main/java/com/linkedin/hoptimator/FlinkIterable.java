package com.linkedin.hoptimator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;

/** Runs Flink SQL in-process and iterates over the result. */
public class FlinkIterable implements Iterable<Object> {
  private final Logger logger = LoggerFactory.getLogger(FlinkIterable.class);
  private final String sql;
  private final long timeoutMillis;

  /** Execute SQL, stopping after timeoutMillis. */
  public FlinkIterable(String sql, int timeoutMillis) {
    this.sql = sql;
    this.timeoutMillis = timeoutMillis;
  }

  /** Execute SQL. */
  public FlinkIterable(String sql) {
    this.sql = sql;
    this.timeoutMillis = Long.MAX_VALUE;
  }

  /**
   * Returns an Iterator that returns results from a local Flink job.
   *
   * The Flink job runs on a local in-process worker.
   */
  @Override 
  public Iterator<Object> iterator() {
    try {
      return closeExpired(datastream().map(r -> toArray(r)).executeAndCollect());
    } catch (Exception e) {
      return new ExceptionalIterator<>(e);
    }
  }

  /**
   * Returns an Iterator that returns results from a local Flink job, in Flink's native Row container.
   *
   * The Flink job runs on a local in-process worker.
   */
  public Iterator<Row> rowIterator() {
    try {
      return closeExpired(datastream().executeAndCollect());
    } catch (Exception e) {
      return new ExceptionalIterator<>(e);
    }
  }

  /** Iterates over the selected field/column only. */
  public <T> Iterable<T> field(int pos) {
    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator() {
        try {
          return closeExpired(datastream().map(r -> r.<T>getFieldAs(pos)).executeAndCollect());
        } catch (Exception e) {
          return new ExceptionalIterator<>(e);
        }
      }
    };
  }

  /** Iterates over the selected field/column only. */
  public <T> Iterable<T> field(String name) {
    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator() {
        try {
          return closeExpired(datastream().map(r -> r.<T>getFieldAs(name)).executeAndCollect());
        } catch (Exception e) {
          return new ExceptionalIterator<>(e);
        }
      }
    };
  }

  private <T> Iterator<T> closeExpired(CloseableIterator<T> inner) {
    if (timeoutMillis < Long.MAX_VALUE) {
      (new Thread(() -> {
        try {
          Thread.sleep(timeoutMillis);
          inner.close();
        } catch (Exception e) {
          // nop
        }
      })).start();
    }
    return inner;
  }

  private DataStream<Row> datastream() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // Assume that DDL statements come first, and that the last statement is a query
    String[] statements = sql.trim().split(";");
    String[] ddl = Arrays.copyOfRange(statements, 0, statements.length - 1);
    String query = statements[statements.length - 1];

    // Execute DDL statements
    for (String stmt : ddl) {
      logger.info("Flink DDL: {}", stmt.replaceAll("\\n", "").trim());
      if (!stmt.isEmpty() && !stmt.startsWith("--")) {
        tEnv.executeSql(stmt);
      }
    }
 
    // Run query
    logger.info("Flink SQL: {}", query.replaceAll("\\n", ""));
    Table resultTable = tEnv.sqlQuery(query);
    return tEnv.toChangelogStream(resultTable);
  }

  static private Object toArray(Row r) {
    if (r.getArity() == 1) {
      return r.getField(0);
    }
    Object[] fields = new Object[r.getArity()];
    for (int i = 0; i < fields.length; i++) {
      fields[i] = r.getField(i);
    }
    return fields;
  }

  static class ExceptionalIterator<T> implements Iterator<T> {
    private final Exception e;

    ExceptionalIterator(Exception e) {
      this.e = e;
    }

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public T next() {
      throw new RuntimeException(e);
    }
  }
}
