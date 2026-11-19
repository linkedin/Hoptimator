package com.linkedin.hoptimator.jdbc;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class ConnectionResourcesTest {
  @Test
  void closesUniqueResourcesOnceInReverseOrder() throws SQLException {
    ConnectionResources resources = new ConnectionResources();
    List<Integer> closed = new ArrayList<>();
    AutoCloseable first = () -> closed.add(1);
    resources.add(first);
    resources.add(first);
    resources.add(() -> closed.add(2));

    resources.close();
    resources.close();

    assertEquals(List.of(2, 1), closed);
  }

  @Test
  void attemptsAllCleanupAndPreservesFailures() throws SQLException {
    ConnectionResources resources = new ConnectionResources();
    AtomicBoolean closed = new AtomicBoolean();
    IOException first = new IOException("first");
    IOException second = new IOException("second");
    resources.add(() -> closed.set(true));
    resources.add(() -> {
      throw second;
    });
    resources.add(() -> {
      throw first;
    });

    SQLException failure = assertThrows(SQLException.class, resources::close);

    assertTrue(closed.get());
    assertSame(first, failure.getCause());
    assertEquals(1, failure.getSuppressed().length);
    assertSame(second, failure.getSuppressed()[0]);
    resources.close();
  }

  @Test
  void disposesResourceRejectedAfterClose() throws SQLException {
    ConnectionResources resources = new ConnectionResources();
    resources.close();
    AtomicBoolean closed = new AtomicBoolean();

    assertThrows(SQLException.class, () -> resources.add(() -> closed.set(true)));

    assertTrue(closed.get());
  }

  @Test
  void preservesRejectedResourceCleanupFailure() throws SQLException {
    ConnectionResources resources = new ConnectionResources();
    resources.close();
    IOException cleanup = new IOException("cleanup");

    SQLException failure = assertThrows(SQLException.class,
        () -> resources.add(() -> {
          throw cleanup;
        }));

    assertEquals(1, failure.getSuppressed().length);
    assertSame(cleanup, failure.getSuppressed()[0]);
  }

  @Test
  void preservesInterruptionAndContinuesCleanup() throws SQLException {
    assertFalse(Thread.currentThread().isInterrupted());
    ConnectionResources resources = new ConnectionResources();
    AtomicBoolean closed = new AtomicBoolean();
    resources.add(() -> closed.set(true));
    resources.add(() -> {
      throw new InterruptedException("interrupted");
    });
    try {
      assertThrows(SQLException.class, resources::close);
      assertTrue(Thread.currentThread().isInterrupted());
      assertTrue(closed.get());
    } finally {
      Thread.interrupted();
    }
  }
}
