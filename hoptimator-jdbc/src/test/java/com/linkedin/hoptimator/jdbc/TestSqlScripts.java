package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;


public class TestSqlScripts extends QuidemTestBase {

  @Test
  public void basicDdlScript() throws Exception {
    run("basic-ddl.id");
  }

  @Test
  public void createViewWithAValidatorRejectingCreateViewThrowsException() throws Exception {
    // Runs the test in a separate thread to isolate the context class loader changes.
    Thread testThread = new Thread(() -> {
      useTestValidatorsUnchecked();

      AssertionFailedError exception = Assertions.assertThrows(AssertionFailedError.class, () -> run("basic-ddl.id"));
      Assertions.assertTrue(exception.getMessage().contains(SqlCreateViewValidator.ERROR_MESSAGE),
          "Expected error message not found: " + exception.getMessage());
    });
    testThread.start();
    testThread.join(); // Wait for the thread to finish
  }

  @Test
  public void createMaterializedViewWithAValidatorRejectingCreateViewThrowsException() throws Exception {
    // Runs the test in a separate thread to isolate the context class loader changes.
    Thread testThread = new Thread(() -> {
      useTestValidatorsUnchecked();

      AssertionFailedError exception =
          Assertions.assertThrows(AssertionFailedError.class, () -> run("create-materialized-view-ddl.id"));
      Assertions.assertTrue(exception.getMessage().contains(SqlCreateViewValidator.ERROR_MESSAGE),
          "Expected error message not found: " + exception.getMessage());
    });
    testThread.start();
    testThread.join(); // Wait for the thread to finish
  }

  private void useTestValidatorsUnchecked() {
    try {
      useTestValidators();
    } catch (IOException e) {
      throw new RuntimeException("Failed to set up test validators", e);
    }
  }

  private void useTestValidators() throws IOException {
    Path tempDir = Files.createTempDirectory("spi-test");
    Path servicesDir = tempDir.resolve("META-INF/services");
    Files.createDirectories(servicesDir);
    Files.writeString(servicesDir.resolve("com.linkedin.hoptimator.ValidatorProvider"),
        "com.linkedin.hoptimator.jdbc.TestSqlScripts$CreateViewValidatorProvider\n");

    URLClassLoader cl = new URLClassLoader(new URL[]{tempDir.toUri().toURL()}, getClass().getClassLoader());
    Thread.currentThread().setContextClassLoader(cl);
  }

  @SuppressWarnings("unused")
  public static class CreateViewValidatorProvider implements ValidatorProvider {
    @Override
    public <T> Collection<Validator> validators(T obj) {
      if (obj instanceof SqlCreateView || obj instanceof SqlCreateMaterializedView) {
        return List.of(new SqlCreateViewValidator());
      }
      return List.of();
    }
  }

  static class SqlCreateViewValidator implements Validator {
    static final String ERROR_MESSAGE = "Create view is not allowed in this test.";

    @Override
    public void validate(Issues issues) {
      issues.error(ERROR_MESSAGE);
    }
  }
}
