package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.sql.SQLException;

public final class ValidationService {

  private ValidationService() {
  }

  public static Validator.Issues validate(Connection connection) {
    if (!(connection instanceof CalciteConnection)) {
      throw new IllegalArgumentException("This connection is unsupported.");
    }
    CalciteConnection conn = (CalciteConnection) connection;
    Validator.Issues issues = new Validator.Issues("");
    walk(conn.getRootSchema(), issues);
    return issues;
  }

  private static void walk(SchemaPlus schema, Validator.Issues issues) {
    validate(schema, SchemaPlus.class, issues);
    for (String x : schema.getSubSchemaNames()) {
      walk(schema.getSubSchema(x), issues.child(x));
    }
    for (String x : schema.getTableNames()) {
      walk(schema.getTable(x), issues.child(x));
    }
  }

  private static void walk(Table table, Validator.Issues issues) {
    validate(table, Table.class, issues);
  }

  public static <T> void validate(T object, Class<? super T> clazz, Validator.Issues issues) {
    validators(clazz).forEach(x -> x.validate(object, issues));
  }

  public static <T> void validateOrThrow(T object, Class<? super T> clazz) throws SQLException {
    Validator.Issues issues = new Validator.Issues("");
    validate(object, clazz, issues);
    if (!issues.valid()) {
      throw new SQLException("Failed validation:\n" + issues.toString());
    }
  }

  public static Collection<ValidatorProvider> providers() {
    ServiceLoader<ValidatorProvider> loader = ServiceLoader.load(ValidatorProvider.class);
    List<ValidatorProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(x -> providers.add(x));
    return providers;
  }

  public static <T> Collection<Validator<? super T>> validators(Class<? super T> clazz) {
    return providers().stream().flatMap(x -> x.validators(clazz).stream())
        .collect(Collectors.toList());
  }
}
