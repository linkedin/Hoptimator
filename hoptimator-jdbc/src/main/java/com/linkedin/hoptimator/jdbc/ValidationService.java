package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;


public final class ValidationService {

  private ValidationService() {
  }

  public static <T> void validate(T obj, Validator.Issues issues, DeploymentContext context) throws SQLException {
    validators(obj, context).forEach(x -> x.validate(issues, context));
  }

  public static <T> void validateOrThrow(T obj, DeploymentContext context) throws SQLException {
    Validator.Issues issues = new Validator.Issues("");
    validate(obj, issues, context);
    if (!issues.valid()) {
      throw new SQLDataException("Failed validation:\n" + issues);
    }
  }

  public static <T> void validateOrThrow(Collection<T> objs, DeploymentContext context) throws SQLException {
    Validator.Issues issues = new Validator.Issues("");
    for (T obj : objs) {
      validate(obj, issues, context);
      if (!issues.valid()) {
        throw new SQLDataException("Failed validation:\n" + issues);
      }
    }
  }

  public static Collection<ValidatorProvider> providers() {
    ServiceLoader<ValidatorProvider> loader = ServiceLoader.load(ValidatorProvider.class);
    List<ValidatorProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(providers::add);
    return providers;
  }

  public static <T> Collection<Validator> validators(T obj, DeploymentContext context) throws SQLException {
    // A loop (not a stream) so provider.validators() can propagate a checked SQLException.
    List<Validator> validators = new ArrayList<>();
    for (ValidatorProvider provider : providers()) {
      validators.addAll(provider.validators(obj, context));
    }
    return validators;
  }
}
