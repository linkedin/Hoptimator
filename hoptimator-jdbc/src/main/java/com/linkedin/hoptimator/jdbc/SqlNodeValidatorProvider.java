package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.sql.SqlNode;


/** * Provides validators for SQL nodes, specifically to ensure that no stateful operators are used.
 * This is used in the context of validating SQL queries in Hoptimator.
 * TODO: add config to skip validation for stateful operators.
 *
 * Should we move this to the li-hoptimator?
 */
public class SqlNodeValidatorProvider implements ValidatorProvider {

  @Override
  public <T> Collection<Validator> validators(T obj) {
    if (obj instanceof SqlNode) {
      return List.of(new NoStatefulOperatorValidator((SqlNode) obj));
    }

    return List.of();
  }
}
