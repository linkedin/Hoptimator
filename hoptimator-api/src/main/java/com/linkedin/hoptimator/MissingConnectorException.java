package com.linkedin.hoptimator;

import java.sql.SQLNonTransientException;


/**
 * Signals that a table has no connector configuration, and therefore cannot participate
 * in a generated SQL job. Callers that generate SQL-based jobs are expected to catch this
 * and skip SQL generation, while still emitting any non-SQL jobs.
 *
 * <p>This is not necessarily an error: some tables are moved by means other than a SQL job.
 * For example, a JobTemplate may render a non-SQL job (rather than {@code SqlJob}) to move data
 * into or out of such a table.
 */
public class MissingConnectorException extends SQLNonTransientException {

  public MissingConnectorException(String path) {
    super("No connector configured for '" + path + "'.");
  }
}
