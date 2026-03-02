package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.util.planner.HoptimatorJdbcSchema;
import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.util.Map;
import java.util.Properties;

/**
 * Utility methods for Deployer implementations.
 *
 * <p>Provides common functionality for:
 * <ul>
 *   <li>Parsing configuration options from maps</li>
 *   <li>Extracting connection properties from JDBC schemas</li>
 * </ul>
 */
public final class DeployerUtils {
  private static final Logger log = LoggerFactory.getLogger(DeployerUtils.class);

  private DeployerUtils() {
    // Utility class - prevent instantiation
  }

  /**
   * Parses an integer option from a map of options.
   *
   * @param options the options map to parse from
   * @param key the option key to look up
   * @param defaultValue the default value to return if the option is not set or invalid
   * @return the parsed integer value, or defaultValue if not set or invalid
   */
  public static Integer parseIntOption(Map<String, String> options, String key, Integer defaultValue) {
    String value = options.get(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      log.warn("Invalid integer value for option '{}': '{}'. Using default: {}", key, value, defaultValue);
      return defaultValue;
    }
  }

  /**
   * Parses a long option from a map of options.
   *
   * @param options the options map to parse from
   * @param key the option key to look up
   * @param defaultValue the default value to return if the option is not set or invalid
   * @return the parsed long value, or defaultValue if not set or invalid
   */
  public static Long parseLongOption(Map<String, String> options, String key, Long defaultValue) {
    String value = options.get(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      log.warn("Invalid long value for option '{}': '{}'. Using default: {}", key, value, defaultValue);
      return defaultValue;
    }
  }

  /**
   * Parses a boolean option from a map of options.
   *
   * @param options the options map to parse from
   * @param key the option key to look up
   * @param defaultValue the default value to return if the option is not set
   * @return the parsed boolean value, or defaultValue if not set
   */
  public static Boolean parseBooleanOption(Map<String, String> options, String key, Boolean defaultValue) {
    String value = options.get(key);
    if (value == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(value);
  }

  /**
   * Parses a double option from a map of options.
   *
   * @param options the options map to parse from
   * @param key the option key to look up
   * @param defaultValue the default value to return if the option is not set or invalid
   * @return the parsed double value, or defaultValue if not set or invalid
   */
  public static Double parseDoubleOption(Map<String, String> options, String key, Double defaultValue) {
    String value = options.get(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      log.warn("Invalid double value for option '{}': '{}'. Using default: {}", key, value, defaultValue);
      return defaultValue;
    }
  }

  /**
   * Extracts connection properties from a JDBC-backed schema.
   *
   * <p>This method:
   * <ol>
   *   <li>Looks up the schema by name in the connection's root schema</li>
   *   <li>Unwraps it to get the HoptimatorJdbcSchema</li>
   *   <li>Extracts the JDBC URL from the BasicDataSource</li>
   *   <li>Parses the URL (after removing the connection prefix) into Properties</li>
   * </ol>
   *
   * @param catalogName Optional catalog name to look up
   * @param schemaName the name of the schema to look up
   * @param connection the connection to search in
   * @param connectionPrefix the JDBC connection prefix to strip (e.g., "jdbc:kafka://")
   * @param logger optional logger for debug messages
   * @return Properties extracted from the JDBC URL, or null if schema not found or not JDBC-backed
   */
  public static Properties extractPropertiesFromJdbcSchema(@Nullable String catalogName, String schemaName,
      Connection connection, String connectionPrefix, @Nullable Logger logger) {

    if (schemaName == null) {
      return null;
    }

    try {
      if (!(connection instanceof HoptimatorConnection)) {
        return null;
      }

      HoptimatorConnection hoptimatorConnection =
          (HoptimatorConnection) connection;
      SchemaPlus rootSchema = hoptimatorConnection.calciteConnection().getRootSchema();
      if (catalogName != null) {
        rootSchema = rootSchema.subSchemas().get(catalogName);
        if (rootSchema == null) {
          return null;
        }
      }
      SchemaPlus subSchemaPlus = rootSchema.subSchemas().get(schemaName);

      if (subSchemaPlus == null) {
        return null;
      }

      HoptimatorJdbcSchema schema = subSchemaPlus.unwrap(HoptimatorJdbcSchema.class);
      if (schema == null) {
        return null;
      }

      String jdbcUrl = ((BasicDataSource) schema.getDataSource()).getUrl();

      Properties properties = new Properties();
      properties.putAll(ConnectStringParser.parse(jdbcUrl.substring(connectionPrefix.length())));
      return properties;
    } catch (Exception e) {
      if (logger != null) {
        logger.debug("Could not extract properties from schema '{}': {}", schemaName, e.getMessage());
      }
    }
    return null;
  }
}
