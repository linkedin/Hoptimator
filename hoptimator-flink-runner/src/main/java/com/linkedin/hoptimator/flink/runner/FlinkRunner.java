package com.linkedin.hoptimator.flink.runner;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/** Runs SQL from a SqlJob CR or from command-line args. */
public final class FlinkRunner {
  private static final Logger logger = LoggerFactory.getLogger(FlinkRunner.class);
  private static final String SQLJOB_PREFIX = "--sqljob=";
  private static final String UDF_DIR_PROPERTY = "hoptimator.udf.dir";
  private static final String UDF_DIR_DEFAULT = "/opt/python-udfs";

  private FlinkRunner() {
  }

  public static void main(String[] args) throws Exception {
    List<String> sqlStatements;

    // Check for --sqljob argument to fetch SQL and files from a SqlJob CR
    String sqlJobRef = findSqlJobRef(args);
    if (sqlJobRef != null) {
      logger.info("Fetching SqlJob: {}", sqlJobRef);
      sqlStatements = loadFromSqlJob(sqlJobRef);
    } else {
      // Backward compatibility: SQL from command-line args
      logger.info("No --sqljob argument found. Using SQL from command-line args.");
      sqlStatements = new ArrayList<>();
      for (String arg : args) {
        String stmt = arg.replaceAll("\\n", "").trim();
        if (!stmt.isEmpty() && !stmt.startsWith("--")) {
          sqlStatements.add(stmt);
        }
      }
    }

    // Execute SQL statements
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

    for (int i = 0; i < sqlStatements.size(); i++) {
      String stmt = sqlStatements.get(i);
      logger.info("Executing statement #{}: {}", i, stmt);
      try {
        tEnv.executeSql(stmt);
      } catch (Exception e) {
        logger.error("Error executing SQL statement #{}: `{}`", i, stmt, e);
        throw e;
      }
    }
  }

  /** Finds the --sqljob=namespace/name argument, or null if not present. */
  static String findSqlJobRef(String[] args) {
    for (String arg : args) {
      if (arg.startsWith(SQLJOB_PREFIX)) {
        return arg.substring(SQLJOB_PREFIX.length());
      }
    }
    return null;
  }

  /** Fetches a SqlJob CR and returns the SQL statements, writing any files to the UDF directory. */
  @SuppressWarnings("unchecked")
  static List<String> loadFromSqlJob(String ref) throws Exception {
    String[] parts = ref.split("/", 2);
    if (parts.length != 2) {
      throw new IllegalArgumentException("SqlJob reference must be namespace/name, got: " + ref);
    }
    String namespace = parts[0];
    String name = parts[1];

    ApiClient client = Config.defaultClient();
    DynamicKubernetesApi api = new DynamicKubernetesApi("hoptimator.linkedin.com", "v1alpha1", "sqljobs", client);
    DynamicKubernetesObject obj = api.get(namespace, name).throwsApiException().getObject();

    Map<String, Object> spec = (Map<String, Object>) obj.getRaw().get("spec");
    if (spec == null) {
      throw new IllegalStateException("SqlJob " + ref + " has no spec");
    }

    // Extract and execute files
    Map<String, String> files = (Map<String, String>) spec.get("files");
    if (files != null && !files.isEmpty()) {
      Path udfDir = Paths.get(System.getProperty(UDF_DIR_PROPERTY, UDF_DIR_DEFAULT));
      for (Map.Entry<String, String> entry : files.entrySet()) {
        writeFile(entry.getKey(), entry.getValue(), udfDir);
      }
    }

    // Extract SQL statements
    List<String> sql = (List<String>) spec.get("sql");
    if (sql == null) {
      return Collections.emptyList();
    }

    List<String> statements = new ArrayList<>();
    for (String stmt : sql) {
      String trimmed = stmt.replaceAll("\\n", "").trim();
      if (!trimmed.isEmpty()) {
        statements.add(trimmed);
      }
    }
    return statements;
  }

  /** Writes a file to the given directory. */
  static void writeFile(String filename, String content, Path targetDir) throws IOException {
    Files.createDirectories(targetDir);
    Path filePath = targetDir.resolve(filename);
    Files.write(filePath, content.getBytes(StandardCharsets.UTF_8));
    logger.info("Wrote UDF file: {}", filePath);
  }
}
