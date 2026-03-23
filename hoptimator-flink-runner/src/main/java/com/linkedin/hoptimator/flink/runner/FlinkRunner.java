package com.linkedin.hoptimator.flink.runner;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;


/** Runs SQL from command-line args. */
public final class FlinkRunner {
  private static final Logger logger = LoggerFactory.getLogger(FlinkRunner.class);
  static final String FILE_PREFIX = "--file:";
  private static final String UDF_DIR_PROPERTY = "hoptimator.udf.dir";
  private static final String UDF_DIR_DEFAULT = "/opt/python-udfs";

  private FlinkRunner() {
  }

  public static void main(String[] args) throws IOException {
    // Phase 1: Extract and write file directives before executing SQL
    Path udfDir = Paths.get(System.getProperty(UDF_DIR_PROPERTY, UDF_DIR_DEFAULT));
    for (String arg : args) {
      String stmt = arg.replaceAll("\\n", "").trim();
      if (stmt.startsWith(FILE_PREFIX)) {
        writeFile(stmt, udfDir);
      }
    }

    // Phase 2: Execute SQL statements
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

    for (int i = 0; i < args.length; i++) {
      String stmt = args[i].replaceAll("\\n", "").trim();
      if (!stmt.isEmpty() && !stmt.startsWith("--")) {
        logger.info("Executing statement #{}: {}", i, stmt);
        try {
          tEnv.executeSql(stmt);
        } catch (Exception e) {
          logger.error("Error executing SQL statement #{}: `{}`", i, stmt, e);
          throw e;
        }
      }
    }
  }

  /** Parses a --file:name:base64content directive and writes the file to the given directory. */
  static void writeFile(String directive, Path targetDir) throws IOException {
    String payload = directive.substring(FILE_PREFIX.length());
    int colonIdx = payload.indexOf(':');
    if (colonIdx < 0) {
      throw new IllegalArgumentException("Invalid file directive (missing ':'): " + directive);
    }
    String filename = payload.substring(0, colonIdx);
    String encoded = payload.substring(colonIdx + 1);
    byte[] content = Base64.getDecoder().decode(encoded);
    Files.createDirectories(targetDir);
    Path filePath = targetDir.resolve(filename);
    Files.write(filePath, content);
    logger.info("Wrote UDF file: {}", filePath);
  }
}
