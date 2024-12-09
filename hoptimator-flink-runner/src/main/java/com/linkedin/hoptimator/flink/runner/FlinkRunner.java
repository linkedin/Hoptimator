package com.linkedin.hoptimator.flink.runner;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Runs SQL from command-line args. */
public final class FlinkRunner {
  private static final Logger logger = LoggerFactory.getLogger(FlinkRunner.class);

  private FlinkRunner() {
  }

  public static void main(String[] args) {
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
}
