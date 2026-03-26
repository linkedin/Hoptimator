package com.linkedin.hoptimator.flink.runner;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;


class FlinkRunnerTest {

  @Test
  void findSqlJobRefPresent() {
    String[] args = {"--sqljob=default/my-job", "CREATE TABLE t (x INT)"};
    assertThat(FlinkRunner.findSqlJobRef(args)).isEqualTo("default/my-job");
  }

  @Test
  void findSqlJobRefAbsent() {
    String[] args = {"CREATE TABLE t (x INT)", "INSERT INTO t SELECT 1"};
    assertThat(FlinkRunner.findSqlJobRef(args)).isNull();
  }

  @Test
  void writeFileCreatesFileOnDisk(@TempDir Path tempDir) throws Exception {
    FlinkRunner.writeFile("test.py", "print('hello')", tempDir);

    Path written = tempDir.resolve("test.py");
    assertThat(written).exists();
    assertThat(Files.readString(written, StandardCharsets.UTF_8)).isEqualTo("print('hello')");
  }

  @Test
  void writeFileHandlesMultilineContent(@TempDir Path tempDir) throws Exception {
    String content = "from pyflink.table.udf import udf\n\n@udf(result_type=DataTypes.STRING())\ndef reverse(s):\n    return s[::-1]\n";
    FlinkRunner.writeFile("my_udfs.py", content, tempDir);

    Path written = tempDir.resolve("my_udfs.py");
    assertThat(written).exists();
    assertThat(Files.readString(written, StandardCharsets.UTF_8)).isEqualTo(content);
  }
}
