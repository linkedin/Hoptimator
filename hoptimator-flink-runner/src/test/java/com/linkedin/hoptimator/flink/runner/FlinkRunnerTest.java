package com.linkedin.hoptimator.flink.runner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;


class FlinkRunnerTest {

  @Test
  void writeFileDecodesBase64AndWritesToDisk(@TempDir Path tempDir) throws Exception {
    String content = "print('hello world')";
    String encoded = Base64.getEncoder().encodeToString(content.getBytes(StandardCharsets.UTF_8));
    String directive = "--file:test.py:" + encoded;

    FlinkRunner.writeFile(directive, tempDir);

    Path written = tempDir.resolve("test.py");
    assertThat(written).exists();
    assertThat(Files.readString(written)).isEqualTo(content);
  }

  @Test
  void writeFileHandlesMultilineContent(@TempDir Path tempDir) throws Exception {
    String content = "from pyflink.table.udf import udf\n\n@udf(result_type=DataTypes.STRING())\ndef reverse(s):\n    return s[::-1]\n";
    String encoded = Base64.getEncoder().encodeToString(content.getBytes(StandardCharsets.UTF_8));
    String directive = "--file:my_udfs.py:" + encoded;

    FlinkRunner.writeFile(directive, tempDir);

    Path written = tempDir.resolve("my_udfs.py");
    assertThat(written).exists();
    assertThat(Files.readString(written)).isEqualTo(content);
  }

  @Test
  void writeFileRejectsMalformedDirective(@TempDir Path tempDir) {
    assertThatThrownBy(() -> FlinkRunner.writeFile("--file:no-colon-after-name", tempDir))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing ':'");
  }
}
