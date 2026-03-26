package com.linkedin.hoptimator.flink.runner.functions;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;


class StringLengthTest {

  private final StringLength stringLength = new StringLength();

  @Test
  void lengthOfString() {
    assertThat(stringLength.eval("hello")).isEqualTo(5);
  }

  @Test
  void lengthOfEmptyString() {
    assertThat(stringLength.eval("")).isEqualTo(0);
  }

  @Test
  void lengthOfNull() {
    assertThat(stringLength.eval(null)).isNull();
  }
}
