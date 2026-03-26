package com.linkedin.hoptimator.flink.runner.functions;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;


class GreetTest {

  private final Greet greet = new Greet();

  @Test
  void greetName() {
    assertThat(greet.eval("Alice")).isEqualTo("Hello, Alice!");
  }

  @Test
  void greetEmptyString() {
    assertThat(greet.eval("")).isEqualTo("Hello, !");
  }

  @Test
  void greetNull() {
    assertThat(greet.eval(null)).isNull();
  }
}
