package com.linkedin.hoptimator.jdbc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


class DatabaseConfigResolverProviderTest {

  @Test
  void priorityDefaultsToZero() {
    // A provider that does not override priority() (e.g. the jdbc test fixture) reports the default.
    DatabaseConfigResolverProvider provider = new TestDatabaseConfigResolverProvider();

    assertThat(provider.priority()).isEqualTo(0);
  }
}
