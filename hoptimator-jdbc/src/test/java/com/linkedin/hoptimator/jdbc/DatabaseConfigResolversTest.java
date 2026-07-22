package com.linkedin.hoptimator.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;


class DatabaseConfigResolversTest {

  @Test
  void forPropertiesReturnsServiceLoadedResolver() throws SQLException {
    // The jdbc test classpath registers TestDatabaseConfigResolverProvider (priority 0), which
    // resolves the database identifier to the path's schema segment.
    DatabaseConfigResolver resolver = DatabaseConfigResolvers.forProperties(new Properties());

    assertThat(resolver).isNotNull();
    assertThat(resolver.databaseName(List.of("MYDB", "myTable"))).isEqualTo("MYDB");
  }
}
