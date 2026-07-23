package com.linkedin.hoptimator.util;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;


class SimpleDeploymentContextTest {

  @Test
  void propertiesReturnsSuppliedBag() {
    Properties props = new Properties();
    props.setProperty("k8s.watch.namespace", "ns");
    SimpleDeploymentContext context = new SimpleDeploymentContext(props);

    assertThat(context.properties()).isSameAs(props);
  }

  @Test
  void databasePropertiesAlwaysReturnsNull() {
    SimpleDeploymentContext context = new SimpleDeploymentContext(new Properties());

    assertThat(context.databaseProperties("CAT", "SCHEMA", "jdbc:kafka://")).isNull();
    assertThat(context.databaseProperties(null, null, "jdbc:venice://")).isNull();
  }
}
