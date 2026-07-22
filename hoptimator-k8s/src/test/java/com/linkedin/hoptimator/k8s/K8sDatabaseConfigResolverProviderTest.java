package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.jdbc.DatabaseConfigResolver;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;


class K8sDatabaseConfigResolverProviderTest {

  private final K8sDatabaseConfigResolverProvider provider = new K8sDatabaseConfigResolverProvider();

  @Test
  void resolverReturnsK8sResolver() {
    DatabaseConfigResolver resolver = provider.resolver(new Properties());

    assertThat(resolver).isInstanceOf(K8sDatabaseConfigResolver.class);
  }

  @Test
  void priorityIsOne() {
    assertThat(provider.priority()).isEqualTo(1);
  }
}
