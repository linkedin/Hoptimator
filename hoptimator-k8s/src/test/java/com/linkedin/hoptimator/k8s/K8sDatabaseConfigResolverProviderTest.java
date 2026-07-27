package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.jdbc.DatabaseConfigResolver;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;


class K8sDatabaseConfigResolverProviderTest {

  private final K8sDatabaseConfigResolverProvider provider = new K8sDatabaseConfigResolverProvider();

  @Test
  void resolverReturnsK8sResolver() {
    // Offline connection properties: server+token make K8sContext build an ApiClient via
    // Config.fromToken (no kubeconfig read, no network), so constructing the resolver does not
    // require an ambient cluster. The test only asserts the resolver type.
    Properties props = new Properties();
    props.setProperty("k8s.server", "https://localhost:1");
    props.setProperty("k8s.token", "test-token");
    props.setProperty("k8s.namespace", "default");
    DatabaseConfigResolver resolver = provider.resolver(props);

    assertThat(resolver).isInstanceOf(K8sDatabaseConfigResolver.class);
  }

  @Test
  void priorityIsOne() {
    assertThat(provider.priority()).isEqualTo(1);
  }
}
