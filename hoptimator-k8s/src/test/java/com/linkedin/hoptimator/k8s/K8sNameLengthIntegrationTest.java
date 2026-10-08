package com.linkedin.hoptimator.k8s;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins down exactly how the real Kubernetes apiserver rejects a resource name that exceeds the
 * DNS-subdomain limit of 253 characters. {@link K8sUtils#checkK8sName} guards Hoptimator's own
 * resources at the same ceiling, but the apiserver is the ultimate authority, so this test asserts
 * against it directly. A plain ConfigMap is used because its name is validated as a DNS subdomain
 * (253) with no required spec fields, isolating the name-length check from CRD schema validation.
 */
@Tag("integration")
public class K8sNameLengthIntegrationTest {

  private static final K8sApiEndpoint<V1ConfigMap, V1ConfigMapList> CONFIG_MAPS = K8sApiEndpoints.CONFIG_MAPS;

  private K8sApi<V1ConfigMap, V1ConfigMapList> configMapApi() {
    Properties properties = new Properties();
    properties.setProperty(K8sContext.NAMESPACE_KEY, "default");
    return new K8sApi<>(K8sContext.create(properties), CONFIG_MAPS);
  }

  @Test
  void createFailsWhenNameExceeds253Characters() {
    String name = "a".repeat(K8sUtils.MAX_NAME_LENGTH + 1);
    V1ConfigMap configMap = new V1ConfigMap().metadata(new V1ObjectMeta().name(name));

    // The apiserver returns HTTP 422 (Invalid); K8sApi surfaces that as a non-transient SQLException
    // whose vendor code is the HTTP status and whose cause carries the apiserver's validation detail.
    assertThatThrownBy(() -> configMapApi().create(configMap))
        .isInstanceOf(SQLNonTransientException.class)
        .satisfies(thrown -> {
          SQLNonTransientException sql = (SQLNonTransientException) thrown;
          assertThat(sql.getErrorCode())
              .as("HTTP 422 Invalid is surfaced as the vendor code")
              .isEqualTo(422);
          assertThat(sql.getCause())
              .as("apiserver detail is preserved on the cause")
              .isInstanceOf(ApiException.class);
          assertThat(((ApiException) sql.getCause()).getResponseBody())
              .contains("must be no more than 253 characters");
        });
  }

  @Test
  void createSucceedsAtMaxNameLength() throws SQLException {
    String name = "a".repeat(K8sUtils.MAX_NAME_LENGTH);
    K8sApi<V1ConfigMap, V1ConfigMapList> api = configMapApi();
    try {
      api.create(new V1ConfigMap().metadata(new V1ObjectMeta().name(name)));
    } finally {
      api.delete(name);
    }
  }
}
