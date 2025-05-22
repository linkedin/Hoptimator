package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1SqlJob;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.Yaml;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class TestK8sSnapshotProvider {
  private FakeK8sYamlApi fakeApi;
  private K8sSnapshotProvider snapshotProvider;
  private Map<String, String> yamls;

  @BeforeEach
  void setUp() {
    yamls = new HashMap<>();
    fakeApi = new FakeK8sYamlApi(yamls);
    snapshotProvider = new K8sSnapshotProvider(fakeApi);
  }

  @Test
  void testSnapshotRestoreDelete() throws SQLException {
    V1alpha1SqlJob sqlJob = new V1alpha1SqlJob();
    sqlJob.setApiVersion("hoptimator.linkedin.com/v1alpha1");
    sqlJob.setKind("SqlJob");
    sqlJob.setMetadata(new V1ObjectMeta().name("test-sql-job"));

    snapshotProvider.snapshot(Collections.singletonList(Yaml.dump(sqlJob)), new Properties());
    fakeApi.create(Yaml.dump(sqlJob));
    assertEquals(yamls.size(), 1);
    assertEquals(yamls.get("test-sql-job"), Yaml.dump(sqlJob));

    snapshotProvider.restore();
    assertTrue(yamls.isEmpty());
  }

  @Test
  void testRestoreWithUpdate() throws SQLException {
    V1alpha1SqlJob oldSqlJob = new V1alpha1SqlJob();
    oldSqlJob.setApiVersion("hoptimator.linkedin.com/v1alpha1");
    oldSqlJob.setKind("SqlJob");
    oldSqlJob.setMetadata(new V1ObjectMeta().name("test-sql-job").putAnnotationsItem("key", "old-value"));

    V1alpha1SqlJob newSqlJob = new V1alpha1SqlJob();
    newSqlJob.setApiVersion("hoptimator.linkedin.com/v1alpha1");
    newSqlJob.setKind("SqlJob");
    newSqlJob.setMetadata(new V1ObjectMeta().name("test-sql-job").putAnnotationsItem("key", "new-value"));

    fakeApi.create(Yaml.dump(oldSqlJob));
    snapshotProvider.snapshot(Collections.singletonList(Yaml.dump(newSqlJob)), new Properties());
    fakeApi.create(Yaml.dump(newSqlJob));
    assertEquals(yamls.size(), 1);
    assertEquals(yamls.get("test-sql-job"), Yaml.dump(newSqlJob));

    snapshotProvider.restore();
    assertEquals(yamls.get("test-sql-job"), Yaml.dump(oldSqlJob));
  }

}
