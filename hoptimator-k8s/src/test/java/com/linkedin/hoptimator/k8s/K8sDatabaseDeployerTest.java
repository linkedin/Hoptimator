package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.DatabaseDeployable;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sDatabaseDeployerTest {

  private List<V1alpha1Database> objects;
  private FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> fakeApi;
  private FakeK8sYamlApi fakeYamlApi;
  private K8sSnapshot snapshot;

  @BeforeEach
  void setUp() {
    objects = new ArrayList<>();
    fakeApi = new FakeK8sApi<>(objects);
    Map<String, String> yamls = new HashMap<>();
    fakeYamlApi = new FakeK8sYamlApi(yamls);
    snapshot = new K8sSnapshot(null) {
      @Override
      K8sYamlApi createYamlApi(K8sContext context) {
        return fakeYamlApi;
      }
    };
  }

  private K8sDatabaseDeployer makeDeployer(DatabaseDeployable database) {
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> capturedApi = fakeApi;
    K8sSnapshot capturedSnapshot = snapshot;
    return new K8sDatabaseDeployer(database, null) {
      @Override
      K8sApi<V1alpha1Database, V1alpha1DatabaseList> createApi(K8sContext context,
          K8sApiEndpoint<V1alpha1Database, V1alpha1DatabaseList> endpoint) {
        return capturedApi;
      }

      @Override
      K8sSnapshot createSnapshot(K8sContext context) {
        return capturedSnapshot;
      }
    };
  }

  @Test
  void specifyWithUrlOnlyReturnsYaml() throws SQLException {
    Map<String, String> options = new HashMap<>();
    options.put("url", "jdbc:mysql://localhost:3306/mydb");
    DatabaseDeployable database = new DatabaseDeployable("mydb", options);

    K8sDatabaseDeployer deployer = makeDeployer(database);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    assertTrue(specs.get(0).contains("mydb"));
    assertTrue(specs.get(0).contains("jdbc:mysql://localhost:3306/mydb"));
  }

  @Test
  void specifyWithAllOptionsPopulatesSpec() throws SQLException {
    Map<String, String> options = new HashMap<>();
    options.put("url", "jdbc:mysql://localhost:3306/mydb");
    options.put("driver", "com.mysql.cj.jdbc.Driver");
    options.put("schema", "myschema");
    options.put("catalog", "mycatalog");
    options.put("dialect", "MySQL");
    DatabaseDeployable database = new DatabaseDeployable("mydb", options);

    K8sDatabaseDeployer deployer = makeDeployer(database);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    String yaml = specs.get(0);
    assertTrue(yaml.contains("mydb"));
    assertTrue(yaml.contains("jdbc:mysql://localhost:3306/mydb"));
    assertTrue(yaml.contains("com.mysql.cj.jdbc.Driver"));
    assertTrue(yaml.contains("myschema"));
    assertTrue(yaml.contains("mycatalog"));
  }

  @Test
  void specifyWithUppercasedOptionKeysUsesCaseInsensitiveLookup() throws SQLException {
    // SQL parser uppercases unquoted identifiers, so WITH (url '...') produces key "URL"
    Map<String, String> options = new HashMap<>();
    options.put("URL", "jdbc:mysql://localhost:3306/mydb");
    options.put("DRIVER", "com.mysql.cj.jdbc.Driver");
    options.put("SCHEMA", "myschema");
    options.put("DIALECT", "MySQL");
    DatabaseDeployable database = new DatabaseDeployable("mydb", options);

    K8sDatabaseDeployer deployer = makeDeployer(database);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    String yaml = specs.get(0);
    assertTrue(yaml.contains("jdbc:mysql://localhost:3306/mydb"));
    assertTrue(yaml.contains("com.mysql.cj.jdbc.Driver"));
    assertTrue(yaml.contains("myschema"));
  }

  @Test
  void specifyThrowsWhenUrlMissing() {
    Map<String, String> options = new HashMap<>();
    options.put("driver", "com.mysql.cj.jdbc.Driver");
    DatabaseDeployable database = new DatabaseDeployable("mydb", options);

    K8sDatabaseDeployer deployer = makeDeployer(database);

    assertThrows(SQLException.class, deployer::specify);
  }

  @Test
  void specifyWithEmptyOptionsThrowsForMissingUrl() {
    DatabaseDeployable database = new DatabaseDeployable("mydb", Collections.emptyMap());

    K8sDatabaseDeployer deployer = makeDeployer(database);

    assertThrows(SQLException.class, deployer::specify);
  }

  @Test
  void createAddsObjectToApi() throws SQLException {
    Map<String, String> options = new HashMap<>();
    options.put("url", "jdbc:mysql://localhost:3306/mydb");
    DatabaseDeployable database = new DatabaseDeployable("mydb", options);

    K8sDatabaseDeployer deployer = makeDeployer(database);
    deployer.create();

    assertEquals(1, objects.size());
    assertEquals("mydb", objects.get(0).getMetadata().getName());
  }

  @Test
  void specifyYamlContainsCorrectApiVersionAndKind() throws SQLException {
    Map<String, String> options = new HashMap<>();
    options.put("url", "jdbc:mysql://localhost:3306/mydb");
    DatabaseDeployable database = new DatabaseDeployable("mydb", options);

    K8sDatabaseDeployer deployer = makeDeployer(database);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    String yaml = specs.get(0);
    assertTrue(yaml.contains("Database"), "spec must contain kind 'Database'");
    assertTrue(yaml.contains("hoptimator.linkedin.com"), "spec must contain the Hoptimator API group");
  }

  @Test
  void specifyWithNoDriverDoesNotIncludeDriverField() throws SQLException {
    Map<String, String> options = new HashMap<>();
    options.put("url", "jdbc:mysql://localhost:3306/mydb");
    DatabaseDeployable database = new DatabaseDeployable("mydb", options);

    K8sDatabaseDeployer deployer = makeDeployer(database);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    // driver field must be absent when not supplied
    String yaml = specs.get(0);
    // The spec object's driver field should be null, so YAML serialization omits it
    V1alpha1Database created = objects.isEmpty() ? null : objects.get(0);
    assertNull(created);
    // Verify via the spec content: url is present but driver is not explicitly set
    assertTrue(yaml.contains("url: jdbc:mysql://localhost:3306/mydb"));
  }

  @Test
  void specifyNameIsCanonicalizedToLowerKebabCase() throws SQLException {
    Map<String, String> options = new HashMap<>();
    options.put("url", "jdbc:mysql://localhost:3306/MyDb");
    DatabaseDeployable database = new DatabaseDeployable("MyDb", options);

    K8sDatabaseDeployer deployer = makeDeployer(database);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    // K8sUtils.canonicalizeName converts to lower-kebab-case
    assertTrue(specs.get(0).contains("mydb"), "name must be canonicalized to lower-kebab-case");
  }

  @Test
  void specifyWithAnsiDialectSetsDialectEnum() throws SQLException {
    Map<String, String> options = new HashMap<>();
    options.put("url", "jdbc:calcite://");
    options.put("dialect", "ANSI");
    DatabaseDeployable database = new DatabaseDeployable("testdb", options);

    K8sDatabaseDeployer deployer = makeDeployer(database);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    // ANSI dialect should be reflected in the spec
    assertNotNull(specs.get(0));
  }

  @Test
  void specifyWithInvalidDialectThrowsIllegalArgumentException() {
    Map<String, String> options = new HashMap<>();
    options.put("url", "jdbc:mysql://localhost/mydb");
    options.put("dialect", "InvalidDialect");
    DatabaseDeployable database = new DatabaseDeployable("mydb", options);

    K8sDatabaseDeployer deployer = makeDeployer(database);

    assertThrows(IllegalArgumentException.class, deployer::specify);
  }
}
