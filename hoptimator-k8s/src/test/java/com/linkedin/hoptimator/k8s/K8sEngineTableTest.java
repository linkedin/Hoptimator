package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Engine;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.k8s.models.V1alpha1Engine;
import com.linkedin.hoptimator.k8s.models.V1alpha1EngineSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.apache.calcite.schema.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;


@ExtendWith(MockitoExtension.class)
class K8sEngineTableTest {

  @Test
  void toRowMapsAllFields() {
    V1alpha1Engine engine = new V1alpha1Engine()
        .metadata(new V1ObjectMeta().name("my-engine"))
        .spec(new V1alpha1EngineSpec()
            .url("jdbc:test://host")
            .dialect(V1alpha1EngineSpec.DialectEnum.FLINK)
            .driver("com.test.Driver")
            .databases(Arrays.asList("db1", "db2")));

    K8sEngineTable table = new K8sEngineTable(null);
    K8sEngineTable.Row row = table.toRow(engine);

    assertEquals("my-engine", row.NAME);
    assertEquals("jdbc:test://host", row.URL);
    assertEquals(V1alpha1EngineSpec.DialectEnum.FLINK.toString(), row.DIALECT);
    assertEquals("com.test.Driver", row.DRIVER);
    assertEquals(2, row.DATABASES.length);
    assertEquals("db1", row.DATABASES[0]);
    assertEquals("db2", row.DATABASES[1]);
  }

  @Test
  void toRowWithNullDialect() {
    V1alpha1Engine engine = new V1alpha1Engine()
        .metadata(new V1ObjectMeta().name("eng"))
        .spec(new V1alpha1EngineSpec().url("jdbc:test://host").dialect(null));

    K8sEngineTable table = new K8sEngineTable(null);
    K8sEngineTable.Row row = table.toRow(engine);

    assertNull(row.DIALECT);
  }

  @Test
  void toRowWithNullDatabases() {
    V1alpha1Engine engine = new V1alpha1Engine()
        .metadata(new V1ObjectMeta().name("eng"))
        .spec(new V1alpha1EngineSpec().url("jdbc:test://host").databases(null));

    K8sEngineTable table = new K8sEngineTable(null);
    K8sEngineTable.Row row = table.toRow(engine);

    assertNull(row.DATABASES);
  }

  @Test
  void fromRowSetsK8sFields() {
    K8sEngineTable.Row row = new K8sEngineTable.Row("my-engine", "jdbc:test://host", "ANSI", "com.test.Driver",
        new String[]{"db1", "db2"});

    K8sEngineTable table = new K8sEngineTable(null);
    V1alpha1Engine engine = table.fromRow(row);

    assertEquals("my-engine", engine.getMetadata().getName());
    assertEquals("jdbc:test://host", engine.getSpec().getUrl());
    assertEquals("com.test.Driver", engine.getSpec().getDriver());
    assertEquals(2, engine.getSpec().getDatabases().size());
    assertEquals("Engine", engine.getKind());
    assertNotNull(engine.getApiVersion());
  }

  @Test
  void fromRowWithInvalidNameThrows() {
    K8sEngineTable.Row row = new K8sEngineTable.Row("INVALID_NAME", "jdbc:test://host", "ANSI", null,
        new String[]{});

    K8sEngineTable table = new K8sEngineTable(null);

    assertThrows(IllegalArgumentException.class, () -> table.fromRow(row));
  }

  @Test
  void getJdbcTableTypeReturnsSystemTable() {
    K8sEngineTable table = new K8sEngineTable(null);
    assertEquals(Schema.TableType.SYSTEM_TABLE, table.getJdbcTableType());
  }

  @Test
  void forDatabaseMatchesEngineWithMatchingDatabase() {
    K8sEngineTable table = spy(new K8sEngineTable(null));
    K8sEngineTable.Row row = new K8sEngineTable.Row("engine-1", "jdbc:test://host", "ANSI", "com.test.Driver",
        new String[]{"db1", "db2"});
    doReturn(List.of(row)).when(table).rows();

    List<Engine> engines = table.forDatabase("db1");

    assertEquals(1, engines.size());
    assertEquals("engine-1", engines.get(0).engineName());
    assertEquals(SqlDialect.ANSI, engines.get(0).dialect());
  }

  @Test
  void forDatabaseExcludesEngineWithoutMatchingDatabase() {
    K8sEngineTable table = spy(new K8sEngineTable(null));
    K8sEngineTable.Row row = new K8sEngineTable.Row("engine-1", "jdbc:test://host", "FLINK", "com.test.Driver",
        new String[]{"db1", "db2"});
    doReturn(List.of(row)).when(table).rows();

    List<Engine> engines = table.forDatabase("db3");

    assertTrue(engines.isEmpty());
  }

  @Test
  void forDatabaseIncludesEngineWithNullDatabases() {
    K8sEngineTable table = spy(new K8sEngineTable(null));
    K8sEngineTable.Row row = new K8sEngineTable.Row("engine-1", "jdbc:test://host", "FLINK", null, null);
    doReturn(List.of(row)).when(table).rows();

    List<Engine> engines = table.forDatabase("any-db");

    assertEquals(1, engines.size());
    assertEquals(SqlDialect.FLINK, engines.get(0).dialect());
  }

  @Test
  void forDatabaseIncludesEngineWithEmptyDatabases() {
    K8sEngineTable table = spy(new K8sEngineTable(null));
    K8sEngineTable.Row row = new K8sEngineTable.Row("engine-1", "jdbc:test://host", null, null, new String[]{});
    doReturn(List.of(row)).when(table).rows();

    List<Engine> engines = table.forDatabase("any-db");

    assertEquals(1, engines.size());
    assertEquals(SqlDialect.ANSI, engines.get(0).dialect());
  }
}
