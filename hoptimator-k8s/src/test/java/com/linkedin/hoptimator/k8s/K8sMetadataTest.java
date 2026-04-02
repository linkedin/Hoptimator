package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.Lookup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


@ExtendWith(MockitoExtension.class)
class K8sMetadataTest {

  @Mock
  private HoptimatorConnection connection;

  @Mock
  private K8sContext context;

  private K8sMetadata metadata;

  @BeforeEach
  void setUp() {
    metadata = new K8sMetadata(connection, context);
  }

  @Test
  void constructorAcceptsNullContext() {
    K8sMetadata m = new K8sMetadata(connection, null);
    assertNotNull(m);
  }

  @Test
  void tablesReturnsLookup() {
    Lookup<Table> tables = metadata.tables();
    assertNotNull(tables);
  }

  @Test
  void tablesReturnsSameLookupOnMultipleCalls() {
    Lookup<Table> first = metadata.tables();
    Lookup<Table> second = metadata.tables();
    assertNotNull(first);
    assertNotNull(second);
  }

  @Test
  void databaseTableReturnsDatabaseTable() {
    Table table = metadata.tables().get("DATABASES");
    assertInstanceOf(K8sDatabaseTable.class, table);
  }

  @Test
  void viewTableReturnsViewTable() {
    Table table = metadata.tables().get("VIEWS");
    assertInstanceOf(K8sViewTable.class, table);
  }

  @Test
  void enginesTableReturnsEngineTable() {
    Table table = metadata.tables().get("ENGINES");
    assertInstanceOf(K8sEngineTable.class, table);
  }

  @Test
  void pipelinesTableReturnsPipelineTable() {
    Table table = metadata.tables().get("PIPELINES");
    assertInstanceOf(K8sPipelineTable.class, table);
  }

  @Test
  void pipelineElementsTableReturnsPipelineElementTable() {
    Table table = metadata.tables().get("PIPELINE_ELEMENTS");
    assertInstanceOf(K8sPipelineElementTable.class, table);
  }

  @Test
  void pipelineElementMapTableReturnsPipelineElementMapTable() {
    Table table = metadata.tables().get("PIPELINE_ELEMENT_MAP");
    assertInstanceOf(K8sPipelineElementMapTable.class, table);
  }

  @Test
  void tableTriggersTableReturnsTableTriggerTable() {
    Table table = metadata.tables().get("TABLE_TRIGGERS");
    assertInstanceOf(K8sTableTriggerTable.class, table);
  }

  @Test
  void unknownTableReturnsNull() {
    Table table = metadata.tables().get("NONEXISTENT");
    assertNull(table);
  }

  @Test
  void databaseTableMethodReturnsCorrectType() {
    K8sDatabaseTable dbTable = metadata.databaseTable();
    assertNotNull(dbTable);
  }

  @Test
  void viewTableMethodReturnsCorrectType() {
    K8sViewTable viewTable = metadata.viewTable();
    assertNotNull(viewTable);
  }
}
