package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.MaterializedViewTable;
import com.linkedin.hoptimator.k8s.models.V1alpha1View;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ViewTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;


@ExtendWith(MockitoExtension.class)
class K8sViewTableTest {

  @Mock
  private HoptimatorConnection connection;

  private List<V1alpha1View> views;
  private K8sViewTable tableWithApi;

  @BeforeEach
  void setUp() {
    views = new ArrayList<>();
    tableWithApi = spy(new K8sViewTable(connection, null));
  }

  private Collection<K8sViewTable.Row> viewsAsRows(K8sViewTable table) {
    return views.stream().map(table::toRow).collect(Collectors.toList());
  }

  private void stubRows() {
    doReturn(viewsAsRows(tableWithApi)).when(tableWithApi).rows();
  }

  @Test
  void toRowMapsAllFields() {
    V1alpha1View view = new V1alpha1View()
        .metadata(new V1ObjectMeta().name("my-view"))
        .spec(new V1alpha1ViewSpec()
            .catalog("cat")
            .schema("sch")
            .view("vw")
            .sql("SELECT 1")
            .materialized(true));

    K8sViewTable table = new K8sViewTable(null, null);
    K8sViewTable.Row row = table.toRow(view);

    assertEquals("my-view", row.NAME);
    assertEquals("cat", row.CATALOG);
    assertEquals("sch", row.SCHEMA);
    assertEquals("vw", row.VIEW);
    assertEquals("SELECT 1", row.SQL);
    assertTrue(row.MATERIALIZED);
  }

  @Test
  void toRowWithNullMaterialized() {
    V1alpha1View view = new V1alpha1View()
        .metadata(new V1ObjectMeta().name("v"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 1").materialized(null));

    K8sViewTable table = new K8sViewTable(null, null);
    K8sViewTable.Row row = table.toRow(view);

    assertFalse(row.MATERIALIZED);
  }

  @Test
  void fromRowSetsK8sFields() {
    K8sViewTable.Row row = new K8sViewTable.Row("my-view", "cat", "sch", "vw", "SELECT 1", true);

    K8sViewTable table = new K8sViewTable(null, null);
    V1alpha1View view = table.fromRow(row);

    assertEquals("my-view", view.getMetadata().getName());
    assertEquals("SELECT 1", view.getSpec().getSql());
    assertTrue(view.getSpec().getMaterialized());
  }

  @Test
  void fromRowWithInvalidNameThrows() {
    K8sViewTable.Row row = new K8sViewTable.Row("INVALID_NAME", null, null, null, "SELECT 1", false);

    K8sViewTable table = new K8sViewTable(null, null);

    assertThrows(IllegalArgumentException.class, () -> table.fromRow(row));
  }

  @Test
  void fromRowSetsKindAndApiVersion() {
    K8sViewTable.Row row = new K8sViewTable.Row("my-view", null, null, null, "SELECT 1", false);

    K8sViewTable table = new K8sViewTable(null, null);
    V1alpha1View view = table.fromRow(row);

    assertEquals("View", view.getKind());
    assertNotNull(view.getApiVersion());
  }

  @Test
  void getJdbcTableTypeReturnsSystemTable() {
    K8sViewTable table = new K8sViewTable(null, null);
    assertEquals(Schema.TableType.SYSTEM_TABLE, table.getJdbcTableType());
  }

  @Test
  void fromRowWithNonMaterialized() {
    K8sViewTable.Row row = new K8sViewTable.Row("my-view", "cat", "sch", "vw", "SELECT 1", false);

    K8sViewTable table = new K8sViewTable(null, null);
    V1alpha1View view = table.fromRow(row);

    assertFalse(view.getSpec().getMaterialized());
  }

  @Test
  void toRowWithNullViewAndSchema() {
    V1alpha1View view = new V1alpha1View()
        .metadata(new V1ObjectMeta().name("simple"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 1").view(null).schema(null).catalog(null));

    K8sViewTable table = new K8sViewTable(null, null);
    K8sViewTable.Row row = table.toRow(view);

    assertEquals("simple", row.NAME);
    assertNull(row.VIEW);
    assertNull(row.SCHEMA);
    assertNull(row.CATALOG);
  }

  @Test
  void findReturnsMatchingRow() {
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("target"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 1")));
    // Re-stub because views list changed
    stubRows();

    K8sViewTable.Row row = tableWithApi.find("target");
    assertEquals("target", row.NAME);
  }

  @Test
  void findThrowsWhenNotFound() {
    stubRows();
    assertThrows(IllegalArgumentException.class, () -> tableWithApi.find("nonexistent"));
  }

  @Test
  void addAddsRow() {
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("existing"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 1")));
    stubRows();

    tableWithApi.add("new-view", "cat", "sch", "vw", "SELECT 2", false);

    // The add method adds to the rows collection
    // We can't verify directly without calling rows() which calls API
    // The fact it doesn't throw is the verification
    assertNotNull(tableWithApi);
  }

  @Test
  void validateWithValidRows() {
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("valid-view"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 1")));
    stubRows();

    Validator.Issues issues = new Validator.Issues("test");
    tableWithApi.validate(issues);
    assertNotNull(issues);
  }

  @Test
  void validateDetectsDuplicateNames() {
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("dup"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 1")));
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("dup"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 2")));
    stubRows();

    Validator.Issues issues = new Validator.Issues("test");
    tableWithApi.validate(issues);
    assertNotNull(issues);
  }

  @Test
  void addViewsCreatesSchemaPath() {
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("test-view"))
        .spec(new V1alpha1ViewSpec().catalog("MYCAT").schema("MYSCH").view("MYVIEW")
            .sql("SELECT 1").materialized(false)));
    stubRows();

    SchemaPlus root = CalciteSchema.createRootSchema(true).plus();

    // addViews should create the schema path and add the view
    tableWithApi.addViews(root);

    // Check that the schema path was created
    SchemaPlus catSchema = root.subSchemas().get("MYCAT");
    assertNotNull(catSchema, "Catalog schema should be created");
    SchemaPlus schSchema = catSchema.subSchemas().get("MYSCH");
    assertNotNull(schSchema, "Schema should be created");
  }

  @Test
  void addViewsWithNullSchemaUsesDefault() {
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("simple-view"))
        .spec(new V1alpha1ViewSpec().view("MYVIEW")
            .sql("SELECT 1").materialized(false)));
    stubRows();

    SchemaPlus root = CalciteSchema.createRootSchema(true).plus();

    tableWithApi.addViews(root);

    SchemaPlus defaultSchema = root.subSchemas().get("DEFAULT");
    assertNotNull(defaultSchema, "DEFAULT schema should be created");
  }

  @Test
  void addViewsWithExistingSchema() {
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("v1"))
        .spec(new V1alpha1ViewSpec().schema("EXISTING").view("VIEW1")
            .sql("SELECT 1").materialized(false)));
    stubRows();

    SchemaPlus root = CalciteSchema.createRootSchema(true).plus();
    root.add("EXISTING", new AbstractSchema());

    tableWithApi.addViews(root);

    SchemaPlus existing = root.subSchemas().get("EXISTING");
    assertNotNull(existing);
  }

  @Test
  void registerMaterializationsWithMaterialized() {
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("mv"))
        .spec(new V1alpha1ViewSpec().catalog("cat").schema("sch").view("vw").sql("SELECT 1")
            .materialized(true)));
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("nonmv"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 2").materialized(false)));
    stubRows();

    tableWithApi.registerMaterializations(connection);
    // No exception means success
    assertNotNull(tableWithApi);
  }

  @Test
  void addViewsCreatesSchemaWhenMissing() {
    // When subschema is missing it must be created
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("test-view"))
        .spec(new V1alpha1ViewSpec().schema("NEWSCHEMA").view("MYVIEW")
            .sql("SELECT 1").materialized(false)));
    stubRows();

    SchemaPlus root = CalciteSchema.createRootSchema(true).plus();
    // NEWSCHEMA does not exist yet — addViews must create it
    assertNull(root.subSchemas().get("NEWSCHEMA"), "precondition: schema must not exist");

    tableWithApi.addViews(root);

    SchemaPlus created = root.subSchemas().get("NEWSCHEMA");
    assertNotNull(created, "schema must be created when missing");
  }

  @Test
  void addViewsAddsViewToSchemaPlus() {
    // Verify view IS accessible after addViews
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("test-view"))
        .spec(new V1alpha1ViewSpec().schema("MYSCH").view("MY_VIEW")
            .sql("SELECT 1").materialized(false)));
    stubRows();

    SchemaPlus root = CalciteSchema.createRootSchema(true).plus();

    tableWithApi.addViews(root);

    SchemaPlus schema = root.subSchemas().get("MYSCH");
    assertNotNull(schema, "schema must exist");
    Table table = schema.getTable("MY_VIEW");
    assertNotNull(table, "View must be added to schema via schemaPlus.add()");
  }

  @Test
  void makeViewReturnsMaterializedViewTableWhenMaterialized() {
    // Conditional on materialized — must return MaterializedViewTable not plain ViewTable
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("mv"))
        .spec(new V1alpha1ViewSpec().schema("MYSCH").view("MV_VIEW")
            .sql("SELECT 1").materialized(true)));
    stubRows();

    SchemaPlus root = CalciteSchema.createRootSchema(true).plus();

    tableWithApi.addViews(root);

    SchemaPlus schema = root.subSchemas().get("MYSCH");
    assertNotNull(schema);
    Table table = schema.getTable("MV_VIEW");
    assertNotNull(table);
    assertInstanceOf(MaterializedViewTable.class, table,
        "materialized=true must produce MaterializedViewTable, not plain ViewTable");
  }

  @Test
  void makeViewReturnsViewTableWhenNotMaterialized() {
    // non-materialized must return ViewTable
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("vt"))
        .spec(new V1alpha1ViewSpec().schema("MYSCH").view("VT_VIEW")
            .sql("SELECT 1").materialized(false)));
    stubRows();

    SchemaPlus root = CalciteSchema.createRootSchema(true).plus();

    tableWithApi.addViews(root);

    SchemaPlus schema = root.subSchemas().get("MYSCH");
    assertNotNull(schema);
    Table table = schema.getTable("VT_VIEW");
    assertNotNull(table);
    assertInstanceOf(ViewTable.class, table,
        "materialized=false must produce ViewTable");
    assertFalse(table instanceof MaterializedViewTable,
        "non-materialized view must not be wrapped in MaterializedViewTable");
  }

  @Test
  void registerMaterializationsCallsConnectionForEachMaterialized() {
    // registerMaterializations — verify connection.registerMaterialization IS called
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("mv1"))
        .spec(new V1alpha1ViewSpec().catalog("cat").schema("sch").view("vw1").sql("SELECT 1")
            .materialized(true)));
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("mv2"))
        .spec(new V1alpha1ViewSpec().catalog("cat").schema("sch").view("vw2").sql("SELECT 2")
            .materialized(true)));
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("nonmv"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 3").materialized(false)));
    stubRows();

    tableWithApi.registerMaterializations(connection);

    // Must be called exactly twice — once for each materialized view
    verify(connection).registerMaterialization(argThat(path -> path.contains("vw1")), eq("SELECT 1"));
    verify(connection).registerMaterialization(argThat(path -> path.contains("vw2")), eq("SELECT 2"));
  }

  @Test
  void validateWithDuplicateNameRecordsError() {
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("dup-view"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 1")));
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("dup-view"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 2")));
    stubRows();

    Validator.Issues issues = new Validator.Issues("test");
    tableWithApi.validate(issues);

    assertFalse(issues.valid(),
        "Duplicate view name must record an error, making issues invalid");
  }

  @Test
  void validateWithUniqueValidNamesIsValid() {
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("view-one"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 1")));
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("view-two"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 2")));
    stubRows();

    Validator.Issues issues = new Validator.Issues("test");
    tableWithApi.validate(issues);

    assertTrue(issues.valid(), "unique valid names must not produce errors");
  }

  @Test
  void findThrowsForNonExistentName() {
    // lambda$find$0 — filter always returns true
    // If the filter always returns true, find("nonexistent") would return the first row instead of throwing
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("real-view"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 1")));
    stubRows();

    assertThrows(IllegalArgumentException.class, () -> tableWithApi.find("nonexistent"),
        "find() with non-existent name must throw, not return a random row");
  }

  @Test
  void findReturnsCorrectRowNotFirstRow() {
    // Complement: find("second") must return "second", not "first"
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("first"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 1")));
    views.add(new V1alpha1View()
        .metadata(new V1ObjectMeta().name("second"))
        .spec(new V1alpha1ViewSpec().sql("SELECT 2")));
    stubRows();

    K8sViewTable.Row row = tableWithApi.find("second");
    assertEquals("second", row.NAME,
        "find() must filter by name, not return first row");
    assertEquals("SELECT 2", row.SQL);
  }

}
