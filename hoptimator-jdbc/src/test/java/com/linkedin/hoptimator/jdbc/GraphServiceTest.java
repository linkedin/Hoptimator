package com.linkedin.hoptimator.jdbc;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.lookup.Lookup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linkedin.hoptimator.graph.GraphNode;
import com.linkedin.hoptimator.graph.GraphTarget;
import com.linkedin.hoptimator.graph.PipelineGraph;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcSchema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;


/**
 * Unit tests for {@link GraphService}. Covers SPI discovery (renderer registration via
 * {@code META-INF/services}) and the resolver that walks Calcite's schema tree to figure out
 * what kind of entity a SQL identifier refers to.
 */
@ExtendWith(MockitoExtension.class)
class GraphServiceTest {

  @Mock
  private HoptimatorConnection connection;
  @Mock
  private CalciteConnection calciteConnection;

  // ─── SPI discovery ───────────────────────────────────────────────────────

  @Test
  void availableFormatsIncludesMermaid() {
    List<String> formats = GraphService.availableFormats();
    assertTrue(formats.contains("mermaid"),
        "MermaidRenderer should be discoverable via ServiceLoader: " + formats);
  }

  @Test
  void renderUnknownFormatThrowsWithHelpfulMessage() {
    GraphNode root = new GraphNode.External("db", List.of("t"));
    PipelineGraph graph = new PipelineGraph(root, Set.of(root), Set.of());

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> GraphService.render(graph, "definitely-not-a-format"));
    assertTrue(ex.getMessage().contains("definitely-not-a-format"),
        "error should mention the unknown format: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("Available"),
        "error should list available formats: " + ex.getMessage());
  }

  // ─── Identifier resolution via Calcite ────────────────────────────────────

  @Test
  void resolveTwoLevelResourceFindsDatabaseCrdName() throws SQLException {
    // User typed ADS.AD_CLICKS. Schema ADS is a Database in the connection with CRD name ads-database,
    // and AD_CLICKS is a real table in its catalog (a plain physical table, not a view / LogicalTable).
    SchemaPlus ads = schemaWithDatabaseAndTable("ads-database", "AD_CLICKS", plainTable());
    stubRootSchema(schemaWithSubs("ADS", ads));

    GraphTarget.Resource out = (GraphTarget.Resource) GraphService.resolve("ADS.AD_CLICKS", connection);

    assertEquals("ads-database", out.database());
    assertEquals(Arrays.asList("ADS", "AD_CLICKS"), out.path());
  }

  @Test
  void resolveThreeLevelResourceWalksCatalogAndSchema() throws SQLException {
    // User typed MYSQL.testdb.orders. MYSQL is the catalog sub-schema; testdb is the Database;
    // orders is a real physical table in the catalog.
    SchemaPlus testdb = schemaWithDatabaseAndTable("mysql", "orders", plainTable());
    SchemaPlus mysqlCatalog = schemaWithSubs("testdb", testdb);
    stubRootSchema(schemaWithSubs("MYSQL", mysqlCatalog));

    GraphTarget.Resource out = (GraphTarget.Resource) GraphService.resolve(
        "MYSQL.testdb.orders", connection);

    assertEquals("mysql", out.database());
    assertEquals(Arrays.asList("MYSQL", "testdb", "orders"), out.path());
  }

  @Test
  void resolveUnknownTableInKnownSchemaThrowsSqlException() {
    // Schema ADS resolves to a Database, but no AD_CLICKS table exists in the catalog.
    // Distinct from "schema doesn't exist": this is "you typo'd the table name", which is
    // different from "table exists but no pipeline references it" (the no-pipelines path,
    // which doesn't reach this codepath — it's render-time).
    SchemaPlus ads = schemaWithDatabaseAndTable("ads-database", /*tableName=*/null, /*table=*/null);
    stubRootSchema(schemaWithSubs("ADS", ads));

    SQLException ex = assertThrows(SQLException.class,
        () -> GraphService.resolve("ADS.AD_CLICKS", connection));
    assertTrue(ex.getMessage().contains("ADS.AD_CLICKS"),
        "error should name the offending identifier: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("AD_CLICKS"),
        "error should pinpoint the missing table name: " + ex.getMessage());
  }

  @Test
  void resolveMaterializedViewProducesViewTarget() throws SQLException {
    // The leaf table is a MaterializedViewTable — kind detection returns GraphTarget.View.
    MaterializedViewTable mv = new MaterializedViewTable(mock(ViewTable.class));
    SchemaPlus profile = schemaWithDatabaseAndTable("profile-database", "AUDIENCE", mv);
    stubRootSchema(schemaWithSubs("PROFILE", profile));

    GraphTarget out = GraphService.resolve("PROFILE.AUDIENCE", connection);

    GraphTarget.View view = assertInstanceOf(GraphTarget.View.class, out);
    assertEquals("PROFILE.AUDIENCE", view.name(),
        "View target carries the SQL-side identifier; the builder canonicalizes downstream");
  }

  @Test
  void resolveLogicalTableProducesLogicalTableTarget() throws SQLException {
    // The schema unwraps to a HoptimatorJdbcSchema whose isLogical() is true — detection runs at
    // the schema level because the outer JDBC adapter wraps tables and erases the LogicalTable
    // class identity.
    SchemaPlus logical = schemaWithDatabaseAndTable("logical-database", "MEMBERS", plainTable(),
        /*isLogical=*/true);
    stubRootSchema(schemaWithSubs("LOGICAL", logical));

    GraphTarget out = GraphService.resolve("LOGICAL.MEMBERS", connection);

    GraphTarget.LogicalTable ltTarget = assertInstanceOf(GraphTarget.LogicalTable.class, out);
    assertEquals("LOGICAL.MEMBERS", ltTarget.name());
  }

  @Test
  void resolveUnknownSchemaSegmentThrowsSqlException() {
    // User typed UNKNOWN.FOO. Root has no UNKNOWN sub-schema — bail with a clear error.
    stubRootSchema(schemaWithSubs("UNKNOWN", null));

    SQLException ex = assertThrows(SQLException.class,
        () -> GraphService.resolve("UNKNOWN.FOO", connection));
    assertTrue(ex.getMessage().contains("UNKNOWN.FOO"),
        "error should name the offending identifier: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("UNKNOWN"),
        "error should pinpoint which segment failed: " + ex.getMessage());
  }

  @Test
  void resolveSchemaNotBackedByHoptimatorJdbcSchemaThrowsSqlException() {
    // The schema resolves AND a leaf table exists, but the schema isn't a Hoptimator JDBC
    // schema — e.g. an information_schema with system tables. We can't produce a K8s-side
    // identifier, so we error rather than silently building an unresolvable Resource.
    SchemaPlus nonHoptimator = schemaWithSubs(null, null);
    Lookup<?> tables = mock(Lookup.class);
    lenient().doReturn(plainTable()).when(tables).get("TABLE");
    lenient().doReturn(tables).when(nonHoptimator).tables();
    lenient().when(nonHoptimator.unwrap(HoptimatorJdbcSchema.class)).thenReturn(null);
    stubRootSchema(schemaWithSubs("INFO", nonHoptimator));

    SQLException ex = assertThrows(SQLException.class,
        () -> GraphService.resolve("INFO.TABLE", connection));
    assertTrue(ex.getMessage().contains("INFO.TABLE"),
        "error should name the identifier: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("database"),
        "error should explain why: " + ex.getMessage());
  }

  @Test
  void renderMatchingFormatDispatchesToRenderer() {
    // The happy-path complement of renderUnknownFormatThrows — verifies the renderer is
    // actually invoked when format matches (case-insensitive).
    GraphNode root = new GraphNode.External("db", List.of("t"));
    PipelineGraph graph = new PipelineGraph(root, Set.of(root), Set.of());

    String out = GraphService.render(graph, "MERMAID");
    assertTrue(out.startsWith("flowchart"),
        "Mermaid renderer should be picked case-insensitively and produce a flowchart: " + out);
  }

  @Test
  void buildGraphSurfacesNoProviderError() {
    // No GraphProvider is registered on this module's test classpath, so any resolvable
    // identifier reaches the "no provider supports target" branch. Wires through the resolver
    // first (identifier must walk Calcite cleanly), then fails dispatch.
    SchemaPlus ads = schemaWithDatabaseAndTable("ads-database", "AD_CLICKS", plainTable());
    stubRootSchema(schemaWithSubs("ADS", ads));

    SQLException ex = assertThrows(SQLException.class,
        () -> GraphService.buildGraph("ADS.AD_CLICKS", 1, connection));
    assertTrue(ex.getMessage().contains("No GraphProvider supports"),
        "error should explain the dispatch failure: " + ex.getMessage());
  }

  @Test
  void buildGraphRejectsNegativeDepth() {
    // Negative depths are nonsense — error rather than silently clamping. depth 0 is valid
    // (renders just the root); only strictly-negative depths fail.
    SQLException ex = assertThrows(SQLException.class,
        () -> GraphService.buildGraph("PROFILE.MEMBERS", -1, connection));
    assertTrue(ex.getMessage().contains("depth"),
        "error should mention the bad parameter: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("-1"),
        "error should echo the bad value: " + ex.getMessage());
  }

  @Test
  void resolveSinglePartIdentifierThrowsSqlException() {
    // Bare names like "audience" walk no schema segments and hit the root for the table lookup.
    // Root has no tables in this fixture → "table not found" with the root's empty schema path.
    stubRootSchema(schemaWithSubs(null, null));

    SQLException ex = assertThrows(SQLException.class,
        () -> GraphService.resolve("audience", connection));
    assertTrue(ex.getMessage().contains("audience"),
        "error should name the offending identifier: " + ex.getMessage());
  }

  // ─── Mock plumbing ───────────────────────────────────────────────────────

  /** Plain physical table — not a view, not a LogicalTable. Resolver should treat as Resource. */
  private static Table plainTable() {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory factory) {
        return factory.createStructType(List.of(), List.of());
      }
    };
  }

  /** Convenience overload: physical (non-logical) database. */
  private static SchemaPlus schemaWithDatabaseAndTable(String crdName, String tableName, Table table) {
    return schemaWithDatabaseAndTable(crdName, tableName, table, /*isLogical=*/false);
  }

  /**
   * Mock a SchemaPlus that's (a) unwrappable as a {@link HoptimatorJdbcSchema} with the given
   * CRD name and {@code isLogical()} flag, and (b) when {@code tableName != null}, exposes
   * {@code table} via {@code tables().get(tableName)}.
   */
  private static SchemaPlus schemaWithDatabaseAndTable(String crdName, String tableName, Table table,
      boolean isLogical) {
    SchemaPlus schema = schemaWithSubs(null, null);
    HoptimatorJdbcSchema hjs = mock(HoptimatorJdbcSchema.class);
    lenient().when(hjs.databaseName()).thenReturn(crdName);
    lenient().when(hjs.isLogical()).thenReturn(isLogical);
    lenient().when(schema.unwrap(HoptimatorJdbcSchema.class)).thenReturn(hjs);
    if (tableName != null) {
      Lookup<?> tables = mock(Lookup.class);
      lenient().doReturn(table).when(tables).get(tableName);
      lenient().doReturn(tables).when(schema).tables();
    }
    return schema;
  }

  private static SchemaPlus schemaWithSubs(String childName, SchemaPlus child) {
    Lookup<?> subs = mock(Lookup.class);
    if (childName != null) {
      lenient().doReturn(child).when(subs).get(childName);
    }
    // Default tables() to an empty Lookup so resolve.tables().get(...) returns null cleanly
    // for schemas that don't host a relevant Table — individual tests can override.
    Lookup<?> emptyTables = mock(Lookup.class);
    SchemaPlus s = mock(SchemaPlus.class);
    lenient().doReturn(subs).when(s).subSchemas();
    lenient().doReturn(emptyTables).when(s).tables();
    return s;
  }

  /** Wire {@code connection.calciteConnection().getRootSchema()} to return {@code root}. */
  private void stubRootSchema(SchemaPlus root) {
    lenient().when(connection.calciteConnection()).thenReturn(calciteConnection);
    lenient().when(calciteConnection.getRootSchema()).thenReturn(root);
  }

}
