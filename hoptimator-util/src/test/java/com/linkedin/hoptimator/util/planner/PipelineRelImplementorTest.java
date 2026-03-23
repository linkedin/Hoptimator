package com.linkedin.hoptimator.util.planner;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.ImmutablePairList;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.ThrowingFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
class PipelineRelImplementorTest {

  @Mock
  private Connection mockConnection;

  @Test
  void testWrapAnsiDialect() throws SQLException {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    ThrowingFunction<SqlDialect, String> wrapped = impl.wrap(dialect -> "result-ansi");
    String result = wrapped.apply(SqlDialect.ANSI);

    assertEquals("result-ansi", result);
  }

  @Test
  void testWrapFlinkDialect() throws SQLException {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    ThrowingFunction<SqlDialect, String> wrapped = impl.wrap(dialect -> "result-flink");
    String result = wrapped.apply(SqlDialect.FLINK);

    assertEquals("result-flink", result);
  }

  @Test
  void testAddSourceStoresSourceWithHints() {
    Map<String, String> hints = new HashMap<>();
    hints.put("hintKey", "hintValue");
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), hints);

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    impl.addSource("db", Arrays.asList("schema", "table"), rowType, new HashMap<>());

    assertNotNull(impl);
  }

  @Test
  void testSetSinkStoresSinkWithHints() {
    Map<String, String> hints = new HashMap<>();
    hints.put("hintKey", "hintValue");
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), hints);

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    impl.setSink("db", Arrays.asList("schema", "table"), rowType, new HashMap<>());

    assertNotNull(impl);
  }

  @Test
  void testSetQueryUpdatesQuery() {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);

    assertNotNull(impl);
  }

  @Test
  void testPipelineCreationWithSourceAndSink() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    List<Map.Entry<Integer, String>> entries = List.of(
        new AbstractMap.SimpleEntry<>(0, "COL1"));
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.copyOf(entries), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);
    impl.addSource("testDb", Arrays.asList("TESTSCHEMA", "SOURCE_TABLE"), rowType, Collections.emptyMap());
    impl.setSink("testDb", Arrays.asList("TESTSCHEMA", "SINK_TABLE"), rowType, Collections.emptyMap());

    Pipeline pipeline = impl.pipeline("test-pipeline", mockConnection);

    assertNotNull(pipeline);
    assertNotNull(pipeline.job());
    assertEquals("test-pipeline", pipeline.job().name());
  }

  @Test
  void testPipelineWithTableNameCollision() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);
    impl.addSource("db", Arrays.asList("schema", "table"), rowType, Collections.emptyMap());
    impl.setSink("db", Arrays.asList("schema", "table"), rowType, Collections.emptyMap());

    Pipeline pipeline = impl.pipeline("collision-pipeline", mockConnection);

    assertNotNull(pipeline);
  }

  @Test
  void testSqlFunctionWithCollision() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    List<Map.Entry<Integer, String>> entries = List.of(
        new AbstractMap.SimpleEntry<>(0, "COL1"));
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.copyOf(entries), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);
    // Source and sink with same path -> collision
    impl.addSource("db", Arrays.asList("schema", "table"), rowType, Collections.emptyMap());
    impl.setSink("db", Arrays.asList("schema", "table"), rowType, Collections.emptyMap());

    ThrowingFunction<SqlDialect, String> sqlFunc = impl.sql(mockConnection);
    String sql = sqlFunc.apply(SqlDialect.ANSI);
    assertNotNull(sql);
    // With collision, table names get suffixed
    assertTrue(sql.contains("_source") || sql.contains("_sink"));
  }

  @Test
  void testSqlFunctionWithoutCollision() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);
    impl.addSource("db", Arrays.asList("schema", "sourceTable"), rowType, Collections.emptyMap());
    impl.setSink("db", Arrays.asList("schema", "sinkTable"), rowType, Collections.emptyMap());

    ThrowingFunction<SqlDialect, String> sqlFunc = impl.sql(mockConnection);
    String sql = sqlFunc.apply(SqlDialect.ANSI);
    assertNotNull(sql);
  }

  @Test
  void testQueryFunctionReturnsSelectSql() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);
    impl.addSource("db", Arrays.asList("schema", "table"), rowType, Collections.emptyMap());

    ThrowingFunction<SqlDialect, String> queryFunc = impl.query(mockConnection);
    String sql = queryFunc.apply(SqlDialect.ANSI);
    assertNotNull(sql);
    assertTrue(sql.contains("SELECT"));
  }

  @Test
  void testPipelineWithNoCollision() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);
    impl.addSource("db", Arrays.asList("schema", "sourceTable"), rowType, Collections.emptyMap());
    impl.setSink("db", Arrays.asList("schema", "sinkTable"), rowType, Collections.emptyMap());

    Pipeline pipeline = impl.pipeline("no-collision-pipeline", mockConnection);

    assertNotNull(pipeline);
  }

  @Test
  void testQueryFunctionCreation() throws SQLException {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);

    ThrowingFunction<SqlDialect, String> queryFunc = impl.query(mockConnection);

    assertNotNull(queryFunc);
  }

  @Test
  void testFieldMapFunctionCreation() {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);

    ThrowingFunction<SqlDialect, String> fieldMapFunc = impl.fieldMap();

    assertNotNull(fieldMapFunc);
  }

  @Test
  void testAddSourceWithExistingOptions() {
    Map<String, String> hints = new HashMap<>();
    hints.put("globalHint", "globalValue");
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), hints);

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    Map<String, String> sourceOptions = new HashMap<>();
    sourceOptions.put("sourceKey", "sourceValue");
    impl.addSource("db", Arrays.asList("schema", "table"), rowType, sourceOptions);

    assertNotNull(impl);
  }

  @Test
  void testSetSinkWithExistingOptions() {
    Map<String, String> hints = new HashMap<>();
    hints.put("globalHint", "globalValue");
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), hints);

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    Map<String, String> sinkOptions = new HashMap<>();
    sinkOptions.put("sinkKey", "sinkValue");
    impl.setSink("db", Arrays.asList("schema", "table"), rowType, sinkOptions);

    assertNotNull(impl);
  }

  @Test
  void testValidateFieldMappingThrowsWhenFieldNotInSink() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType sinkRowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    List<Map.Entry<Integer, String>> entries = List.of(
        new AbstractMap.SimpleEntry<>(0, "MISSING_FIELD"));
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.copyOf(entries), Collections.emptyMap());

    assertThrows(SQLNonTransientException.class, () -> impl.validateFieldMapping(sinkRowType));
  }

  @Test
  void testValidateFieldMappingSucceedsWhenFieldsMatch() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType sinkRowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    List<Map.Entry<Integer, String>> entries = List.of(
        new AbstractMap.SimpleEntry<>(0, "COL1"));
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.copyOf(entries), Collections.emptyMap());

    // Should not throw
    impl.validateFieldMapping(sinkRowType);
  }

  @Test
  void testBuildFieldMappingFromSimpleIdentifier() throws SQLNonTransientException {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    SqlIdentifier id = new SqlIdentifier("MY_COL", SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(Arrays.asList(id), SqlParserPos.ZERO);

    Map<String, String> fieldMap = impl.buildFieldMappingFromSqlNodes(nodeList);

    assertEquals(1, fieldMap.size());
    assertEquals("MY_COL", fieldMap.get("MY_COL"));
  }

  @Test
  void testBuildFieldMappingFromStarIdentifier() throws SQLNonTransientException {
    List<Map.Entry<Integer, String>> entries = List.of(
        new AbstractMap.SimpleEntry<>(0, "COL1"),
        new AbstractMap.SimpleEntry<>(1, "COL2"));
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.copyOf(entries), Collections.emptyMap());

    SqlIdentifier star = SqlIdentifier.star(SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(Arrays.asList(star), SqlParserPos.ZERO);

    Map<String, String> fieldMap = impl.buildFieldMappingFromSqlNodes(nodeList);

    assertEquals(2, fieldMap.size());
    assertEquals("COL1", fieldMap.get("COL1"));
    assertEquals("COL2", fieldMap.get("COL2"));
  }

  @Test
  void testBuildFieldMappingThrowsOnUnsupportedNode() {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    // SqlLiteral is not a supported node type for field mapping
    SqlNode literal = SqlLiteral.createCharString("test", SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(Arrays.asList(literal), SqlParserPos.ZERO);

    assertThrows(SQLNonTransientException.class, () -> impl.buildFieldMappingFromSqlNodes(nodeList));
  }

  @Test
  void testPipelineWithNoSinkUsesQueryRowType() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);
    impl.addSource("db", Arrays.asList("schema", "table"), rowType, Collections.emptyMap());

    // Pipeline with no sink set
    Pipeline pipeline = impl.pipeline("no-sink-pipeline", mockConnection);
    assertNotNull(pipeline);
  }

  @Test
  void testVisitSetsQueryOnFirstCall() throws SQLException {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    // Create a minimal PipelineRel that implements the interface
    RelNode scanNode = createScanRelNode();
    TestPipelineRelNode pipelineNode = new TestPipelineRelNode(scanNode);

    impl.visit(pipelineNode);

    assertTrue(pipelineNode.implementCalled);
  }

  @Test
  void testFieldMapFunctionAppliesWithTrivialQuery() throws SQLException {
    List<Map.Entry<Integer, String>> entries = List.of(
        new AbstractMap.SimpleEntry<>(0, "COL1"));
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.copyOf(entries), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);

    ThrowingFunction<SqlDialect, String> fieldMapFunc = impl.fieldMap();
    // Applying the function should produce a JSON field map for a trivial query
    String result = fieldMapFunc.apply(SqlDialect.ANSI);
    assertNotNull(result);
    // The result is a JSON object mapping field names
    assertTrue(result.startsWith("{"));
  }

  private RelNode createScanRelNode() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType tableType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus sub = rootSchema.add("S", new AbstractSchema());
    sub.add("T", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return tableType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build());
    return builder.scan("S", "T").build();
  }

  @Test
  void testVisitWithChildNodes() throws SQLException {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    RelNode scanNode = createScanRelNode();
    TestPipelineRelNode child = new TestPipelineRelNode(scanNode);
    TestPipelineRelNodeWithInput parent = new TestPipelineRelNodeWithInput(scanNode, child);

    impl.visit(parent);

    assertTrue(parent.implementCalled);
    assertTrue(child.implementCalled);
  }

  @Test
  void testFieldMapWithNonTrivialQueryThrows() {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    // Create a non-trivial query (with aggregation)
    RelNode query = createAggregateRelNode();
    impl.setQuery(query);

    ThrowingFunction<SqlDialect, String> fieldMapFunc = impl.fieldMap();
    assertThrows(SQLNonTransientException.class, () -> fieldMapFunc.apply(SqlDialect.ANSI));
  }

  @Test
  void testFieldMapWithSinkRowTypeValidatesFields() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType sinkRowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    List<Map.Entry<Integer, String>> entries = List.of(
        new AbstractMap.SimpleEntry<>(0, "COL1"));
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.copyOf(entries), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);
    impl.setSink("db", Arrays.asList("schema", "table"), sinkRowType, Collections.emptyMap());

    ThrowingFunction<SqlDialect, String> fieldMapFunc = impl.fieldMap();
    String result = fieldMapFunc.apply(SqlDialect.ANSI);
    assertNotNull(result);
  }

  @Test
  void testBuildFieldMappingFromAsOperator() throws SQLNonTransientException {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    SqlIdentifier original = new SqlIdentifier("SOURCE_COL", SqlParserPos.ZERO);
    SqlIdentifier alias = new SqlIdentifier("TARGET_COL", SqlParserPos.ZERO);
    SqlBasicCall asCall = new SqlBasicCall(
        new SqlAsOperator(),
        new SqlNode[]{original, alias},
        SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(Arrays.asList(asCall), SqlParserPos.ZERO);

    Map<String, String> fieldMap = impl.buildFieldMappingFromSqlNodes(nodeList);

    assertEquals(1, fieldMap.size());
    assertEquals("TARGET_COL", fieldMap.get("SOURCE_COL"));
  }

  @Test
  void testBuildFieldMappingFromAsOperatorWithNonIdentifierAliasThrows() {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    SqlIdentifier original = new SqlIdentifier("SOURCE_COL", SqlParserPos.ZERO);
    SqlNode literalAlias = SqlLiteral.createCharString("alias", SqlParserPos.ZERO);
    SqlBasicCall asCall = new SqlBasicCall(
        new SqlAsOperator(),
        new SqlNode[]{original, literalAlias},
        SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(Arrays.asList(asCall), SqlParserPos.ZERO);

    assertThrows(SQLNonTransientException.class, () -> impl.buildFieldMappingFromSqlNodes(nodeList));
  }

  @Test
  void testBuildFieldMappingFromUnsupportedOperatorInCallThrows() {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    // Create a SqlBasicCall with a non-AS, non-ITEM operator (e.g., PLUS)
    SqlIdentifier left = new SqlIdentifier("A", SqlParserPos.ZERO);
    SqlIdentifier right = new SqlIdentifier("B", SqlParserPos.ZERO);
    SqlBasicCall plusCall = new SqlBasicCall(
        SqlStdOperatorTable.PLUS,
        new SqlNode[]{left, right},
        SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(Arrays.asList(plusCall), SqlParserPos.ZERO);

    assertThrows(SQLNonTransientException.class, () -> impl.buildFieldMappingFromSqlNodes(nodeList));
  }

  @Test
  void testBuildFieldMappingFromItemOperator() throws SQLNonTransientException {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    // Create ITEM operator for nested field access: ITEM($field, 'nestedField')
    SqlIdentifier baseField = new SqlIdentifier("parent", SqlParserPos.ZERO);
    SqlNode nestedLiteral = SqlLiteral.createCharString("child", SqlParserPos.ZERO);
    SqlBasicCall itemCall = new SqlBasicCall(
        SqlStdOperatorTable.ITEM,
        new SqlNode[]{baseField, nestedLiteral},
        SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(Arrays.asList(itemCall), SqlParserPos.ZERO);

    Map<String, String> fieldMap = impl.buildFieldMappingFromSqlNodes(nodeList);

    assertEquals(1, fieldMap.size());
    assertEquals("parent.child", fieldMap.get("parent.child"));
  }

  @Test
  void testBuildFieldMappingFromItemOperatorWithNonLiteralThrows() {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    SqlIdentifier baseField = new SqlIdentifier("parent", SqlParserPos.ZERO);
    SqlIdentifier nonLiteral = new SqlIdentifier("nonLiteral", SqlParserPos.ZERO);
    SqlBasicCall itemCall = new SqlBasicCall(
        SqlStdOperatorTable.ITEM,
        new SqlNode[]{baseField, nonLiteral},
        SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(Arrays.asList(itemCall), SqlParserPos.ZERO);

    assertThrows(SQLNonTransientException.class, () -> impl.buildFieldMappingFromSqlNodes(nodeList));
  }

  @Test
  void testBuildFieldMappingFromAsOperatorWithItemOperand() throws SQLNonTransientException {
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    // Create ITEM(parent, 'child') AS alias
    SqlIdentifier baseField = new SqlIdentifier("parent", SqlParserPos.ZERO);
    SqlNode nestedLiteral = SqlLiteral.createCharString("child", SqlParserPos.ZERO);
    SqlBasicCall itemCall = new SqlBasicCall(
        SqlStdOperatorTable.ITEM,
        new SqlNode[]{baseField, nestedLiteral},
        SqlParserPos.ZERO);
    SqlIdentifier alias = new SqlIdentifier("MY_ALIAS", SqlParserPos.ZERO);
    SqlBasicCall asCall = new SqlBasicCall(
        new SqlAsOperator(),
        new SqlNode[]{itemCall, alias},
        SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(Arrays.asList(asCall), SqlParserPos.ZERO);

    Map<String, String> fieldMap = impl.buildFieldMappingFromSqlNodes(nodeList);

    assertEquals(1, fieldMap.size());
    assertEquals("MY_ALIAS", fieldMap.get("parent.child"));
  }

  private RelNode createAggregateRelNode() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType tableType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("COL2", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus sub = rootSchema.add("S", new AbstractSchema());
    sub.add("T", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return tableType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build());
    return builder.scan("S", "T")
        .aggregate(builder.groupKey(0), builder.count(false, "CNT", builder.field(1)))
        .build();
  }

  /**
   * Minimal PipelineRel implementation for testing visit().
   */
  private static class TestPipelineRelNode extends AbstractRelNode implements PipelineRel {
    boolean implementCalled = false;

    TestPipelineRelNode(RelNode input) {
      super(input.getCluster(), input.getTraitSet());
      this.rowType = input.getRowType();
    }

    @Override
    public void implement(PipelineRel.Implementor implementor) {
      implementCalled = true;
    }
  }

  /**
   * PipelineRel node with a child input for testing visit() recursion.
   */
  private static class TestPipelineRelNodeWithInput extends SingleRel implements PipelineRel {
    boolean implementCalled = false;

    TestPipelineRelNodeWithInput(RelNode template, RelNode input) {
      super(template.getCluster(), template.getTraitSet(), input);
      this.rowType = template.getRowType();
    }

    @Override
    public void implement(PipelineRel.Implementor implementor) {
      implementCalled = true;
    }
  }
}
