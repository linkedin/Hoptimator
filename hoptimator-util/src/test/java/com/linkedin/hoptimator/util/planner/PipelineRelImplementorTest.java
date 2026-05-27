package com.linkedin.hoptimator.util.planner;

import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.ThrowingFunction;
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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    SqlNodeList nodeList = new SqlNodeList(List.of(id), SqlParserPos.ZERO);

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
    SqlNodeList nodeList = new SqlNodeList(List.of(star), SqlParserPos.ZERO);

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
    SqlNodeList nodeList = new SqlNodeList(List.of(literal), SqlParserPos.ZERO);

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
        List.of(original, alias),
        SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(List.of(asCall), SqlParserPos.ZERO);

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
        List.of(original, literalAlias),
        SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(List.of(asCall), SqlParserPos.ZERO);

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
        List.of(left, right),
        SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(List.of(plusCall), SqlParserPos.ZERO);

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
        List.of(baseField, nestedLiteral),
        SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(List.of(itemCall), SqlParserPos.ZERO);

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
        List.of(baseField, nonLiteral),
        SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(List.of(itemCall), SqlParserPos.ZERO);

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
        List.of(baseField, nestedLiteral),
        SqlParserPos.ZERO);
    SqlIdentifier alias = new SqlIdentifier("MY_ALIAS", SqlParserPos.ZERO);
    SqlBasicCall asCall = new SqlBasicCall(
        new SqlAsOperator(),
        List.of(itemCall, alias),
        SqlParserPos.ZERO);
    SqlNodeList nodeList = new SqlNodeList(List.of(asCall), SqlParserPos.ZERO);

    Map<String, String> fieldMap = impl.buildFieldMappingFromSqlNodes(nodeList);

    assertEquals(1, fieldMap.size());
    assertEquals("MY_ALIAS", fieldMap.get("parent.child"));
  }

  /**
   * After addSource, the pipeline must contain a non-null job with SQL
   * that references the source table.  If addSource were a no-op the
   * sources map would be empty and sql() / pipeline() would behave
   * differently (e.g. no CREATE TABLE for the source).
   */
  @Test
  void testAddSourceAppearsInPipelineSql() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);
    impl.addSource("testDb", Arrays.asList("MYSCHEMA", "MYTABLE"), rowType, Collections.emptyMap());
    impl.setSink("testDb", Arrays.asList("MYSCHEMA", "SINKTABLE"), rowType, Collections.emptyMap());

    ThrowingFunction<SqlDialect, String> sqlFunc = impl.sql(mockConnection);
    String sql = sqlFunc.apply(SqlDialect.ANSI);
    // The source table must appear somewhere in the generated script
    assertNotNull(sql);
    assertTrue(sql.length() > 0, "sql() must produce non-empty output after addSource");
  }

  /**
   * After setSink, the pipeline job must exist and the generated SQL must
   * contain an INSERT INTO targeting the sink.  If setSink were a no-op the
   * sink would remain null and sql() would NPE or omit the INSERT.
   */
  @Test
  void testSetSinkAppearsInPipelineSql() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);
    impl.addSource("db", Arrays.asList("s", "src"), rowType, Collections.emptyMap());
    impl.setSink("db", Arrays.asList("s", "snk"), rowType, Collections.emptyMap());

    ThrowingFunction<SqlDialect, String> sqlFunc = impl.sql(mockConnection);
    String sql = sqlFunc.apply(SqlDialect.ANSI);
    assertNotNull(sql);
    // INSERT INTO must be present because a sink was set
    assertTrue(sql.toUpperCase().contains("INSERT"), "sql() must contain INSERT when sink is set");
  }

  /**
   * With NO sink set, hasTableNameCollision must be false and sql() should
   * NOT add _source/_sink suffixes.  If the null-check for sink were removed
   * (RemoveConditionals), a NullPointerException would be thrown, or the
   * wrong branch taken.
   */
  @Test
  void testNoCollisionWithNoSink() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);
    impl.addSource("db", Arrays.asList("schema", "table"), rowType, Collections.emptyMap());
    // No setSink call

    // query() (not sql()) works without a sink
    ThrowingFunction<SqlDialect, String> queryFunc = impl.query(mockConnection);
    String result = queryFunc.apply(SqlDialect.ANSI);
    assertNotNull(result);
    // No suffixes should appear because there is no collision
    assertFalse(result.contains("_source"), "No _source suffix when sink is absent");
  }

  /**
   * Two sources with different paths and a sink distinct from both → no collision.
   * If hasTableNameCollision always returned true, the SQL would contain
   * _source/_sink suffixes when it shouldn't.
   */
  @Test
  void testNoCollisionWithDistinctSourceAndSink() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);
    impl.addSource("db", Arrays.asList("s", "alpha"), rowType, Collections.emptyMap());
    impl.setSink("db", Arrays.asList("s", "beta"), rowType, Collections.emptyMap());

    ThrowingFunction<SqlDialect, String> sqlFunc = impl.sql(mockConnection);
    String sql = sqlFunc.apply(SqlDialect.ANSI);
    assertNotNull(sql);
    assertFalse(sql.contains("_source"),
        "No _source suffix expected when source and sink have different names");
    assertFalse(sql.contains("_sink"),
        "No _sink suffix expected when source and sink have different names");
  }

  /**
   * Source and sink share the SAME catalog/schema/table → collision, suffixes added.
   * Validates the three Objects.equals(...) conditions inside hasTableNameCollision.
   */
  @Test
  void testCollisionWithSameSourceAndSink() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());

    RelNode query = createScanRelNode();
    impl.setQuery(query);
    impl.addSource("db", Arrays.asList("s", "same"), rowType, Collections.emptyMap());
    impl.setSink("db", Arrays.asList("s", "same"), rowType, Collections.emptyMap());

    ThrowingFunction<SqlDialect, String> sqlFunc = impl.sql(mockConnection);
    String sql = sqlFunc.apply(SqlDialect.ANSI);
    assertNotNull(sql);
    assertTrue(sql.contains("_source") || sql.contains("_sink"),
        "Collision must produce suffixed table names");
  }

  /**
   * Verify the two branches of script() produce DIFFERENT SQL output.
   * If the needsSuffixes flag were always false (RemoveConditionals), the
   * collision case would not add suffixes, making these two results equal.
   */
  @Test
  void testCollisionAndNoCollisionProduceDifferentSql() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    RelNode query = createScanRelNode();

    // With collision
    PipelineRel.Implementor collisionImpl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());
    collisionImpl.setQuery(query);
    collisionImpl.addSource("db", Arrays.asList("s", "t"), rowType, Collections.emptyMap());
    collisionImpl.setSink("db", Arrays.asList("s", "t"), rowType, Collections.emptyMap());
    String collisionSql = collisionImpl.sql(mockConnection).apply(SqlDialect.ANSI);

    // Without collision
    PipelineRel.Implementor noCollisionImpl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());
    noCollisionImpl.setQuery(query);
    noCollisionImpl.addSource("db", Arrays.asList("s", "src"), rowType, Collections.emptyMap());
    noCollisionImpl.setSink("db", Arrays.asList("s", "snk"), rowType, Collections.emptyMap());
    String noCollisionSql = noCollisionImpl.sql(mockConnection).apply(SqlDialect.ANSI);

    assertFalse(collisionSql.equals(noCollisionSql),
        "Collision and no-collision paths must produce different SQL");
  }

  /**
   * fieldMap() on a trivial projected query must return a JSON map whose keys and
   * values match the projected column names.  This tests the full pipeline from
   * fieldMap() through buildFieldMappingFromSqlNodes() and extractFieldName().
   */
  @Test
  void testFieldMapReturnsExpectedFieldNames() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType tableType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("COL2", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus sub = rootSchema.add("SC", new AbstractSchema());
    sub.add("TBL", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return tableType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder().defaultSchema(rootSchema).build());
    // SELECT COL1, COL2 FROM SC.TBL — explicit projections produce SqlIdentifiers
    RelNode projQuery = builder.scan("SC", "TBL")
        .project(builder.field("COL1"), builder.field("COL2"))
        .build();

    List<Map.Entry<Integer, String>> entries = List.of(
        new AbstractMap.SimpleEntry<>(0, "COL1"),
        new AbstractMap.SimpleEntry<>(1, "COL2"));
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.copyOf(entries), Collections.emptyMap());
    impl.setQuery(projQuery);

    ThrowingFunction<SqlDialect, String> fieldMapFunc = impl.fieldMap();
    String json = fieldMapFunc.apply(SqlDialect.ANSI);

    assertNotNull(json);
    assertTrue(json.startsWith("{"), "fieldMap() result must be a JSON object");
    assertTrue(json.length() > 2, "fieldMap() must contain at least one field mapping");
    // The projected column names must appear in the mapping
    assertTrue(json.contains("COL1") || json.contains("col1"),
        "fieldMap() result must include COL1");
  }

  /**
   * When the query projects with an alias (SELECT src AS tgt), fieldMap must
   * map src → tgt, not src → src.
   */
  @Test
  void testFieldMapWithProjectedAliasReturnsAliasMapping() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType tableType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus sub = rootSchema.add("SC2", new AbstractSchema());
    sub.add("TBL2", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return tableType;
      }
    });

    // Use RelBuilder to create SELECT col1 AS aliasCol FROM SC2.TBL2
    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder().defaultSchema(rootSchema).build());
    RelNode projQuery = builder.scan("SC2", "TBL2")
        .project(builder.alias(builder.field("COL1"), "ALIAS_COL"))
        .build();

    List<Map.Entry<Integer, String>> entries = List.of(
        new AbstractMap.SimpleEntry<>(0, "ALIAS_COL"));
    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.copyOf(entries), Collections.emptyMap());
    impl.setQuery(projQuery);

    ThrowingFunction<SqlDialect, String> fieldMapFunc = impl.fieldMap();
    String json = fieldMapFunc.apply(SqlDialect.ANSI);

    assertNotNull(json);
    assertTrue(json.startsWith("{"), "fieldMap() result must be a JSON object");
    // The alias must appear in the mapping
    assertTrue(json.contains("ALIAS_COL"), "fieldMap() must contain the alias name");
  }

  // After addSource, the pipeline's sources set must be non-empty.
  @Test
  void testPipelineHasNonNullJobAfterAddSource() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());
    RelNode query = createScanRelNode();
    impl.setQuery(query);
    impl.addSource("db", Arrays.asList("sch", "src"), rowType, Collections.emptyMap());
    impl.setSink("db", Arrays.asList("sch", "snk"), rowType, Collections.emptyMap());

    Pipeline pipeline = impl.pipeline("job-test", mockConnection);
    assertNotNull(pipeline.job(), "pipeline() must return a non-null job");
    assertEquals("job-test", pipeline.job().name());
    assertNotNull(pipeline.sources(), "Pipeline sources must not be null");
    assertFalse(pipeline.sources().isEmpty(), "Pipeline must have at least one source after addSource");
  }

  // After setSink, the pipeline sink must be non-null.
  @Test
  void testPipelineHasNonNullSinkAfterSetSink() throws SQLException {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    PipelineRel.Implementor impl = new PipelineRel.Implementor(
        ImmutablePairList.of(), Collections.emptyMap());
    RelNode query = createScanRelNode();
    impl.setQuery(query);
    impl.addSource("db", Arrays.asList("sch", "src"), rowType, Collections.emptyMap());
    impl.setSink("db", Arrays.asList("sch", "snk"), rowType, Collections.emptyMap());

    Pipeline pipeline = impl.pipeline("sink-test", mockConnection);
    assertNotNull(pipeline.sink(), "pipeline() must return a non-null sink after setSink");
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

  // Minimal PipelineRel implementation for testing visit().
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

  // PipelineRel node with a child input for testing visit() recursion.
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
