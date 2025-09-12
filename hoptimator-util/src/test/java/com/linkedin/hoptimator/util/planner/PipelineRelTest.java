package com.linkedin.hoptimator.util.planner;

import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.ThrowingFunction;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.ImmutablePairList;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class PipelineRelTest {

    @Mock
    private Connection mockConnection;

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private PipelineRel.Implementor implementor;

    @BeforeEach
    void setUp() {
        typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        rexBuilder = new RexBuilder(typeFactory);

        // Create sample target fields mapping
        List<Map.Entry<Integer, String>> entries = List.of(
            new AbstractMap.SimpleEntry<>(0, "dest_field1"),
            new AbstractMap.SimpleEntry<>(1, "dest_field2")
        );
        implementor = new PipelineRel.Implementor(ImmutablePairList.copyOf(entries), new HashMap<>());
    }

    @Test
    void testFieldMapThrowsExceptionForNonTrivialQuery() {
        RelDataType rowType = createSimpleRowType();
        RelNode nonTrivialQuery = createNonTrivialQuery(rowType);
        implementor.setQuery(nonTrivialQuery);

        ThrowingFunction<SqlDialect, String> fieldMapFunc = implementor.fieldMap();
        assertNotNull(fieldMapFunc, "Field map function should be created");

        SQLNonTransientException exception = assertThrows(SQLNonTransientException.class,
            () -> fieldMapFunc.apply(SqlDialect.ANSI),
            "Non-trivial queries should throw SQLNonTransientException");

        assertTrue(exception.getMessage().contains("Field mapping is only supported for trivial queries"));
    }

    @Test
    void testPipelineCreation() throws SQLException {
        RelNode trivialQuery = createTrivialQuery(createSimpleRowType());
        implementor.setQuery(trivialQuery);
        implementor.addSource("test_db", List.of("source_table"), createSimpleRowType(), Collections.emptyMap());
        implementor.setSink("test_db", List.of("sink_table"), createSimpleRowType(), Collections.emptyMap());

        Pipeline pipeline = implementor.pipeline("test-pipeline", mockConnection);

        assertNotNull(pipeline);

        Job job = pipeline.job();
        assertNotNull(job);
        assertEquals("test-pipeline", job.name());

        // Test that lazy evaluations are properly set up
        ThrowingFunction<SqlDialect, String> sqlEval = job.eval("sql");
        ThrowingFunction<SqlDialect, String> queryEval = job.eval("query");
        ThrowingFunction<SqlDialect, String> fieldMapEval = job.eval("fieldMap");

        assertNotNull(sqlEval);
        assertNotNull(queryEval);
        assertNotNull(fieldMapEval);
    }

    @Test
    void testFunctionExceptionHandling() {
        // Test with null query to trigger exception
        implementor.setQuery(null);

        ThrowingFunction<SqlDialect, String> fieldMapFunc = implementor.fieldMap();
        assertNotNull(fieldMapFunc, "Function should be created even with null query");

        assertThrows(SQLNonTransientException.class, () -> fieldMapFunc.apply(SqlDialect.ANSI),
            "Function should propagate exceptions from null query");
    }

    @Test
    void testLazyEvaluation() {
        RelNode trivialQuery = createTrivialQuery(createSimpleRowType());
        implementor.setQuery(trivialQuery);

        // Function creation should not throw, even if query processing might fail
        assertDoesNotThrow(() -> {
            ThrowingFunction<SqlDialect, String> fieldMapFunc = implementor.fieldMap();
            assertNotNull(fieldMapFunc, "Function should be created lazily");

            ThrowingFunction<SqlDialect, String> queryFunc = implementor.query(mockConnection);
            assertNotNull(queryFunc, "Query function should be created lazily");
        }, "Function creation should be lazy and not execute immediately");
    }

    @Test
    void testValidateFieldMappingSuccess() {
        // Create a row type that contains all target fields
        RelDataType rowType = createRowTypeWithFields("dest_field1", "dest_field2", "extra_field");

        // Should not throw any exception
        assertDoesNotThrow(() -> implementor.validateFieldMapping(rowType),
            "Validation should pass when all target fields exist in row type");
    }

    @Test
    void testValidateFieldMappingFailsForMissingFields() {
        // Create a row type missing some target fields
        RelDataType rowType = createRowTypeWithFields("dest_field1"); // missing dest_field2

        SQLNonTransientException exception = assertThrows(SQLNonTransientException.class,
            () -> implementor.validateFieldMapping(rowType),
            "Validation should fail when target fields are missing from row type");

        assertTrue(exception.getMessage().contains("dest_field2"),
            "Exception should mention the missing field");
    }

    @Test
    void testBuildFieldMappingFromSqlNodesSimpleIdentifiers() throws SQLNonTransientException {
        // Create SQL node list with simple identifiers
        SqlNodeList nodeList = new SqlNodeList(SqlParserPos.ZERO);
        nodeList.add(new SqlIdentifier("field1", SqlParserPos.ZERO));
        nodeList.add(new SqlIdentifier("field2", SqlParserPos.ZERO));

        Map<String, String> result = implementor.buildFieldMappingFromSqlNodes(nodeList);

        assertEquals(2, result.size(), "Should have mappings for both fields");
        assertEquals("field1", result.get("field1"), "field1 should map to itself");
        assertEquals("field2", result.get("field2"), "field2 should map to itself");
    }

    @Test
    void testBuildFieldMappingFromSqlNodesStarProjection() throws SQLNonTransientException {
        // Create SQL node list with star projection using the proper Calcite SINGLETON_STAR
        SqlNodeList nodeList = SqlNodeList.SINGLETON_STAR;

        Map<String, String> result = implementor.buildFieldMappingFromSqlNodes(nodeList);

        // Should map all target fields to themselves
        assertEquals(2, result.size(), "Should have mappings for all target fields");
        assertEquals("dest_field1", result.get("dest_field1"), "dest_field1 should map to itself");
        assertEquals("dest_field2", result.get("dest_field2"), "dest_field2 should map to itself");
    }

    @Test
    void testBuildFieldMappingFromSqlNodesAliases() throws SQLNonTransientException {
        // Create SQL node list with aliases (AS operator)
        SqlNodeList nodeList = new SqlNodeList(SqlParserPos.ZERO);

        SqlIdentifier original = new SqlIdentifier("original_field", SqlParserPos.ZERO);
        SqlIdentifier alias = new SqlIdentifier("alias_field", SqlParserPos.ZERO);
        SqlBasicCall asCall = new SqlBasicCall(SqlStdOperatorTable.AS,
            List.of(original, alias), SqlParserPos.ZERO);

        nodeList.add(asCall);

        Map<String, String> result = implementor.buildFieldMappingFromSqlNodes(nodeList);

        assertEquals(1, result.size(), "Should have one mapping for the alias");
        assertEquals("alias_field", result.get("original_field"), "original_field should map to alias_field");
    }

    @Test
    void testBuildFieldMappingFromSqlNodesNestedFieldAccess() throws SQLNonTransientException {
        // Create SQL node list with nested field access (ITEM operator)
        SqlNodeList nodeList = new SqlNodeList(SqlParserPos.ZERO);

        SqlIdentifier baseField = new SqlIdentifier("baseField", SqlParserPos.ZERO);
        SqlLiteral nestedFieldName = SqlLiteral.createCharString("nestedField", SqlParserPos.ZERO);
        SqlBasicCall itemCall = new SqlBasicCall(SqlStdOperatorTable.ITEM,
            List.of(baseField, nestedFieldName), SqlParserPos.ZERO);

        nodeList.add(itemCall);

        Map<String, String> result = implementor.buildFieldMappingFromSqlNodes(nodeList);

        assertEquals(1, result.size(), "Should have one mapping for nested field");
        assertEquals("baseField.nestedField", result.get("baseField.nestedField"),
            "Nested field should map to itself with dot notation");
    }

    @Test
    void testBuildFieldMappingFromSqlNodesNestedAlias() throws SQLNonTransientException {
        // Create SQL node list with mixed field types and a nested alias
        SqlNodeList nodeList = new SqlNodeList(SqlParserPos.ZERO);

        // Simple identifier
        nodeList.add(new SqlIdentifier("simple_field", SqlParserPos.ZERO));

        // Alias
        SqlIdentifier original = new SqlIdentifier("original", SqlParserPos.ZERO);
        SqlIdentifier alias = new SqlIdentifier("aliased", SqlParserPos.ZERO);
        SqlBasicCall asCall = new SqlBasicCall(SqlStdOperatorTable.AS,
            List.of(original, alias), SqlParserPos.ZERO);
        nodeList.add(asCall);

        // Aliased Nested field
        SqlIdentifier baseField = new SqlIdentifier("nested", SqlParserPos.ZERO);
        SqlLiteral nestedFieldName = SqlLiteral.createCharString("field", SqlParserPos.ZERO);
        SqlBasicCall itemCall = new SqlBasicCall(SqlStdOperatorTable.ITEM,
            new SqlNode[]{baseField, nestedFieldName}, SqlParserPos.ZERO);
        SqlIdentifier itemAlias = new SqlIdentifier("item_alias", SqlParserPos.ZERO);
        SqlBasicCall itemAsCall = new SqlBasicCall(SqlStdOperatorTable.AS,
            List.of(itemCall, itemAlias), SqlParserPos.ZERO);
        nodeList.add(itemAsCall);

        Map<String, String> result = implementor.buildFieldMappingFromSqlNodes(nodeList);

        assertEquals(3, result.size(), "Should have mappings for all three field types");
        assertEquals("simple_field", result.get("simple_field"), "Simple field should map to itself");
        assertEquals("aliased", result.get("original"), "Original should map to alias");
        assertEquals("item_alias", result.get("nested.field"), "Nested field should map to itself");
    }

    @Test
    void testBuildFieldMappingFromSqlNodesDeeplyNestedField() throws SQLNonTransientException {
        // Create SQL node list with deeply nested field access (level1.level2.level3)
        SqlNodeList nodeList = new SqlNodeList(SqlParserPos.ZERO);

        // Create nested ITEM calls: ITEM(ITEM(ITEM(baseField, 'level1'), 'level2'), 'level3')
        SqlIdentifier baseField = new SqlIdentifier("baseField", SqlParserPos.ZERO);
        SqlLiteral level1Name = SqlLiteral.createCharString("level1", SqlParserPos.ZERO);
        SqlLiteral level2Name = SqlLiteral.createCharString("level2", SqlParserPos.ZERO);
        SqlLiteral level3Name = SqlLiteral.createCharString("level3", SqlParserPos.ZERO);

        // Build nested structure: baseField.level1.level2.level3
        SqlBasicCall level1Call = new SqlBasicCall(SqlStdOperatorTable.ITEM,
            List.of(baseField, level1Name), SqlParserPos.ZERO);
        SqlBasicCall level2Call = new SqlBasicCall(SqlStdOperatorTable.ITEM,
            List.of(level1Call, level2Name), SqlParserPos.ZERO);
        SqlBasicCall level3Call = new SqlBasicCall(SqlStdOperatorTable.ITEM,
            List.of(level2Call, level3Name), SqlParserPos.ZERO);

        nodeList.add(level3Call);

        Map<String, String> result = implementor.buildFieldMappingFromSqlNodes(nodeList);

        assertEquals(1, result.size(), "Should have one mapping for deeply nested field");
        assertEquals("baseField.level1.level2.level3", result.get("baseField.level1.level2.level3"),
            "Deeply nested field should map to itself with dot notation");
    }

    @Test
    void testBuildFieldMappingFromSqlNodesUnsupportedNode() {
        // Create SQL node list with unsupported node type
        SqlNodeList nodeList = new SqlNodeList(SqlParserPos.ZERO);
        SqlLiteral unsupportedNode = SqlLiteral.createCharString("literal", SqlParserPos.ZERO);
        nodeList.add(unsupportedNode);

        SQLNonTransientException exception = assertThrows(SQLNonTransientException.class,
            () -> implementor.buildFieldMappingFromSqlNodes(nodeList),
            "Should throw exception for unsupported SQL node types");

        assertTrue(exception.getMessage().contains("Unsupported SQL node for field mapping"),
            "Exception should mention unsupported node type");
    }

    @Test
    void testBuildFieldMappingFromSqlNodesUnsupportedOperator() {
        // Create SQL node list with unsupported operator
        SqlNodeList nodeList = new SqlNodeList(SqlParserPos.ZERO);
        SqlIdentifier field = new SqlIdentifier("field", SqlParserPos.ZERO);
        SqlBasicCall unsupportedCall = new SqlBasicCall(SqlStdOperatorTable.PLUS,
            new SqlNode[]{field, field}, SqlParserPos.ZERO);
        nodeList.add(unsupportedCall);

        SQLNonTransientException exception = assertThrows(SQLNonTransientException.class,
            () -> implementor.buildFieldMappingFromSqlNodes(nodeList),
            "Should throw exception for unsupported operators");

        assertTrue(exception.getMessage().contains("Unsupported SQL operator for field mapping"),
            "Exception should mention unsupported operator");
    }

    // Helper methods for creating test data

    private RelDataType createSimpleRowType() {
        return createRowTypeWithFields("field1", "field2");
    }

    private RelDataType createRowTypeWithFields(String... fieldNames) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (String fieldName : fieldNames) {
            builder.add(fieldName, SqlTypeName.VARCHAR);
        }
        return builder.build();
    }

    private RelNode createTrivialQuery(RelDataType rowType) {
        // Create a mock trivial query that will pass TrivialQueryChecker
        LogicalProject mockTrivialQuery = mock(LogicalProject.class);
        when(mockTrivialQuery.getRowType()).thenReturn(rowType);
        when(mockTrivialQuery.getInputs()).thenReturn(Collections.emptyList());
        when(mockTrivialQuery.accept(any(RelShuttle.class))).thenCallRealMethod();
        when(mockTrivialQuery.getCluster()).thenReturn(mock(RelOptCluster.class));

        // Create simple projection expressions (field references)
        List<RexNode> projects = List.of(
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1)
        );
        when(mockTrivialQuery.getProjects()).thenReturn(projects);
        return mockTrivialQuery;
    }

    private RelNode createNonTrivialQuery(RelDataType rowType) {
        // Create a mock non-trivial query that will fail TrivialQueryChecker
        LogicalFilter mockNonTrivialQuery = mock(LogicalFilter.class);
        when(mockNonTrivialQuery.getRowType()).thenReturn(rowType);
        when(mockNonTrivialQuery.getInputs()).thenReturn(Collections.emptyList());
        when(mockNonTrivialQuery.accept(any(RelShuttle.class))).thenCallRealMethod();
        when(mockNonTrivialQuery.getCluster()).thenReturn(mock(RelOptCluster.class));
        return mockNonTrivialQuery;
    }
}
