package com.linkedin.hoptimator.util.planner;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for TrivialQueryChecker to verify correct identification of trivial vs non-trivial queries.
 *
 * A trivial query should only contain:
 * - Simple table scans
 * - Simple projections (field references and nested field access only, no functions or calculations)
 * - Nested field access using ITEM operator (e.g., ITEM($3, 'nestedField'))
 *
 * Non-trivial queries include:
 * - Joins, aggregations, filters, sorts, unions
 * - Complex expressions in projections (functions, calculations, etc.)
 * - Any other relational operations
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TrivialQueryCheckerTest {

    @Mock
    private PipelineRules.PipelineTableScan mockTableScan;

    @Mock
    private PipelineRules.PipelineProject mockProject;

    @Mock
    private LogicalFilter mockFilter;

    @Mock
    private LogicalJoin mockJoin;

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;

    @BeforeEach
    void setUp() {
        typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        rexBuilder = new RexBuilder(typeFactory);

        // Set up accept() methods to properly dispatch to TrivialQueryChecker visit methods
        when(mockTableScan.accept(any(RelShuttle.class))).thenCallRealMethod();
        when(mockProject.accept(any(RelShuttle.class))).thenCallRealMethod();
        when(mockFilter.accept(any(RelShuttle.class))).thenCallRealMethod();
        when(mockJoin.accept(any(RelShuttle.class))).thenCallRealMethod();
    }

    @Test
    void testTableScanIsTrivial() {
        when(mockTableScan.getInputs()).thenReturn(Collections.emptyList());

        boolean result = TrivialQueryChecker.isTrivialQuery(mockTableScan);
        assertTrue(result, "Simple table scan should be considered trivial");
    }

    @Test
    void testSimpleProjectionIsTrivial() {
        // Create simple field reference expressions
        RelDataType rowType = createRowType("field1", "field2");
        List<RexNode> simpleProjections = List.of(
            new RexInputRef(0, rowType.getFieldList().get(0).getType()),
            new RexInputRef(1, rowType.getFieldList().get(1).getType())
        );

        when(mockProject.getProjects()).thenReturn(simpleProjections);
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(mockTableScan));
        when(mockTableScan.getInputs()).thenReturn(Collections.emptyList());

        boolean result = TrivialQueryChecker.isTrivialQuery(mockProject);
        assertTrue(result, "Simple projection with field references should be trivial");
    }

    @Test
    void testComplexProjectionIsNotTrivial() {
        // Create a complex expression (function call)
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        // Create a simple complex expression - a function call
        RexNode complexExpression = rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeExactLiteral(java.math.BigDecimal.ONE)
        );

        when(mockProject.getProjects()).thenReturn(Collections.singletonList(complexExpression));
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(mockTableScan));

        boolean result = TrivialQueryChecker.isTrivialQuery(mockProject);
        assertFalse(result, "Projection with complex expressions should not be trivial");
    }

    @Test
    void testOtherNodesNotTrivial() {
        when(mockFilter.getInputs()).thenReturn(Collections.singletonList(mockTableScan));
        when(mockJoin.getInputs()).thenReturn(List.of(mockTableScan, mockTableScan));

        boolean result = TrivialQueryChecker.isTrivialQuery(mockFilter);
        assertFalse(result, "Queries with filters should not be trivial");

        result = TrivialQueryChecker.isTrivialQuery(mockJoin);
        assertFalse(result, "Queries with joins should not be trivial");
    }

    @Test
    void testComplexQueryIsNotTrivial() {
        // Create a complex query: SELECT ... FROM table1 JOIN table2 WHERE ...
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(mockFilter));
        when(mockFilter.getInputs()).thenReturn(Collections.singletonList(mockJoin));
        when(mockJoin.getInputs()).thenReturn(List.of(mockTableScan, mockTableScan));
        when(mockTableScan.getInputs()).thenReturn(Collections.emptyList());

        boolean result = TrivialQueryChecker.isTrivialQuery(mockProject);
        assertFalse(result, "Complex queries with multiple operations should not be trivial");
    }

    @Test
    void testExceptionHandling() {
        RelNode faultyNode = mock(RelNode.class);
        when(faultyNode.accept(any(RelShuttle.class))).thenThrow(new RuntimeException("Test exception"));

        boolean result = TrivialQueryChecker.isTrivialQuery(faultyNode);
        assertFalse(result, "Queries that throw exceptions should be considered non-trivial");
    }

    /**
     * Test nested trivial operations (table scan -> simple project -> simple project)
     */
    @Test
    void testNestedTrivialOperationsAreTrivial() {
        RelDataType rowType = createRowType("field1", "field2");
        List<RexNode> simpleProjections = List.of(
            new RexInputRef(0, rowType.getFieldList().get(0).getType()),
            new RexInputRef(1, rowType.getFieldList().get(1).getType())
        );

        LogicalProject innerProject = mock(LogicalProject.class);
        when(innerProject.getProjects()).thenReturn(simpleProjections);
        when(innerProject.getInputs()).thenReturn(Collections.singletonList(mockTableScan));

        when(mockProject.getProjects()).thenReturn(simpleProjections);
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(innerProject));
        when(mockTableScan.getInputs()).thenReturn(Collections.emptyList());

        boolean result = TrivialQueryChecker.isTrivialQuery(mockProject);
        assertTrue(result, "Nested trivial operations should remain trivial");
    }

    @Test
    void testNestedFieldAccessIsTrivial() {
        // Create nested field access expression: ITEM($0, 'nestedField')
        // First create a structured type (ROW) that contains nested fields
        RelDataType nestedFieldType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType structType = typeFactory.builder()
            .add("nestedField", nestedFieldType)
            .add("anotherField", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .build();

        RexInputRef baseField = rexBuilder.makeInputRef(structType, 0);
        RexLiteral nestedFieldName = rexBuilder.makeLiteral("nestedField");

        RexCall nestedFieldAccess = (RexCall) rexBuilder.makeCall(
            SqlStdOperatorTable.ITEM,
            baseField,
            nestedFieldName
        );

        when(mockProject.getProjects()).thenReturn(Collections.singletonList(nestedFieldAccess));
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(mockTableScan));
        when(mockTableScan.getInputs()).thenReturn(Collections.emptyList());

        boolean result = TrivialQueryChecker.isTrivialQuery(mockProject);
        assertTrue(result, "Projection with nested field access should be trivial");
    }

    @Test
    void testMixedSimpleAndNestedFieldsIsTrivial() {
        // Create mixed projections: simple field reference and nested field access
        RelDataType simpleType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexInputRef simpleField = rexBuilder.makeInputRef(simpleType, 0);

        // Create structured type for nested field access
        RelDataType structType = typeFactory.builder()
            .add("nestedField", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .add("anotherField", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .build();

        RexInputRef baseField = rexBuilder.makeInputRef(structType, 1);
        RexLiteral nestedFieldName = rexBuilder.makeLiteral("nestedField");
        RexCall nestedFieldAccess = (RexCall) rexBuilder.makeCall(
            SqlStdOperatorTable.ITEM,
            baseField,
            nestedFieldName
        );

        List<RexNode> mixedProjections = List.of(simpleField, nestedFieldAccess);

        when(mockProject.getProjects()).thenReturn(mixedProjections);
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(mockTableScan));
        when(mockTableScan.getInputs()).thenReturn(Collections.emptyList());

        boolean result = TrivialQueryChecker.isTrivialQuery(mockProject);
        assertTrue(result, "Projection with mixed simple and nested fields should be trivial");
    }

    @Test
    void testDeeplyNestedFieldAccessIsTrivial() {
        // Create deeply nested field access: ITEM(ITEM($0, 'level1'), 'level2')
        // Create proper nested structure: root has level1 field, level1 field has level2 field
        RelDataType level2Type = typeFactory.builder()
            .add("level2", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .build();

        RelDataType rootType = typeFactory.builder()
            .add("level1", level2Type)
            .build();

        RexInputRef baseField = rexBuilder.makeInputRef(rootType, 0);
        RexLiteral level1Name = rexBuilder.makeLiteral("level1");

        RexCall level1Access = (RexCall) rexBuilder.makeCall(
            SqlStdOperatorTable.ITEM,
            baseField,
            level1Name
        );

        RexLiteral level2Name = rexBuilder.makeLiteral("level2");
        RexCall level2Access = (RexCall) rexBuilder.makeCall(
            SqlStdOperatorTable.ITEM,
            level1Access,
            level2Name
        );

        when(mockProject.getProjects()).thenReturn(Collections.singletonList(level2Access));
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(mockTableScan));
        when(mockTableScan.getInputs()).thenReturn(Collections.emptyList());

        boolean result = TrivialQueryChecker.isTrivialQuery(mockProject);
        assertTrue(result, "Projection with deeply nested field access should be trivial");
    }

    @Test
    void testItemOperatorWithValidLiteralIsTrivial() {
        // Test that ITEM operator with valid literal field name is considered trivial
        // This verifies the core functionality we added to TrivialQueryChecker
        RelDataType structType = typeFactory.builder()
            .add("validField", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .add("anotherField", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .build();

        RexInputRef baseField = rexBuilder.makeInputRef(structType, 0);
        RexLiteral validFieldName = rexBuilder.makeLiteral("validField");

        RexCall validItemCall = (RexCall) rexBuilder.makeCall(
            SqlStdOperatorTable.ITEM,
            baseField,
            validFieldName
        );

        when(mockProject.getProjects()).thenReturn(Collections.singletonList(validItemCall));
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(mockTableScan));
        when(mockTableScan.getInputs()).thenReturn(Collections.emptyList());

        boolean result = TrivialQueryChecker.isTrivialQuery(mockProject);
        assertTrue(result, "ITEM operator with valid literal field name should be trivial");
    }

    @Test
    void testUnknownNodeTypeIsNotTrivial() {
        // Create a mock RelNode that doesn't match any of the known trivial types
        RelNode unknownNode = mock(RelNode.class);
        when(unknownNode.getInputs()).thenReturn(Collections.emptyList());
        when(unknownNode.getRowType()).thenReturn(createRowType("field1"));

        // Mock the accept method to call visit(RelNode other) which returns false
        when(unknownNode.accept(any(RelShuttle.class))).thenAnswer(invocation -> {
            org.apache.calcite.rel.RelShuttle shuttle = invocation.getArgument(0);
            return shuttle.visit(unknownNode);
        });

        boolean result = TrivialQueryChecker.isTrivialQuery(unknownNode);
        assertFalse(result, "Unknown RelNode types should be considered non-trivial");
    }

    private RelDataType createRowType(String... fieldNames) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (String fieldName : fieldNames) {
            builder.add(fieldName, SqlTypeName.VARCHAR);
        }
        return builder.build();
    }
}
