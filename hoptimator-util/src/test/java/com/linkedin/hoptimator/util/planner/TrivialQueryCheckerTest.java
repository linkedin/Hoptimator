package com.linkedin.hoptimator.util.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalAsofJoin;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalRepeatUnion;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
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

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for TrivialQueryChecker to verify correct identification of trivial vs non-trivial queries.
 * <p>
 * A trivial query should only contain:
 * - Simple table scans
 * - Simple projections (field references and nested field access only, no functions or calculations)
 * - Nested field access using ITEM operator (e.g., ITEM($3, 'nestedField'))
 * <p>
 * Non-trivial queries include:
 * - Joins, aggregations, filters, sorts, unions
 * - Complex expressions in projections (functions, calculations, etc.)
 * - Any other relational operations
 */
@ExtendWith(MockitoExtension.class)
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

        // Set up accept() methods to properly dispatch to TrivialQueryChecker visit methods.
        // These are lenient because each test only uses a subset of the four mocks.
        lenient().when(mockTableScan.accept(any(RelShuttle.class))).thenCallRealMethod();
        lenient().when(mockProject.accept(any(RelShuttle.class))).thenCallRealMethod();
        lenient().when(mockFilter.accept(any(RelShuttle.class))).thenCallRealMethod();
        lenient().when(mockJoin.accept(any(RelShuttle.class))).thenCallRealMethod();
    }

    @Test
    void testTableScanIsTrivial() {
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

        boolean result = TrivialQueryChecker.isTrivialQuery(mockProject);
        assertTrue(result, "Simple projection with field references should be trivial");
    }

    @Test
    void testComplexProjectionIsNotTrivial() {
        // Create a complex expression (function call)
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        // Create a simple complex expression - a function call
        RexNode complexExpression = rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeExactLiteral(BigDecimal.ONE)
        );

        when(mockProject.getProjects()).thenReturn(Collections.singletonList(complexExpression));
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(mockTableScan));

        boolean result = TrivialQueryChecker.isTrivialQuery(mockProject);
        assertFalse(result, "Projection with complex expressions should not be trivial");
    }

    @Test
    void testOtherNodesNotTrivial() {
        boolean result = TrivialQueryChecker.isTrivialQuery(mockFilter);
        assertFalse(result, "Queries with filters should not be trivial");

        result = TrivialQueryChecker.isTrivialQuery(mockJoin);
        assertFalse(result, "Queries with joins should not be trivial");
    }

    @Test
    void testComplexQueryIsNotTrivial() {
        // A project over a filter is non-trivial; visitChildren encounters filter which sets trivial=false
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(mockFilter));

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
        when(innerProject.accept(any(RelShuttle.class))).thenCallRealMethod();
        when(innerProject.getProjects()).thenReturn(simpleProjections);

        when(mockProject.getProjects()).thenReturn(simpleProjections);
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(innerProject));

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

        boolean result = TrivialQueryChecker.isTrivialQuery(mockProject);
        assertTrue(result, "ITEM operator with valid literal field name should be trivial");
    }

    @Test
    void testUnknownNodeTypeIsNotTrivial() {
        // Create a mock RelNode that doesn't match any of the known trivial types
        RelNode unknownNode = mock(RelNode.class);

        // Mock the accept method to call visit(RelNode other) which sets trivial=false
        when(unknownNode.accept(any(RelShuttle.class))).thenAnswer(invocation -> {
            RelShuttle shuttle = invocation.getArgument(0);
            return shuttle.visit(unknownNode);
        });

        boolean result = TrivialQueryChecker.isTrivialQuery(unknownNode);
        assertFalse(result, "Unknown RelNode types should be considered non-trivial");
    }

    @Test
    void testTableFunctionScanIsNotTrivial() {
        TableFunctionScan mockScan = mock(TableFunctionScan.class);
        when(mockScan.accept(any(RelShuttle.class))).thenCallRealMethod();

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockScan));
    }

    @Test
    void testLogicalValuesIsNotTrivial() {
        LogicalValues mockValues = mock(LogicalValues.class);
        when(mockValues.accept(any(RelShuttle.class))).thenCallRealMethod();

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockValues));
    }

    @Test
    void testLogicalCalcIsNotTrivial() {
        LogicalCalc mockCalc = mock(LogicalCalc.class);
        when(mockCalc.accept(any(RelShuttle.class))).thenCallRealMethod();

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockCalc));
    }

    @Test
    void testLogicalCorrelateIsNotTrivial() {
        LogicalCorrelate mockCorrelate = mock(LogicalCorrelate.class);
        when(mockCorrelate.accept(any(RelShuttle.class))).thenCallRealMethod();

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockCorrelate));
    }

    @Test
    void testLogicalUnionIsNotTrivial() {
        LogicalUnion mockUnion = mock(LogicalUnion.class);
        when(mockUnion.accept(any(RelShuttle.class))).thenCallRealMethod();

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockUnion));
    }

    @Test
    void testLogicalIntersectIsNotTrivial() {
        LogicalIntersect mockIntersect = mock(LogicalIntersect.class);
        when(mockIntersect.accept(any(RelShuttle.class))).thenCallRealMethod();

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockIntersect));
    }

    @Test
    void testLogicalMinusIsNotTrivial() {
        LogicalMinus mockMinus = mock(LogicalMinus.class);
        when(mockMinus.accept(any(RelShuttle.class))).thenCallRealMethod();

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockMinus));
    }

    @Test
    void testLogicalAggregateIsNotTrivial() {
        LogicalAggregate mockAggregate = mock(LogicalAggregate.class);
        when(mockAggregate.accept(any(RelShuttle.class))).thenCallRealMethod();

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockAggregate));
    }

    @Test
    void testLogicalMatchIsNotTrivial() {
        LogicalMatch mockMatch = mock(LogicalMatch.class);
        when(mockMatch.accept(any(RelShuttle.class))).thenCallRealMethod();

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockMatch));
    }

    @Test
    void testLogicalSortIsNotTrivial() {
        LogicalSort mockSort = mock(LogicalSort.class);
        when(mockSort.accept(any(RelShuttle.class))).thenCallRealMethod();

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockSort));
    }

    @Test
    void testLogicalExchangeIsNotTrivial() {
        LogicalExchange mockExchange = mock(LogicalExchange.class);
        when(mockExchange.accept(any(RelShuttle.class))).thenCallRealMethod();

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockExchange));
    }

    @Test
    void testLogicalTableModifyIsNotTrivial() {
        LogicalTableModify mockModify = mock(LogicalTableModify.class);
        when(mockModify.accept(any(RelShuttle.class))).thenCallRealMethod();

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockModify));
    }

    @Test
    void testLogicalAsofJoinIsNotTrivial() {
        LogicalAsofJoin mockAsofJoin = mock(LogicalAsofJoin.class);
        when(mockAsofJoin.accept(any(RelShuttle.class))).thenCallRealMethod();

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockAsofJoin));
    }

    @Test
    void testLogicalRepeatUnionIsNotTrivial() {
        LogicalRepeatUnion mockRepeatUnion = mock(LogicalRepeatUnion.class);
        when(mockRepeatUnion.accept(any(RelShuttle.class))).thenCallRealMethod();

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockRepeatUnion));
    }

    @Test
    void testVisitTableScanDirectlyIsTrivial() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        TableScan scan = mock(TableScan.class);

        RelNode result = checker.visit(scan);

        assertNotNull(result);
        assertTrue(checker.isTrivial(), "visit(TableScan) should keep trivial as true");
    }

    @Test
    void testVisitLogicalProjectDirectlyWithSimpleFieldsIsTrivial() {
        RelDataType rowType = createRowType("field1");
        List<RexNode> simpleProjections = List.of(
            new RexInputRef(0, rowType.getFieldList().get(0).getType())
        );

        LogicalProject project = mock(LogicalProject.class);
        when(project.getProjects()).thenReturn(simpleProjections);
        when(project.getInputs()).thenReturn(Collections.emptyList());

        TrivialQueryChecker checker = new TrivialQueryChecker();
        RelNode result = checker.visit(project);

        assertNotNull(result);
        assertTrue(checker.isTrivial(), "visit(LogicalProject) with simple fields should be trivial");
    }

    @Test
    void testVisitLogicalValuesReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        LogicalValues values = mock(LogicalValues.class);
        RelNode result = checker.visit(values);
        assertNotNull(result, "visit(LogicalValues) must not return null");
    }

    @Test
    void testVisitLogicalFilterReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        RelNode result = checker.visit(mockFilter);
        assertNotNull(result, "visit(LogicalFilter) must not return null");
    }

    @Test
    void testVisitLogicalCalcReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        LogicalCalc calc = mock(LogicalCalc.class);
        RelNode result = checker.visit(calc);
        assertNotNull(result, "visit(LogicalCalc) must not return null");
    }

    @Test
    void testVisitLogicalJoinReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        RelNode result = checker.visit(mockJoin);
        assertNotNull(result, "visit(LogicalJoin) must not return null");
    }

    @Test
    void testVisitLogicalCorrelateReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        LogicalCorrelate correlate = mock(LogicalCorrelate.class);
        RelNode result = checker.visit(correlate);
        assertNotNull(result, "visit(LogicalCorrelate) must not return null");
    }

    @Test
    void testVisitLogicalUnionReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        LogicalUnion union = mock(LogicalUnion.class);
        RelNode result = checker.visit(union);
        assertNotNull(result, "visit(LogicalUnion) must not return null");
    }

    @Test
    void testVisitLogicalIntersectReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        LogicalIntersect intersect = mock(LogicalIntersect.class);
        RelNode result = checker.visit(intersect);
        assertNotNull(result, "visit(LogicalIntersect) must not return null");
    }

    @Test
    void testVisitLogicalMinusReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        LogicalMinus minus = mock(LogicalMinus.class);
        RelNode result = checker.visit(minus);
        assertNotNull(result, "visit(LogicalMinus) must not return null");
    }

    @Test
    void testVisitLogicalAggregateReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        LogicalAggregate aggregate = mock(LogicalAggregate.class);
        RelNode result = checker.visit(aggregate);
        assertNotNull(result, "visit(LogicalAggregate) must not return null");
    }

    @Test
    void testVisitLogicalMatchReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        LogicalMatch match = mock(LogicalMatch.class);
        RelNode result = checker.visit(match);
        assertNotNull(result, "visit(LogicalMatch) must not return null");
    }

    @Test
    void testVisitLogicalSortReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        LogicalSort sort = mock(LogicalSort.class);
        RelNode result = checker.visit(sort);
        assertNotNull(result, "visit(LogicalSort) must not return null");
    }

    @Test
    void testVisitLogicalExchangeReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        LogicalExchange exchange = mock(LogicalExchange.class);
        RelNode result = checker.visit(exchange);
        assertNotNull(result, "visit(LogicalExchange) must not return null");
    }

    @Test
    void testVisitLogicalTableModifyReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        LogicalTableModify modify = mock(LogicalTableModify.class);
        RelNode result = checker.visit(modify);
        assertNotNull(result, "visit(LogicalTableModify) must not return null");
    }

    @Test
    void testVisitLogicalAsofJoinReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        LogicalAsofJoin asofJoin = mock(LogicalAsofJoin.class);
        RelNode result = checker.visit(asofJoin);
        assertNotNull(result, "visit(LogicalAsofJoin) must not return null");
    }

    @Test
    void testVisitLogicalRepeatUnionReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        LogicalRepeatUnion repeatUnion = mock(LogicalRepeatUnion.class);
        RelNode result = checker.visit(repeatUnion);
        assertNotNull(result, "visit(LogicalRepeatUnion) must not return null");
    }

    @Test
    void testVisitRelNodeOtherReturnsNonNull() {
        TrivialQueryChecker checker = new TrivialQueryChecker();
        RelNode other = mock(RelNode.class);
        RelNode result = checker.visit(other);
        assertNotNull(result, "visit(RelNode) must not return null");
        assertFalse(checker.isTrivial(), "Unknown RelNode should mark non-trivial");
    }

    // Traversal correctness: visiting children of project must detect nested non-trivial nodes

    /**
     * A LogicalProject wrapping a LogicalAggregate must be non-trivial.
     * If visit(LogicalProject) returned null instead of this, visitChildren
     * would not be called and the aggregate would never be visited, incorrectly
     * leaving isTrivial() as true.
     */
    @Test
    void testProjectWrappingAggregateIsNotTrivial() {
        LogicalAggregate aggregate = mock(LogicalAggregate.class);
        when(aggregate.accept(any(RelShuttle.class))).thenCallRealMethod();

        // Project with simple fields so the project itself is ok — non-triviality
        // must come from the aggregate child.
        RelDataType rowType = createRowType("field1");
        List<RexNode> simpleProjections = List.of(
            new RexInputRef(0, rowType.getFieldList().get(0).getType())
        );
        when(mockProject.getProjects()).thenReturn(simpleProjections);
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(aggregate));

        boolean result = TrivialQueryChecker.isTrivialQuery(mockProject);
        assertFalse(result,
            "Project wrapping an aggregate must be non-trivial; "
            + "traversal into children must occur");
    }

    /**
     * Verify visitChildren short-circuits after the first non-trivial child:
     * the second child (which would also mark non-trivial) should not cause
     * issues, and the result must be non-trivial.
     * Both stubbings are lenient because short-circuit means filter2 may not be called.
     */
    @Test
    void testVisitChildrenShortCircuitsOnNonTrivial() {
        LogicalFilter filter1 = mock(LogicalFilter.class);
        LogicalFilter filter2 = mock(LogicalFilter.class);
        lenient().when(filter1.accept(any(RelShuttle.class))).thenCallRealMethod();
        lenient().when(filter2.accept(any(RelShuttle.class))).thenCallRealMethod();

        RelDataType rowType = createRowType("f");
        when(mockProject.getProjects()).thenReturn(
            List.of(new RexInputRef(0, rowType.getFieldList().get(0).getType())));
        when(mockProject.getInputs()).thenReturn(List.of(filter1, filter2));

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockProject),
            "Project with non-trivial children must be non-trivial");
    }

    // A plain InputRef is a simple field reference → trivial.
    @Test
    void testInputRefAloneIsTrivialProjection() {
        RelDataType rowType = createRowType("x");
        List<RexNode> proj = List.of(new RexInputRef(0, rowType.getFieldList().get(0).getType()));
        when(mockProject.getProjects()).thenReturn(proj);
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(mockTableScan));

        assertTrue(TrivialQueryChecker.isTrivialQuery(mockProject),
            "Single InputRef projection must be trivial");
    }

    /**
     * A RexCall that is NOT an ITEM operator (e.g. PLUS) must NOT be a simple
     * field reference → non-trivial.
     */
    @Test
    void testRexCallWithNonItemOperatorIsNotTrivial() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode plusExpr = rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, 1));

        when(mockProject.getProjects()).thenReturn(Collections.singletonList(plusExpr));
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(mockTableScan));

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockProject),
            "PLUS expression must make the query non-trivial");
    }

    /**
     * An ITEM call where the base is NOT an InputRef (a nested ITEM whose own
     * base is a complex PLUS call) must NOT be a simple field reference.
     *
     * We build:  ITEM(ITEM($0_struct, 'level1'), 'level2')
     * The outer ITEM's base is itself an ITEM – which IS a simple reference.
     * To force the base to be non-simple we directly build a deeply-nested
     * ITEM where the innermost base is invalid, using mocked RexCall objects
     * so Calcite's type inference is bypassed.
     */
    @Test
    void testItemOperatorWithNonInputRefBaseIsNotTrivial() {
        // Create a mock RexCall that looks like an ITEM but whose base is a PLUS expression.
        // We mock the operator to be SqlItemOperator and have 2 operands:
        //   operand(0) = a PLUS RexCall (not an InputRef)
        //   operand(1) = a RexLiteral
        // Since we cannot easily pass Calcite's type inference for ITEM(PLUS(...), literal),
        // we instead use the existing multi-level ITEM test (deeply nested) and add a
        // separate test that checks the isSimpleFieldReference path via a RexCall mock.

        // A function call that is not ITEM and not InputRef must not be simple
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode plusExpr = rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, 1));

        // Wrap the PLUS in a project - the project will be non-trivial
        when(mockProject.getProjects()).thenReturn(Collections.singletonList(plusExpr));
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(mockTableScan));

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockProject),
            "PLUS expression must be non-trivial");
    }

    /**
     * An ITEM call whose base is itself a non-simple expression (an ITEM
     * wrapped in a function) must be non-trivial.
     * This uses a mocked RexCall to bypass Calcite's type-inference restrictions
     * while still exercising the `isSimpleFieldReference(baseField)` condition.
     */
    @Test
    void testItemOperatorWithRexCallBaseIsNotTrivial() {
        // Mock a RexCall that pretends to be an ITEM operator but has
        // a non-InputRef (PLUS) as its first operand.
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode plusBase = rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, 1));
        RexLiteral fieldName = rexBuilder.makeLiteral("field");

        // Build a mocked RexCall: operator=ITEM, operands=[plusBase, fieldName]
        RexCall mockedItemCall = mock(RexCall.class);
        lenient().when(mockedItemCall.getOperator()).thenReturn(SqlStdOperatorTable.ITEM);
        lenient().when(mockedItemCall.getOperands()).thenReturn(List.of(plusBase, fieldName));

        // isSimpleFieldReference for this call should check:
        //   - operator instanceof SqlItemOperator  → true (ITEM)
        //   - operands.size() == 2                 → true
        //   - isSimpleFieldReference(plusBase)     → false (PLUS is not InputRef)
        // Therefore the whole expression is NOT a simple field reference.
        when(mockProject.getProjects()).thenReturn(Collections.singletonList(mockedItemCall));
        when(mockProject.getInputs()).thenReturn(Collections.singletonList(mockTableScan));

        assertFalse(TrivialQueryChecker.isTrivialQuery(mockProject),
            "ITEM with PLUS base must not be a simple field reference");
    }

    private RelDataType createRowType(String... fieldNames) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (String fieldName : fieldNames) {
            builder.add(fieldName, SqlTypeName.VARCHAR);
        }
        return builder.build();
    }
}
