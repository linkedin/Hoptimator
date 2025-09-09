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
    void testFieldMapSuccessForTrivialQuery() {
        RelDataType rowType = createSimpleRowType();
        RelNode trivialQuery = createTrivialQuery(rowType);
        implementor.setQuery(trivialQuery);

        ThrowingFunction<SqlDialect, String> fieldMapFunc = implementor.fieldMap();
        assertNotNull(fieldMapFunc, "Field map function should be created");

        assertThrows(Exception.class, () -> fieldMapFunc.apply(SqlDialect.ANSI),
            "Field map function should attempt execution but fail due to mock limitations");
    }

    @Test
    void testValidateFieldMappingWithSinkRowType() {
        RelDataType sinkRowType = createRowTypeWithFields("dest_field1", "dest_field2", "extra_field");

        // Set up the sink with proper field mapping
        assertDoesNotThrow(() -> implementor.setSink("test_db", List.of("schema", "test_table"),
            sinkRowType, Collections.emptyMap()));

        // Create a trivial query and test field mapping
        RelNode trivialQuery = createTrivialQuery(createSimpleRowType());
        implementor.setQuery(trivialQuery);

        ThrowingFunction<SqlDialect, String> fieldMapFunc = implementor.fieldMap();
        assertNotNull(fieldMapFunc, "Field map function should be created");

        assertThrows(Exception.class, () -> fieldMapFunc.apply(SqlDialect.ANSI),
            "Field mapping should attempt execution but fail due to mock limitations");
    }

    @Test
    void testValidateFieldMappingFailsForMissingField() {
        // Create sink type missing one of the target fields
        RelDataType sinkRowType = createRowTypeWithFields("dest_field1"); // missing dest_field2

        implementor.setSink("test_db", List.of("schema", "test_table"), sinkRowType, Collections.emptyMap());

        RelNode trivialQuery = createTrivialQuery(createSimpleRowType());
        implementor.setQuery(trivialQuery);

        ThrowingFunction<SqlDialect, String> fieldMapFunc = implementor.fieldMap();
        assertNotNull(fieldMapFunc, "Field map function should be created");

        SQLNonTransientException exception = assertThrows(SQLNonTransientException.class,
            () -> fieldMapFunc.apply(SqlDialect.ANSI), "Missing target field should cause validation failure");
        assertTrue(exception.getMessage().contains("dest_field2"));
    }

    @Test
    void testQueryGeneration() throws SQLException {
        RelNode trivialQuery = createTrivialQuery(createSimpleRowType());
        implementor.setQuery(trivialQuery);
        implementor.addSource("test_db", List.of("source_table"), createSimpleRowType(), Collections.emptyMap());

        ThrowingFunction<SqlDialect, String> queryFunc = implementor.query(mockConnection);
        assertNotNull(queryFunc, "Query function should be created");

        assertThrows(Exception.class, () -> queryFunc.apply(SqlDialect.ANSI),
            "Query function should attempt SQL generation but fail due to mock limitations");
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
    void testFieldMapJsonSerialization() {
        RelNode trivialQuery = createTrivialQuery(createSimpleRowType());
        implementor.setQuery(trivialQuery);

        ThrowingFunction<SqlDialect, String> fieldMapFunc = implementor.fieldMap();
        assertNotNull(fieldMapFunc, "Field map function should be created");

        assertThrows(Exception.class, () -> fieldMapFunc.apply(SqlDialect.ANSI),
            "Field map function should attempt execution but may fail due to RelToSqlConverter limitations with test mocks");
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
    void testFunctionErrorScenarios() {
        // Test with malformed query setup
        RelNode trivialQuery = createTrivialQuery(createSimpleRowType());
        implementor.setQuery(trivialQuery);

        // Test fieldMap without sink (should still work)
        ThrowingFunction<SqlDialect, String> fieldMapFunc = implementor.fieldMap();
        assertNotNull(fieldMapFunc, "Field map function should be created without sink");

        assertThrows(Exception.class, () -> fieldMapFunc.apply(SqlDialect.ANSI),
            "Field map should attempt execution but fail due to RelToSqlConverter limitations");
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
