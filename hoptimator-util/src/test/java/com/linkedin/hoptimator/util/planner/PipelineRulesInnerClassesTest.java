package com.linkedin.hoptimator.util.planner;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests for the inner Pipeline* RelNode classes inside PipelineRules.
 * <p>
 * The PipelineRule.convert() methods (e.g. PipelineFilterRule.convert()) call
 * ConverterRule.convert(RelNode, RelTrait) which routes through the Calcite planner's
 * changeTraits() path and cannot be exercised without a full VolcanoPlanner planning
 * session.  Those paths are therefore not covered here; instead we cover the
 * constructors, copy() methods, and implement() methods of the produced Pipeline* nodes
 * directly, which represent the majority of the uncovered lines.
 */
@ExtendWith(MockitoExtension.class)
class PipelineRulesInnerClassesTest {

  private RelOptCluster cluster;
  private RelTraitSet pipelineTraitSet;
  private RelDataTypeFactory typeFactory;
  private RexBuilder rexBuilder;

  @Mock
  private PipelineRel.Implementor mockImplementor;

  @Mock
  private RelNode mockRelNode;

  @BeforeEach
  void setUp() {
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    cluster = RelOptCluster.create(planner, rexBuilder);
    pipelineTraitSet = cluster.traitSetOf(PipelineRel.CONVENTION);
  }

  private RelDataType simpleRowType() {
    return typeFactory.builder()
        .add("COL", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
  }

  // ── PipelineFilter ────────────────────────────────────────────────────────

  @Test
  void testPipelineFilterConstructAndImplement() throws SQLException {
    RelNode inputNode = buildMinimalPipelineNode();
    RexNode condition = rexBuilder.makeLiteral(true);

    PipelineRules.PipelineFilter filter = new PipelineRules.PipelineFilter(
        cluster, pipelineTraitSet, Collections.emptyList(), inputNode, condition);

    assertNotNull(filter);
    assertDoesNotThrow(() -> filter.implement(mockImplementor));
  }

  @Test
  void testPipelineFilterCopy() throws SQLException {
    RelNode inputNode = buildMinimalPipelineNode();
    RexNode condition = rexBuilder.makeLiteral(true);

    PipelineRules.PipelineFilter filter = new PipelineRules.PipelineFilter(
        cluster, pipelineTraitSet, Collections.emptyList(), inputNode, condition);

    PipelineRules.PipelineFilter copied = filter.copy(pipelineTraitSet, inputNode, condition);

    assertNotNull(copied);
    assertInstanceOf(PipelineRules.PipelineFilter.class, copied);
  }

  // ── PipelineProject ───────────────────────────────────────────────────────

  @Test
  void testPipelineProjectConstructAndImplement() throws SQLException {
    RelNode inputNode = buildMinimalPipelineNode();
    RelDataType rowType = simpleRowType();
    RexInputRef ref = rexBuilder.makeInputRef(inputNode, 0);

    PipelineRules.PipelineProject project = new PipelineRules.PipelineProject(
        cluster, pipelineTraitSet, Collections.emptyList(), inputNode,
        Collections.singletonList(ref), rowType);

    assertNotNull(project);
    assertDoesNotThrow(() -> project.implement(mockImplementor));
  }

  @Test
  void testPipelineProjectCopy() {
    RelNode inputNode = buildMinimalPipelineNode();
    RelDataType rowType = simpleRowType();
    RexInputRef ref = rexBuilder.makeInputRef(inputNode, 0);
    List<RexNode> projects = Collections.singletonList(ref);

    PipelineRules.PipelineProject project = new PipelineRules.PipelineProject(
        cluster, pipelineTraitSet, Collections.emptyList(), inputNode, projects, rowType);

    PipelineRules.PipelineProject copied = project.copy(pipelineTraitSet, inputNode, projects, rowType);

    assertNotNull(copied);
    assertInstanceOf(PipelineRules.PipelineProject.class, copied);
  }

  // ── PipelineJoin ──────────────────────────────────────────────────────────

  @Test
  void testPipelineJoinConstructAndImplement() throws SQLException {
    RelNode leftNode = buildMinimalPipelineNode();
    RelNode rightNode = buildMinimalPipelineNode();
    RexNode condition = rexBuilder.makeLiteral(true);

    PipelineRules.PipelineJoin join = new PipelineRules.PipelineJoin(
        cluster, pipelineTraitSet, Collections.emptyList(), leftNode, rightNode,
        condition, Collections.emptySet(), JoinRelType.INNER);

    assertNotNull(join);
    assertDoesNotThrow(() -> join.implement(mockImplementor));
  }

  @Test
  void testPipelineJoinCopy() {
    RelNode leftNode = buildMinimalPipelineNode();
    RelNode rightNode = buildMinimalPipelineNode();
    RexNode condition = rexBuilder.makeLiteral(true);

    PipelineRules.PipelineJoin join = new PipelineRules.PipelineJoin(
        cluster, pipelineTraitSet, Collections.emptyList(), leftNode, rightNode,
        condition, Collections.emptySet(), JoinRelType.INNER);

    PipelineRules.PipelineJoin copied = join.copy(pipelineTraitSet, condition, leftNode, rightNode,
        JoinRelType.LEFT, false);

    assertNotNull(copied);
    assertInstanceOf(PipelineRules.PipelineJoin.class, copied);
  }

  // ── PipelineCalc ──────────────────────────────────────────────────────────

  @Test
  void testPipelineCalcConstructAndImplement() throws SQLException {
    RelNode inputNode = buildMinimalPipelineNode();
    RelDataType rowType = simpleRowType();
    RexProgram program = RexProgram.createIdentity(rowType);

    PipelineRules.PipelineCalc calc = new PipelineRules.PipelineCalc(
        cluster, pipelineTraitSet, Collections.emptyList(), inputNode, program);

    assertNotNull(calc);
    assertDoesNotThrow(() -> calc.implement(mockImplementor));
  }

  @Test
  void testPipelineCalcCopy() {
    RelNode inputNode = buildMinimalPipelineNode();
    RexProgram program = RexProgram.createIdentity(simpleRowType());

    PipelineRules.PipelineCalc calc = new PipelineRules.PipelineCalc(
        cluster, pipelineTraitSet, Collections.emptyList(), inputNode, program);

    PipelineRules.PipelineCalc copied = calc.copy(pipelineTraitSet, inputNode, program);

    assertNotNull(copied);
    assertInstanceOf(PipelineRules.PipelineCalc.class, copied);
  }

  // ── PipelineAggregate ─────────────────────────────────────────────────────

  @Test
  void testPipelineAggregateConstructAndImplement() throws SQLException {
    RelNode inputNode = buildMinimalPipelineNode();
    ImmutableBitSet groupSet = ImmutableBitSet.of(0);

    PipelineRules.PipelineAggregate agg = new PipelineRules.PipelineAggregate(
        cluster, pipelineTraitSet, Collections.emptyList(), inputNode,
        groupSet, Collections.singletonList(groupSet), Collections.emptyList());

    assertNotNull(agg);
    assertDoesNotThrow(() -> agg.implement(mockImplementor));
  }

  @Test
  void testPipelineAggregateCopy() {
    RelNode inputNode = buildMinimalPipelineNode();
    ImmutableBitSet groupSet = ImmutableBitSet.of(0);
    List<AggregateCall> aggCalls = Collections.emptyList();

    PipelineRules.PipelineAggregate agg = new PipelineRules.PipelineAggregate(
        cluster, pipelineTraitSet, Collections.emptyList(), inputNode,
        groupSet, Collections.singletonList(groupSet), aggCalls);

    PipelineRules.PipelineAggregate copied = agg.copy(pipelineTraitSet, inputNode, groupSet,
        Collections.singletonList(groupSet), aggCalls);

    assertNotNull(copied);
    assertInstanceOf(PipelineRules.PipelineAggregate.class, copied);
  }

  // ── PipelineUncollect ─────────────────────────────────────────────────────

  @Test
  void testPipelineUncollectConstructAndImplement() throws SQLException {
    RelNode inputNode = buildUncollectInputNode();

    PipelineRules.PipelineUncollect uncollect = new PipelineRules.PipelineUncollect(
        cluster, pipelineTraitSet, inputNode, false, Collections.emptyList());

    assertNotNull(uncollect);
    assertDoesNotThrow(() -> uncollect.implement(mockImplementor));
  }

  @Test
  void testPipelineUncollectCopy() {
    RelNode inputNode = buildUncollectInputNode();

    PipelineRules.PipelineUncollect uncollect = new PipelineRules.PipelineUncollect(
        cluster, pipelineTraitSet, inputNode, false, Collections.emptyList());

    PipelineRules.PipelineUncollect copied = uncollect.copy(pipelineTraitSet, inputNode);

    assertNotNull(copied);
    assertInstanceOf(PipelineRules.PipelineUncollect.class, copied);
  }

  // ── PipelineTableScan ─────────────────────────────────────────────────────

  @Test
  void testPipelineTableScanConstructAndImplement() throws SQLException {
    RelDataType rowType = simpleRowType();
    RelOptTable mockTable = mock(RelOptTable.class);
    when(mockTable.getRowType()).thenReturn(rowType);
    when(mockTable.getQualifiedName()).thenReturn(Arrays.asList("testDb", "T"));

    PipelineRules.PipelineTableScan scan = new PipelineRules.PipelineTableScan(
        cluster, pipelineTraitSet, Collections.emptyList(), "testDb", mockTable);

    assertNotNull(scan);
    assertDoesNotThrow(() -> scan.implement(mockImplementor));
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  /**
   * Creates an input node whose row type contains an array column, which is required
   * by Uncollect (UNNEST).  The Uncollect constructor validates that all input fields
   * are collection types.
   */
  private RelNode buildUncollectInputNode() {
    RelDataType componentType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType arrayType = typeFactory.createArrayType(componentType, -1);
    RelDataType rowType = typeFactory.builder()
        .add("ARR", arrayType)
        .build();

    when(mockRelNode.getRowType()).thenReturn(rowType);
    lenient().when(mockRelNode.getCluster()).thenReturn(cluster);
    lenient().when(mockRelNode.getTraitSet()).thenReturn(pipelineTraitSet);
    lenient().when(mockRelNode.getInputs()).thenReturn(Collections.emptyList());

    return new PipelineRules.PipelineFilter(
        cluster, pipelineTraitSet, Collections.emptyList(),
        mockRelNode, rexBuilder.makeLiteral(true));
  }

  /**
   * Creates a minimal PipelineFilter with a literal true condition to serve as an
   * already-converted input node in the PIPELINE convention.  Used when a Pipeline
   * node constructor requires a child already in that convention.
   */
  private RelNode buildMinimalPipelineNode() {
    RelDataType rowType = simpleRowType();
    // Build a self-referential pipeline node that acts as a leaf: a PipelineFilter
    // wrapping a mock-valued node.  We cannot use a real TableScan here because that
    // requires a registered table; instead we create a PipelineFilter on top of a
    // minimal mock RelNode whose row type is known.
    //
    // Lenient stubbing is used because individual tests may not exercise all methods
    // on the inner mock (e.g. copy()-only tests never call getRowType()).
    when(mockRelNode.getRowType()).thenReturn(rowType);
    lenient().when(mockRelNode.getCluster()).thenReturn(cluster);
    lenient().when(mockRelNode.getTraitSet()).thenReturn(pipelineTraitSet);
    lenient().when(mockRelNode.getInputs()).thenReturn(Collections.emptyList());

    return new PipelineRules.PipelineFilter(
        cluster, pipelineTraitSet, Collections.emptyList(),
        mockRelNode, rexBuilder.makeLiteral(true));
  }
}
