package com.linkedin.hoptimator.util.planner;

import java.sql.SQLNonTransientException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.ThrowingFunction;
import com.linkedin.hoptimator.util.ConnectionService;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.ImmutablePairList;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;


/**
 * End-to-end coverage (through the real {@link PipelineRel.Implementor#sql} → {@link ScriptImplementor}
 * generation) of the source/sink type-compatibility validation and assignment-cast injection driven
 * by the {@code castMode} hint. The canonical case is a raw primitive Kafka key exposed as
 * {@code STRING} projected onto a typed {@code BIGINT} sink key column.
 *
 * <p>The matrix covers: a safe cast and an unsafe (rejected) conversion in each of the three modes,
 * an explicit user {@code CAST}/{@code SAFE_CAST} already present in the query (never double-wrapped),
 * nullable vs. {@code NOT NULL} handling, and complex-type mismatches.
 */
@ExtendWith(MockitoExtension.class)
class PipelineRelCastTest {

  private static final RelDataTypeFactory TYPE_FACTORY = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private static final String SINK_COLUMN = "KEY_member_id";

  private static final Map<String, String> STRICT = Collections.emptyMap();
  private static final Map<String, String> ASSIGN = Map.of("castMode", "assign");
  private static final Map<String, String> EXPLICIT = Map.of("castMode", "explicit");

  @Mock
  private DeploymentContext mockContext;

  @Mock
  private MockedStatic<ConnectionService> connectionServiceStatic;

  private void stubHasConnector() {
    connectionServiceStatic.when(() -> ConnectionService.configure(any(), any()))
        .thenReturn(Collections.singletonMap("connector", "test-connector"));
  }

  // --- type helpers ---

  private RelDataType type(SqlTypeName name) {
    return TYPE_FACTORY.createSqlType(name);
  }

  private RelDataType nullable(RelDataType base) {
    return TYPE_FACTORY.createTypeWithNullability(base, true);
  }

  private RelDataType notNull(RelDataType base) {
    return TYPE_FACTORY.createTypeWithNullability(base, false);
  }

  private RelDataType sink(RelDataType column) {
    return TYPE_FACTORY.builder().add(SINK_COLUMN, column).build();
  }

  // --- query builders ---

  private RelBuilder builderOverSource(RelDataType sourceColumnType) {
    RelDataType tableType = TYPE_FACTORY.builder().add("SRC", sourceColumnType).build();
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus sub = rootSchema.add("S", new AbstractSchema());
    sub.add("T", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return tableType;
      }
    });
    return RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(rootSchema).build());
  }

  /** {@code SELECT SRC AS KEY_member_id} — the column keeps its source type. */
  private RelNode selectAs(RelDataType sourceColumnType) {
    RelBuilder builder = builderOverSource(sourceColumnType);
    builder.scan("S", "T");
    return builder.project(builder.alias(builder.field("SRC"), SINK_COLUMN)).build();
  }

  /** {@code SELECT CAST(SRC AS castTo) AS KEY_member_id} (optionally SAFE_CAST). */
  private RelNode selectCast(RelDataType sourceColumnType, SqlTypeName castTo, boolean safe) {
    RelBuilder builder = builderOverSource(sourceColumnType);
    builder.scan("S", "T");
    RexNode field = builder.field("SRC");
    RexNode castExpr = safe
        ? builder.getRexBuilder().makeCast(nullable(type(castTo)), field, false, true)
        : builder.cast(field, castTo);
    return builder.project(builder.alias(castExpr, SINK_COLUMN)).build();
  }

  private ThrowingFunction<SqlDialect, String> sqlFor(RelNode query, RelDataType sinkRowType,
      Map<String, String> hints) throws Exception {
    ImmutablePairList<Integer, String> targetFields = ImmutablePairList.copyOf(
        List.of(new AbstractMap.SimpleEntry<>(0, SINK_COLUMN)));
    PipelineRel.Implementor impl = new PipelineRel.Implementor(targetFields, hints);
    impl.setQuery(query);
    impl.addSource("db", List.of("schema", "sourceTable"), query.getRowType(), Collections.emptyMap());
    impl.setSink("db", List.of("schema", "sinkTable"), sinkRowType, Collections.emptyMap());
    return impl.sql(mockContext);
  }

  private String generate(RelNode query, RelDataType sinkRowType, Map<String, String> hints) throws Exception {
    return sqlFor(query, sinkRowType, hints).apply(SqlDialect.ANSI);
  }

  private void assertRejects(RelNode query, RelDataType sinkRowType, Map<String, String> hints) {
    assertThatThrownBy(() -> sqlFor(query, sinkRowType, hints).apply(SqlDialect.ANSI))
        .isInstanceOf(SQLNonTransientException.class)
        .hasMessageContaining(SINK_COLUMN);
  }

  private int castCount(String sql) {
    return sql.split("CAST", -1).length - 1;
  }

  // --- safe cast in each mode ---

  @Test
  void strictInjectsCastForSameFamilyWidening() throws Exception {
    stubHasConnector();
    String sql = generate(selectAs(type(SqlTypeName.INTEGER)), sink(type(SqlTypeName.BIGINT)), STRICT);
    assertThat(sql).contains("CAST").contains("BIGINT");
  }

  @Test
  void assignInjectsCastForCharToScalar() throws Exception {
    stubHasConnector();
    String sql = generate(selectAs(type(SqlTypeName.VARCHAR)), sink(type(SqlTypeName.BIGINT)), ASSIGN);
    assertThat(sql).contains("CAST").contains("BIGINT");
  }

  @Test
  void explicitInjectsCastForScalarToChar() throws Exception {
    stubHasConnector();
    String sql = generate(selectAs(type(SqlTypeName.BIGINT)), sink(type(SqlTypeName.VARCHAR)), EXPLICIT);
    assertThat(sql).contains("CAST").contains("VARCHAR");
  }

  // --- unsafe conversion rejected in each mode ---

  @Test
  void strictRejectsStringToNumeric() {
    stubHasConnector();
    assertRejects(selectAs(type(SqlTypeName.VARCHAR)), sink(type(SqlTypeName.BIGINT)), STRICT);
  }

  @Test
  void assignRejectsScalarToChar() {
    stubHasConnector();
    // char->scalar is the assign carve-out; the reverse (numeric->char) is not, so it stays an error.
    assertRejects(selectAs(type(SqlTypeName.BIGINT)), sink(type(SqlTypeName.VARCHAR)), ASSIGN);
  }

  @Test
  void explicitRejectsStructMismatch() {
    stubHasConnector();
    RelDataType sourceStruct = TYPE_FACTORY.builder().add("a", type(SqlTypeName.INTEGER)).build();
    RelDataType sinkStruct = TYPE_FACTORY.builder().add("a", type(SqlTypeName.VARCHAR)).build();
    assertRejects(selectAs(sourceStruct), sink(sinkStruct), EXPLICIT);
  }

  // --- user-provided CAST is respected, never double-wrapped ---

  @Test
  void userCastMatchingSinkPassesUnderStrictWithoutDoubleWrap() throws Exception {
    stubHasConnector();
    // SELECT CAST(SRC AS BIGINT) AS KEY_member_id, sink BIGINT: output already matches, no injection.
    String sql = generate(selectCast(type(SqlTypeName.VARCHAR), SqlTypeName.BIGINT, false),
        sink(type(SqlTypeName.BIGINT)), STRICT);
    assertThat(castCount(sql)).isEqualTo(1);
  }

  @Test
  void userSafeCastRespectedIntoNullableSink() throws Exception {
    stubHasConnector();
    // SAFE_CAST(SRC AS INTEGER) yields a nullable INTEGER; sink is a nullable BIGINT. INTEGER is
    // assignable to BIGINT, so recognizing the SAFE_CAST must prevent a second injected cast.
    String sql = generate(selectCast(type(SqlTypeName.VARCHAR), SqlTypeName.INTEGER, true),
        sink(nullable(type(SqlTypeName.BIGINT))), STRICT);
    assertThat(castCount(sql)).isEqualTo(1);
    assertThat(sql).contains("INTEGER");
  }

  @Test
  void userCastToDifferentButAssignableTypeIsNotRestacked() throws Exception {
    stubHasConnector();
    // User cast to INTEGER, sink BIGINT: INTEGER is implicitly assignable to BIGINT, so we respect
    // the user's cast and do not add a second one.
    String sql = generate(selectCast(type(SqlTypeName.VARCHAR), SqlTypeName.INTEGER, false),
        sink(type(SqlTypeName.BIGINT)), STRICT);
    assertThat(castCount(sql)).isEqualTo(1);
    assertThat(sql).contains("INTEGER");
  }

  @Test
  void userCastNotAssignableToSinkFailsEarly() {
    stubHasConnector();
    // User cast to VARCHAR but sink is BIGINT: we don't stack a second cast; we fail early because
    // the user's cast result isn't assignable to the sink column.
    assertRejects(selectCast(type(SqlTypeName.INTEGER), SqlTypeName.VARCHAR, false),
        sink(type(SqlTypeName.BIGINT)), EXPLICIT);
  }

  // --- nullability is never fixable by a cast ---

  @Test
  void nullableSourceIntoNotNullSinkRejectedEvenWhenCastable() {
    stubHasConnector();
    assertRejects(selectAs(nullable(type(SqlTypeName.VARCHAR))), sink(notNull(type(SqlTypeName.BIGINT))), ASSIGN);
  }

  @Test
  void nullableSourceIntoNotNullSinkAllowedForEqualTypes() throws Exception {
    // Types already match, so this is a plain assignment (no cast); nullability is enforced at
    // runtime by the engine, matching a native INSERT.
    stubHasConnector();
    String sql = generate(selectAs(nullable(type(SqlTypeName.BIGINT))), sink(notNull(type(SqlTypeName.BIGINT))),
        STRICT);
    assertThat(sql).doesNotContain("CAST");
  }

  @Test
  void notNullSourceIntoNullableSinkIsAllowed() throws Exception {
    stubHasConnector();
    String sql = generate(selectAs(notNull(type(SqlTypeName.INTEGER))), sink(nullable(type(SqlTypeName.BIGINT))),
        STRICT);
    assertThat(sql).contains("CAST").contains("BIGINT");
  }

  // --- matching types need no cast ---

  @Test
  void matchingScalarTypesNeedNoCast() throws Exception {
    stubHasConnector();
    String sql = generate(selectAs(type(SqlTypeName.VARCHAR)), sink(type(SqlTypeName.VARCHAR)), STRICT);
    assertThat(sql).doesNotContain("CAST");
  }

  @Test
  void sameFamilyDifferentPrecisionNeedsNoCast() throws Exception {
    // VARCHAR(20) -> VARCHAR(10): same family, engine handles length at assignment; no injected cast.
    stubHasConnector();
    RelDataType vc20 = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, 20);
    RelDataType vc10 = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, 10);
    String sql = generate(selectAs(vc20), sink(vc10), STRICT);
    assertThat(sql).doesNotContain("CAST");
  }

  @Test
  void matchingStructTypesNeedNoCast() throws Exception {
    stubHasConnector();
    RelDataType struct = TYPE_FACTORY.builder().add("a", type(SqlTypeName.INTEGER)).build();
    String sql = generate(selectAs(struct), sink(struct), STRICT);
    assertThat(sql).doesNotContain("CAST");
  }
}
