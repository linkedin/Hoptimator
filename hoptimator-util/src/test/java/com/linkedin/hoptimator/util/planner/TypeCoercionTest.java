package com.linkedin.hoptimator.util.planner;

import java.util.Collections;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.util.planner.TypeCoercion.CastMode;
import com.linkedin.hoptimator.util.planner.TypeCoercion.Decision;

import static org.assertj.core.api.Assertions.assertThat;


class TypeCoercionTest {

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  private RelDataType type(SqlTypeName name) {
    return typeFactory.createSqlType(name);
  }

  private RelDataType nullable(SqlTypeName name) {
    return typeFactory.createTypeWithNullability(typeFactory.createSqlType(name), true);
  }

  // --- CastMode.fromHints ---

  @Test
  void fromHintsDefaultsToStrictWhenAbsent() {
    assertThat(CastMode.fromHints(Collections.emptyMap())).isEqualTo(CastMode.STRICT);
  }

  @Test
  void fromHintsDefaultsToStrictWhenNull() {
    assertThat(CastMode.fromHints(null)).isEqualTo(CastMode.STRICT);
  }

  @Test
  void fromHintsParsesEachLevelCaseInsensitively() {
    assertThat(CastMode.fromHints(Map.of("castMode", "assign"))).isEqualTo(CastMode.ASSIGN);
    assertThat(CastMode.fromHints(Map.of("castMode", "EXPLICIT"))).isEqualTo(CastMode.EXPLICIT);
    assertThat(CastMode.fromHints(Map.of("castMode", "Strict"))).isEqualTo(CastMode.STRICT);
  }

  @Test
  void fromHintsFailsClosedOnUnknownValue() {
    assertThat(CastMode.fromHints(Map.of("castMode", "yolo"))).isEqualTo(CastMode.STRICT);
    assertThat(CastMode.fromHints(Map.of("castMode", ""))).isEqualTo(CastMode.STRICT);
  }

  // --- decide: matching types ---

  @Test
  void decideReturnsNoneForEqualTypes() {
    assertThat(TypeCoercion.decide(type(SqlTypeName.BIGINT), type(SqlTypeName.BIGINT), CastMode.STRICT))
        .isEqualTo(Decision.NONE);
  }

  @Test
  void decideReturnsNoneForSameFamilyDifferentPrecision() {
    // VARCHAR length differences are handled by the engine at assignment time; no cast, no error,
    // in either direction (including a narrowing that would otherwise truncate).
    RelDataType vc10 = typeFactory.createSqlType(SqlTypeName.VARCHAR, 10);
    RelDataType vc20 = typeFactory.createSqlType(SqlTypeName.VARCHAR, 20);
    assertThat(TypeCoercion.decide(vc10, vc20, CastMode.STRICT)).isEqualTo(Decision.NONE);
    assertThat(TypeCoercion.decide(vc20, vc10, CastMode.STRICT)).isEqualTo(Decision.NONE);
  }

  @Test
  void decideReturnsNoneWhenNonNullSourceIntoNullableSink() {
    assertThat(TypeCoercion.decide(type(SqlTypeName.BIGINT), nullable(SqlTypeName.BIGINT), CastMode.STRICT))
        .isEqualTo(Decision.NONE);
  }

  // --- decide: the raw-key case, STRING -> BIGINT ---

  @Test
  void decideRejectsStringToBigintUnderStrict() {
    assertThat(TypeCoercion.decide(type(SqlTypeName.VARCHAR), type(SqlTypeName.BIGINT), CastMode.STRICT))
        .isEqualTo(Decision.INCOMPATIBLE);
  }

  @Test
  void decideAllowsStringToBigintUnderAssign() {
    assertThat(TypeCoercion.decide(type(SqlTypeName.VARCHAR), type(SqlTypeName.BIGINT), CastMode.ASSIGN))
        .isEqualTo(Decision.CAST);
  }

  @Test
  void decideAllowsStringToBigintUnderExplicit() {
    assertThat(TypeCoercion.decide(type(SqlTypeName.VARCHAR), type(SqlTypeName.BIGINT), CastMode.EXPLICIT))
        .isEqualTo(Decision.CAST);
  }

  // --- decide: same-family conversion is allowed even under strict ---

  @Test
  void decideAllowsIntegerToBigintUnderStrict() {
    assertThat(TypeCoercion.decide(type(SqlTypeName.INTEGER), type(SqlTypeName.BIGINT), CastMode.STRICT))
        .isEqualTo(Decision.CAST);
  }

  @Test
  void decideRejectsLossyNumericNarrowingUnderStrictAndAssign() {
    // Narrowing numeric conversions are lossy, so implicit-assignment (strict/assign) rejects them.
    assertThat(TypeCoercion.decide(type(SqlTypeName.BIGINT), type(SqlTypeName.INTEGER), CastMode.STRICT))
        .isEqualTo(Decision.INCOMPATIBLE);
    assertThat(TypeCoercion.decide(type(SqlTypeName.BIGINT), type(SqlTypeName.INTEGER), CastMode.ASSIGN))
        .isEqualTo(Decision.INCOMPATIBLE);
    assertThat(TypeCoercion.decide(type(SqlTypeName.DOUBLE), type(SqlTypeName.INTEGER), CastMode.STRICT))
        .isEqualTo(Decision.INCOMPATIBLE);
  }

  @Test
  void decideAllowsLossyNumericNarrowingOnlyUnderExplicit() {
    assertThat(TypeCoercion.decide(type(SqlTypeName.BIGINT), type(SqlTypeName.INTEGER), CastMode.EXPLICIT))
        .isEqualTo(Decision.CAST);
  }

  // --- decide: nullability is never fixable by a cast, at any mode ---

  @Test
  void decideRejectsNullableSourceIntoNotNullSinkWhenTypesDiffer() {
    assertThat(TypeCoercion.decide(nullable(SqlTypeName.VARCHAR), type(SqlTypeName.BIGINT), CastMode.EXPLICIT))
        .isEqualTo(Decision.INCOMPATIBLE);
  }

  @Test
  void decideAllowsNullableSourceIntoNotNullSinkWhenTypesMatch() {
    // Types already line up, so this is a plain assignment; nullability is enforced at runtime.
    assertThat(TypeCoercion.decide(nullable(SqlTypeName.BIGINT), type(SqlTypeName.BIGINT), CastMode.STRICT))
        .isEqualTo(Decision.NONE);
  }

  // --- decide: complex/structural mismatches are never cast ---

  @Test
  void decideRejectsMismatchedStructsEvenUnderExplicit() {
    RelDataType structA = typeFactory.builder().add("a", type(SqlTypeName.INTEGER)).build();
    RelDataType structB = typeFactory.builder().add("a", type(SqlTypeName.VARCHAR)).build();
    assertThat(TypeCoercion.decide(structA, structB, CastMode.EXPLICIT)).isEqualTo(Decision.INCOMPATIBLE);
  }

  @Test
  void decideReturnsNoneForEqualStructs() {
    RelDataType structA = typeFactory.builder().add("a", type(SqlTypeName.INTEGER)).build();
    RelDataType structB = typeFactory.builder().add("a", type(SqlTypeName.INTEGER)).build();
    assertThat(TypeCoercion.decide(structA, structB, CastMode.STRICT)).isEqualTo(Decision.NONE);
  }

  @Test
  void decideRejectsScalarIntoArrayEvenUnderExplicit() {
    RelDataType array = typeFactory.createArrayType(type(SqlTypeName.INTEGER), -1);
    assertThat(TypeCoercion.decide(type(SqlTypeName.INTEGER), array, CastMode.EXPLICIT))
        .isEqualTo(Decision.INCOMPATIBLE);
  }

  // --- isImplicitlyAssignable ---

  @Test
  void isImplicitlyAssignableTrueForWidening() {
    assertThat(TypeCoercion.isImplicitlyAssignable(type(SqlTypeName.INTEGER), type(SqlTypeName.BIGINT))).isTrue();
  }

  @Test
  void isImplicitlyAssignableFalseForStringToNumeric() {
    assertThat(TypeCoercion.isImplicitlyAssignable(type(SqlTypeName.VARCHAR), type(SqlTypeName.BIGINT))).isFalse();
  }
}
