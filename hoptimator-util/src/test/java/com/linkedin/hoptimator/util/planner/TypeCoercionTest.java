package com.linkedin.hoptimator.util.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import com.linkedin.hoptimator.util.planner.TypeCoercion.CastMode;
import com.linkedin.hoptimator.util.planner.TypeCoercion.Decision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;


class TypeCoercionTest {

  private static final RelDataTypeFactory TF = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  private static RelDataType type(SqlTypeName name) {
    return TF.createSqlType(name);
  }

  private static RelDataType nullable(SqlTypeName name) {
    return TF.createTypeWithNullability(TF.createSqlType(name), true);
  }

  private static RelDataType nullable(RelDataType base) {
    return TF.createTypeWithNullability(base, true);
  }

  private static RelDataType notNull(RelDataType base) {
    return TF.createTypeWithNullability(base, false);
  }

  private static RelDataType array(SqlTypeName element) {
    return TF.createArrayType(type(element), -1);
  }

  private static RelDataType multiset(SqlTypeName element) {
    return TF.createMultisetType(type(element), -1);
  }

  private static RelDataType map(SqlTypeName key, SqlTypeName value) {
    return TF.createMapType(type(key), type(value));
  }

  private static RelDataType row(String field, SqlTypeName type) {
    return TF.builder().add(field, type(type)).build();
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

  // --- decide: cases not covered by the parametrized matrix below ---

  @Test
  void decideReturnsNoneForSameFamilyDifferentPrecision() {
    // VARCHAR length differences are handled by the engine at assignment time; no cast, no error,
    // in either direction (including a narrowing that would otherwise truncate).
    RelDataType vc10 = TF.createSqlType(SqlTypeName.VARCHAR, 10);
    RelDataType vc20 = TF.createSqlType(SqlTypeName.VARCHAR, 20);
    assertThat(TypeCoercion.decide(vc10, vc20, CastMode.STRICT)).isEqualTo(Decision.NONE);
    assertThat(TypeCoercion.decide(vc20, vc10, CastMode.STRICT)).isEqualTo(Decision.NONE);
  }

  @Test
  void decideReturnsNoneWhenNonNullSourceIntoNullableSink() {
    assertThat(TypeCoercion.decide(type(SqlTypeName.BIGINT), nullable(SqlTypeName.BIGINT), CastMode.STRICT))
        .isEqualTo(Decision.NONE);
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

  // --- Exact scalar matrix (source, target, strict, assign, explicit) ---

  @ParameterizedTest(name = "{0} -> {1}")
  @CsvSource({
      // exact-numeric widening: lossless, allowed everywhere
      "TINYINT,  INTEGER, CAST,         CAST,         CAST",
      "SMALLINT, INTEGER, CAST,         CAST,         CAST",
      "INTEGER,  BIGINT,  CAST,         CAST,         CAST",
      "INTEGER,  DECIMAL, CAST,         CAST,         CAST",
      "INTEGER,  DOUBLE,  CAST,         CAST,         CAST",
      // exact-numeric narrowing: lossy, only explicit
      "BIGINT,   INTEGER, INCOMPATIBLE, INCOMPATIBLE, CAST",
      "DOUBLE,   INTEGER, INCOMPATIBLE, INCOMPATIBLE, CAST",
      "DECIMAL,  INTEGER, INCOMPATIBLE, INCOMPATIBLE, CAST",
      // approximate-numeric: implicitly assignable both ways in Calcite (note: DOUBLE->FLOAT is lossy)
      "FLOAT,    DOUBLE,  CAST,         CAST,         CAST",
      "DOUBLE,   FLOAT,   CAST,         CAST,         CAST",
      // character -> scalar: the assign carve-out (not strict); explicit too
      "VARCHAR,  BIGINT,  INCOMPATIBLE, CAST,         CAST",
      "VARCHAR,  DOUBLE,  INCOMPATIBLE, CAST,         CAST",
      "VARCHAR,  DECIMAL, INCOMPATIBLE, CAST,         CAST",
      "VARCHAR,  BOOLEAN, INCOMPATIBLE, CAST,         CAST",
      "VARCHAR,  DATE,    INCOMPATIBLE, CAST,         CAST",
      "VARCHAR,  TIME,    INCOMPATIBLE, CAST,         CAST",
      "VARCHAR,  TIMESTAMP, INCOMPATIBLE, CAST,       CAST",
      // scalar -> character: not part of the assign carve-out; explicit only
      "BIGINT,   VARCHAR, INCOMPATIBLE, INCOMPATIBLE, CAST",
      "BOOLEAN,  VARCHAR, INCOMPATIBLE, INCOMPATIBLE, CAST",
      "TIMESTAMP, VARCHAR, INCOMPATIBLE, INCOMPATIBLE, CAST",
      // character <-> binary: binary is not a scalar carve-out target; explicit only
      "VARCHAR,  BINARY,    INCOMPATIBLE, INCOMPATIBLE, CAST",
      "VARCHAR,  VARBINARY, INCOMPATIBLE, INCOMPATIBLE, CAST",
      "VARBINARY, VARCHAR,  INCOMPATIBLE, INCOMPATIBLE, CAST",
      // char family and binary family widening: implicitly assignable
      "CHAR,     VARCHAR,   CAST,         CAST,         CAST",
      "VARCHAR,  CHAR,      INCOMPATIBLE, INCOMPATIBLE, CAST",
      "BINARY,   VARBINARY, CAST,         CAST,         CAST",
      "VARBINARY, BINARY,   CAST,         CAST,         CAST",
      // temporal: not implicitly assignable across kinds; explicit only
      "DATE,     TIMESTAMP, INCOMPATIBLE, INCOMPATIBLE, CAST",
      "TIMESTAMP, DATE,     INCOMPATIBLE, INCOMPATIBLE, CAST",
      "TIME,     TIMESTAMP, INCOMPATIBLE, INCOMPATIBLE, CAST",
      // boolean <-> numeric: numeric->boolean is explicit-castable; boolean->numeric is never castable
      "INTEGER,  BOOLEAN,   INCOMPATIBLE, INCOMPATIBLE, CAST",
      "BOOLEAN,  INTEGER,   INCOMPATIBLE, INCOMPATIBLE, INCOMPATIBLE"
  })
  void scalarMatrix(SqlTypeName source, SqlTypeName target, Decision strict, Decision assign, Decision explicit) {
    assertThat(TypeCoercion.decide(type(source), type(target), CastMode.STRICT)).as("strict").isEqualTo(strict);
    assertThat(TypeCoercion.decide(type(source), type(target), CastMode.ASSIGN)).as("assign").isEqualTo(assign);
    assertThat(TypeCoercion.decide(type(source), type(target), CastMode.EXPLICIT)).as("explicit").isEqualTo(explicit);
  }

  // --- Exact complex-type matrix (decisions are mode-independent for complex types) ---

  static List<Arguments> complexMatrix() {
    return List.of(
        arguments(Named.of("ARRAY<INT>", array(SqlTypeName.INTEGER)),
            Named.of("ARRAY<INT>", array(SqlTypeName.INTEGER)), Decision.NONE),
        arguments(Named.of("ARRAY<INT>", array(SqlTypeName.INTEGER)),
            Named.of("ARRAY<VARCHAR>", array(SqlTypeName.VARCHAR)), Decision.INCOMPATIBLE),
        arguments(Named.of("ARRAY<INT>", array(SqlTypeName.INTEGER)),
            Named.of("INTEGER", type(SqlTypeName.INTEGER)), Decision.INCOMPATIBLE),
        arguments(Named.of("INTEGER", type(SqlTypeName.INTEGER)),
            Named.of("ARRAY<INT>", array(SqlTypeName.INTEGER)), Decision.INCOMPATIBLE),
        arguments(Named.of("MULTISET<INT>", multiset(SqlTypeName.INTEGER)),
            Named.of("MULTISET<INT>", multiset(SqlTypeName.INTEGER)), Decision.NONE),
        arguments(Named.of("ARRAY<INT>", array(SqlTypeName.INTEGER)),
            Named.of("MULTISET<INT>", multiset(SqlTypeName.INTEGER)), Decision.INCOMPATIBLE),
        arguments(Named.of("MAP<VC,INT>", map(SqlTypeName.VARCHAR, SqlTypeName.INTEGER)),
            Named.of("MAP<VC,INT>", map(SqlTypeName.VARCHAR, SqlTypeName.INTEGER)), Decision.NONE),
        arguments(Named.of("MAP<VC,INT>", map(SqlTypeName.VARCHAR, SqlTypeName.INTEGER)),
            Named.of("MAP<VC,BIGINT>", map(SqlTypeName.VARCHAR, SqlTypeName.BIGINT)), Decision.INCOMPATIBLE),
        arguments(Named.of("ROW(a INT)", row("a", SqlTypeName.INTEGER)),
            Named.of("ROW(a INT)", row("a", SqlTypeName.INTEGER)), Decision.NONE),
        arguments(Named.of("ROW(a INT)", row("a", SqlTypeName.INTEGER)),
            Named.of("ROW(a VARCHAR)", row("a", SqlTypeName.VARCHAR)), Decision.INCOMPATIBLE),
        arguments(Named.of("ROW(a INT)", row("a", SqlTypeName.INTEGER)),
            Named.of("ROW(b INT)", row("b", SqlTypeName.INTEGER)), Decision.INCOMPATIBLE),
        arguments(Named.of("ROW(a INT)", row("a", SqlTypeName.INTEGER)),
            Named.of("INTEGER", type(SqlTypeName.INTEGER)), Decision.INCOMPATIBLE));
  }

  @ParameterizedTest(name = "{0} -> {1}")
  @MethodSource("complexMatrix")
  void complexMatrix(RelDataType source, RelDataType target, Decision expected) {
    for (CastMode mode : CastMode.values()) {
      assertThat(TypeCoercion.decide(source, target, mode)).as(mode.name()).isEqualTo(expected);
    }
  }

  // --- Properties over the full cartesian product of a rich type list ---

  static List<Named<RelDataType>> richTypes() {
    List<Named<RelDataType>> types = new ArrayList<>();
    for (SqlTypeName n : new SqlTypeName[]{SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.INTEGER,
        SqlTypeName.BIGINT, SqlTypeName.DECIMAL, SqlTypeName.FLOAT, SqlTypeName.REAL, SqlTypeName.DOUBLE,
        SqlTypeName.BOOLEAN, SqlTypeName.CHAR, SqlTypeName.VARCHAR, SqlTypeName.BINARY, SqlTypeName.VARBINARY,
        SqlTypeName.DATE, SqlTypeName.TIME, SqlTypeName.TIMESTAMP}) {
      types.add(Named.of(n.getName(), type(n)));
    }
    types.add(Named.of("ARRAY<INT>", array(SqlTypeName.INTEGER)));
    types.add(Named.of("ARRAY<VARCHAR>", array(SqlTypeName.VARCHAR)));
    types.add(Named.of("MULTISET<INT>", multiset(SqlTypeName.INTEGER)));
    types.add(Named.of("MAP<VC,INT>", map(SqlTypeName.VARCHAR, SqlTypeName.INTEGER)));
    types.add(Named.of("ROW(a INT)", row("a", SqlTypeName.INTEGER)));
    return types;
  }

  static List<Arguments> allPairs() {
    List<Named<RelDataType>> types = richTypes();
    List<Arguments> pairs = new ArrayList<>();
    for (Named<RelDataType> s : types) {
      for (Named<RelDataType> target : types) {
        pairs.add(arguments(s, target));
      }
    }
    return pairs;
  }

  private static boolean accepts(RelDataType s, RelDataType target, CastMode mode) {
    return TypeCoercion.decide(s, target, mode) != Decision.INCOMPATIBLE;
  }

  private static boolean isComplex(RelDataType type) {
    if (type.isStruct()) {
      return true;
    }
    SqlTypeName n = type.getSqlTypeName();
    return n == SqlTypeName.ARRAY || n == SqlTypeName.MULTISET || n == SqlTypeName.MAP;
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("richTypes")
  void reflexivityIsNone(RelDataType type) {
    for (CastMode mode : CastMode.values()) {
      assertThat(TypeCoercion.decide(type, type, mode)).as(mode.name()).isEqualTo(Decision.NONE);
    }
  }

  @ParameterizedTest(name = "{0} -> {1}")
  @MethodSource("allPairs")
  void monotonicAcrossModes(RelDataType source, RelDataType target) {
    // Anything accepted by a stricter mode must be accepted by a more permissive one:
    // strict => assign => explicit.
    if (accepts(source, target, CastMode.STRICT)) {
      assertThat(accepts(source, target, CastMode.ASSIGN)).as("strict=>assign").isTrue();
    }
    if (accepts(source, target, CastMode.ASSIGN)) {
      assertThat(accepts(source, target, CastMode.EXPLICIT)).as("assign=>explicit").isTrue();
    }
  }

  @ParameterizedTest(name = "{0} -> {1}")
  @MethodSource("allPairs")
  void complexVsScalarAlwaysIncompatible(RelDataType source, RelDataType target) {
    // A complex type paired with a scalar (or a different-kind complex) can never be cast.
    if (isComplex(source) != isComplex(target)) {
      for (CastMode mode : CastMode.values()) {
        assertThat(TypeCoercion.decide(source, target, mode)).as(mode.name()).isEqualTo(Decision.INCOMPATIBLE);
      }
    }
  }

  @ParameterizedTest(name = "{0} -> {1}")
  @MethodSource("allPairs")
  void nullableSourceIntoNotNullSinkNeverWidensAcceptance(RelDataType source, RelDataType target) {
    // Scoped to scalars: making a struct nullable also flips its inner field nullability, which is a
    // structural change governed by the complex-type rule rather than this scalar nullability rule.
    if (isComplex(source) || isComplex(target)) {
      return;
    }
    // Making the source nullable and the sink NOT NULL can only remove acceptance, never add it,
    // and when the types differ it must turn any cast into an error.
    RelDataType nullableSource = nullable(source);
    RelDataType notNullTarget = notNull(target);
    for (CastMode mode : CastMode.values()) {
      Decision base = TypeCoercion.decide(notNull(source), notNull(target), mode);
      Decision withNulls = TypeCoercion.decide(nullableSource, notNullTarget, mode);
      if (base == Decision.CAST) {
        // A cast can't add a null guard, so a nullable source into a NOT NULL sink is rejected.
        assertThat(withNulls).as(mode.name()).isEqualTo(Decision.INCOMPATIBLE);
      } else if (base == Decision.NONE) {
        // Same type: still a plain assignment regardless of nullability.
        assertThat(withNulls).as(mode.name()).isEqualTo(Decision.NONE);
      }
    }
  }
}
