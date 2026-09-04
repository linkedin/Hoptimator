package com.linkedin.hoptimator.util.planner;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;


/**
 * Decides whether a query output column can be assigned to a sink column, and whether Hoptimator
 * should inject an assignment {@code CAST} to make the two schemas line up.
 *
 * <p>Hoptimator emits an {@code INSERT INTO sink SELECT ...} script to the execution engine, which
 * validates the query row type against the sink row type column-by-column and rejects incompatible
 * assignments. Left to the engine a mismatch typically fails late — at job submission — with an
 * opaque error, and some conversions the engine will not perform implicitly at all (e.g. a raw
 * primitive key exposed as {@code STRING} projected onto a typed {@code BIGINT} key column). This
 * class lets Hoptimator either inject an assignment cast up front so the projection lines up with
 * the sink, or fail early with a clear error naming the column and both types.
 *
 * <p>The permissiveness is controlled by a {@link CastMode} sourced from the {@code castMode} hint.
 * Nullability is never a plan-time concern: a nullable source may be assigned to a {@code NOT NULL}
 * sink column (with or without a cast), and the engine enforces the {@code NOT NULL} constraint at
 * runtime, exactly as for a native {@code INSERT} — this is what lets a nullable Kafka {@code KEY}
 * project onto a {@code NOT NULL} key column. One rule holds at <em>every</em> mode and cannot be
 * relaxed:
 * <ul>
 *   <li>Complex types ({@code ROW}/{@code ARRAY}/{@code MAP}/{@code MULTISET}) must match
 *       structurally — same shape and same base scalar types at every level — because a cast cannot
 *       reshape a record or cast an individual nested field. Nullability is ignored recursively
 *       (the engine enforces any nested {@code NOT NULL} at runtime)</li>
 * </ul>
 */
public final class TypeCoercion {

  /** Hint key used to select the {@link CastMode}. */
  public static final String CAST_MODE_HINT = "castMode";

  private TypeCoercion() {
  }

  /**
   * How aggressively Hoptimator inserts assignment casts when a query column type differs from its
   * sink column type. Each level strictly widens the previous one.
   */
  public enum CastMode {
    /**
     * Inject a cast only for cross-family assignment-compatible conversions — for example
     * {@code INTEGER -> BIGINT} (numeric to numeric), i.e. what Calcite treats as a cast without
     * string coercion. Conversions within the same type family (e.g. a {@code VARCHAR} length change
     * or {@code DECIMAL} precision change) need no cast and are assigned as-is. Cross-family string
     * coercions like {@code STRING -> BIGINT} are <em>not</em> allowed here, so the raw-key case
     * fails early with a clear message. This is the default and the fail-closed fallback for a
     * missing or unrecognized hint.
     */
    STRICT,

    /**
     * {@link #STRICT} plus a narrow character-source to scalar-target carve-out. Covers the raw
     * primitive-key placeholder, e.g. {@code STRING -> BIGINT}, without opening up arbitrary
     * cross-family conversions.
     */
    ASSIGN,

    /**
     * Allow any explicitly-castable scalar pair — the deliberate "risky" opt-in for passthroughs
     * that intentionally coerce across type families or perform a lossy numeric narrowing (e.g.
     * {@code BIGINT -> INTEGER}), which strict/assign reject. Structural and nullability rules still
     * hold.
     */
    EXPLICIT;

    /**
     * Resolves the mode from the pipeline hints, defaulting to {@link #STRICT}. Unknown values also
     * fall back to {@link #STRICT} so a typo or a dropped hint can never silently enable a more
     * permissive policy.
     */
    public static CastMode fromHints(Map<String, String> hints) {
      String raw = hints == null ? null : hints.get(CAST_MODE_HINT);
      if (raw == null) {
        return STRICT;
      }
      switch (raw.trim().toLowerCase(Locale.ROOT)) {
        case "assign":
          return ASSIGN;
        case "explicit":
          return EXPLICIT;
        case "strict":
        case "":
        default:
          return STRICT;
      }
    }
  }

  /** The outcome of comparing a query column type against a sink column type. */
  public enum Decision {
    /** Types already line up; no cast is needed. */
    NONE,
    /** Types differ but the conversion is allowed; Hoptimator should inject a cast. */
    CAST,
    /** Types are not safely convertible under the given mode; the caller should raise an error. */
    INCOMPATIBLE
  }

  /**
   * Classifies assigning a value of type {@code source} into a sink column of type {@code target}.
   *
   * @param source the query output column type
   * @param target the sink column type
   * @param mode   the active cast permissiveness
   * @return whether the columns match, need a cast, or are incompatible
   */
  public static Decision decide(RelDataType source, RelDataType target, CastMode mode) {
    // Complex types must match structurally (same shape + base types, nullability ignored
    // recursively); never synthesize a row/collection cast.
    if (source.isStruct() || target.isStruct() || isCollection(source) || isCollection(target)) {
      return structurallyEqualSansNullability(source, target) ? Decision.NONE : Decision.INCOMPATIBLE;
    }

    // Types already line up (same family, ignoring precision/scale/charset/nullability): a plain
    // assignment, no cast needed. The engine handles length/precision/nullability at assignment time
    // exactly as for a native INSERT, so we only intervene for genuine cross-family mismatches.
    if (source.getSqlTypeName() == target.getSqlTypeName()) {
      return Decision.NONE;
    }

    boolean allowed;
    switch (mode) {
      case EXPLICIT:
        allowed = SqlTypeUtil.canCastFrom(target, source, true);
        break;
      case ASSIGN:
        allowed = SqlTypeUtil.canCastFrom(target, source, false)
            || (SqlTypeUtil.isCharacter(source) && isScalar(target));
        break;
      case STRICT:
      default:
        allowed = SqlTypeUtil.canCastFrom(target, source, false);
        break;
    }
    return allowed ? Decision.CAST : Decision.INCOMPATIBLE;
  }

  /** Whether {@code source} is implicitly assignable to {@code target} (the {@link CastMode#STRICT} set). */
  public static boolean isImplicitlyAssignable(RelDataType source, RelDataType target) {
    if (source.isStruct() || target.isStruct() || isCollection(source) || isCollection(target)) {
      return structurallyEqualSansNullability(source, target);
    }
    return source.getSqlTypeName() == target.getSqlTypeName()
        || SqlTypeUtil.canCastFrom(target, source, false);
  }

  /**
   * Recursively compares two types for structural equality, ignoring nullability at every level.
   *
   * <p>Two types match when they have the same shape and the same base scalar {@link SqlTypeName}
   * throughout. Nested field nullability (and precision/scale/charset) is ignored — the engine
   * enforces any nested {@code NOT NULL} at runtime for a plain assignment, exactly as for a
   * top-level scalar.
   *
   * <p>Nested scalars must match exactly by {@link SqlTypeName} (a nested {@code INTEGER -> BIGINT}
   * is a mismatch, not a cast) because Hoptimator does not synthesize casts for individual fields
   * inside a struct/collection projection.
   *
   * <p>TODO: Nested casting is not supported. This is deliberately conservative — an execution
   * engine may accept a nested widening (e.g. {@code ROW<x INTEGER>} into {@code ROW<x BIGINT>}) on
   * its own, but because we cannot rewrite an individual nested field we reject the whole column
   * rather than emit a row/collection cast. If nested coercion is ever needed, rebuild the
   * row/collection in the projection with the per-field casts applied.
   *
   * <p>TODO: Consider upleveling a version of this to replace the {@link SqlTypeUtil#equalSansNullability}
   * family (which only strips the outermost nullability and does not recurse into nested structs); this walks
   * the whole type tree.
   */
  static boolean structurallyEqualSansNullability(RelDataType a, RelDataType b) {
    if (a == b) {
      return true;
    }

    if (a.isStruct() || b.isStruct()) {
      if (!a.isStruct() || !b.isStruct()) {
        return false;
      }
      List<RelDataTypeField> fieldsA = a.getFieldList();
      List<RelDataTypeField> fieldsB = b.getFieldList();
      if (fieldsA.size() != fieldsB.size()) {
        return false;
      }
      for (int i = 0; i < fieldsA.size(); i++) {
        if (!fieldsA.get(i).getName().equals(fieldsB.get(i).getName())) {
          return false;
        }
        if (!structurallyEqualSansNullability(fieldsA.get(i).getType(), fieldsB.get(i).getType())) {
          return false;
        }
      }
      return true;
    }

    if (isCollection(a) || isCollection(b)) {
      // ARRAY vs MULTISET vs MAP (or collection vs scalar) never match.
      if (a.getSqlTypeName() != b.getSqlTypeName()) {
        return false;
      }
      if (a.getSqlTypeName() == SqlTypeName.MAP) {
        return structurallyEqualSansNullability(a.getKeyType(), b.getKeyType())
            && structurallyEqualSansNullability(a.getValueType(), b.getValueType());
      }
      return structurallyEqualSansNullability(a.getComponentType(), b.getComponentType());
    }

    // Scalars: same family; precision/scale/charset/nullability are ignored.
    return a.getSqlTypeName() == b.getSqlTypeName();
  }

  private static boolean isCollection(RelDataType type) {
    SqlTypeName name = type.getSqlTypeName();
    return name == SqlTypeName.ARRAY || name == SqlTypeName.MULTISET || name == SqlTypeName.MAP;
  }

  private static boolean isScalar(RelDataType type) {
    return SqlTypeUtil.isNumeric(type) || SqlTypeUtil.isBoolean(type) || SqlTypeUtil.isDatetime(type);
  }
}
