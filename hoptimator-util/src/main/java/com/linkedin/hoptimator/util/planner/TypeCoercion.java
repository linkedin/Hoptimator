package com.linkedin.hoptimator.util.planner;

import java.util.Locale;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;


/**
 * Decides whether a query output column can be assigned to a sink column, and whether Hoptimator
 * should inject an assignment {@code CAST} to make the two schemas line up.
 *
 * <p>Hoptimator emits a Flink {@code INSERT INTO sink SELECT ...} script. Flink validates the
 * query row type against the sink row type column-by-column and rejects mismatches. Because
 * Hoptimator's generated {@code SELECT} loses the implicit assignment-cast context a native
 * {@code INSERT} would carry, a source column whose type differs from the sink column (e.g. a raw
 * primitive Kafka key exposed as {@code STRING} projected onto a typed {@code BIGINT} key column)
 * fails only once the job reaches Flink. This class lets Hoptimator either insert the cast up front
 * or fail early with a clear error, instead of deferring to Flink.
 *
 * <p>The permissiveness is controlled by a {@link CastMode} sourced from the {@code castMode} hint.
 * Two rules hold at <em>every</em> mode and cannot be relaxed:
 * <ul>
 *   <li>When a cast is needed (the source and sink types differ), a nullable source into a
 *       {@code NOT NULL} sink column is incompatible — a cast cannot add a null guard. When the
 *       types already match, assignment is allowed regardless of nullability and the engine enforces
 *       any {@code NOT NULL} constraint at runtime, exactly as for a native {@code INSERT}.</li>
 *   <li>Structural mismatches between complex types ({@code ROW}/{@code ARRAY}/{@code MAP}/
 *       {@code MULTISET}) are incompatible — a cast cannot reshape a record.</li>
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
    // Complex types must match structurally; never synthesize a row/collection cast.
    if (source.isStruct() || target.isStruct() || isCollection(source) || isCollection(target)) {
      return SqlTypeUtil.equalSansNullability(target, source) ? Decision.NONE : Decision.INCOMPATIBLE;
    }

    // Types already line up (same family, ignoring precision/scale/charset/nullability): a plain
    // assignment, no cast needed. The engine handles length/precision/nullability at assignment time
    // exactly as for a native INSERT, so we only intervene for genuine cross-family mismatches.
    if (source.getSqlTypeName() == target.getSqlTypeName()) {
      return Decision.NONE;
    }

    // Types differ, so a cast is required. A cast cannot turn a possibly-null value into a
    // guaranteed non-null one, so a nullable source feeding a NOT NULL sink column is rejected.
    if (source.isNullable() && !target.isNullable()) {
      return Decision.INCOMPATIBLE;
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
    return SqlTypeUtil.equalSansNullability(target, source)
        || (!source.isStruct() && !target.isStruct()
            && !isCollection(source) && !isCollection(target)
            && SqlTypeUtil.canCastFrom(target, source, false));
  }

  private static boolean isCollection(RelDataType type) {
    SqlTypeName name = type.getSqlTypeName();
    return name == SqlTypeName.ARRAY || name == SqlTypeName.MULTISET || name == SqlTypeName.MAP;
  }

  private static boolean isScalar(RelDataType type) {
    return SqlTypeUtil.isNumeric(type) || SqlTypeUtil.isBoolean(type) || SqlTypeUtil.isDatetime(type);
  }
}
