package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.NameMap;
import org.apache.calcite.util.NameMultimap;
import org.apache.calcite.util.NameSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.NavigableMap;
import java.util.Set;


/**
 * A {@link CalciteSchema} implementation that makes implicit sub-schemas stable
 * for the lifetime of a connection.
 *
 * <p>This is functionally identical to Calcite's {@code SimpleCalciteSchema}
 * except for the {@link #createSubSchema} override. {@code SimpleCalciteSchema}
 * is package-private so cannot be subclassed directly.
 *
 * <h2>Why this exists</h2>
 *
 * <p>{@code SimpleCalciteSchema.createSubSchema()} creates a fresh {@link CalciteSchema}
 * wrapper on every implicit {@code subSchemas().get(name)} call. Any tables or views
 * added to the returned wrapper via {@code add()} are invisible on the next access
 * because a new wrapper is created.
 *
 * <p>By overriding {@code createSubSchema()} to pin each discovered schema into
 * {@link #subSchemaMap}, the first {@code subSchemas().get("testdb")} call creates
 * and registers the wrapper; subsequent calls find it via {@code NameMapLookup}
 * (which checks {@code subSchemaMap} before the implicit lookup) and return the
 * same object. This makes implicit sub-schemas (e.g. MySQL databases under a
 * catalog schema) behave identically to explicit ones for the purpose of
 * registering views and temporary tables.
 *
 * <p>This is preferable to wrapping the lookup with {@code LoadingCacheLookup}
 * because {@code LoadingCacheLookup} evicts entries after one minute, which can
 * cause a fresh wrapper to be returned after in-session views were registered
 * to the previous one. Schemas should be stable for the lifetime of a connection;
 * explicit mutations (CREATE / DROP) already call {@code add()} or
 * {@link #removeTable} to evolve schema state as needed.
 *
 * <h2>Concurrency</h2>
 *
 * <p>All writes to {@code subSchemaMap} in this class — both the explicit {@link #add}
 * and the lazy {@link #createSubSchema} — are synchronized on {@code subSchemaMap}.
 * This prevents racing writers from corrupting the backing {@link java.util.TreeMap}
 * and gives {@code createSubSchema} double-checked-lock semantics so concurrent lookups
 * of the same name return the same instance rather than creating duplicates.
 *
 * <p>Reads through the inherited {@code subSchemas()} {@link org.apache.calcite.schema.lookup.Lookup}
 * API (used by JDBC metadata calls and query planning) go through
 * {@code NameMapLookup}, which reads the underlying {@code TreeMap} without
 * synchronization. A read that precisely coincides with a first-access write from
 * {@code createSubSchema} could observe a transiently-inconsistent tree. In practice
 * this is rare: explicit additions happen at connection init on a single thread, and
 * each implicit name triggers only one write (its first lookup). Fully serializing
 * reads would require replacing Calcite's {@code NameMap} backing, which is a larger
 * refactor. This class matches stock {@code SimpleCalciteSchema}'s thread-safety
 * posture — concurrent {@code add()} and {@code getSubSchemaMap()} have always been
 * racey in Calcite — and adds synchronization only where a new hazard was introduced.
 */
public class HoptimatorCalciteSchema extends CalciteSchema {

  /**
   * Creates a root {@link HoptimatorCalciteSchema}, optionally including the Calcite
   * built-in {@code metadata} schema ({@code metadata.COLUMNS}, {@code metadata.TABLES},
   * etc.).
   *
   * <p>When {@code addMetadataSchema} is true, borrows {@code MetadataSchema}
   * (package-private) via the public {@link CalciteSchema#createRootSchema(boolean, boolean,
   * String)} factory — the same technique used to borrow {@code CalciteConnectionImpl
   * .RootSchema} for the backing — so no internal Calcite APIs are accessed directly.
   *
   * @param rootSchema optional rootSchema override e.g. used by catalog drivers providing their own lazy backed schema
   * @param addMetadataSchema boolean value indicating whether to add metadata fields
   */
  public static HoptimatorCalciteSchema createRootSchema(@Nullable Schema rootSchema, boolean addMetadataSchema) {
    HoptimatorCalciteSchema root = new HoptimatorCalciteSchema(null,
        rootSchema == null ? new RootSchema() : rootSchema, "");
    if (addMetadataSchema) {
      // This is a work around since MetadataSchema is package private, ideally we can extend this internally
      // to expose whatever metadata tables we want
      CalciteSchema withMetadata = CalciteSchema.createRootSchema(true, false, "");
      CalciteSchema metadata = withMetadata.getSubSchema("metadata", true);
      if (metadata != null) {
        root.add("metadata", metadata.schema);
      }
    }
    return root;
  }

  /**
   * Backing schema for the root {@link HoptimatorCalciteSchema}.
   *
   * <p>Replicates the behavior of the package-private
   * {@code CalciteConnectionImpl.RootSchema}: overrides {@code getExpression()}
   * to handle a {@code null} parent by generating an expression that returns
   * the runtime root schema via {@code DataContext.ROOT.getRootSchema()}.
   */
   private static class RootSchema extends AbstractSchema {
     @Override
     public Expression getExpression(SchemaPlus parentSchema, String name) {
       return Expressions.call(DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
     }
   }

  public HoptimatorCalciteSchema(@Nullable CalciteSchema parent, Schema schema, String name) {
    this(parent, schema, name, null, null, null, null,
        null, null, null, null);
  }

  private HoptimatorCalciteSchema(
      @Nullable CalciteSchema parent,
      Schema schema,
      String name,
      @Nullable NameMap<CalciteSchema> subSchemaMap,
      @Nullable NameMap<TableEntry> tableMap,
      @Nullable NameMap<LatticeEntry> latticeMap,
      @Nullable NameMap<TypeEntry> typeMap,
      @Nullable NameMultimap<FunctionEntry> functionMap,
      @Nullable NameSet functionNames,
      @Nullable NameMap<FunctionEntry> nullaryFunctionMap,
      @Nullable List<? extends List<String>> path) {
    super(parent, schema, name, subSchemaMap, tableMap, latticeMap, typeMap,
        functionMap, functionNames, nullaryFunctionMap, path);
  }

  @Override
  public void setCache(boolean cache) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CalciteSchema add(String name, Schema schema) {
    final CalciteSchema calciteSchema = new HoptimatorCalciteSchema(this, schema, name);
    synchronized (subSchemaMap) {
      subSchemaMap.put(name, calciteSchema);
    }
    return calciteSchema;
  }

  /**
   * Creates an implicit sub-schema and pins it into {@link #subSchemaMap} so subsequent
   * lookups resolve via {@code NameMapLookup} without re-entering this method. If an
   * entry already exists (from a prior {@link #add} or a racing lazy lookup), returns
   * it so any tables registered to it remain visible.
   */
  @Override
  protected CalciteSchema createSubSchema(Schema schema, String name) {
    synchronized (subSchemaMap) {
      NavigableMap<String, CalciteSchema> existing = subSchemaMap.range(name, false);
      if (!existing.isEmpty()) {
        return existing.firstEntry().getValue();
      }
      HoptimatorCalciteSchema child = new HoptimatorCalciteSchema(this, schema, name);
      subSchemaMap.put(name, child);
      return child;
    }
  }

  @Override
  protected @Nullable TypeEntry getImplicitType(String name, boolean caseSensitive) {
    // Check implicit types.
    final String name2 = caseSensitive ? name : caseInsensitiveLookup(schema.getTypeNames(), name);
    if (name2 == null) {
      return null;
    }
    final RelProtoDataType type = schema.getType(name2);
    if (type == null) {
      return null;
    }
    return typeEntry(name2, type);
  }

  @Override
  protected void addImplicitFunctionsToBuilder(
      ImmutableList.Builder<Function> builder, String name, boolean caseSensitive) {
    Collection<Function> functions = schema.getFunctions(name);
    if (functions != null) {
      builder.addAll(functions);
    }
  }

  @Override
  protected void addImplicitFuncNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {
    builder.addAll(schema.getFunctionNames());
  }

  @Override
  protected void addImplicitTypeNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {
    builder.addAll(schema.getTypeNames());
  }

  @Override
  protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder(
      ImmutableSortedMap.Builder<String, Table> builder) {
    ImmutableSortedMap<String, Table> explicitTables = builder.build();

    for (String s : schema.getFunctionNames()) {
      // explicit table wins.
      if (explicitTables.containsKey(s)) {
        continue;
      }
      for (Function function : schema.getFunctions(s)) {
        if (function instanceof TableMacro && function.getParameters().isEmpty()) {
          final Table table = ((TableMacro) function).apply(ImmutableList.of());
          builder.put(s, table);
        }
      }
    }
  }

  @Override
  protected @Nullable TableEntry getImplicitTableBasedOnNullaryFunction(
      String tableName, boolean caseSensitive) {
    Collection<Function> functions = schema.getFunctions(tableName);
    if (functions != null) {
      for (Function function : functions) {
        if (function instanceof TableMacro && function.getParameters().isEmpty()) {
          final Table table = ((TableMacro) function).apply(ImmutableList.of());
          return tableEntry(tableName, table);
        }
      }
    }
    return null;
  }

  @Override
  protected CalciteSchema snapshot(@Nullable CalciteSchema parent, SchemaVersion version) {
    HoptimatorCalciteSchema snapshot = new HoptimatorCalciteSchema(
        parent, schema.snapshot(version), name, null,
        tableMap, latticeMap, typeMap,
        functionMap, functionNames, nullaryFunctionMap, getPath());
    synchronized (subSchemaMap) {
      for (CalciteSchema subSchema : subSchemaMap.map().values()) {
        CalciteSchema subSchemaSnapshot = subSchema instanceof HoptimatorCalciteSchema
            ? ((HoptimatorCalciteSchema) subSchema).snapshot(snapshot, version)
            : subSchema;
        snapshot.subSchemaMap.put(subSchema.name, subSchemaSnapshot);
      }
    }
    return snapshot;
  }

  @Override
  protected boolean isCacheEnabled() {
    return false;
  }

  private static @Nullable String caseInsensitiveLookup(Set<String> candidates, String name) {
    // Exact string lookup
    if (candidates.contains(name)) {
      return name;
    }
    // Upper case string lookup
    final String upperCaseName = name.toUpperCase(Locale.ROOT);
    if (candidates.contains(upperCaseName)) {
      return upperCaseName;
    }
    // Lower case string lookup
    final String lowerCaseName = name.toLowerCase(Locale.ROOT);
    if (candidates.contains(lowerCaseName)) {
      return lowerCaseName;
    }
    // Fall through: Set iteration
    for (String candidate : candidates) {
      if (candidate.equalsIgnoreCase(name)) {
        return candidate;
      }
    }
    return null;
  }
}
