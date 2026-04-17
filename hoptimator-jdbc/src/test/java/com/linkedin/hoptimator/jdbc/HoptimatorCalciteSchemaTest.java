package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.lookup.IgnoreCaseLookup;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Tests for {@link HoptimatorCalciteSchema}.
 *
 * <p>Key invariant: {@code subSchemas().get(name)} must return the <em>same</em>
 * {@link CalciteSchema} object on repeated calls for the same name, so that
 * tables or views added to the schema via {@code add()} remain visible on the
 * next access. This is the core property that {@code SimpleCalciteSchema} lacks.
 */
class HoptimatorCalciteSchemaTest {

  // ---- Implicit sub-schema stability ----

  @Test
  void implicitSubSchemaSameObjectOnRepeatedGet() {
    AbstractSchema backingWithImplicit = new AbstractSchema() {
      @Override
      public Lookup<Schema> subSchemas() {
        return new IgnoreCaseLookup<>() {
          @Override public Schema get(String name) {
            return "child".equalsIgnoreCase(name) ? new AbstractSchema() : null;
          }

          @Override public Set<String> getNames(LikePattern pattern) {
            return Collections.singleton("child");
          }
        };
      }
    };

    HoptimatorCalciteSchema root = new HoptimatorCalciteSchema(null, backingWithImplicit, "");

    CalciteSchema first = root.getSubSchema("child", true);
    CalciteSchema second = root.getSubSchema("child", true);

    assertNotNull(first);
    assertNotNull(second);
    assertSame(first, second, "Implicit sub-schema must be the same object on repeated get()");
  }

  @Test
  void tableAddedToImplicitSubSchemaVisibleOnNextAccess() {
    AbstractSchema backing = new AbstractSchema() {
      @Override
      public Lookup<Schema> subSchemas() {
        return new IgnoreCaseLookup<>() {
          @Override public Schema get(String name) {
            return "ns".equalsIgnoreCase(name) ? new AbstractSchema() : null;
          }

          @Override public Set<String> getNames(LikePattern pattern) {
            return Collections.singleton("ns");
          }
        };
      }
    };

    HoptimatorCalciteSchema root = new HoptimatorCalciteSchema(null, backing, "");

    SchemaPlus nsSchema = root.plus().subSchemas().get("ns");
    assertNotNull(nsSchema);
    nsSchema.add("MY_VIEW", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory f) {
        return f.builder().build();
      }
    });

    SchemaPlus nsAgain = root.plus().subSchemas().get("ns");
    assertNotNull(nsAgain);
    Table found = nsAgain.tables().get("MY_VIEW");
    assertNotNull(found, "Table added to implicit sub-schema must survive to next access");
  }

  // ---- Explicit sub-schema propagation ----

  @Test
  void explicitSubSchemaAddedViaAddIsFoundByGet() {
    HoptimatorCalciteSchema root = HoptimatorCalciteSchema.createRootSchema(null, false);
    AbstractSchema child = new AbstractSchema();
    root.add("CHILD", child);

    CalciteSchema found = root.getSubSchema("CHILD", true);
    assertNotNull(found);
    assertSame(child, found.schema, "Explicit sub-schema must be retrievable by name");
  }

  @Test
  void addedSubSchemasAreHoptimatorCalciteSchemaInstances() {
    HoptimatorCalciteSchema root = HoptimatorCalciteSchema.createRootSchema(null, false);
    root.add("CHILD", new AbstractSchema());

    CalciteSchema child = root.getSubSchema("CHILD", true);
    assertNotNull(child);
    assertInstanceOf(HoptimatorCalciteSchema.class, child,
        "Sub-schemas must be HoptimatorCalciteSchema so pinning propagates");
  }

  // ---- Sub-schema name listing ----

  @Test
  void getSubSchemaMapIncludesExplicitAndImplicitSchemas() {
    AbstractSchema backing = new AbstractSchema() {
      @Override
      public Lookup<Schema> subSchemas() {
        return new IgnoreCaseLookup<>() {
          @Override public Schema get(String name) {
            return "implicit".equalsIgnoreCase(name) ? new AbstractSchema() : null;
          }

          @Override public Set<String> getNames(LikePattern p) {
            return Collections.singleton("implicit");
          }
        };
      }
    };

    HoptimatorCalciteSchema root = new HoptimatorCalciteSchema(null, backing, "");
    root.add("explicit", new AbstractSchema());

    assertTrue(root.getSubSchemaMap().containsKey("explicit"));
    assertTrue(root.getSubSchemaMap().containsKey("implicit"));
  }

  // ---- Factory methods ----

  @Test
  void createRootSchemaReturnsHoptimatorCalciteSchema() {
    HoptimatorCalciteSchema root = HoptimatorCalciteSchema.createRootSchema(null, false);
    assertNotNull(root);
    assertInstanceOf(HoptimatorCalciteSchema.class, root);
  }

  @Test
  void createRootSchemaDoesNotIncludeMetadataByDefault() {
    HoptimatorCalciteSchema root = HoptimatorCalciteSchema.createRootSchema(null, false);
    assertNull(root.getSubSchema("metadata", false),
        "createRootSchema() must not pre-populate metadata; drivers add their own");
  }

  @Test
  void createRootSchemaWithMetadataIncludesMetadataSubSchema() {
    HoptimatorCalciteSchema root = HoptimatorCalciteSchema.createRootSchema(null, true);
    assertNotNull(root.getSubSchema("metadata", false),
        "createRootSchema(true) must include the built-in metadata schema");
  }

  @Test
  void createRootSchemaWithCustomBackingUsesProvidedSchema() {
    AbstractSchema custom = new AbstractSchema();
    HoptimatorCalciteSchema root = HoptimatorCalciteSchema.createRootSchema(custom, false);
    assertSame(custom, root.schema, "Custom root schema must be used as backing");
  }

  @Test
  void subSchemaNotFoundReturnsNull() {
    HoptimatorCalciteSchema root = HoptimatorCalciteSchema.createRootSchema(null, false);
    assertNull(root.getSubSchema("nonexistent", true));
  }

  // ---- setCache / isCacheEnabled ----

  @Test
  void setCacheThrowsUnsupportedOperationException() {
    HoptimatorCalciteSchema root = HoptimatorCalciteSchema.createRootSchema(null, false);
    assertThrows(UnsupportedOperationException.class, () -> root.setCache(true));
  }

  @Test
  void isCacheEnabledReturnsFalse() {
    HoptimatorCalciteSchema root = new HoptimatorCalciteSchema(null, new AbstractSchema(), "");
    assertFalse(root.isCacheEnabled());
  }

  // ---- Snapshot ----

  @Test
  void snapshotContainsExplicitSubSchemas() {
    HoptimatorCalciteSchema root = HoptimatorCalciteSchema.createRootSchema(null, false);
    root.add("CHILD", new AbstractSchema());

    SchemaVersion version = other -> false;
    CalciteSchema snap = root.snapshot(null, version);
    assertNotNull(snap.getSubSchema("CHILD", true),
        "Snapshot must include explicit sub-schemas");
  }

  @Test
  void snapshotSharesTableMapWithOriginal() {
    HoptimatorCalciteSchema root = new HoptimatorCalciteSchema(null, new AbstractSchema(), "root");
    SchemaVersion version = other -> false;
    CalciteSchema snap = root.snapshot(null, version);

    // Add table after snapshot — both share the same tableMap NameMap instance
    root.add("MY_TABLE", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory f) {
        return f.builder().build();
      }
    });

    assertNotNull(root.tables().get("MY_TABLE"));
    assertNotNull(snap.tables().get("MY_TABLE"),
        "Snapshot shares tableMap, so tables added after snapshot are visible");
  }

  // ---- Implicit types ----

  @Test
  void implicitTypesFromBackingSchemaAreExposed() {
    RelProtoDataType protoType = f -> f.createJavaType(String.class);
    AbstractSchema withType = new AbstractSchema() {
      @Override public Set<String> getTypeNames() {
        return Collections.singleton("MYTYPE");
      }
      @Override public @Nullable RelProtoDataType getType(String name) {
        return "MYTYPE".equals(name) ? protoType : null;
      }
    };
    HoptimatorCalciteSchema schema = new HoptimatorCalciteSchema(null, withType, "");

    assertNotNull(schema.getType("MYTYPE", true), "Case-sensitive type lookup must find MYTYPE");
    assertTrue(schema.getTypeNames().contains("MYTYPE"), "Type names must include MYTYPE");
  }

  @Test
  void caseInsensitiveTypeLookupFindsType() {
    RelProtoDataType protoType = f -> f.createJavaType(String.class);
    // Candidates contain "MYTYPE" (uppercase); lookup with lowercase "mytype"
    AbstractSchema withType = new AbstractSchema() {
      @Override public Set<String> getTypeNames() {
        return Collections.singleton("MYTYPE");
      }
      @Override public @Nullable RelProtoDataType getType(String name) {
        return "MYTYPE".equals(name) ? protoType : null;
      }
    };
    HoptimatorCalciteSchema schema = new HoptimatorCalciteSchema(null, withType, "");

    assertNotNull(schema.getType("mytype", false),
        "Case-insensitive type lookup must find MYTYPE via uppercase branch");
    assertNull(schema.getType("nonexistent", false),
        "Unknown type name must return null");
  }

  @Test
  void caseInsensitiveTypeLookupViaLowercaseCandidate() {
    RelProtoDataType protoType = f -> f.createJavaType(String.class);
    // Candidates contain "mytype" (lowercase); lookup with uppercase "MYTYPE"
    AbstractSchema withType = new AbstractSchema() {
      @Override public Set<String> getTypeNames() {
        return Collections.singleton("mytype");
      }
      @Override public @Nullable RelProtoDataType getType(String name) {
        return "mytype".equals(name) ? protoType : null;
      }
    };
    HoptimatorCalciteSchema schema = new HoptimatorCalciteSchema(null, withType, "");

    assertNotNull(schema.getType("MYTYPE", false),
        "Case-insensitive type lookup must find mytype via lowercase branch");
  }

  // ---- Implicit functions ----

  @Test
  void implicitFunctionsFromBackingSchemaAreExposed() {
    // Non-nullary macro — just needs to appear in function lookups, not as a table.
    TableMacro macro = makeMacro();
    AbstractSchema withFunctions = new AbstractSchema() {
      @Override protected Multimap<String, Function> getFunctionMultimap() {
        return ImmutableMultimap.of("MYFUNC", macro);
      }
    };
    HoptimatorCalciteSchema schema = new HoptimatorCalciteSchema(null, withFunctions, "");

    assertFalse(schema.getFunctions("MYFUNC", true).isEmpty(),
        "Functions from backing schema must be exposed");
    assertTrue(schema.getFunctionNames().contains("MYFUNC"),
        "Function names from backing schema must be included");
  }

  // ---- Nullary function tables (TableMacro) ----

  @Test
  void nullaryFunctionTableMacroIsExposedAsTable() {
    TableMacro macro = makeMacro();
    AbstractSchema withMacro = new AbstractSchema() {
      @Override protected Multimap<String, Function> getFunctionMultimap() {
        return ImmutableMultimap.of("MYTABLE", macro);
      }
    };
    HoptimatorCalciteSchema schema = new HoptimatorCalciteSchema(null, withMacro, "");

    // getTablesBasedOnNullaryFunctions exercises both addImplicitTablesBasedOnNullaryFunctionsToBuilder
    // and getImplicitTableBasedOnNullaryFunction
    assertTrue(schema.getTablesBasedOnNullaryFunctions().containsKey("MYTABLE"),
        "Nullary TableMacro must appear in the nullary-function table map");
    assertNotNull(schema.getTableBasedOnNullaryFunction("MYTABLE", true),
        "Nullary TableMacro must be findable by name");
  }

  /**
   * Builds a nullary {@link TableMacro} that returns a zero-column table.
   * Uses a raw-type override to sidestep the Checker Framework {@code @Nullable}
   * annotation on {@code TableMacro.apply()}'s parameter, which causes a compile
   * error with strict generic bounds.
   */
  private static TableMacro makeMacro() {
    TranslatableTable table = new TranslatableTable() {
          @Override public RelDataType getRowType(RelDataTypeFactory f) {
            return f.builder().build();
          }
          @Override public RelNode toRel(RelOptTable.ToRelContext ctx, RelOptTable relOptTable) {
            throw new UnsupportedOperationException();
          }
          @Override public Schema.TableType getJdbcTableType() {
            return Schema.TableType.TABLE;
          }
          @Override public Statistic getStatistic() {
            return Statistics.UNKNOWN;
          }
          @Override public boolean isRolledUp(String column) {
            return false;
          }
          @Override public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent,
              CalciteConnectionConfig config) {
            return false;
          }
        };
    return new TableMacro() {
      // Raw List to bypass Checker Framework's @Nullable type annotation on the parameter.
      @Override public TranslatableTable apply(List args) {
        return table;
      }
      @Override public List<FunctionParameter> getParameters() {
        return Collections.emptyList();
      }
    };
  }
}
