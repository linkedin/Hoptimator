package com.linkedin.hoptimator.pinot;

import com.linkedin.hoptimator.jdbc.schema.LazyLookup;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.LazyReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calcite schema exposing Pinot tables via a {@link PinotClient}.
 *
 * <p>Tables are discovered lazily: {@code load(name)} resolves a single table with one targeted
 * existence check, while {@code loadAll()} lists every table in the cluster. Results are cached by
 * {@link LazyLookup}, so repeated lookups are O(1) map accesses with no extra round-trips. Per-table
 * failures during {@code loadAll} are isolated so one bad table does not blank out the catalog.
 */
public class PinotSchema extends AbstractSchema {

  private static final Logger log = LoggerFactory.getLogger(PinotSchema.class);

  protected final PinotClient client;
  private final LazyReference<Lookup<Table>> tables = new LazyReference<>();

  public PinotSchema(PinotClient client) {
    this.client = client;
  }

  @Override
  public Lookup<Table> tables() {
    return tables.getOrCompute(() -> new LazyLookup<>() {

      @Override
      protected Map<String, Table> loadAll() throws Exception {
        Map<String, Table> tableMap = new HashMap<>();
        for (String tableName : client.listTables()) {
          try {
            tableMap.put(tableName, new PinotTable(tableName, client));
          } catch (Exception e) {
            log.warn("Skipping Pinot table {} due to setup failure", tableName, e);
          }
        }
        log.info("Discovered {} Pinot tables", tableMap.size());
        return tableMap;
      }

      @Override
      protected @Nullable Table load(String name) throws Exception {
        if (client.tableExists(name)) {
          return new PinotTable(name, client);
        }
        return null;
      }

      @Override
      protected String getDescription() {
        return "Pinot tables";
      }
    });
  }
}
