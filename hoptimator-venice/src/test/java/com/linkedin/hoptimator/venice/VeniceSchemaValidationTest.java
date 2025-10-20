package com.linkedin.hoptimator.venice;

import com.linkedin.hoptimator.jdbc.JdbcTestBase;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


/**
 * Tests Venice table creation and schema validation without requiring all drivers
 * to be loaded via metadata system queries.
 */
@Tag("integration")
public class VeniceSchemaValidationTest extends JdbcTestBase {

    @Test
    public void testVeniceTableCreationAndSchema() throws Exception {
        sql("create or replace materialized view \"VENICE\".\"test-store$insert-partial\" (\"KEY_id\", \"intField\") "
            + "as select \"KEY\", \"intField\" from \"VENICE\".\"test-store-primitive\"");

        // Validate the table was created with expected schema
        Map<String, String> expectedColumns = Map.of(
            "KEY_id", "INTEGER",
            "intField", "INTEGER"
        );
        validateTableSchema(List.of("VENICE", "test-store$insert-partial"), expectedColumns);

        sql("drop view venice.\"test-store$insert-partial\"");
    }
}
