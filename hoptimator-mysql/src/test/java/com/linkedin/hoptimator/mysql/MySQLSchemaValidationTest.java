package com.linkedin.hoptimator.mysql;

import com.linkedin.hoptimator.jdbc.JdbcTestBase;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


/**
 * Tests MySQL table creation and schema validation without requiring all drivers
 * to be loaded via metadata system queries.
 */
@Tag("integration")
public class MySQLSchemaValidationTest extends JdbcTestBase {

    @Test
    public void testKafkaTableCreationAndSchema() throws Exception {
        sql("create or replace materialized view MYSQL.\"testdb\".\"users$partial\" as select \"user_id\" from mysql.\"testdb\".\"orders\"");

        // Validate the table was created with expected schema
        Map<String, String> expectedColumns = Map.of(
            "user_id", "INTEGER"
        );
        validateTableSchema(List.of("MYSQL", "testdb", "users$partial"), expectedColumns);

        sql("drop materialized view MYSQL.\"testdb\".\"users$partial\"");
    }
}
