package com.linkedin.hoptimator.kafka;

import com.linkedin.hoptimator.jdbc.JdbcTestBase;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Tests Kafka table creation and schema validation without requiring all drivers
 * to be loaded via metadata system queries.
 */
@Tag("integration")
public class KafkaSchemaValidationTest extends JdbcTestBase {

    @Test
    public void testKafkaTableCreationAndSchema() throws Exception {
        sql("create or replace view kafka.\"new-topic$kafka-test\" as select * from kafka.\"existing-topic-2\"");

        // Validate the table was created with expected schema
        Map<String, String> expectedColumns = Map.of(
            "KEY", "VARCHAR",
            "VALUE", "BINARY"
        );
        validateTableSchema(List.of("KAFKA", "new-topic$kafka-test"), expectedColumns);

        sql("drop view kafka.\"new-topic$kafka-test\"");
    }
}
