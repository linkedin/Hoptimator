package com.linkedin.hoptimator.jdbc;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


class DirectDeploymentContextTest {

  private static Schema avroSchema() {
    return new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\","
        + "\"namespace\":\"com.example\",\"fields\":[{\"name\":\"ID\",\"type\":\"int\"}]}");
  }

  private static DatabaseConfigResolver resolverReturning(@Nullable Properties props) {
    return new DatabaseConfigResolver() {
      @Override
      public @Nullable Properties databaseProperties(@Nullable String catalog, @Nullable String schema,
          String connectionPrefix) {
        return props;
      }

      @Override
      public String databaseName(List<String> tablePath) {
        return tablePath.get(tablePath.size() - 2);
      }
    };
  }

  @Test
  void propertiesReturnsSuppliedBag() {
    Properties props = new Properties();
    props.setProperty("k8s.namespace", "ns");
    DirectDeploymentContext context = new DirectDeploymentContext(props, resolverReturning(null), avroSchema());

    assertThat(context.properties()).isSameAs(props);
  }

  @Test
  void rowTypeDerivedFromAvroSchema() {
    Schema avroSchema = avroSchema();
    DirectDeploymentContext context =
        new DirectDeploymentContext(new Properties(), resolverReturning(null), avroSchema);

    assertThat(context.avroSchema()).isSameAs(avroSchema);
    assertThat(context.rowType().isStruct()).isTrue();
    assertThat(context.rowType().getFieldNames()).contains("ID");
  }

  @Test
  void rowTypeThrowsWhenAbsent() {
    DirectDeploymentContext context =
        new DirectDeploymentContext(new Properties(), resolverReturning(null));

    assertThatThrownBy(context::rowType)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No Avro schema is carried");
  }

  @Test
  void databasePropertiesDelegatesToResolver() {
    Properties dbProps = new Properties();
    dbProps.setProperty("bootstrap.servers", "localhost:9092");
    DirectDeploymentContext context =
        new DirectDeploymentContext(new Properties(), resolverReturning(dbProps));

    assertThat(context.databaseProperties(null, "KAFKA", "jdbc:kafka://")).isSameAs(dbProps);
  }
}
