package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


class DirectDeploymentContextTest {

  private static RelDataType rowType() {
    RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    return factory.builder().add("ID", factory.createSqlType(SqlTypeName.INTEGER)).build();
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
    DirectDeploymentContext context = new DirectDeploymentContext(props, resolverReturning(null), rowType());

    assertThat(context.properties()).isSameAs(props);
  }

  @Test
  void rowTypeReturnsCarriedType() {
    RelDataType rowType = rowType();
    DirectDeploymentContext context =
        new DirectDeploymentContext(new Properties(), resolverReturning(null), rowType);

    assertThat(context.rowType()).isSameAs(rowType);
  }

  @Test
  void rowTypeThrowsWhenAbsent() {
    DirectDeploymentContext context =
        new DirectDeploymentContext(new Properties(), resolverReturning(null), null);

    assertThatThrownBy(context::rowType)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No row type is carried");
  }

  @Test
  void databasePropertiesDelegatesToResolver() throws SQLException {
    Properties dbProps = new Properties();
    dbProps.setProperty("bootstrap.servers", "localhost:9092");
    DirectDeploymentContext context =
        new DirectDeploymentContext(new Properties(), resolverReturning(dbProps), null);

    assertThat(context.databaseProperties(null, "KAFKA", "jdbc:kafka://")).isSameAs(dbProps);
  }
}
