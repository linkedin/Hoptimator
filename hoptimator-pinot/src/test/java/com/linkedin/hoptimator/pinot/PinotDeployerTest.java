package com.linkedin.hoptimator.pinot;

import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PinotDeployerTest {

  private static final String TABLE = "myTable";
  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  private static Source source(Map<String, String> options) {
    return new Source("pinot", List.of("PINOT", TABLE), options);
  }

  private static Properties properties() {
    Properties properties = new Properties();
    properties.setProperty(PinotDriver.CONTROLLER_URL, "http://localhost:9000");
    return properties;
  }

  /** Deployer with an injected mock client and canned schema/table config (no context needed). */
  private static final class TestablePinotDeployer extends PinotDeployer {
    private final PinotControllerClient client;

    TestablePinotDeployer(Source source, Properties properties, PinotControllerClient client) {
      super(source, properties, mock(DeploymentContext.class));
      this.client = client;
    }

    @Override
    protected PinotControllerClient createControllerClient() {
      return client;
    }

    @Override
    protected Schema buildSchema() {
      return new Schema.SchemaBuilder().setSchemaName(TABLE)
          .addDimensionField("id", FieldSpec.DataType.STRING).build();
    }

    @Override
    protected TableConfig buildTableConfig() {
      return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE).setNumReplicas(1).build();
    }
  }

  @Test
  void deleteToleratesSchemaDropFailure() throws Exception {
    // A shared/hybrid schema can't be dropped; that must not fail the table delete.
    PinotControllerClient client = mock(PinotControllerClient.class);
    when(client.tableExists(TABLE)).thenReturn(true);
    org.mockito.Mockito.doThrow(new java.io.IOException("schema still referenced"))
        .when(client).dropSchema(TABLE);

    new TestablePinotDeployer(source(Map.of()), properties(), client).delete();

    verify(client).dropTable(TABLE);
    verify(client).dropSchema(TABLE);
  }

  @Test
  void deployerProviderIsDiscoverableAndReturnsDeployer() throws Exception {
    // SPI discovery: the provider is registered and returns a deployer for a pinot source.
    boolean found = false;
    for (com.linkedin.hoptimator.DeployerProvider provider
        : java.util.ServiceLoader.load(com.linkedin.hoptimator.DeployerProvider.class)) {
      if (provider instanceof PinotDeployerProvider) {
        found = true;
      }
    }
    assertThat(found).isTrue();

    DeploymentContext context = mock(DeploymentContext.class);
    when(context.databaseProperties(any(), eq("PINOT"), eq(PinotDriver.CONNECTION_PREFIX)))
        .thenReturn(properties());
    java.util.Collection<com.linkedin.hoptimator.Deployer> deployers =
        new PinotDeployerProvider().deployers(source(Map.of()), context);
    assertThat(deployers).hasSize(1);
    assertThat(deployers.iterator().next()).isInstanceOf(PinotDeployer.class);
  }

  @Test
  void createProvisionsSchemaAndTableWhenAbsent() throws Exception {
    PinotControllerClient client = mock(PinotControllerClient.class);
    when(client.tableExists(TABLE)).thenReturn(false);

    new TestablePinotDeployer(source(Map.of()), properties(), client).create();

    verify(client).addSchema(any(Schema.class), eq(false));
    verify(client).addTable(any(TableConfig.class));
  }

  @Test
  void createSkipsWhenTableExists() throws Exception {
    PinotControllerClient client = mock(PinotControllerClient.class);
    when(client.tableExists(TABLE)).thenReturn(true);

    new TestablePinotDeployer(source(Map.of()), properties(), client).create();

    verify(client, never()).addSchema(any(Schema.class), eq(false));
    verify(client, never()).addTable(any(TableConfig.class));
  }

  @Test
  void updateEvolvesSchemaAndConfigWhenTableExists() throws Exception {
    PinotControllerClient client = mock(PinotControllerClient.class);
    when(client.tableExists(TABLE)).thenReturn(true);

    new TestablePinotDeployer(source(Map.of()), properties(), client).update();

    verify(client).updateSchema(any(Schema.class));
    verify(client).updateTable(any(TableConfig.class));
    verify(client, never()).addTable(any(TableConfig.class));
  }

  @Test
  void updateCreatesWhenTableAbsent() throws Exception {
    PinotControllerClient client = mock(PinotControllerClient.class);
    when(client.tableExists(TABLE)).thenReturn(false);

    new TestablePinotDeployer(source(Map.of()), properties(), client).update();

    verify(client).addSchema(any(Schema.class), eq(false));
    verify(client).addTable(any(TableConfig.class));
  }

  @Test
  void deleteDropsTableAndSchema() throws Exception {
    PinotControllerClient client = mock(PinotControllerClient.class);
    when(client.tableExists(TABLE)).thenReturn(true);

    new TestablePinotDeployer(source(Map.of()), properties(), client).delete();

    verify(client).dropTable(TABLE);
    verify(client).dropSchema(TABLE);
  }

  @Test
  void deleteSkipsWhenTableAbsent() throws Exception {
    PinotControllerClient client = mock(PinotControllerClient.class);
    when(client.tableExists(TABLE)).thenReturn(false);

    new TestablePinotDeployer(source(Map.of()), properties(), client).delete();

    verify(client, never()).dropTable(TABLE);
  }

  @Test
  void existsDelegatesToClient() throws Exception {
    PinotControllerClient client = mock(PinotControllerClient.class);
    when(client.tableExists(TABLE)).thenReturn(true);

    assertThat(new TestablePinotDeployer(source(Map.of()), properties(), client).exists()).isTrue();
  }

  @Test
  void createWrapsClientFailureAsSqlException() throws Exception {
    PinotControllerClient client = mock(PinotControllerClient.class);
    when(client.tableExists(TABLE)).thenThrow(new java.io.IOException("controller down"));

    assertThatThrownBy(() -> new TestablePinotDeployer(source(Map.of()), properties(), client).create())
        .isInstanceOf(java.sql.SQLException.class)
        .hasMessageContaining(TABLE);
  }

  @Test
  void updateWrapsClientFailureAsSqlException() throws Exception {
    PinotControllerClient client = mock(PinotControllerClient.class);
    when(client.tableExists(TABLE)).thenThrow(new java.io.IOException("controller down"));

    assertThatThrownBy(() -> new TestablePinotDeployer(source(Map.of()), properties(), client).update())
        .isInstanceOf(java.sql.SQLException.class)
        .hasMessageContaining(TABLE);
  }

  @Test
  void deleteWrapsClientFailureAsSqlException() throws Exception {
    PinotControllerClient client = mock(PinotControllerClient.class);
    when(client.tableExists(TABLE)).thenThrow(new java.io.IOException("controller down"));

    assertThatThrownBy(() -> new TestablePinotDeployer(source(Map.of()), properties(), client).delete())
        .isInstanceOf(java.sql.SQLException.class)
        .hasMessageContaining(TABLE);
  }

  @Test
  void existsWrapsClientFailureAsSqlException() throws Exception {
    PinotControllerClient client = mock(PinotControllerClient.class);
    when(client.tableExists(TABLE)).thenThrow(new java.io.IOException("controller down"));

    assertThatThrownBy(() -> new TestablePinotDeployer(source(Map.of()), properties(), client).exists())
        .isInstanceOf(java.sql.SQLException.class)
        .hasMessageContaining(TABLE);
  }

  @Test
  void buildSchemaWithoutHintsMakesEverythingADimension() {
    PinotDeployer deployer = new PinotDeployer(source(Map.of()), properties(), mock(DeploymentContext.class));

    RelDataType rowType = new RelDataTypeFactory.Builder(typeFactory)
        .add("id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("value", typeFactory.createSqlType(SqlTypeName.DOUBLE))
        .build();

    Schema schema = deployer.buildSchema(rowType);

    assertThat(schema.getDimensionNames()).containsExactlyInAnyOrder("id", "value");
    assertThat(schema.getMetricNames()).isEmpty();
    assertThat(schema.getDateTimeNames()).isEmpty();
  }

  @Test
  void specifyIsEmptyAndRestoreIsNoop() throws Exception {
    PinotDeployer deployer = new PinotDeployer(source(Map.of()), properties(), mock(DeploymentContext.class));
    assertThat(deployer.specify()).isEmpty();
    deployer.restore(); // must not throw
  }

  @Test
  void validateReportsMissingControllerUrl() {
    PinotDeployer deployer = new PinotDeployer(source(Map.of()), new Properties(), mock(DeploymentContext.class));
    Validator.Issues issues = new Validator.Issues("pinot");

    deployer.validate(issues, mock(DeploymentContext.class));

    assertThat(issues.valid()).isFalse();
  }

  @Test
  void buildSchemaClassifiesDimensionMetricTimeAndMultiValue() {
    Map<String, String> options = Map.of("metrics", "count", "timeColumns", "ts");
    PinotDeployer deployer = new PinotDeployer(source(options), properties(), mock(DeploymentContext.class));

    RelDataType rowType = new RelDataTypeFactory.Builder(typeFactory)
        .add("id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("count", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("ts", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .add("tags", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1))
        .build();

    Schema schema = deployer.buildSchema(rowType);

    assertThat(schema.getDimensionNames()).contains("id", "tags");
    assertThat(schema.getMetricNames()).containsExactly("count");
    assertThat(schema.getDateTimeNames()).containsExactly("ts");
    assertThat(schema.getFieldSpecFor("tags").isSingleValueField()).isFalse();
    assertThat(schema.getFieldSpecFor("count").getDataType()).isEqualTo(FieldSpec.DataType.LONG);
  }

  @Test
  void buildTableConfigDefaultsToOfflineWithTimeColumn() {
    Map<String, String> options = Map.of("timeColumns", "ts", "numReplicas", "2");
    PinotDeployer deployer = new PinotDeployer(source(options), properties(), mock(DeploymentContext.class));

    TableConfig tableConfig = deployer.buildTableConfig();

    assertThat(tableConfig.getTableType()).isEqualTo(TableType.OFFLINE);
    assertThat(tableConfig.getTableName()).isEqualTo(TABLE + "_OFFLINE");
    assertThat(tableConfig.getValidationConfig().getTimeColumnName()).isEqualTo("ts");
  }

  @Test
  void buildSchemaAppliesPerColumnFormatAndGranularity() {
    Map<String, String> options = Map.of(
        "timeColumns", "day",
        "format.day", "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd",
        "granularity.day", "3:DAYS");
    PinotDeployer deployer = new PinotDeployer(source(options), properties(), mock(DeploymentContext.class));

    RelDataType rowType = new RelDataTypeFactory.Builder(typeFactory)
        .add("day", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    Schema schema = deployer.buildSchema(rowType);
    org.apache.pinot.spi.data.DateTimeFieldSpec spec =
        (org.apache.pinot.spi.data.DateTimeFieldSpec) schema.getFieldSpecFor("day");

    assertThat(spec.getFormat()).isEqualTo("1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd");
    assertThat(spec.getGranularity()).isEqualTo("3:DAYS");
  }

  @Test
  void buildTableConfigUsesPrimaryTimeColumn() {
    Map<String, String> options = Map.of("timeColumns", "created,day", "primaryTimeColumn", "day");
    PinotDeployer deployer = new PinotDeployer(source(options), properties(), mock(DeploymentContext.class));

    assertThat(deployer.buildTableConfig().getValidationConfig().getTimeColumnName()).isEqualTo("day");
  }

  // -- all-inclusive validation (a bad spec must not pass through) --------------------------------

  private static Validator.Issues validateRowType(Map<String, String> options, RelDataType rowType) {
    PinotDeployer deployer = new PinotDeployer(source(options), properties(), mock(DeploymentContext.class));
    Validator.Issues issues = new Validator.Issues("pinot");
    deployer.validateRowType(issues, rowType);
    return issues;
  }

  @Test
  void validateAcceptsAValidSpec() {
    RelDataType rowType = new RelDataTypeFactory.Builder(typeFactory)
        .add("id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("revenue", typeFactory.createSqlType(SqlTypeName.DOUBLE))
        .add("ts", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .build();
    assertThat(validateRowType(Map.of("metrics", "revenue", "timeColumns", "ts"), rowType).valid()).isTrue();
  }

  @Test
  void validateRejectsNonNumericMetric() {
    // Pinot METRIC columns must be numeric/BYTES; a STRING metric is illegal.
    RelDataType rowType = new RelDataTypeFactory.Builder(typeFactory)
        .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
    assertThat(validateRowType(Map.of("metrics", "name"), rowType).valid()).isFalse();
  }

  @Test
  void validateRejectsBadTimeFormat() {
    RelDataType rowType = new RelDataTypeFactory.Builder(typeFactory)
        .add("ts", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .build();
    assertThat(validateRowType(Map.of("timeColumns", "ts", "format.ts", "not-a-format"), rowType).valid()).isFalse();
  }

  @Test
  void validateRejectsBadGranularity() {
    RelDataType rowType = new RelDataTypeFactory.Builder(typeFactory)
        .add("ts", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .build();
    assertThat(validateRowType(Map.of("timeColumns", "ts", "granularity.ts", "5:BANANAS"), rowType).valid()).isFalse();
  }

  @Test
  void validateRejectsMultiValuedMetric() {
    RelDataType rowType = new RelDataTypeFactory.Builder(typeFactory)
        .add("amounts", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.DOUBLE), -1))
        .build();
    assertThat(validateRowType(Map.of("metrics", "amounts"), rowType).valid()).isFalse();
  }

  @Test
  void validateRejectsMultiValuedTimeColumn() {
    RelDataType rowType = new RelDataTypeFactory.Builder(typeFactory)
        .add("ts", typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BIGINT), -1))
        .build();
    assertThat(validateRowType(Map.of("timeColumns", "ts"), rowType).valid()).isFalse();
  }

  @Test
  void validateRejectsComplexColumn() {
    RelDataType rowType = new RelDataTypeFactory.Builder(typeFactory)
        .add("props", typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.INTEGER)))
        .build();
    assertThat(validateRowType(Map.of(), rowType).valid()).isFalse();
  }

  @Test
  void validateRejectsUnknownPrimaryTimeColumn() {
    RelDataType rowType = new RelDataTypeFactory.Builder(typeFactory)
        .add("ts", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .build();
    assertThat(validateRowType(Map.of("timeColumns", "ts", "primaryTimeColumn", "nope"), rowType).valid()).isFalse();
  }

  // -- REALTIME stream ingestion -----------------------------------------------------------------

  private RelDataType realtimeRowType() {
    return new RelDataTypeFactory.Builder(typeFactory)
        .add("id", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("ts", typeFactory.createSqlType(SqlTypeName.BIGINT))
        .build();
  }

  private static Map<String, String> realtimeHints() {
    Map<String, String> hints = new java.util.HashMap<>();
    hints.put("tableType", "REALTIME");
    hints.put("timeColumns", "ts");
    hints.put("kafkaTopic", "events");
    hints.put("kafkaBrokerList", "broker1:9092,broker2:9092");
    return hints;
  }

  @Test
  void buildTableConfigSynthesizesRealtimeStreamConfig() {
    PinotDeployer deployer = new PinotDeployer(source(realtimeHints()), properties(), mock(DeploymentContext.class));

    TableConfig tableConfig = deployer.buildTableConfig();

    assertThat(tableConfig.getTableType()).isEqualTo(TableType.REALTIME);
    assertThat(tableConfig.getValidationConfig().getTimeColumnName()).isEqualTo("ts");
    assertThat(tableConfig.getIngestionConfig()).isNotNull();
    Map<String, String> streamConfigs =
        tableConfig.getIngestionConfig().getStreamIngestionConfig().getStreamConfigMaps().get(0);
    assertThat(streamConfigs).containsEntry("streamType", "kafka");
    assertThat(streamConfigs).containsEntry("stream.kafka.topic.name", "events");
    assertThat(streamConfigs).containsEntry("stream.kafka.broker.list", "broker1:9092,broker2:9092");
    assertThat(streamConfigs).containsKey("stream.kafka.consumer.factory.class.name");
    assertThat(streamConfigs).containsKey("stream.kafka.decoder.class.name");
  }

  @Test
  void validateAcceptsValidRealtimeSpec() {
    assertThat(validateRowType(realtimeHints(), realtimeRowType()).valid()).isTrue();
  }

  @Test
  void validateRejectsRealtimeWithoutTimeColumn() {
    Map<String, String> hints = realtimeHints();
    hints.remove("timeColumns");
    assertThat(validateRowType(hints, realtimeRowType()).valid()).isFalse();
  }

  @Test
  void validateRejectsRealtimeWithoutKafkaTopic() {
    Map<String, String> hints = realtimeHints();
    hints.remove("kafkaTopic");
    assertThat(validateRowType(hints, realtimeRowType()).valid()).isFalse();
  }

  @Test
  void validateRejectsRealtimeWithoutBrokerList() {
    Map<String, String> hints = realtimeHints();
    hints.remove("kafkaBrokerList");
    assertThat(validateRowType(hints, realtimeRowType()).valid()).isFalse();
  }
}
