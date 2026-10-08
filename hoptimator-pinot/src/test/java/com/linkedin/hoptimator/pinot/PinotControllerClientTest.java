package com.linkedin.hoptimator.pinot;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises {@link PinotControllerClient} against an in-process fake Pinot controller. */
class PinotControllerClientTest {

  private HttpServer server;
  private PinotControllerClient client;
  private final List<String> requests = new ArrayList<>();
  private volatile int status = 200;
  private volatile String responseBody = "{}";

  @BeforeEach
  void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::handle);
    server.start();
    String url = "http://127.0.0.1:" + server.getAddress().getPort();
    client = new PinotControllerClient(url, HttpClient.newHttpClient());
  }

  @AfterEach
  void tearDown() {
    server.stop(0);
  }

  private void handle(HttpExchange exchange) throws IOException {
    String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    String query = exchange.getRequestURI().getQuery();
    requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath()
        + (query == null ? "" : "?" + query) + (body.isEmpty() ? "" : " " + body));
    byte[] out = responseBody.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, out.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(out);
    }
  }

  @Test
  void listTablesParsesResponse() throws IOException {
    responseBody = "{\"tables\":[\"a\",\"b\"]}";
    assertThat(client.listTables()).containsExactly("a", "b");
    assertThat(requests).containsExactly("GET /tables");
  }

  @Test
  void listTablesHandlesMissingTablesKey() throws IOException {
    responseBody = "{}";
    assertThat(client.listTables()).isEmpty();
  }

  @Test
  void getSchemaParsesSchema() throws IOException {
    responseBody = new Schema.SchemaBuilder().setSchemaName("t")
        .addDimensionField("id", FieldSpec.DataType.STRING).build().toSingleLineJsonString();
    Schema schema = client.getSchema("t");
    assertThat(schema).isNotNull();
    assertThat(schema.getSchemaName()).isEqualTo("t");
    assertThat(requests).containsExactly("GET /tables/t/schema");
  }

  @Test
  void getSchemaReturnsNullOn404() throws IOException {
    status = 404;
    assertThat(client.getSchema("missing")).isNull();
  }

  @Test
  void tableExistsTrueWhenConfigPresent() throws IOException {
    responseBody = "{\"OFFLINE\":{\"tableName\":\"t_OFFLINE\"}}";
    assertThat(client.tableExists("t")).isTrue();
  }

  @Test
  void tableExistsFalseWhenEmpty() throws IOException {
    responseBody = "{}";
    assertThat(client.tableExists("t")).isFalse();
  }

  @Test
  void tableExistsFalseOn404() throws IOException {
    status = 404;
    assertThat(client.tableExists("t")).isFalse();
  }

  @Test
  void addSchemaPostsWithOverrideFlag() throws IOException {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("t")
        .addDimensionField("id", FieldSpec.DataType.STRING).build();
    client.addSchema(schema, true);
    assertThat(requests).hasSize(1);
    assertThat(requests.get(0)).startsWith("POST /schemas?override=true ");
  }

  @Test
  void dropTableDeletes() throws IOException {
    client.dropTable("t");
    assertThat(requests).containsExactly("DELETE /tables/t");
  }

  @Test
  void dropSchemaDeletes() throws IOException {
    client.dropSchema("t");
    assertThat(requests).containsExactly("DELETE /schemas/t");
  }

  @Test
  void updateSchemaPuts() throws IOException {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("t")
        .addDimensionField("id", FieldSpec.DataType.STRING).build();
    client.updateSchema(schema);
    assertThat(requests).hasSize(1);
    assertThat(requests.get(0)).startsWith("PUT /schemas/t ");
  }

  @Test
  void addTablePostsConfig() throws IOException {
    org.apache.pinot.spi.config.table.TableConfig tableConfig =
        new org.apache.pinot.spi.utils.builder.TableConfigBuilder(
            org.apache.pinot.spi.config.table.TableType.OFFLINE)
            .setTableName("t").setNumReplicas(1).build();
    client.addTable(tableConfig);
    assertThat(requests).hasSize(1);
    assertThat(requests.get(0)).startsWith("POST /tables ");
  }

  @Test
  void updateTablePutsConfigByRawName() throws IOException {
    org.apache.pinot.spi.config.table.TableConfig tableConfig =
        new org.apache.pinot.spi.utils.builder.TableConfigBuilder(
            org.apache.pinot.spi.config.table.TableType.OFFLINE)
            .setTableName("t").setNumReplicas(1).build();
    client.updateTable(tableConfig);
    assertThat(requests).hasSize(1);
    // Raw table name in the path (not the _OFFLINE suffix that getTableName() carries).
    assertThat(requests.get(0)).startsWith("PUT /tables/t ");
  }

  @Test
  void mutationFailurePropagates() {
    status = 500;
    responseBody = "boom";
    assertThatThrownBy(() -> client.dropTable("t"))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("HTTP 500");
  }

  @Test
  void getSchemaFailurePropagates() {
    status = 500;
    responseBody = "kaboom";
    assertThatThrownBy(() -> client.getSchema("t"))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("HTTP 500");
  }

  @Test
  void tableExistsFailurePropagates() {
    status = 500;
    assertThatThrownBy(() -> client.tableExists("t"))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("HTTP 500");
  }

  @Test
  void listTablesFailurePropagates() {
    status = 503;
    assertThatThrownBy(() -> client.listTables())
        .isInstanceOf(IOException.class)
        .hasMessageContaining("HTTP 503");
  }
}
