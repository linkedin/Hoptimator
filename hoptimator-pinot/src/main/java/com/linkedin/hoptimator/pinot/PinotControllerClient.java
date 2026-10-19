package com.linkedin.hoptimator.pinot;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PinotClient} backed by the Apache Pinot Controller REST API.
 *
 * <p>Read endpoints ({@code GET /tables}, {@code GET /tables/{name}/schema}, {@code GET
 * /tables/{name}}) back the JDBC catalog; the provisioning endpoints ({@code POST /schemas},
 * {@code POST /tables}, {@code DELETE /tables/{name}}) back {@link PinotDeployer}. A single
 * {@link HttpClient} is reused for all calls.
 */
public final class PinotControllerClient implements PinotClient {

  private static final Logger log = LoggerFactory.getLogger(PinotControllerClient.class);

  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

  private final String controllerUrl;
  private final HttpClient httpClient;

  public PinotControllerClient(String controllerUrl) {
    this(controllerUrl, HttpClient.newBuilder()
        // Force HTTP/1.1: the Pinot controller's Grizzly server mishandles the Java client's default
        // HTTP/2 (h2c) upgrade on POST/PUT, dropping the request body server-side (EOFException) so
        // the call hangs until timeout.
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(Duration.ofSeconds(10)).build());
  }

  PinotControllerClient(String controllerUrl, HttpClient httpClient) {
    if (controllerUrl == null || controllerUrl.isEmpty()) {
      throw new IllegalArgumentException("Missing required Pinot controller url");
    }
    // Normalize away any trailing slash so path concatenation is unambiguous.
    this.controllerUrl = controllerUrl.endsWith("/")
        ? controllerUrl.substring(0, controllerUrl.length() - 1) : controllerUrl;
    this.httpClient = httpClient;
  }

  @Override
  public List<String> listTables() throws IOException {
    HttpResponse<String> response = send(HttpRequest.newBuilder(uri("/tables")).GET());
    if (response.statusCode() != 200) {
      throw new IOException("Failed to list Pinot tables: HTTP " + response.statusCode() + " " + response.body());
    }
    JsonNode tables = JsonUtils.stringToJsonNode(response.body()).get("tables");
    List<String> result = new ArrayList<>();
    if (tables != null && tables.isArray()) {
      tables.forEach(node -> result.add(node.asText()));
    }
    log.debug("Discovered {} Pinot tables via controller {}", result.size(), controllerUrl);
    return result;
  }

  @Override
  @Nullable
  public Schema getSchema(String tableName) throws IOException {
    HttpResponse<String> response = send(HttpRequest.newBuilder(uri("/tables/" + tableName + "/schema")).GET());
    if (response.statusCode() == 404) {
      return null;
    }
    if (response.statusCode() != 200) {
      throw new IOException("Failed to get schema for Pinot table " + tableName + ": HTTP "
          + response.statusCode() + " " + response.body());
    }
    return Schema.fromString(response.body());
  }

  @Override
  public boolean tableExists(String tableName) throws IOException {
    HttpResponse<String> response = send(HttpRequest.newBuilder(uri("/tables/" + tableName)).GET());
    if (response.statusCode() == 404) {
      return false;
    }
    if (response.statusCode() != 200) {
      throw new IOException("Failed to check Pinot table " + tableName + ": HTTP "
          + response.statusCode() + " " + response.body());
    }
    // GET /tables/{name} returns {"OFFLINE": {...}} and/or {"REALTIME": {...}}; an empty object
    // ({}) means no such table.
    JsonNode body = JsonUtils.stringToJsonNode(response.body());
    return body != null && body.size() > 0;
  }

  /** Creates (or, when {@code override} is true, replaces) a Pinot schema. */
  public void addSchema(Schema schema, boolean override) throws IOException {
    HttpResponse<String> response = send(HttpRequest.newBuilder(uri("/schemas?override=" + override))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(schema.toSingleLineJsonString())));
    ensureSuccess(response, "add schema " + schema.getSchemaName());
  }

  /** Updates an existing Pinot schema in place. */
  public void updateSchema(Schema schema) throws IOException {
    HttpResponse<String> response = send(HttpRequest.newBuilder(uri("/schemas/" + schema.getSchemaName()))
        .header("Content-Type", "application/json")
        .PUT(HttpRequest.BodyPublishers.ofString(schema.toSingleLineJsonString())));
    ensureSuccess(response, "update schema " + schema.getSchemaName());
  }

  /** Creates a Pinot table from a table config. */
  public void addTable(TableConfig tableConfig) throws IOException {
    String body;
    try {
      body = JsonUtils.objectToString(tableConfig);
    } catch (Exception e) {
      throw new IOException("Failed to serialize Pinot table config for " + tableConfig.getTableName(), e);
    }
    HttpResponse<String> response = send(HttpRequest.newBuilder(uri("/tables"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body)));
    ensureSuccess(response, "add table " + tableConfig.getTableName());
  }

  /** Updates an existing Pinot table's config ({@code PUT /tables/{tableName}}). */
  public void updateTable(TableConfig tableConfig) throws IOException {
    String body;
    try {
      body = JsonUtils.objectToString(tableConfig);
    } catch (Exception e) {
      throw new IOException("Failed to serialize Pinot table config for " + tableConfig.getTableName(), e);
    }
    // The update endpoint is keyed by the raw table name; the type comes from the body's tableType.
    String rawTableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
    HttpResponse<String> response = send(HttpRequest.newBuilder(uri("/tables/" + rawTableName))
        .header("Content-Type", "application/json")
        .PUT(HttpRequest.BodyPublishers.ofString(body)));
    ensureSuccess(response, "update table " + tableConfig.getTableName());
  }

  /** Deletes a Pinot table (all table types) by raw table name. */
  public void dropTable(String tableName) throws IOException {
    HttpResponse<String> response = send(HttpRequest.newBuilder(uri("/tables/" + tableName)).DELETE());
    ensureSuccess(response, "delete table " + tableName);
  }

  /** Deletes a Pinot schema by name. */
  public void dropSchema(String schemaName) throws IOException {
    HttpResponse<String> response = send(HttpRequest.newBuilder(uri("/schemas/" + schemaName)).DELETE());
    ensureSuccess(response, "delete schema " + schemaName);
  }

  private URI uri(String path) {
    return URI.create(controllerUrl + path);
  }

  private HttpResponse<String> send(HttpRequest.Builder builder) throws IOException {
    try {
      return httpClient.send(builder.timeout(REQUEST_TIMEOUT).build(), HttpResponse.BodyHandlers.ofString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while calling Pinot controller " + controllerUrl, e);
    }
  }

  private static void ensureSuccess(HttpResponse<String> response, String action) throws IOException {
    if (response.statusCode() / 100 != 2) {
      throw new IOException("Failed to " + action + ": HTTP " + response.statusCode() + " " + response.body());
    }
  }
}
