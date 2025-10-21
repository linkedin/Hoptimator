package com.linkedin.hoptimator.mcp.server;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Integration tests for HoptimatorMcpServer.
 * These tests start the MCP server as a subprocess and communicate with it via MCP protocol.
 * This provides real end-to-end testing of the server's functionality.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("integration")
class HoptimatorMcpServerTest {

    private static final String JDBC_URL = "jdbc:hoptimator://fun=mysql";

    private Process mcpServerProcess;
    private BufferedWriter serverInput;
    private BufferedReader serverOutput;
    private Gson gson;
    private int requestId = 1;

    @BeforeEach
    void setUp() throws Exception {
        gson = new Gson();
        startMcpServerSubprocess();
        initializeMcpSession();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mcpServerProcess != null && mcpServerProcess.isAlive()) {
            mcpServerProcess.destroyForcibly();
            mcpServerProcess.waitFor(5, TimeUnit.SECONDS);
        }
        if (serverInput != null) {
            serverInput.close();
        }
        if (serverOutput != null) {
            serverOutput.close();
        }
    }

    /**
     * Test that we can start the MCP server subprocess and initialize session
     */
    @Test
    void testServerStartupAndInitialization() {
        assertNotNull(mcpServerProcess);
        assertTrue(mcpServerProcess.isAlive());
        assertNotNull(serverInput);
        assertNotNull(serverOutput);
    }

    /**
     * Test the fetch_schemas MCP tool
     */
    @Test
    void testFetchSchemasTool() throws Exception {
        JsonObject response = callMcpTool("fetch_schemas", new JsonObject());

        assertNotNull(response);
        assertFalse(response.has("error"));
        assertTrue(response.has("result"));

        JsonObject result = response.getAsJsonObject("result");
        assertTrue(result.has("content"), "Result missing 'content' field");

        JsonArray content = result.getAsJsonArray("content");
        assertNotNull(content, "Content is null");
        assertFalse(content.isEmpty(), "Content is empty");

        String text = content.get(0).getAsJsonObject().get("text").getAsString();
        assertNotNull(text, "Text is null");
        assertFalse(text.isEmpty(), "Text is empty");
        assertFalse(text.contains("ERROR"));

        @SuppressWarnings("unchecked")
        List<String> schemas = gson.fromJson(text, List.class);
        assertNotNull(schemas, "Schemas is null");
        assertFalse(schemas.isEmpty(), "Schemas are empty");
        System.out.println("Schemas: " + schemas);
    }

    /**
     * Test the fetch_tables MCP tool
     */
    @Test
    void testFetchTablesTool() throws Exception {
        JsonObject response = callMcpTool("fetch_tables", new JsonObject());

        assertNotNull(response);
        assertFalse(response.has("error"));
        assertTrue(response.has("result"));

        JsonObject result = response.getAsJsonObject("result");
        JsonArray content = result.get("content").getAsJsonArray();
        assertNotNull(content, "Content is null");
        assertFalse(content.isEmpty(), "Content is empty");

        String text = content.get(0).getAsJsonObject().get("text").getAsString();
        assertNotNull(text, "Text is null");
        assertFalse(text.isEmpty(), "Text is empty");
        assertFalse(text.contains("ERROR"));

        @SuppressWarnings("unchecked")
        List<Object> tables = gson.fromJson(text, List.class);
        assertFalse(tables.isEmpty());
        System.out.println("Tables: " + tables);
    }

    /**
     * Test the fetch_tables MCP tool with a schema filter
     */
    @Test
    void testFetchTablesToolWithSchema() throws Exception {
        JsonObject args = new JsonObject();
        args.addProperty("schema", "ADS");
        JsonObject response = callMcpTool("fetch_tables", args);

        assertNotNull(response);
        assertFalse(response.has("error"));
        assertTrue(response.has("result"));

        JsonObject result = response.getAsJsonObject("result");
        JsonArray content = result.get("content").getAsJsonArray();
        assertNotNull(content, "Content is null");
        assertFalse(content.isEmpty(), "Content is empty");

        String text = content.get(0).getAsJsonObject().get("text").getAsString();
        assertNotNull(text, "Text is null");
        assertFalse(text.isEmpty(), "Text is empty");
        assertFalse(text.contains("ERROR"));

        @SuppressWarnings("unchecked")
        List<Object> tables = gson.fromJson(text, List.class);
        assertFalse(tables.isEmpty());
        System.out.println("Tables in ADS schema: " + tables);
    }

    /**
     * Test the fetch_pipelines MCP tool
     */
    @Test
    void testFetchPipelines() throws Exception {
        JsonObject response = callMcpTool("fetch_pipelines", new JsonObject());

        assertNotNull(response);
        assertFalse(response.has("error"));
        assertTrue(response.has("result"));

        JsonObject result = response.getAsJsonObject("result");
        JsonArray content = result.get("content").getAsJsonArray();
        assertNotNull(content, "Content is null");
        assertFalse(content.isEmpty(), "Content is empty");

        String text = content.get(0).getAsJsonObject().get("text").getAsString();
        assertNotNull(text, "Text is null");
        assertFalse(text.isEmpty(), "Text is empty");
        assertFalse(text.contains("ERROR"));

        @SuppressWarnings("unchecked")
        List<Object> pipelines = gson.fromJson(text, List.class);
        System.out.println("Pipelines: " + pipelines);
    }

    /**
     * Test the fetch_pipeline_status MCP tool
     */
    @Test
    void testFetchPipelineStatusUnknownPipeline() throws Exception {
        JsonObject args = new JsonObject();
        args.addProperty("pipeline", "unknown");
        JsonObject response = callMcpTool("fetch_pipeline_status", args);

        assertNotNull(response);
        assertFalse(response.has("error"));
        assertTrue(response.has("result"));

        JsonObject result = response.getAsJsonObject("result");
        JsonArray content = result.get("content").getAsJsonArray();
        assertNotNull(content, "Content is null");
        assertFalse(content.isEmpty(), "Content is empty");

        String text = content.get(0).getAsJsonObject().get("text").getAsString();
        assertNotNull(text, "Text is null");
        assertFalse(text.isEmpty(), "Text is empty");
        assertTrue(text.contains("ERROR"));
    }

    /**
     * Test the describe_table MCP tool
     */
    @Test
    void testDescribeTable() throws Exception {
        JsonObject args = new JsonObject();
        args.addProperty("table", "AD_CLICKS");
        JsonObject response = callMcpTool("describe_table", args);

        assertNotNull(response);
        assertFalse(response.has("error"));
        assertTrue(response.has("result"));

        JsonObject result = response.getAsJsonObject("result");
        JsonArray content = result.get("content").getAsJsonArray();
        assertNotNull(content, "Content is null");
        assertFalse(content.isEmpty(), "Content is empty");

        String text = content.get(0).getAsJsonObject().get("text").getAsString();
        assertNotNull(text, "Text is null");
        assertFalse(text.isEmpty(), "Text is empty");
        assertFalse(text.contains("ERROR"));

        @SuppressWarnings("unchecked")
        List<Object> tableDefinitions = gson.fromJson(text, List.class);
        assertFalse(tableDefinitions.isEmpty());
        System.out.println("Table definitions: " + tableDefinitions);
    }

    /**
     * Test the describe_pipeline MCP tool
     */
    @Test
    void testDescribePipelineUnknownPipeline() throws Exception {
        JsonObject args = new JsonObject();
        args.addProperty("pipeline", "unknown");
        JsonObject response = callMcpTool("describe_pipeline", args);

        assertNotNull(response);
        assertFalse(response.has("error"));
        assertTrue(response.has("result"));

        JsonObject result = response.getAsJsonObject("result");
        JsonArray content = result.get("content").getAsJsonArray();
        assertNotNull(content, "Content is null");
        assertFalse(content.isEmpty(), "Content is empty");

        String text = content.get(0).getAsJsonObject().get("text").getAsString();
        assertNotNull(text, "Text is null");
        assertFalse(text.isEmpty(), "Text is empty");
        assertTrue(text.contains("ERROR"));
    }

    /**
     * Test the plan MCP tool
     */
    @Test
    void testPlanTool() throws Exception {
        JsonObject args = new JsonObject();
        args.addProperty("sql", "create or replace materialized view ads.test as select * from ads.page_views");
        JsonObject response = callMcpTool("plan", args);

        assertNotNull(response);
        assertFalse(response.has("error"));
        assertTrue(response.has("result"));

        JsonObject result = response.getAsJsonObject("result");
        JsonArray content = result.get("content").getAsJsonArray();
        assertNotNull(content, "Content is null");
        assertFalse(content.isEmpty(), "Content is empty");

        String text = content.get(0).getAsJsonObject().get("text").getAsString();
        assertNotNull(text, "Text is null");
        assertFalse(text.isEmpty(), "Text is empty");
        assertFalse(text.contains("ERROR"));

        @SuppressWarnings("unchecked")
        List<Object> planResults = gson.fromJson(text, List.class);
        assertFalse(planResults.isEmpty());
        System.out.println("Plan results: " + planResults);
    }

    /**
     * Test the query MCP tool
     */
    @Test
    void testQueryTool() throws Exception {
        JsonObject args = new JsonObject();
        args.addProperty("sql", "SELECT * FROM ADS.PAGE_VIEWS");
        JsonObject response = callMcpTool("query", args);

        assertNotNull(response);
        assertFalse(response.has("error"));
        assertTrue(response.has("result"));

        JsonObject result = response.getAsJsonObject("result");
        JsonArray content = result.get("content").getAsJsonArray();
        assertNotNull(content, "Content is null");
        assertFalse(content.isEmpty(), "Content is empty");

        String text = content.get(0).getAsJsonObject().get("text").getAsString();
        assertNotNull(text, "Text is null");
        assertFalse(text.isEmpty(), "Text is empty");
        assertFalse(text.contains("ERROR"));

        @SuppressWarnings("unchecked")
        List<Object> queryResults = gson.fromJson(text, List.class);
        assertFalse(queryResults.isEmpty());
        System.out.println("Query results: " + queryResults);
    }

    /**
     * Test the modify MCP tool
     */
    @Test
    void testModifyToolThrowsException() throws Exception {
        JsonObject args = new JsonObject();
        args.addProperty("sql", "SELECT * FROM ADS.PAGE_VIEWS");
        JsonObject response = callMcpTool("modify", args);

        assertNotNull(response);
        assertFalse(response.has("error"));
        assertTrue(response.has("result"));

        JsonObject result = response.getAsJsonObject("result");
        JsonArray content = result.get("content").getAsJsonArray();
        assertNotNull(content, "Content is null");
        assertFalse(content.isEmpty(), "Content is empty");

        String text = content.get(0).getAsJsonObject().get("text").getAsString();
        assertNotNull(text, "Text is null");
        assertFalse(text.isEmpty(), "Text is empty");
        assertTrue(text.contains("ERROR"));
    }

    // Helper methods for MCP server communication

    private void startMcpServerSubprocess() throws Exception {
        // Build the command to start HoptimatorMcpServer
        ProcessBuilder processBuilder = new ProcessBuilder(
            "java",
            "-cp", System.getProperty("java.class.path"),
            "com.linkedin.hoptimator.mcp.server.HoptimatorMcpServer",
            JDBC_URL
        );

        // Start the process
        mcpServerProcess = processBuilder.start();

        // Set up communication streams
        serverInput = new BufferedWriter(new OutputStreamWriter(mcpServerProcess.getOutputStream(), StandardCharsets.UTF_8));
        serverOutput = new BufferedReader(new InputStreamReader(mcpServerProcess.getInputStream(), StandardCharsets.UTF_8));

        // Give the server a moment to start up
        Thread.sleep(1000);

        if (!mcpServerProcess.isAlive()) {
            throw new RuntimeException("Failed to start HoptimatorMcpServer subprocess");
        }

        System.out.println("Started HoptimatorMcpServer subprocess");
    }

    private void initializeMcpSession() throws Exception {
        // Send MCP initialization request
        JsonObject initRequest = new JsonObject();
        initRequest.addProperty("jsonrpc", "2.0");
        initRequest.addProperty("id", requestId++);
        initRequest.addProperty("method", "initialize");

        JsonObject params = new JsonObject();
        params.addProperty("protocolVersion", "2024-11-05");

        JsonObject clientInfo = new JsonObject();
        clientInfo.addProperty("name", "test-client");
        clientInfo.addProperty("version", "1.0.0");
        params.add("clientInfo", clientInfo);

        initRequest.add("params", params);

        sendMessage(initRequest);
        JsonObject response = receiveMessageWithTimeout(15000);

        if (response.has("error")) {
            throw new RuntimeException("MCP initialization failed: " + response.get("error"));
        }

        // Send the required "initialized" notification to complete the handshake
        JsonObject initializedNotification = new JsonObject();
        initializedNotification.addProperty("jsonrpc", "2.0");
        initializedNotification.addProperty("method", "notifications/initialized");
        sendMessage(initializedNotification);

        System.out.println("MCP session initialized successfully");
    }

    private JsonObject callMcpTool(String toolName, JsonObject arguments) throws Exception {
        JsonObject request = new JsonObject();
        request.addProperty("jsonrpc", "2.0");
        request.addProperty("id", requestId++);
        request.addProperty("method", "tools/call");

        JsonObject params = new JsonObject();
        params.addProperty("name", toolName);
        params.add("arguments", arguments);
        request.add("params", params);

        sendMessage(request);
        return receiveMessageWithTimeout(60000); // 60 second timeout for tool calls
    }

    private void sendMessage(JsonObject message) throws IOException {
        String messageStr = gson.toJson(message);
        serverInput.write(messageStr);
        serverInput.newLine();
        serverInput.flush();
    }

    private JsonObject receiveMessageWithTimeout(long timeoutMs) throws IOException {
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            // Check if data is available to read (non-blocking check)
            if (!serverOutput.ready()) {
                try {
                    Thread.sleep(100); // Wait 100ms before checking again
                    continue;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for message", e);
                }
            }

            String line = serverOutput.readLine();
            if (line == null) {
                throw new IOException("Server connection closed");
            }

            JsonObject message;
            try {
                message = JsonParser.parseString(line).getAsJsonObject();
            } catch (Exception e) {
                System.out.println("Failed to parse message: " + line);
                continue; // Ignore malformed messages
            }

            // Skip notifications - we only want responses to our requests
            if (message.has("method")) {
                System.out.println("Received notification: " + gson.toJson(message));
                continue;
            }

            // This should be a response to our request
            return message;
        }

        throw new IOException("Timeout waiting for MCP response after " + timeoutMs + "ms");
    }
}
