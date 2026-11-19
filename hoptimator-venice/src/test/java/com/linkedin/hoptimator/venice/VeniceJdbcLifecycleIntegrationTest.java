package com.linkedin.hoptimator.venice;

import com.linkedin.hoptimator.jdbc.CalciteDriver;
import com.linkedin.hoptimator.jdbc.DatabaseConfigResolvers;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.JdbcDataSourcePool;
import com.linkedin.hoptimator.jdbc.TableService;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcSchema;
import com.linkedin.venice.client.store.StoreMetadataFetcher;
import org.apache.avro.Schema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;


/** Exercises the public JDBC entry point against the local integration environment. */
@Tag("integration")
@Isolated
@Execution(ExecutionMode.SAME_THREAD)
@Timeout(value = 2, unit = TimeUnit.MINUTES)
final class VeniceJdbcLifecycleIntegrationTest {
  private static final String SCHEMA = "VENICE";
  private static final List<String> INITIAL_COLUMNS = List.of("KEY_id", "i");
  private static final List<String> EVOLVED_COLUMNS = List.of("KEY_id", "i", "new_field");

  @Test
  void cliProcessShutdownClosesBackendRetainedAfterOuterConnectionClose(@TempDir Path temporary) throws Exception {
    try (Session session = new Session();
        TestStore store = session.store()) {
      store.create();
      Path closed = temporary.resolve("backend-closed");
      Path output = temporary.resolve("cli.log");
      Process process = new ProcessBuilder(
          Path.of(System.getProperty("java.home"), "bin", "java").toString(),
          "-cp", System.getProperty("hoptimator.test.classpath"),
          CliSession.class.getName(), store.name, closed.toString())
          .redirectErrorStream(true).redirectOutput(output.toFile()).start();
      try {
        assertThat(process.waitFor(30, TimeUnit.SECONDS)).as("CLI process exits without hanging").isTrue();
        assertThat(process.exitValue()).as(Files.readString(output)).isZero();
        assertThat(Files.readString(closed)).isEqualTo("physical-close\n");
      } finally {
        if (process.isAlive()) {
          process.destroyForcibly();
          assertThat(process.waitFor(10, TimeUnit.SECONDS)).as("Timed-out child process terminated").isTrue();
        }
      }
    }
  }

  @Test
  void retiringPoolPreservesOverlappingRequestsAndClosesOnlyAfterFinalConnection() throws Exception {
    try (Session session = new Session();
        TestStore store = session.store();
        HoptimatorConnection first = session.connect();
        HoptimatorConnection second = session.connect()) {
      store.create();
      assertThat(columns(first, store.name)).containsExactlyElementsOf(INITIAL_COLUMNS);
      assertThat(columns(second, store.name)).containsExactlyElementsOf(INITIAL_COLUMNS);
      BasicDataSource retired = source(first);
      assertThat(source(second)).isSameAs(retired);
      CalciteConnection physical = physical(retired);
      AtomicInteger ownedCloses = new AtomicInteger();
      CalciteDriver.addConnectionResource(physical, ownedCloses::incrementAndGet);
      BorrowedSchema borrowed = new BorrowedSchema();
      physical.getRootSchema().add("BORROWED", borrowed);

      session.retire(retired);
      try (HoptimatorConnection replacement = session.connect()) {
        BasicDataSource replacementSource = source(replacement);
        assertThat(replacementSource).isNotSameAs(retired);
        assertThat(columns(replacement, store.name)).containsExactlyElementsOf(INITIAL_COLUMNS);
        first.close();
        assertThat(retired.isClosed()).isFalse();
        assertThat(physical.isClosed()).isFalse();
        assertThat(ownedCloses.get()).isZero();
        assertThat(columns(second, store.name)).containsExactlyElementsOf(INITIAL_COLUMNS);

        second.close();
        assertThat(retired.isClosed()).isTrue();
        assertThat(physical.isClosed()).isTrue();
        assertThat(ownedCloses.get()).isEqualTo(1);
        assertThat(borrowed.closes.get()).isZero();
        assertThat(replacementSource.isClosed()).isFalse();
        assertThat(columns(replacement, store.name)).containsExactlyElementsOf(INITIAL_COLUMNS);
      }
    }
  }

  @Test
  void retiringPoolRefreshesStoresCreatedAfterEarlierEnumeration() throws Exception {
    try (Session session = new Session()) {
      String name = storeName();
      BasicDataSource pooled;
      CalciteConnection physical;
      try (HoptimatorConnection first = session.connect()) {
        assertThat(tables(first)).doesNotContain(name);
        pooled = source(first);
        physical = physical(pooled);
      }
      assertThat(pooled.isClosed()).isFalse();
      try (TestStore store = session.store(name)) {
        store.create();
        session.awaitRouterStore(name, true);
        // Request-level catalog refresh is separate; these checks require pool retirement.
        session.retire(pooled);
        assertThat(pooled.isClosed()).isTrue();
        assertThat(physical.isClosed()).isTrue();
        try (HoptimatorConnection fresh = session.connect()) {
          assertThat(source(fresh)).isNotSameAs(pooled);
          assertThat(physical(source(fresh))).isNotSameAs(physical);
          assertThat(tables(fresh)).contains(store.name);
          assertThat(columns(fresh, store.name)).containsExactlyElementsOf(INITIAL_COLUMNS);
        }
      }
    }
  }

  @Test
  void newRequestSeesSchemaChangesMadeBetweenRequests() throws Exception {
    try (Session session = new Session();
        TestStore store = session.store()) {
      store.create();
      BasicDataSource pooled;
      CalciteConnection physical;
      try (HoptimatorConnection first = session.connect()) {
        assertThat(columns(first, store.name)).containsExactlyElementsOf(INITIAL_COLUMNS);
        pooled = source(first);
        physical = physical(pooled);
      }
      session.create(store.name, true);
      session.awaitDirectColumns(store.name, EVOLVED_COLUMNS);
      try (HoptimatorConnection next = session.connect()) {
        assertThat(source(next)).isSameAs(pooled);
        assertThat(physical(pooled)).isSameAs(physical);
        assertThat(columns(next, store.name)).containsExactlyElementsOf(EVOLVED_COLUMNS);
      }
    }
  }

  @Test
  void retiringPoolDoesNotResolveDroppedStore() throws Exception {
    try (Session session = new Session();
        TestStore store = session.store()) {
      store.create();
      BasicDataSource pooled;
      CalciteConnection physical;
      try (HoptimatorConnection first = session.connect()) {
        assertThat(columns(first, store.name)).containsExactlyElementsOf(INITIAL_COLUMNS);
        pooled = source(first);
        physical = physical(pooled);
      }
      store.delete();
      session.awaitRouterStore(store.name, false);
      session.awaitDirectColumns(store.name, List.of());
      session.retire(pooled);
      assertThat(pooled.isClosed()).isTrue();
      assertThat(physical.isClosed()).isTrue();
      try (HoptimatorConnection fresh = session.connect()) {
        assertThat(source(fresh)).isNotSameAs(pooled);
        assertThat(physical(source(fresh))).isNotSameAs(physical);
        assertThat(columns(fresh, store.name)).isEmpty();
      }
    }
  }

  private static List<String> columns(HoptimatorConnection connection, String store) throws SQLException {
    List<String> columns = new ArrayList<>();
    try (ResultSet result = connection.getMetaData().getColumns(null, SCHEMA, store, "%")) {
      while (result.next()) {
        columns.add(result.getString("COLUMN_NAME"));
      }
    }
    return columns;
  }

  private static List<String> tables(HoptimatorConnection connection) throws SQLException {
    List<String> tables = new ArrayList<>();
    try (ResultSet result = connection.getMetaData().getTables(null, SCHEMA, "%", null)) {
      while (result.next()) {
        tables.add(result.getString("TABLE_NAME"));
      }
    }
    return tables;
  }

  private static BasicDataSource source(HoptimatorConnection connection) {
    SchemaPlus schema = Objects.requireNonNull(
        connection.calciteConnection().getRootSchema().subSchemas().get(SCHEMA), "VENICE database is configured");
    HoptimatorJdbcSchema jdbc = Objects.requireNonNull(schema.unwrap(HoptimatorJdbcSchema.class));
    return (BasicDataSource) jdbc.getDataSource();
  }

  private static CalciteConnection physical(BasicDataSource source) throws SQLException {
    try (Connection connection = source.getConnection()) {
      return connection.unwrap(CalciteConnection.class);
    }
  }

  private static String storeName() {
    return "jdbc-lifecycle-" + UUID.randomUUID().toString().replace("-", "");
  }

  private static Schema.Field nullable(String name, Schema.Type type) {
    return new Schema.Field(name, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(type)),
        null, Schema.Field.NULL_DEFAULT_VALUE);
  }

  private static final class BorrowedSchema extends AbstractSchema implements AutoCloseable {
    private final AtomicInteger closes = new AtomicInteger();

    @Override public void close() {
      closes.incrementAndGet();
    }
  }

  /** Separate JVM so its real JDBC shutdown hook cannot close the test runner's shared pools. */
  public static final class CliSession {
    public static void main(String[] args) throws Exception {
      Session session = new Session();
      Path closed = Path.of(args[1]);
      BasicDataSource pooled;
      CalciteConnection physical;
      try (HoptimatorConnection connection = session.connect()) {
        assertThat(columns(connection, args[0])).containsExactlyElementsOf(INITIAL_COLUMNS);
        pooled = source(connection);
        physical = physical(pooled);
        CalciteDriver.addConnectionResource(physical, () ->
            Files.writeString(closed, "physical-close\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND));
      }
      assertThat(pooled.isClosed()).isFalse();
      assertThat(physical.isClosed()).isFalse();
      assertThat(Files.exists(closed)).isFalse();
      System.exit(0);
    }
  }

  private static final class Session implements AutoCloseable {
    private final Properties properties = new Properties();
    private final Properties venice;
    private final List<BasicDataSource> pools = new ArrayList<>();

    private Session() throws SQLException {
      properties.setProperty("catalogs", "k8s");
      properties.setProperty("jdbc.lifecycle.test", UUID.randomUUID().toString());
      venice = DatabaseConfigResolvers.forProperties(properties).databaseProperties(null, SCHEMA, "jdbc:venice://");
      assertThat(URI.create(venice.getProperty("router.url")).getHost())
          .as("These mutation tests require the local integration environment")
          .isIn("localhost", "127.0.0.1", "::1");
    }

    private HoptimatorConnection connect() throws SQLException {
      HoptimatorConnection connection =
          (HoptimatorConnection) DriverManager.getConnection("jdbc:hoptimator://", properties);
      BasicDataSource source = source(connection);
      if (!pools.contains(source)) {
        pools.add(source);
      }
      return connection;
    }

    private TestStore store() {
      return store(storeName());
    }

    private TestStore store(String name) {
      return new TestStore(this, name);
    }

    private void create(String name, boolean evolved) throws SQLException {
      List<Schema.Field> fields = new ArrayList<>(List.of(
          nullable("KEY_id", Schema.Type.INT), nullable("i", Schema.Type.INT)));
      if (evolved) {
        fields.add(nullable("new_field", Schema.Type.STRING));
      }
      Schema schema = Schema.createRecord(name.replace("-", "_"), null,
          "com.linkedin.hoptimator.test", false, fields);
      TableService.create(properties, Collections.emptyList(), List.of(SCHEMA, name), schema, Map.of(), true, false);
    }

    private List<String> directColumns(String name) throws SQLException {
      try (Connection connection = DriverManager.getConnection("jdbc:venice://", venice)) {
        CalciteConnection calcite = connection.unwrap(CalciteConnection.class);
        SchemaPlus schema = calcite.getRootSchema().subSchemas().get(SCHEMA);
        Table table = schema.tables().get(name);
        if (table == null) {
          return List.of();
        }
        RelDataType type = table.getRowType(calcite.getTypeFactory());
        return type.getFieldNames();
      }
    }

    private void awaitDirectColumns(String name, List<String> expected) throws Exception {
      long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      List<String> actual;
      do {
        actual = directColumns(name);
        if (actual.equals(expected)) {
          return;
        }
        TimeUnit.MILLISECONDS.sleep(100);
      } while (System.nanoTime() < deadline);
      assertThat(actual).as("Fresh direct Venice connection after controller update")
          .containsExactlyElementsOf(expected);
    }

    private void awaitRouterStore(String name, boolean exists) throws Exception {
      long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      boolean found;
      do {
        try (StoreMetadataFetcher fetcher = new ClusterSchema(venice).createStoreMetadataFetcher()) {
          found = fetcher.getAllStoreNames().contains(name);
        }
        if (found == exists) {
          return;
        }
        TimeUnit.MILLISECONDS.sleep(100);
      } while (System.nanoTime() < deadline);
      assertThat(found).as("Router /stores must reflect the controller update before testing pooled JDBC")
          .isEqualTo(exists);
    }

    private void retire(BasicDataSource source) throws SQLException {
      JdbcDataSourcePool.shared().invalidate(source.getUrl(), source.getDriverClassName(),
          properties.getProperty("user", "nouser"), properties.getProperty("password", "nopass"));
    }

    @Override public void close() throws SQLException {
      for (BasicDataSource pool : pools) {
        retire(pool);
      }
    }
  }

  private static final class TestStore implements AutoCloseable {
    private final Session session;
    private final String name;
    private boolean creationAttempted;
    private boolean deleted;

    private TestStore(Session session, String name) {
      this.session = session;
      this.name = name;
    }

    private void create() throws Exception {
      creationAttempted = true;
      session.create(name, false);
      session.awaitDirectColumns(name, INITIAL_COLUMNS);
    }

    private void delete() throws SQLException {
      TableService.delete(session.properties, List.of(SCHEMA, name));
      deleted = true;
    }

    @Override public void close() throws SQLException {
      if (creationAttempted && !deleted) {
        delete();
      }
    }
  }
}
