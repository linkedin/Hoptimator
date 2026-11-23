# JDBC routing and resource ownership

Hoptimator uses JDBC both to discover tables and to execute queries. Those are
different paths, with different connection lifetimes. This page describes the
bundled Kubernetes catalog; other catalog providers can choose different wiring.
For adapter implementation details, see [Adding a data source](../extending/data-sources.md).

## Connection layers and routing

```text
Application / CLI
  |
  +-- jdbc:hoptimator://...
        HoptimatorConnection -> outer Calcite connection
          |
          +-- Catalog / metadata lookup
          |     Database URL -> shared metadata pool
          |       -> borrowed JDBC handle -> physical adapter connection
          |                                    -> schemas / backend clients
          |
          +-- Query execution (chosen by Calcite's planner)
                JDBC adapter execution, or
                Engine URL -> execution driver -> remote query results
```

`HoptimatorDriver` creates the outer Calcite connection, wraps it in
`HoptimatorConnection`, and registers catalogs. The wrapper delegates statements
to Calcite; it is not itself a per-statement connection router.
`hoptimator-jdbc-driver` packages the driver and catalog dependencies into a fat
JAR, rather than adding another connection or network layer.

For each Kubernetes `Database`, `K8sDatabaseTable` obtains a pool lease and
registers a `HoptimatorJdbcSchema` or `HoptimatorJdbcCatalogSchema`. These expose
tables and row types through JDBC metadata. The Database URL selects the driver:
a conventional database driver can open a database session, while a
`CalciteDriver` adapter can expose schemas backed by HTTP or other API clients.
A "physical JDBC connection" therefore need not mean a database socket.

Execution routing is separate from discovery. `K8sEngineTable` associates
eligible Engines with each Database. `HoptimatorJdbcConvention` registers
`EngineRules`, which offer remote execution alternatives to Calcite's planner.
For a selected remote plan, `RemoteToEnumerableConverter` generates SQL and
executes it through a `DelegatingDataSource` using the Engine URL and
`DriverManager`. The Database metadata connection is not automatically the
Engine execution connection, nor is every query necessarily sent to an Engine.
Deploying a pipeline through templates is a separate path and does not require
an Engine registration.

## Three lifetimes, not one

The Kubernetes catalog uses `JdbcDataSourcePool.shared()`. Its key is the
**exact effective URL, driver, username, and password**; URLs are not normalized.
The effective URL includes properties appended by catalog registration.

| Resource | Owner and close boundary |
|---|---|
| Outer `HoptimatorConnection` | Caller closes it; closing delegates to its underlying Calcite connection. |
| Catalog-lifetime `JdbcDataSourcePool.Lease` | Registered on the outer connection; released on outer close or failed catalog initialization. Pins pool ownership, but is not a borrowed JDBC connection. |
| Borrowed JDBC handle | Metadata code closes it after use, normally returning the physical connection to the pool. |
| Physical adapter connection and explicitly owned clients | Pool manages physical disposal; adapter clients close when that physical connection closes, not on logical return. |

Consequently, closing an outer connection releases its catalog leases but does
**not** discard a reusable pool or close every downstream physical connection.
Closing a metadata handle likewise does not dispose the adapter's clients.
Execution connections have their own lifecycle; the Database pool owner does
not manage Engine data sources or direct users of Calcite's pool cache.

## Initialization and physical cleanup

Both public `CalciteDriver.connect` overloads are final. The lifecycle is:

```text
accept URL -> connectionProperties -> acquire connection
  -> onConnectionInit -> configureConnection -> return to caller
```

`connectionProperties` merges URL parameters over caller properties and can
validate before allocation. `onConnectionInit` controls Calcite initialization
(including lattice scanning and model processing). `configureConnection`
receives the acquired connection and installs adapter schemas. Subclasses use
these hooks, not `connect` overrides or initialization callbacks.

If initialization fails after acquisition, the base driver closes the
connection before applying `connectionException`. Owned-resource cleanup failures
are logged by the physical close hook; other JDBC close failures are attached
to the original failure as suppressed exceptions. `HoptimatorDriver` is a
separate wrapping facade: it explicitly closes its underlying connection when
wrapping or catalog registration fails. Both drivers also attempt cleanup when
initialization throws an `Error`, then rethrow that same error without SQL
exception mapping. A failure of the cleanup attempt, including another `Error`,
is suppressed on the original initialization failure rather than replacing it.

Adapters transfer **owned** closeables with
`CalciteDriver.addConnectionResource` before publishing their schemas.
`ConnectionResources`, held by `HoptimatorCalciteSchema`, closes them once in
reverse registration order and attempts every close. The physical close hook
logs aggregated cleanup failures rather than throwing, so closing an already
completed operation does not change its result or replace its original failure.
It does not scan schemas for arbitrary `AutoCloseable` objects or assume
ownership of shared clients. A resource created before the physical connection
exists remains its creator's cleanup responsibility.

## Pool retention and cleanup

Like Calcite's original cache, the shared pool can reuse a data source even
after all catalogs close. Live catalog leases strongly pin an ownership token;
after the last lease releases it, the token is only softly reachable. Under
memory pressure GC can reclaim it, and a shared Java `Cleaner` removes and
closes the data source. Cleanup state retains the pool but not the token.

The distinction matters because `AutoCloseable` does not cause GC to call
`close()`. Closing the data source closes its idle physical connections, which
in turn close the resources owned by those connections. Checked-out connections
are disposed when returned. A catalog must keep its lease open while it uses
the data source.

This is soft retention, **not an idle timeout, capacity limit, or guaranteed
cleanup deadline**. There is no explicit invalidation or shutdown hook: JVM
shutdown does not guarantee that cached pools or their clients are closed.
Cleaner failures are logged. DBCP can swallow disposal exceptions, so
owned-resource failures are logged by the physical close hook as well; logging
does not guarantee that a client whose close failed released every resource.
Ordinary JDBC operation and connection-return errors are not suppressed.

For an isolated cleanup check, run
`./gradlew :hoptimator-jdbc:test --tests '*JdbcDataSourcePoolTest.cappedCacheUsesCleanerWithoutClosingActiveCatalogs'`.
Its test-only capacity-one cache forcibly clears evicted soft references and
requests GC in a child JVM. Cleanup uses the same `Cleaner` as production, but
forced eviction can remove an active catalog's cache entry, unlike GC. The
limit applies to cached entries, not active or total physical pools. An isolated
probe can construct `JdbcPoolCacheCap` from the JDBC test fixtures and call
`enforce()` after each operation; the cap is not automatic and must not be
installed in a production JVM.

## Cleanup is not metadata freshness

Opening another outer connection rebuilds catalog wrappers, but may reuse
downstream physical connections and their metadata caches. For example, Venice's
`ClusterSchema` retains targeted lookups and a once-loaded bulk store listing
for the physical connection's lifetime. Later lookups can miss a newly created
store or retain a dropped store after enumeration.

Normal outer close is not a refresh boundary, and soft reclamation provides no
freshness interval or explicit refresh operation.
Do not close shared pools after every request to work around metadata caching.

The direct `TableService` API bypasses the outer JDBC catalog and planning
layers, using a neutral `DeploymentContext` instead. JDBC ownership does not
manage resources acquired by deployment providers; see [Deployers](../extending/deployers.md).
Bypassing JDBC does not imply that providers make no network calls.
See [Creating a table without SQL](architecture.md#creating-a-table-without-sql).
