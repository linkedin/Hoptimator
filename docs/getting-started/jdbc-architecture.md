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
connection before applying `connectionException`. Cleanup failures are attached
to the original failure as suppressed exceptions. `HoptimatorDriver` is a
separate wrapping facade: it explicitly closes its underlying connection when
wrapping or catalog registration fails.

Adapters transfer **owned** closeables with
`CalciteDriver.addConnectionResource` before publishing their schemas.
`ConnectionResources`, held by `HoptimatorCalciteSchema`, closes them once in
reverse registration order, attempts every close, and surfaces cleanup failures.
It does not scan schemas for arbitrary `AutoCloseable` objects or assume
ownership of shared clients. A resource created before the physical connection
exists remains its creator's cleanup responsibility.

## Pool retention and retirement

Live catalog leases strongly pin a pool's ownership token so lazy metadata
lookups remain usable. After the last lease releases the token, it is only
softly reachable. GC can reclaim it, and a shared Java `Cleaner` removes and
closes the pool. Cleanup state retains the pool, but not the token.
This is soft retention, **not an idle timeout, capacity limit, or guaranteed
cleanup deadline**.

`invalidate` and `invalidateAll` explicitly retire pools from reuse. New catalog
acquisitions get replacement pools, while catalogs already leasing a retired
pool can continue borrowing from it. Disposal waits for those leases to close.
Closing the pool owner rejects further acquisitions and retires its pools;
the shared owner also has a JVM shutdown hook.

Pool shutdown closes idle physical connections and their owned resources;
active borrows follow the pool's return/disposal behavior, rather than being
forcibly interrupted. Explicit cleanup failures are surfaced to callers;
asynchronous Cleaner and shutdown-hook failures are logged.

## Cleanup is not metadata freshness

Opening another outer connection rebuilds catalog wrappers, but may reuse
downstream physical connections and their metadata caches. For example, Venice's
`ClusterSchema` retains targeted lookups and a once-loaded bulk store listing
for the physical connection's lifetime. Later lookups can miss a newly created
store or retain a dropped store after enumeration.

Explicit pool retirement gives new catalogs replacement physical connections
and their caches without breaking old catalog leases. Normal outer close is
not a refresh boundary, and soft reclamation provides no freshness interval.
Do not close shared pools after every request to work around metadata caching.

The direct `TableService` API bypasses the outer JDBC catalog and planning
layers, using a neutral `DeploymentContext` instead. JDBC ownership does not
manage resources acquired by deployment providers; see [Deployers](../extending/deployers.md).
Bypassing JDBC does not imply that providers make no network calls.
See [Creating a table without SQL](architecture.md#creating-a-table-without-sql).
