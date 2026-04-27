# CLAUDE.md

Guidance for Claude Code agents working in this repo. Skim once per
session.

## What this repo is

Hoptimator is a SQL control plane for multi-system data pipelines. It
plays three roles in one binary: **planner** (turns SQL into a logical
plan against a unified catalog), **adapter** (translates plan elements
into target-system specs), **operator** (applies specs and reconciles
drift).

Kubernetes is the default deployment substrate but **not** a hard
requirement. The `Deployer` SPI in `hoptimator-api` is the actual
extension point — anything that can materialize a spec works.

## Read these first

Before grepping or guessing, consult:

- `README.md` — elevator pitch.
- `docs/getting-started/concepts.md` — vocabulary (Database, View,
  Pipeline, TableTemplate, JobTemplate, TableTrigger, LogicalTable,
  Subscription, Engine, hints).
- `docs/getting-started/architecture.md` — life of a SQL statement,
  module map.
- `docs/kubernetes/crd-reference.md` — CRD field reference.
- `docs/kubernetes/templates.md` — template placeholder syntax.
- `docs/user-guide/ddl-reference.md` — supported DDL grammar.
- `docs/extending/index.md` — SPI extension points and which to use.

The docs are journey-based and current. Prefer them over inferring from
the code.

## Common commands

```
make build                  # build all modules + Docker images
make test                   # unit tests (excludes @Tag("integration"))
make integration-tests      # spins up a real K8s cluster, runs intTest
make coverage               # JaCoCo aggregate report (per-file HTML)
make generate-models        # regenerate V1alpha1* Java classes from CRDs (requires Docker)
make deploy / deploy-demo / deploy-dev-environment
./hoptimator                # SQL CLI; `!intro`, `!specify`, `!pipeline`, `!resolve`
./start-mcp-server          # MCP server over stdio
```

`make generate-models` is required after any change to a `*.crd.yaml`
file. Commit the regenerated Java alongside the CRD change.

## Module layout

Active modules:

- `hoptimator-api` — public interfaces. `Deployer`, `Engine`,
  `Connector`, `Validator`, `ConfigProvider`, `View`,
  `MaterializedView`, etc.
- `hoptimator-jdbc` — Calcite-based JDBC driver, parser, planner glue.
- `hoptimator-jdbc-driver` — thin wrapper for `DriverManager`
  registration.
- `hoptimator-cli` — sqlline + custom commands (`!specify`, etc.).
- `hoptimator-mcp-server` / `hoptimator-mcp-server-app` — MCP server.
- `hoptimator-util` — planner rules, `DeploymentService`, `Template`.
- `hoptimator-k8s` — default Deployers, all CRDs, operator/catalog glue.
- `hoptimator-operator` / `hoptimator-operator-integration` — reconciler
  loop and entry point.
- `hoptimator-flink-runner` / `hoptimator-flink-adapter` — Flink job
  runtime and catalog integration.
- `hoptimator-kafka` / `hoptimator-kafka-controller` — Kafka adapter.
- `hoptimator-venice` / `hoptimator-mysql` / `hoptimator-logical` /
  `hoptimator-demodb` / `hoptimator-avro` — adapters / utilities.

**Marked for deletion — do not target new contributions here:**
`hoptimator-catalog`, `hoptimator-models`, `hoptimator-planner`. Check
`settings.gradle` for the canonical list.

## Gotchas

- **`WITH (...)` syntax is `'key' 'value'`, not `'key' = 'value'`.** The
  `=` form readers see in `!pipeline` / `!specify` output is Flink's
  syntax for engine consumption, not Hoptimator's input.
- **Partial views (`SCHEMA."SINK$suffix"`) are the recommended default
  for `CREATE MATERIALIZED VIEW`** when more than one pipeline writes
  into the same sink. Without `$`, two views into the same schema/table
  conflict at deploy time.
- **`Engine` CRD is partially developed.** Pipeline materialization
  does *not* require an Engine resource. The Engine path is for query
  execution only, and isn't fully wired up.
- **Reserved-but-unimplemented DDL parses but does nothing today**:
  `REFRESH MATERIALIZED VIEW`, `FIRE *`, `PAUSE/RESUME MATERIALIZED VIEW`,
  `CREATE FUNCTION`. Don't promise these in user-facing changes.
- **Template language `{{var==value}}` and `{{var!=value}}` are
  template-level guards, not inline conditionals.** When the condition
  fails, the entire template returns null. There is no `{{end}}`
  companion. The pattern for "use this template in case A, that one in
  case B" is two templates with mirroring guards.
- **`{{var toName}}` is documented in `Template.java`'s javadoc but
  not implemented.** Only `toLowerCase`, `toUpperCase`, and `concat`
  are wired up in `applyTransform`.
- **MCP `query` tool restrictions** (`ADS`, `PROFILE`, `METADATA`, `K8S`
  only) are not a safety mechanism — they're the only schemas Hoptimator
  can answer queries from without a configured engine. Don't widen them
  blindly.
- **Status: alpha.** CRDs are `v1alpha1`; `hoptimator-api` interfaces
  and SQL grammar are subject to change. Don't add backwards-compat
  shims unless explicitly asked.
- **Style enforcement**: Checkstyle and SpotBugs run as part of `make
  build` and fail CI on violations. JDK 17.

## Patterns to prefer

- **For new sources**: declarative path (`Database` CRD +
  `TableTemplate`) over a custom `Deployer` unless the system needs
  imperative API calls.
- **For policy**: a `Validator` over runtime checks in a `Deployer`.
  Validators run before any side effects; deployers can't reject cleanly.
- **For per-pipeline tuning**: hints over template edits.
- **For namespace-wide values**: configmap over hints.
- **For iterating on a materialized view**: `CREATE OR REPLACE
  MATERIALIZED VIEW`. Without `OR REPLACE`, re-running fails.

## Testing

- `make test` for unit tests; `make integration-tests` for tests that
  need a real K8s cluster; `make coverage` for the per-file JaCoCo
  report.
- Coverage target on changed code is **80%**. CI enforces a softer 60%
  on changed files / 40% overall, but aim for 80% locally.
- **Never add coverage exclusions for new files.** If something feels
  untestable, ask first — the answer is usually a refactor, not an
  exclusion.
- **Refactor for testability rather than fighting Mockito.** Extract
  package-private factory methods instead of `new` calls in untestable
  paths; use package-private constructor injection for complex
  dependencies. Do not use reflection (`Field.setAccessible(true)`) to
  inject test fields — it couples tests to field names.
- **Never set `@MockitoSettings(strictness = Strictness.LENIENT)` at
  the class level** to silence unused-stub warnings. Mark individual
  stubs with `lenient()` if they're unused by some tests.
- **For wildcard-generic stubs, use `doReturn(x).when(mock).method()`**
  instead of `when(mock.method()).thenReturn(x)` — the latter triggers
  wildcard-capture compile errors. This avoids the need for
  `@SuppressWarnings("unchecked")`.
- **`MockedStatic`** as `@Mock` field, not try-with-resources.
  `MockedConstruction` is *not* supported as `@Mock` field — extract a
  factory method instead.
- **Don't fit assertions to what the code does.** Tests are most
  valuable when they find bugs. When writing them, look at: null
  handling (does it NPE? intended?), catch blocks (rethrow vs. swallow),
  error messages (copy-paste accuracy), boolean conditions (De Morgan's
  is tricky), resource lifecycle (every `AutoCloseable` followed to
  close).
- Conventions: JUnit 5 + AssertJ + Mockito; Arrange-Act-Assert; camelCase
  test names; explicit types (no `var`).

## Keep docs in sync

When behavior changes or a feature is added, update the relevant doc
under `docs/`. Specific cases:

- New CRD or CRD field → `docs/kubernetes/crd-reference.md`
  (and run `make generate-models`).
- New DDL form, or change to parsing → `docs/user-guide/ddl-reference.md`.
- New SPI extension point or behavior change → `docs/extending/`.
- Conceptual model change (new abstraction, renamed concept) →
  `docs/getting-started/concepts.md`. Re-check cross-references.
- New `make` target or build flow → `CONTRIBUTING.md`.
- Tagline / framing changes → `README.md` and `docs/index.md` together.

The README is intentionally slim; resist adding detail there. Push
detail into `docs/`. The `docs/` tree is journey-based, not
module-based — group new pages by what a reader is *trying to do*, not
by which module they live in.
