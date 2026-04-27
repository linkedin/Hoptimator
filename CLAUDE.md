# CLAUDE.md

Guidance for Claude Code agents working in this repo.

## Read these first

Before grepping or guessing, consult:

- `README.md` — elevator pitch.
- `docs/getting-started/concepts.md` — vocabulary (Database, View,
  Pipeline, TableTemplate, JobTemplate, TableTrigger, LogicalTable,
  Subscription, Engine, hints).
- `docs/getting-started/architecture.md` — life of a SQL statement,
  module map.
- `docs/kubernetes/crd-reference.md` — CRD field reference.
- `docs/user-guide/ddl-reference.md` — supported DDL grammar.
- `docs/extending/index.md` — SPI extension points and which to use.

The docs are journey-based and current. Prefer them over inferring from
the code.

## Common commands

```
make build && make install  # build all modules + Docker images, runs tests
make integration-tests      # spins up a real K8s cluster, runs intTest
make coverage               # JaCoCo aggregate report (per-file HTML)
make generate-models        # regenerate Java classes from CRDs
```

## Module layout

**Marked for deletion — do not target new contributions here:**
`hoptimator-catalog`, `hoptimator-models`, `hoptimator-planner`. Check
`settings.gradle` for the canonical list.

## Testing

- Coverage target on changed code is **80%**.
- **Never add coverage exclusions for new files.** If something feels
  untestable, ask first.
- **Refactor for testability.** Extract  package-private factory
  methods instead of `new` calls in untestable paths; use
  package-private constructor injection for complex dependencies. 
- **Never use reflection** e.g. (`Field.setAccessible(true)`) to inject
  test fields.
- **Never set `@MockitoSettings(strictness = Strictness.LENIENT)` at
  the class level** to silence unused-stub warnings. Mark individual
  stubs with `lenient()` if they're unused by some tests.
- **For wildcard-generic stubs, use `doReturn(x).when(mock).method()`**
  instead of `when(mock.method()).thenReturn(x)`.
- **Avoid suppressing warnings** unless absolutely necessary, e.g.
  `@SuppressWarnings("unchecked")` / `SuppressFBWarnings`.
- **`MockedStatic`** as `@Mock` field, not try-with-resources.
  `MockedConstruction` is *not* supported as `@Mock` field — extract a
  factory method instead.
- **Don't fit assertions to what the code does.** Tests are most
  valuable when they find bugs. When writing them, look at: null
  handling (does it NPE? intended?), catch blocks (rethrow vs. swallow),
  error messages (copy-paste accuracy), boolean conditions,
  resource lifecycle.
- Conventions: JUnit 5 + AssertJ + Mockito; Arrange-Act-Assert; camelCase
  test names; explicit types (no `var`).

## Keep docs in sync

When behavior changes or a feature is added, update the relevant doc
under `docs/`. Specific cases:

- New CRD or CRD field → `docs/kubernetes/crd-reference.md`.
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
