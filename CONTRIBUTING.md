# Contributing to Hoptimator

Thanks for your interest in contributing! This file covers the basics of how
to file issues, send a pull request, and report security problems. If you're
looking for the full architecture or extension guide, that lives in
[`docs/`](./docs/index.md).

## Filing issues

Bug reports, feature requests, and design discussions all live in
[GitHub Issues](https://github.com/linkedin/Hoptimator/issues). Before opening
a new issue, please search existing ones — there's a good chance someone has
already raised the same point.

For bugs, please include:
- The Hoptimator version (or commit hash) you're running.
- The Kubernetes context — managed cluster, kind, Docker Desktop, etc.
- The minimum SQL or YAML that reproduces the problem.
- Relevant log output or `kubectl describe` output.

## Sending a pull request

1. **Fork the repo** and create a branch off `main`.
2. **Build and test locally:**
   ```bash
   make build
   make test
   ```
   Integration tests (which spin up a real Kubernetes-backed dev environment)
   live behind `make integration-tests` — please run them when changing
   anything that touches deployment or planning.
3. **Cover your changes.** New code needs tests; don't ship behavior that
   isn't exercised by either a unit or an integration test. Generate a
   coverage report with:
   ```bash
   make coverage
   # report at build/reports/jacoco/aggregate/index.html
   ```
   We aim for **80%** line coverage on changed code. The CI workflow
   currently enforces a softer 60% on changed files and 40% overall, but
   targeting 80% locally keeps the trend in the right direction.
4. **Regenerate Java models if you touched a CRD.** The Java classes
   under `hoptimator-k8s/src/main/.../models/` are auto-generated from
   the CRD YAMLs in `hoptimator-k8s/src/main/resources/`. After adding,
   removing, or modifying a CRD field, run:
   ```bash
   make generate-models
   ```
   The script uses the upstream Kubernetes Java client's
   [`crd-model-gen`](https://github.com/kubernetes-client/java/tree/master/crd-model-gen)
   Docker image, so Docker must be running. Commit the regenerated
   files alongside your CRD change.
5. **Match the existing style.** The project runs Checkstyle and SpotBugs as
   part of the build; CI will fail if either complains.
6. **Keep commits focused.** One logical change per commit, with a descriptive
   message. Reference the issue number if there is one.
7. **Open the PR.** Describe what changed and why. Link any relevant issues.

## Contribution agreement

As a contributor, you represent that the code you submit is your original work
or that of your employer (in which case you represent you have the right to
bind your employer). By submitting code, you (and, if applicable, your
employer) are licensing the submitted code to LinkedIn and the open source
community subject to the BSD 2-Clause license that ships with this repository.

## Reporting security vulnerabilities

Please do not file GitHub issues for security problems. Send reports privately
to [security@linkedin.com](mailto:security@linkedin.com), preferably with the
subject line `linkedin/Hoptimator - <short summary>`.
