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
3. **Match the existing style.** The project runs Checkstyle and SpotBugs as
   part of the build; CI will fail if either complains.
4. **Keep commits focused.** One logical change per commit, with a descriptive
   message. Reference the issue number if there is one.
5. **Open the PR.** Describe what changed and why. Link any relevant issues.

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
