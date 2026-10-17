# GitHub Workflows

This directory contains the GitHub Actions workflows for the Hoptimator project.

## Workflows

### CI (`ci.yml`)
**Purpose:** Fast feedback for pull requests and commits to main.

**Triggers:**
- Pull requests to `main`
- Pushes to `main`

**What it does:**
- Validates Gradle wrapper for security
- Builds the project (excluding integration tests and shadowJar)
- Runs unit tests
- Runs Checkstyle for code style validation
- Runs SpotBugs for static analysis
- Uploads test reports, Checkstyle results, and SpotBugs results on failure

**Typical duration:** 5-10 minutes

### Build and Test (`integration-tests.yml`)
**Purpose:** Comprehensive testing with full integration test suite.

**Triggers:**
- Pull requests to `main`
- Pushes to `main`

**What it does:**
- Full build including Docker images
- Sets up a Kubernetes Kind cluster
- Deploys required services (Kafka, Flink, etc.)
- Runs integration tests
- Captures extensive debugging information (logs, cluster state, etc.)

**Typical duration:** 15-20 minutes

### Publish release packages (`release.yml`)
**Purpose:** Publishes release artifacts when a new release is created.

**Triggers:**
- Release created events

**What it does:**
- Validates Gradle wrapper
- Publishes packages to JFrog and GitHub Packages

## Development Workflow

When you create a pull request:
1. The **CI workflow** runs first, providing quick feedback on basic build and test issues
2. The **Build and Test workflow** runs in parallel, validating integration tests
3. Both must pass before the PR can be merged

## Local Testing

To run the same checks locally:

```bash
# Run what CI workflow runs
./gradlew build -x intTest -x shadowJar

# Run what Build and Test workflow runs (requires Kubernetes cluster)
make build
make integration-tests-kind
```
