# Contributing to Kafka Timebridge

## Commit Messages

This project uses [Conventional Commits](https://www.conventionalcommits.org/). Commit messages are the sole source of truth for versioning and changelog generation — semantic-release reads them to decide whether to cut a patch, minor, or major release.

Every commit must follow this format:

```
<type>(<optional scope>): <short description>

<optional body>

<optional footer>
```

### Allowed Types

| Type | Triggers Release | Description |
|------|-----------------|-------------|
| `feat` | minor | A new feature |
| `fix` | patch | A bug fix |
| `perf` | patch | A performance improvement |
| `refactor` | patch | Code restructuring without behaviour change |
| `revert` | patch | Reverts a previous commit |
| `docs` | — | Documentation changes only |
| `style` | — | Formatting, whitespace, missing semicolons |
| `chore` | — | Maintenance tasks (deps, tooling) |
| `test` | — | Adding or fixing tests |
| `build` | — | Build system or external dependency changes |
| `ci` | — | CI configuration changes |

### Breaking Changes

Add `BREAKING CHANGE:` to the commit footer, or append `!` after the type. Either triggers a **major** release.

```
feat!: remove legacy flat config support

BREAKING CHANGE: the --config-file flag no longer accepts flat JSON format.
Use the nested format documented in README.md instead.
```

### Scopes (Optional)

Scopes provide additional context. Use the relevant subsystem:

```
feat(couchbase): add TTL support for stored messages
fix(kafka): handle rebalance during long-running delivery
```

### Examples

Good:
```
feat: add per-topic retry backoff configuration
fix: prevent duplicate delivery on consumer rebalance
docs: document KAFKA_GROUP_ID environment variable
ci: add ARM64 matrix build
```

Bad (will fail the PR check):
```
update stuff
WIP
fixed bug
```

## Pull request checks

Pull requests targeting `main` run the GitHub Actions workflow **PR Check** (see [.github/workflows/pr-check.yml](.github/workflows/pr-check.yml)). Each job must succeed before merge.

| Job | What it runs |
|-----|----------------|
| **lint** | [golangci-lint](https://golangci-lint.run/) on the repo (config: [.golangci.yml](.golangci.yml)), using Go **1.26** |
| **commitlint** | Validates **every commit** in the PR (from base branch tip to PR head) against [commitlint.config.js](commitlint.config.js) |
| **test** | `go test -short ./...` |
| **build** | `CGO_ENABLED=1 go build -o /dev/null ./cmd` |

In the GitHub UI, these appear as separate checks under the **PR Check** workflow.

## Release checks

On push to `main`, the **Release** workflow (see [.github/workflows/release.yml](.github/workflows/release.yml)) runs additional checks before publishing a release:

| Job | What it runs |
|-----|----------------|
| **test** | `go test -short ./...` |
| **e2e** | Full end-to-end test — builds the image from source, starts Kafka and timebridge via Docker Compose, produces a scheduled message, and asserts delivery on the destination topic |
| **semantic-release** | Only runs if both `test` and `e2e` pass; determines version and publishes the release |

## Pull request process

1. Branch from `main`.
2. Make your changes with well-formed commits (see above).
3. Open a PR — all four jobs in **PR Check** must pass, including commitlint on the full commit range.

## Local validation

Mirror CI before you push:

```sh
# Lint (install: https://golangci-lint.run/welcome/install/ )
golangci-lint run

# Tests and build (match PR Check)
go test -short ./...
CGO_ENABLED=1 go build -o /dev/null ./cmd
```

## E2E testing

The e2e test covers the primary use case end-to-end: produce a scheduled message to the timebridge topic, wait for the scheduler to deliver it to the destination topic, and assert correctness.

**What it tests:**
- Full message lifecycle: consume → store in backend → deliver at scheduled time
- Timebridge headers (`X-Timebridge-When`, `X-Timebridge-Where`) are stripped from delivered messages
- Message is not delivered before its scheduled time

**Requirements:** Docker (with Compose plugin).

```sh
export E2E_BROKER=localhost:9092
export E2E_INPUT_TOPIC=timebridge
export E2E_DEST_TOPIC=e2e-destination

# 1. Start broker
docker compose up -d --wait broker

# 2. Create topics
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic $E2E_INPUT_TOPIC --partitions 1 --replication-factor 1
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic $E2E_DEST_TOPIC --partitions 1 --replication-factor 1

# 3. Start timebridge
SCHEDULER_POLL_INTERVAL_SECONDS=1 LOG_LEVEL=debug KAFKA_TOPIC=$E2E_INPUT_TOPIC \
  docker compose up -d --build timebridge

# 4. Run tests
CGO_ENABLED=1 go test -v -timeout 120s -count=1 ./e2e/

# 5. Tear down
docker compose down -v
```

The e2e tests skip automatically when `-short` is passed (`go test -short ./...`), following the same convention as backend integration tests.

To inspect logs if a test fails:

```sh
docker compose logs timebridge
docker compose logs broker
```

**Commit messages** — after `npm install`, validate the last commit:

```sh
npx commitlint --edit
```

Or a one-off message:

```sh
echo "feat: my new feature" | npx commitlint
```

To approximate the PR commit range locally (full history required):

```sh
npx commitlint --from origin/main --to HEAD --verbose
```
