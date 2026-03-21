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
